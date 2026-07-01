#!/usr/bin/env bash
# SingleStore test database container lifecycle.
# Source this file for functions, or run directly: s2-docker.sh {start|stop|status|prepare}

S2_DOCKER_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
S2_DOCKER_REPO_ROOT="$(cd "$S2_DOCKER_SCRIPT_DIR/.." && pwd)"

s2_docker_load_defaults() {
    S2_IMAGE="${S2_IMAGE:-ghcr.io/singlestore-labs/singlestoredb-dev:latest}"
    S2_VERSION="${S2_VERSION:-}"
    ROOT_PASSWORD="${ROOT_PASSWORD:-password}"
    NETWORK="${NETWORK:-sas-libclient-test}"
    S2_CONTAINER="${S2_CONTAINER:-singlestore-sas}"
    S2_HOST="${S2_HOST:-singlestore-sas}"
    S2_SQL_CLIENT="${S2_SQL_CLIENT:-/usr/lib/singlestore-client/singlestore-client}"
    DOCKER_PLATFORM="${DOCKER_PLATFORM:-}"

    if [[ -z "$DOCKER_PLATFORM" ]] && [[ "$(uname -m)" == "arm64" ]]; then
        DOCKER_PLATFORM=linux/amd64
    fi
}

s2_docker_platform_args() {
    if [[ -n "$DOCKER_PLATFORM" ]]; then
        printf '%s\n' --platform "$DOCKER_PLATFORM"
    fi
}

s2_docker_run_sql() {
    docker exec -e MYSQL_PWD="$ROOT_PASSWORD" "$S2_CONTAINER" \
        "$S2_SQL_CLIENT" -h 127.0.0.1 -P 3306 -u root "$@"
}

s2_docker_is_running() {
    docker inspect "$S2_CONTAINER" &>/dev/null \
        && [[ "$(docker inspect --format='{{.State.Running}}' "$S2_CONTAINER" 2>/dev/null || echo false)" == "true" ]]
}

s2_docker_is_ready() {
    s2_docker_is_running \
        && s2_docker_run_sql -e "SELECT 1" &>/dev/null
}

s2_docker_wait_for_singlestore() {
    local attempt=0
    local max_attempts=120

    while [[ $attempt -lt $max_attempts ]]; do
        local health
        health="$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}starting{{end}}' "$S2_CONTAINER" 2>/dev/null || echo "missing")"

        if [[ "$health" == "healthy" ]]; then
            return 0
        fi

        if s2_docker_run_sql -e "SELECT 1" &>/dev/null; then
            return 0
        fi

        attempt=$((attempt + 1))
        echo "Waiting for SingleStore to become ready (${attempt}/${max_attempts})..."
        sleep 5
    done

    echo "SingleStore failed to become ready. Container logs:"
    docker logs "$S2_CONTAINER" || true
    return 1
}

s2_docker_prepare() {
    echo "Preparing test database"
    docker exec -e MYSQL_PWD="$ROOT_PASSWORD" -i "$S2_CONTAINER" \
        "$S2_SQL_CLIENT" -h 127.0.0.1 -P 3306 -u root \
        < "$S2_DOCKER_REPO_ROOT/test/prepare_s2.sql"
}

s2_docker_start() {
    local recreate="${1:-false}"

    if [[ "$recreate" != true ]] && s2_docker_is_ready; then
        echo "SingleStore container already running: $S2_CONTAINER"
        return 0
    fi

    docker network inspect "$NETWORK" &>/dev/null || docker network create "$NETWORK"
    docker rm -f "$S2_CONTAINER" &>/dev/null || true

    local platform_args=()
    while IFS= read -r arg; do
        platform_args+=("$arg")
    done < <(s2_docker_platform_args)

    local s2_env_args=(-e "ROOT_PASSWORD=$ROOT_PASSWORD")
    if [[ -n "$S2_VERSION" ]]; then
        s2_env_args+=(-e "SINGLESTORE_VERSION=$S2_VERSION")
    fi

    echo "Starting SingleStore dev container: $S2_CONTAINER"
    docker run -d "${platform_args[@]}" \
        --name "$S2_CONTAINER" \
        --hostname "$S2_HOST" \
        --network-alias "$S2_HOST" \
        --network "$NETWORK" \
        "${s2_env_args[@]}" \
        "$S2_IMAGE"

    s2_docker_wait_for_singlestore
    s2_docker_prepare
}

s2_docker_stop() {
    docker rm -f "$S2_CONTAINER" &>/dev/null || true
    docker network rm "$NETWORK" &>/dev/null || true
}

s2_docker_status() {
    if s2_docker_is_ready; then
        echo "SingleStore container $S2_CONTAINER is ready on $S2_HOST ($NETWORK)"
        return 0
    fi

    if s2_docker_is_running; then
        echo "SingleStore container $S2_CONTAINER is running but not yet accepting connections"
        return 1
    fi

    echo "SingleStore container $S2_CONTAINER is not running"
    return 1
}

s2_docker_usage() {
    cat <<EOF
Usage: $0 {start|stop|status|prepare} [options]

Manage the SingleStore dev container used for Docker-based testing.

Commands:
  start     Create the Docker network, start SingleStore, wait, and run prepare_s2.sql
  stop      Stop and remove the SingleStore container and test network
  status    Report whether the container is running and accepting connections
  prepare   Re-run test/prepare_s2.sql against a running container

Options:
  --recreate   Force recreate the container (start only)

Environment variables:
  S2_IMAGE        SingleStore dev image
  S2_VERSION      SingleStore version for runtime switch (e.g. 8.5; default: image default)
  S2_HOST         SingleStore hostname on the test Docker network (default: singlestore-sas)
  S2_CONTAINER    Docker container name (default: singlestore-sas)
  ROOT_PASSWORD   SingleStore root password (default: password)
  NETWORK         Docker network name (default: sas-libclient-test)
  DOCKER_PLATFORM Set to linux/amd64 on Apple silicon if needed

Examples:
  $0 start
  $0 status
  $0 prepare
  $0 stop
EOF
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    s2_docker_load_defaults

    command="${1:-}"
    shift || true
    recreate=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --recreate)
                recreate=true
                shift
                ;;
            -h|--help)
                s2_docker_usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1" >&2
                s2_docker_usage >&2
                exit 1
                ;;
        esac
    done

    case "$command" in
        start)
            s2_docker_start "$recreate"
            ;;
        stop)
            s2_docker_stop
            ;;
        status)
            s2_docker_status
            ;;
        prepare)
            s2_docker_prepare
            ;;
        -h|--help|"")
            s2_docker_usage
            exit 0
            ;;
        *)
            echo "Unknown command: $command" >&2
            s2_docker_usage >&2
            exit 1
            ;;
    esac
fi
