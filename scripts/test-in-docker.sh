#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=s2-docker.sh
source "$SCRIPT_DIR/s2-docker.sh"

s2_docker_load_defaults

BUILD_IMAGE="${BUILD_IMAGE:-sas-libclient-build:rhel9}"
REBUILD=false
KEEP_S2=false
RECREATE_S2=false
MANAGE_S2=false
USE_GDB=false

usage() {
    cat <<EOF
Usage: $0 [options] [build_and_test.sh test args...]

Build and run tests inside Docker with a SingleStore dev database.

Options:
  --rebuild       Force rebuild of the build Docker image
  --manage-s2     Start SingleStore before tests and stop it on exit
  --keep-s2       With --manage-s2, leave the SingleStore container running after tests finish
  --recreate-s2   With --manage-s2, force recreate the SingleStore container before tests
  --gdb           Run a single test under gdb (interactive; requires a test name, not "test")

Environment variables:
  BUILD_IMAGE     RHEL 9 build image (default: sas-libclient-build:rhel9)
  S2_IMAGE        SingleStore dev image
  S2_VERSION      SingleStore version for runtime switch (e.g. 8.5; default: image default)
  S2_HOST         SingleStore hostname on the test Docker network (default: singlestore-sas)
  ROOT_PASSWORD   SingleStore root password (default: password)
  DOCKER_PLATFORM Set to linux/amd64 on Apple silicon if needed

Database credentials:
  Tests read connection settings from test/db_creds.h. Edit that file for local
  (non-Docker) runs: host, ma_port, db, user, password, and optional ssl_ca.

  When you run this script, host and password in test/db_creds.h are patched for
  the duration of the test run and then restored, so your working copy stays
  unchanged:
    host     <- S2_HOST
    password <- ROOT_PASSWORD (must match the SingleStore container)

  db, user, ma_port, and ssl_ca always come from test/db_creds.h. Defaults there
  target a local SingleStore on 127.0.0.1 with password "password", matching
  test/prepare_s2.sql and the default ROOT_PASSWORD of the dev SingleStore image.

SingleStore container lifecycle is handled by scripts/s2-docker.sh. By default this
script expects an already-running container; use --manage-s2 to start and stop it.

Examples:
  scripts/s2-docker.sh start
  $0 test
  $0 testcc
  $0 parallel_read 1
  $0 --gdb parallel_read 1
  $0 parallel_read 1 --gdb
  $0 data_types --gdb
  $0 --manage-s2 test
  $0 --manage-s2 --keep-s2 test
  ROOT_PASSWORD=secret $0 test

  scripts/s2-docker.sh stop
EOF
}

ensure_build_image() {
    if [[ "$REBUILD" == true ]] || ! docker image inspect "$BUILD_IMAGE" &>/dev/null; then
        echo "Building Docker image: $BUILD_IMAGE"
        docker build -t "$BUILD_IMAGE" -f "$REPO_ROOT/docker/Dockerfile" "$REPO_ROOT"
    fi
}

test_args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rebuild)
            REBUILD=true
            shift
            ;;
        --keep-s2)
            KEEP_S2=true
            shift
            ;;
        --manage-s2)
            MANAGE_S2=true
            shift
            ;;
        --recreate-s2)
            RECREATE_S2=true
            shift
            ;;
        --gdb)
            USE_GDB=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            test_args+=("$@")
            break
            ;;
        -*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
        *)
            test_args+=("$1")
            shift
            ;;
    esac
done

if [[ ${#test_args[@]} -eq 0 ]]; then
    test_args=(test)
fi

set -- "${test_args[@]}"

if [[ "$USE_GDB" == true ]]; then
    if [[ $# -lt 1 ]] || [[ "$1" == "test" ]] || [[ "$1" == "testcc" ]] || [[ "$1" == "share" ]]; then
        echo "--gdb requires a single test name (e.g. parallel_read 1)" >&2
        exit 1
    fi
fi

cleanup() {
    if [[ "$MANAGE_S2" == true ]] && [[ "$KEEP_S2" == false ]]; then
        s2_docker_stop
    fi
}
trap cleanup EXIT

ensure_build_image

if [[ "$MANAGE_S2" == true ]]; then
    s2_docker_start "$RECREATE_S2"
else
    if ! s2_docker_is_ready; then
        echo "SingleStore is not ready. Start it with: scripts/s2-docker.sh start" >&2
        exit 1
    fi
fi

echo "Running tests"
docker_run_args=(--rm --network "$NETWORK")
if [[ "$USE_GDB" == true ]]; then
    docker_run_args+=(-it --cap-add=SYS_PTRACE --security-opt seccomp=unconfined)
fi

docker_run_env=(
    -e PATH_TO_LIBCLIENT=/workspace
    -e S2_HOST="$S2_HOST"
    -e ROOT_PASSWORD="$ROOT_PASSWORD"
)
if [[ "$USE_GDB" == true ]]; then
    docker_run_env+=(-e USE_GDB=1)
fi

docker run "${docker_run_args[@]}" \
    -v "$REPO_ROOT:/workspace" \
    -w /workspace \
    "${docker_run_env[@]}" \
    "$BUILD_IMAGE" \
    bash -c '
set -euo pipefail
creds=test/db_creds.h
backup="${creds}.docker-test.bak"
cp "$creds" "$backup"
restore_creds() { mv "$backup" "$creds"; }
trap restore_creds EXIT
escape_sed() { printf "%s" "$1" | sed -e "s/[\\\\\\/&]/\\\\&/g"; }
host="$(escape_sed "$S2_HOST")"
password="$(escape_sed "$ROOT_PASSWORD")"
sed -i \
    -e "s/\\.host = \"[^\"]*\"/\\.host = \"${host}\"/" \
    -e "s/\\.password = \"[^\"]*\"/\\.password = \"${password}\"/" \
    "$creds"
./build_and_test.sh "$@"
' _ "$@"
