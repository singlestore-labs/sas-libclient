#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUILD_IMAGE="sas-libclient-build:rhel9"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-RelWithDebInfo}"
build_args=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cmake-build-type)
            CMAKE_BUILD_TYPE="$2"
            shift 2
            ;;
        -h|--help)
            cat <<EOF
Usage: $0 [--cmake-build-type TYPE] [build_and_test.sh args...]

Build libs2client inside a RHEL 9 (UBI9) Docker image.

Options:
  --cmake-build-type    CMake build type (default: RelWithDebInfo)

Arguments:
  Optional args are passed to build_and_test.sh (e.g. share).

Environment variables:
  CMAKE_BUILD_TYPE      Same as --cmake-build-type

The Docker image is built from docker/Dockerfile only when it is not already present.
EOF
            exit 0
            ;;
        --)
            shift
            build_args+=("$@")
            break
            ;;
        -*)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
        *)
            build_args+=("$1")
            shift
            ;;
    esac
done

if ! docker image inspect "$BUILD_IMAGE" &>/dev/null; then
    echo "Building Docker image: $BUILD_IMAGE"
    docker build -t "$BUILD_IMAGE" -f "$REPO_ROOT/docker/Dockerfile" "$REPO_ROOT"
fi

docker run --rm \
    -v "$REPO_ROOT:/workspace" \
    -w /workspace \
    -e PATH_TO_LIBCLIENT=/workspace \
    -e CMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE" \
    "$BUILD_IMAGE" \
    ./build_and_test.sh "${build_args[@]}"
