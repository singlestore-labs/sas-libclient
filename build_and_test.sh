#!/bin/bash
set -euo pipefail
export PATH_TO_LIBCLIENT=$(pwd)
export CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-RelWithDebInfo}"
export BUILD_DIR=build/$CMAKE_BUILD_TYPE
export ARTIFACTS_DIR=build/share
mkdir -p build
mkdir -p build/test
mkdir -p build/$CMAKE_BUILD_TYPE

display_usage() { 
	echo -e "Usage: $0 [lib|share|test_name...]. \n\tUse 'lib' to build sources, 'share' to copy headers and libs2client.so to $ARTIFACTS_DIR directory
    'test_name' ro run 'test_name'_test.[c|cpp] file"
	}

build_lib() {
    cd "${PATH_TO_LIBCLIENT}"/$BUILD_DIR
    cmake -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE ../..
    cmake --build .
    cd $PATH_TO_LIBCLIENT
}

build_lib

if [ $# -lt 1 ] 
then 
    exit 0
fi

place_shared() {
    cd $PATH_TO_LIBCLIENT
    mkdir -p $ARTIFACTS_DIR
    cp s2_client_extern.h chunk_extern.h hdat_write_extern.h ${BUILD_DIR}/libs2client.so $ARTIFACTS_DIR
    echo "Successfully copied files for sharing to $ARTIFACTS_DIR"
}

export LD_LIBRARY_PATH="${PATH_TO_LIBCLIENT}/${BUILD_DIR}"
mkdir -p "${LD_LIBRARY_PATH}"

# compile and run the test binaries
test_c() {
    gcc -I "${PATH_TO_LIBCLIENT}" -I "${PATH_TO_LIBCLIENT}"/vendor/libmariadb/include -I "${PATH_TO_LIBCLIENT}"/vendor/libavro/include -L "${PATH_TO_LIBCLIENT}/${BUILD_DIR}" -L "${PATH_TO_LIBCLIENT}/vendor/libmariadb" "$1" -o build/"$1".o -ls2client -lpthread -lmariadb -lavro -W -g
    echo 'Running' "$1"...
    export LD_LIBRARY_PATH="${PATH_TO_LIBCLIENT}"/vendor/libmariadb:"${PATH_TO_LIBCLIENT}"/vendor/libavro:"${PATH_TO_LIBCLIENT}"/vendor/libjansson:$LD_LIBRARY_PATH
    # ./build/"$1".o $2
    gdb ./build/"$1".o
}

test_cpp() {
    g++ -I "${PATH_TO_LIBCLIENT}" -I "${PATH_TO_LIBCLIENT}"/vendor/libmariadb/include -I "${PATH_TO_LIBCLIENT}"/vendor/libavro/include -L "${PATH_TO_LIBCLIENT}/${BUILD_DIR}" -L "${PATH_TO_LIBCLIENT}/vendor/libmariadb" "$1" -o build/"$1".o -ls2client -lpthread -lmariadb -lavro -g
    echo 'Running' "$1" test...
    export LD_LIBRARY_PATH="${PATH_TO_LIBCLIENT}"/vendor/libmariadb:"${PATH_TO_LIBCLIENT}"/vendor/libavro:"${PATH_TO_LIBCLIENT}"/vendor/libjansson:$LD_LIBRARY_PATH
    ./build/"$1".o $2
}

if [ $1 = "share" ]
then
    place_shared
    exit 0
fi

if [ $1 = "test" ]
then
    for file in `ls test/*.c`
    do
        test_c "$file" 0
    done
    exit 0
fi

if [ $1 = "testcc" ]
then
    for file in `ls test/*.cpp`
    do
        test_cpp "$file" 0
    done
    exit 0
fi

# run single test

if [ -e ./test/"$1"_test.c ]; then
    file=test/"$1"_test.c
    test_c "$file" $2
else
    file=test/"$1"_test.cpp
    test_cpp "$file" $2
fi
