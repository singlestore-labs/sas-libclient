#!/bin/bash
set -euo pipefail

display_usage() { 
	echo -e "Usage: $0 [lib|test_name...]. \n\tUse 'lib' to build sources, 'share' to copy headers to /build directory
    'test_name' ro run the test_{test_name} function"
	}

if [ $# -lt 1 ] 
then 
    display_usage
    exit 1
fi

# build the library along with memsqld
if [ $1 = "lib" ]
then
    cd "${PATH_TO_LIBCLIENT}"/build
    cmake -DCMAKE_BUILD_TYPE=Debug ..
    cmake --build .
    cd "${PATH_TO_LIBCLIENT}"
fi

if [ $1 = "share" ]; then
    cd "${PATH_TO_LIBCLIENT}"
    mkdir -p build/share
    cp test/parallel_read_test.c test/db_creds.h s2_client_extern.h chunk_extern.h build/libs2client.so build/share/
    echo 'gcc -I "${PATH_TO_HEADERS}"/ -L "${PATH_TO_LIBS2CLIENT_SO}" test/parallel_read_test.c -o parallel_read_test -ls2client -lpthread
./parallel_read_test' > build/share/run_test.sh
fi

# compile the test binaries
export LD_LIBRARY_PATH="${PATH_TO_LIBCLIENT}/build"
mkdir -p "${LD_LIBRARY_PATH}"

test_read() {
    gcc -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/parallel_read_test.c -o build/parallel_read_test -ls2client -lpthread
    echo 'Running parallel_read_test...'
    ./build/parallel_read_test
}

test_hdat() {
    g++ -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/hdat_test.cpp -o build/hdat_test -ls2client
    echo 'Running hdat_test...'
    ./build/hdat_test
}

test_queue() {
    g++ -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/thread_safe_queue_test.cpp -o build/thread_safe_queue_test -ls2client -lpthread
    echo 'Running thread_safe_queue_test...'
    ./build/thread_safe_queue_test
}

for var in "$@"; do
    if [ "$var" != "lib" ] && [ "$var" != "share" ]; then
        test_"$var"
    fi
done
