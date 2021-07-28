#!/bin/bash
set -euo pipefail
PATH_TO_LIBCLIENT=$(pwd) 

echo $PATH_TO_LIBCLIENT

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
    mkdir -p build
    cd "${PATH_TO_LIBCLIENT}"/build
    cmake -DCMAKE_BUILD_TYPE=Debug ..
    cmake --build .
    cd $PATH_TO_LIBCLIENT
fi

if [ $1 = "share" ]; then
    cd $PATH_TO_LIBCLIENT
    mkdir -p build/share
    cp test/parallel_read_test.c test/write_test.c test/db_creds.h s2_client_extern.h chunk_extern.h hdat_write_extern.h build/libs2client.so build/share/
    echo 'gcc -I "${PATH_TO_HEADERS}"/ -L "${PATH_TO_LIBS2CLIENT_SO}" test/parallel_read_test.c -o parallel_read_test -ls2client -lpthread
./parallel_read_test' > build/share/run_test.sh
fi

# compile the test binaries
export LD_LIBRARY_PATH="${PATH_TO_LIBCLIENT}/build"
mkdir -p "${LD_LIBRARY_PATH}"

test_read() {
    gcc -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/parallel_read_test.c -o build/parallel_read_test -ls2client -lpthread -g
    echo 'Running parallel_read_test...'
    ./build/parallel_read_test
}

test_multi_pass() {
    gcc -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/multi_pass_test.c -o build/multi_pass_test -ls2client -lpthread -g
    echo 'Running multi_pass_test...'
    ./build/multi_pass_test
}

test_random_read() {
    gcc -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/random_read_test.c -o build/random_read_test -ls2client -lpthread -g
    echo 'Running random_read_test...'
    ./build/random_read_test
}

test_write() {
    gcc -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/write_test.c -o build/write_test -ls2client -lpthread -g
    echo 'Running write_test...'
    ./build/write_test
}

test_hdat() {
    g++ -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/hdat_test.cpp -o build/hdat_test -ls2client -g
    echo 'Running hdat_test...'
    ./build/hdat_test
}

test_write_cpp() {
    g++ -I "${PATH_TO_LIBCLIENT}"/ -L "${LD_LIBRARY_PATH}" test/write_test.cpp -o build/write_test_cpp -ls2client -g
    echo 'Running write_test_cpp...'
    ./build/write_test_cpp
}

test_queue() {
    g++ -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/thread_safe_queue_test.cpp -o build/thread_safe_queue_test -ls2client -lpthread -g
    echo 'Running thread_safe_queue_test...'
    ./build/thread_safe_queue_test
}

test_batch_queue() {
    g++ -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/thread_safe_batch_queue_test.cpp -o build/thread_safe_batch_queue_test -ls2client -lpthread -g
    echo 'Running thread_safe_batch_queue_test...'
    ./build/thread_safe_batch_queue_test
}

for var in "$@"; do
    if [ "$var" != "lib" ] && [ "$var" != "share" ]; then
        test_"$var"
    fi
done
