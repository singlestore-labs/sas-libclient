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

test_c() {
    gcc -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/"$1"_test.c -o build/"$1" -ls2client -lpthread -g
    echo 'Running' "$1" test...
    ./build/"$1"
}

test_cpp() {
    g++ -I "${PATH_TO_LIBCLIENT}" -L "${LD_LIBRARY_PATH}" test/"$1"_test.cpp -o build/"$1" -ls2client -lpthread -g
    echo 'Running' "$1" test...
    ./build/"$1"
}

for var in "$@"; do
    if [ "$var" != "lib" ] && [ "$var" != "share" ]; then
        if [ -e ./test/"$var"_test.c ]; then
            test_c "$var"
        else
            test_cpp "$var"
        fi
    fi
done
