#!/bin/bash
set -euo pipefail
PATH_TO_LIBCLIENT=$(pwd) 

display_usage() { 
	echo -e "Usage: $0 [lib|share|test_name...]. \n\tUse 'lib' to build sources, 'share' to copy headers and libs2client.so to /build/share directory
    'test_name' ro run 'test_name'_test.[c|cpp] file"
	}

build_lib() {
    cd $PATH_TO_LIBCLIENT
    mkdir -p build
    mkdir -p build/test
    cd "${PATH_TO_LIBCLIENT}"/build
    cmake -DCMAKE_BUILD_TYPE=Debug ..
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
    mkdir -p build/share
    cp test/data_types_test.c test/parallel_read_test.c test/write_test.c test/db_creds.h test/helpers.h s2_client_extern.h chunk_extern.h hdat_write_extern.h build/libs2client.so libmariadb/libmariadb.so.3 build/share/
    echo 'gcc -I "${PATH_TO_HEADERS}"/ -L "${PATH_TO_LIBS2CLIENT_SO}" test/parallel_read_test.c -o parallel_read_test -ls2client -lpthread
./parallel_read_test' > build/share/run_test.sh
    echo "Successfully copied files for sharing to /build/share"
}

export LD_LIBRARY_PATH="${PATH_TO_LIBCLIENT}/build"
mkdir -p "${LD_LIBRARY_PATH}"

# compile and run the test binaries
test_c() {
    gcc -I "${PATH_TO_LIBCLIENT}"  -I "${PATH_TO_LIBCLIENT}"/libmariadb/include -L "${LD_LIBRARY_PATH}" "$1" -o build/"$1".o -ls2client -lpthread -lmariadb -g
    echo 'Running' "$1"...
    ./build/"$1".o $2
}

test_cpp() {
    g++ -I "${PATH_TO_LIBCLIENT}"  -I "${PATH_TO_LIBCLIENT}"/libmariadb/include -L "${LD_LIBRARY_PATH}" "$1" -o build/"$1".o -ls2client -lpthread -lmariadb -g
    echo 'Running' "$1" test...
    ./build/"$1" $2
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

# run single test

if [ -e ./test/"$1"_test.c ]; then
    file=test/"$1"_test.c
    test_c "$file" $2
else
    file=test/"$1"_test.cpp
    test_cpp "$file" $2
fi
