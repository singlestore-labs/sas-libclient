set -euo pipefail

# TO_TEST=parallel_read_test
TO_TEST=multi_and_random_test.c.o
END=7

run_cases () {
    for (( i=1; i<=END; i++ )); do
        for (( j=1; j<=END; j++ )); do
            for (( k=1; k<=END; k++ )); do
                echo 'processing case' $i $j $k
                ./build/test/$TO_TEST $i $j $k
                echo 'done'
            done
        done
    done
}

run_cases
