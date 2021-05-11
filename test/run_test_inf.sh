set -euo pipefail

run_cases () {
    END=16
    for (( i=1; i<=END; i++ )); do
        for (( j=1; j<=END; j++ )); do
            for (( k=1; k<=END; k++ )); do
                echo 'processing case' $i $j $k
                ./build/parallel_read_test $i $j $k
                echo 'done'
            done
        done
    done
}

run_cases

# while [ 1 ]
# do
#     run_cases
# done
