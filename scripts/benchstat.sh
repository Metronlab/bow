#!/bin/bash

TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)

: ${BENCH_RESULTS_DIR_PATH:=/tmp/benchmarks}
: ${BENCH_COMPARISON_FILE_PATH:=/tmp/benchmarks/benchstat.${TIMESTAMP}.txt}

old_bench_file=$1
new_bench_file=$2

echo
printf "Running benchstat to compare %s and %s in %s\n" "$old_bench_file" "$new_bench_file" "${BENCH_COMPARISON_FILE_PATH}"

if [ ! -f "$old_bench_file" ]
then
    printf "%s does not exist\n" "$old_bench_file"
    exit 0
fi

if [ ! -f "$new_bench_file" ]
then
    printf "%s does not exist\n" "$new_bench_file"
    exit 0
fi

mkdir -p ${BENCH_RESULTS_DIR_PATH}

benchstat -delta-test none "$old_bench_file" "$new_bench_file" | tee "${BENCH_COMPARISON_FILE_PATH}"