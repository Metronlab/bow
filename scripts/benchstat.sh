#!/bin/bash -e

OLD_BENCH_FILE_PATH=$1
NEW_BENCH_FILE_PATH=$2

TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)

: ${BENCH_RESULTS_DIR_PATH:=/tmp/benchmarks}
: ${BENCH_COMPARISON_FILE_PATH:=/tmp/benchmarks/benchstat.${TIMESTAMP}.txt}

echo
printf "Running benchstat to compare %s and %s in %s\n" "$OLD_BENCH_FILE_PATH" "$NEW_BENCH_FILE_PATH" "${BENCH_COMPARISON_FILE_PATH}"

if [ ! -f "$OLD_BENCH_FILE_PATH" ]
then
    printf "%s does not exist\n" "$OLD_BENCH_FILE_PATH"
    exit 0
fi

if [ ! -f "$NEW_BENCH_FILE_PATH" ]
then
    printf "%s does not exist\n" "$NEW_BENCH_FILE_PATH"
    exit 0
fi

mkdir -p ${BENCH_RESULTS_DIR_PATH}

benchstat -delta-test none "$OLD_BENCH_FILE_PATH" "$NEW_BENCH_FILE_PATH" | tee "${BENCH_COMPARISON_FILE_PATH}"