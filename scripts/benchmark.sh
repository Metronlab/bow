#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)

: "${PKG:="./..."}"
: "${TIMEOUT:="1h"}"
: "${RUN:=".*"}"
: "${BENCH_RESULTS_DIR_PATH:="/tmp/benchmarks"}"
: "${BENCH_RESULTS_FILE_PATH:="/tmp/benchmarks/${TIMESTAMP}.txt"}"

mkdir -p "$BENCH_RESULTS_DIR_PATH"

printf "Run benchmarks into file %s\n" "$BENCH_RESULTS_FILE_PATH"
go test $PKG -run XXX -bench="$RUN" -benchmem -timeout "$TIMEOUT" | tee "$BENCH_RESULTS_FILE_PATH"

printf "Run benchstat on file %s\n" "$BENCH_RESULTS_FILE_PATH"
benchstat "$BENCH_RESULTS_FILE_PATH"