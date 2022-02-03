#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

: ${PKG:=./...}
: ${TIMEOUT:="1h"}
: ${RUN:=".*"}

BENCH_RESULTS_DIR_PATH=$1
BENCH_RESULTS_FILE_PATH=$2

mkdir -p "${BENCH_RESULTS_DIR_PATH}"

printf "Running benchmarks to %s\n" "${BENCH_RESULTS_FILE_PATH}"
go test ${PKG} -run XXX -bench="${RUN}" -benchmem -timeout ${TIMEOUT} | tee "${BENCH_RESULTS_FILE_PATH}"

printf "Running benchstat on %s\n" "${BENCH_RESULTS_FILE_PATH}"
benchstat "${BENCH_RESULTS_FILE_PATH}"