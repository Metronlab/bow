#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

BENCH_RESULTS_DIR_PATH=$1
BENCH_RESULTS_FILE_PATH=$2

TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)

: ${PKG:=./...}
: ${TIMEOUT:="1h"}
: ${BENCH_RESULTS_DIR_PATH:=/tmp/benchmarks}
: ${BENCH_RESULTS_FILE_PATH:=/tmp/benchmarks/${TIMESTAMP}.txt}
: ${RUN:=".*"}

mkdir -p "${BENCH_RESULTS_DIR_PATH}"

printf "Running benchmarks to %s\n" "${BENCH_RESULTS_FILE_PATH}"
go test ${PKG} -run XXX -bench="${RUN}" -benchmem -timeout ${TIMEOUT} | tee "${BENCH_RESULTS_FILE_PATH}"

printf "Running benchstat on %s\n" "${BENCH_RESULTS_FILE_PATH}"
benchstat "${BENCH_RESULTS_FILE_PATH}"