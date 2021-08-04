#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

: ${PKG:=./...}
: ${TIMEOUT:="1h"}

TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
CURR_BRANCH=$(git symbolic-ref HEAD 2>/dev/null | cut -d"/" -f 3)

: ${BENCH_RESULTS_DIR:=/tmp/benchmarks}
: ${BENCH_RESULTS_CURR:=/tmp/benchmarks/${CURR_BRANCH}.${TIMESTAMP}.txt}
: ${BENCH_RESULTS_MAIN:=/tmp/benchmarks/main.${TIMESTAMP}.txt}

mkdir -p ${BENCH_RESULTS_DIR}

printf "Running benchmarks to %s\n" "${BENCH_RESULTS_CURR}"
go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} | tee "${BENCH_RESULTS_CURR}"

echo
printf "Running benchstat on %s\n" "${BENCH_RESULTS_CURR}"
benchstat "${BENCH_RESULTS_CURR}"

if [ -n "$CI" ]; then
  echo
  echo "CI detected - running in comparison mode"

  # compare with main branch
  git checkout -q -f main
  echo
  echo "Running benchmarks on main branch"
  go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} | tee "${BENCH_RESULTS_MAIN}" || echo "Benchmark on main failed"

  git checkout -q -f "$CIRCLE_SHA1"
  bash ./scripts/benchstat.sh "${BENCH_RESULTS_MAIN}" "${BENCH_RESULTS_CURR}"
fi
