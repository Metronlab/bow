#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

: ${PKG:=./...}
: ${TIMEOUT:="1h"}

: ${BENCH_RESULTS_DIR:=/tmp/benchmarks}
: ${BENCH_RESULTS_NEW:=/tmp/benchmarks/results.new.txt}
: ${BENCH_RESULTS_OLD:=/tmp/benchmarks/results.master.old.txt}

mkdir -p ${BENCH_RESULTS_DIR}

echo "Running benchmarks"
go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} | tee ${BENCH_RESULTS_NEW}

echo
printf "Running benchstat on %s" ${BENCH_RESULTS_NEW}
benchstat ${BENCH_RESULTS_NEW}

if [ -n "$CI" ]; then
  echo
  echo "CI detected - running in comparison mode"

  # compare with main branch
  git checkout -q -f main
  echo
  echo "Running benchmarks on main branch"
  go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} | tee ${BENCH_RESULTS_OLD} || echo "Benchmark on main failed"

  git checkout -q -f "$CIRCLE_SHA1"
  bash ./scripts/benchstat.sh ${BENCH_RESULTS_OLD} ${BENCH_RESULTS_NEW}
fi
