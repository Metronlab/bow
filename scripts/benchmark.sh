#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

: ${PKG:=./...}
: ${TIMEOUT:="1h"}

echo "Running benchmarks"
go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} | tee /tmp/benchmarkResults.new.txt

echo
echo "Running benchstat on /tmp/benchmarkResults.new.txt"
benchstat /tmp/benchmarkResults.new.txt

if [ -n "$CI" ]; then
  echo
  echo "CI detected - running in comparison mode"

  # compare with master branch
  git checkout -q -f master
  echo
  echo "Running benchmarks on master branch"
  go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} | tee /tmp/benchmarkResults.master.old.txt || echo "Benchmark on master failed"

  git checkout -q -f "$CIRCLE_SHA1"
  bash ./scripts/benchstat.sh "/tmp/benchmarkResults.master.old.txt" "/tmp/benchmarkResults.new.txt"
fi
