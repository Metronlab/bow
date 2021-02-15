#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

: ${PKG:=./...}
: ${TIMEOUT:="1h"}

echo "Running benchmarks"
go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} | tee ./benchmarkResults.new.txt
echo "Running benchstat on ./benchmarkResults.new.txt"
benchstat ./benchmarkResults.new.txt

if [ -n "$CI" ]; then
  echo
  echo "CI detected - running in comparison mode"

  # compare with master branch
  git checkout -q -f master
  echo "Running benchmarks on master branch"
  go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} | tee ./benchmarkResults.master.old.txt || echo "Benchmark on master failed"

  git checkout -q -f "$CIRCLE_SHA1"
  bash -c ./scripts/benchstat.sh "./benchmarkResults.master.old.txt" "./benchmarkResults.new.txt"
fi
