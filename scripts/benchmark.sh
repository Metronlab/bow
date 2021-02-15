#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

: ${PKG:=./...}
: ${TIMEOUT:="1h"}

echo "Running benchmarks"
new_bench_file="benchmarkResults.new.txt"
go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} > "$new_bench_file"
cat $new_bench_file
benchstat "$new_bench_file"

if [ -n "$CI" ]; then
  echo
  echo "CI detected - running in comparison mode"

  # compare with master branch
  git checkout -q -f master
  echo "Running benchmarks on master branch"
  old_bench_master_file="benchmarkResults.master.old.txt"
  go test ${PKG} -run=XXX -bench=. -benchmem -timeout ${TIMEOUT} > "$old_bench_master_file" || echo "Benchmark on master failed"
  cat $old_bench_master_file

  git checkout -q -f "$CIRCLE_SHA1"
  bash -c ./scripts/benchstat.sh "$old_bench_master_file" "$new_bench_file"
fi
