#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

old_bench_file=$1
new_bench_file=$2

echo "Running benchstat to compare $old_bench_file and $new_bench_file"

if [ ! -f "$old_bench_file" ]; then
    echo "$old_bench_file does not exist"
    exit 1
fi

if [ ! -f "$new_bench_file" ]; then
    echo "$new_bench_file does not exist"
    exit 1
fi

benchstat -delta-test none "$old_bench_file" "$new_bench_file"

first_line=$(benchstat -delta-test none -sort delta -csv "$old_bench_file" "$new_bench_file" | head -2 | tail -1)
delta=$(echo "$first_line" | cut -d , -f 6 | cut -d % -f 1)

if [ -z "$delta" ]; then
    echo "Nothing to compare!"
    exit 1
fi

[ "$delta" = "~" ] && exit 0

limit=0.5
if echo "$delta" "$limit" | awk '{exit !( $1 > $2)}'; then
    echo "Performance degradation detected:"
    echo "$first_line"
    exit 1
fi
