#!/bin/bash

old_bench_file=$1
new_bench_file=$2

echo
printf "Running benchstat to compare %s and %s\n" "$old_bench_file" "$new_bench_file"

if [ ! -f "$old_bench_file" ]
then
    printf "%s does not exist\n" "$old_bench_file"
    exit 0
fi

if [ ! -f "$new_bench_file" ]
then
    printf "%s does not exist\n" "$new_bench_file"
    exit 0
fi

benchstat -delta-test none "$old_bench_file" "$new_bench_file"
