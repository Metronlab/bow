#!/bin/bash -e

# The -o pipefail option is important for the trap to be executed if the "go test" command fails
set -o pipefail

: ${TEST_RESULTS:=/tmp/test-results}
: ${COVER_RESULTS:=/tmp/cover-results}
: ${PKG:=./...}

mkdir -p ${COVER_RESULTS}
mkdir -p ${TEST_RESULTS}

trap "go-junit-report <${TEST_RESULTS}/go-test.out > ${TEST_RESULTS}/go-test-report.xml" EXIT
go test ${PKG} -v -race -cover -covermode=atomic -coverprofile=${COVER_RESULTS}/coverage.cover -timeout 30s \
    | tee ${TEST_RESULTS}/go-test.out \
    | sed ''/PASS/s//$(printf "\033[32mPASS\033[0m")/'' \
    | sed ''/FAIL/s//$(printf "\033[31mFAIL\033[0m")/'' \
    | sed ''/RUN/s//$(printf "\033[34mRUN\033[0m")/''

go tool cover -html=${COVER_RESULTS}/coverage.cover -o ${COVER_RESULTS}/coverage.html

echo "To open the html coverage file use one of the following commands:"
echo "open $COVER_RESULTS/coverage.html on mac"
echo "xdg-open $COVER_RESULTS/coverage.html on linux"