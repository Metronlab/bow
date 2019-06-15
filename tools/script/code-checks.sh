#!/bin/bash -e

# LIST GO FILES
gofiles=$(find . -name '*.go' | grep -v -e '^./vendor' | grep -v -e '^./.tmp')
echo run on ${gofiles}

echo RUN GOLINT
test -z "$(golint ./... | grep -v -e "should have comment" -e "vendor/" -e "mocks/" -e ".tmp/" | tee /dev/stderr )"

echo RUN STATICCHECK
staticcheck ./...

echo RUN MISSPELL
echo ${gofiles} | xargs misspell -error

echo RUN GOVET
go vet ./...
# go vet -vettool=$(which shadow) ./...

echo RUN GOFMT
unformatted=$(gofmt -s -l ${gofiles})
[ -z "${unformatted}" ] && exit 0
echo >&2 "Some files are not formatted, please run docker-compose run --rm fmt or edit the files"
echo >&2 "Unformatted files:"
echo >&2 ${unformatted}
exit 1
