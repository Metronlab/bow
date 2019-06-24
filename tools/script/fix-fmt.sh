#!/bin/bash -e

echo FIX EVENTUAL BAD FORMATED FILE
find * -type f -name "*.go" | grep '.go$' | grep -v '^vendor' | grep -v -e '^.tmp' | xargs gofmt -s -l -w