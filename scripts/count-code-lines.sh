#!/bin/zsh

echo nb of code lines without tests:
cat $(ls **/*.go | grep -v vendor/ | grep -vE '^.*_test.go$') | wc -l
echo nb of code lines:
cat $(ls **/*.go | grep -v vendor/) | wc -l

(for file in $(ls **/*.go | grep -v vendor/ )
 do
    git blame ${file}
done) | cut -f2 -d\( | cut -f1 -d\ | grep -vE '.*.go' | sort | uniq -c
