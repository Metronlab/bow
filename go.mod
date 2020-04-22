module github.com/metronlab/bow

require (
	github.com/apache/arrow/go/arrow v0.0.0-20190627144708-7ae6a58d16ea
	github.com/golangci/golangci-lint v1.23.6 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/stretchr/testify v1.4.0
)

replace github.com/apache/arrow/go/arrow => github.com/street-dev/arrow/go/arrow v0.0.0-20190627153434-3e895f49f0ec

go 1.13
