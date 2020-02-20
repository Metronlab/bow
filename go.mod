module github.com/metronlab/bow

require (
	github.com/apache/arrow/go/arrow v0.0.0-20190627144708-7ae6a58d16ea
	github.com/client9/misspell v0.3.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/stretchr/testify v1.3.0
	golang.org/x/lint v0.0.0-20200130185559-910be7a94367 // indirect
	golang.org/x/tools v0.0.0-20200220051852-2086a0a691c0 // indirect
	honnef.co/go/tools v0.0.1-2019.2.3 // indirect
)

replace github.com/apache/arrow/go/arrow => github.com/street-dev/arrow/go/arrow v0.0.0-20190627153434-3e895f49f0ec

go 1.13
