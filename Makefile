#user overridable variables
PKG=./...
RUN=".*"

all: count fix-fmt check test

install: ## install dependencies
	@go get golang.org/x/lint/golint \
		honnef.co/go/tools/cmd/staticcheck \
		github.com/client9/misspell/cmd/misspell \
		golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow \
		github.com/jstemmer/go-junit-report

test: ## Run unit tests
	@RUN=$(RUN) \
		PKG=$(PKG) \
		bash -c $(PWD)/scripts/test.sh

fix-fmt: ## use fmt -w
	@bash -c $(PWD)/scripts/fix-fmt.sh

check: ## check code syntax
	@bash -c $(PWD)/scripts/code-checks.sh

bench: ## run benchmarks
	@bash -c $(PWD)/scripts/benchmark.sh

count: ## count lines and contributions
	@bash -c $(PWD)/scripts/count-code-lines.sh
