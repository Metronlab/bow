#user overridable variables
all: lint test

install:
	@go install golang.org/x/perf/cmd/benchstat@latest
	@go install github.com/jstemmer/go-junit-report@latest
	@go install github.com/Metronlab/genius@latest
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin latest

lint:
	golangci-lint run --fix -v $(PKG)

test:
	@RUN=$(RUN) PKG=$(PKG) TIMEOUT=$(TIMEOUT) bash -c $(PWD)/scripts/test.sh

bench:
	@RUN=$(RUN) PKG=$(PKG) TIMEOUT=$(TIMEOUT) bash -c $(PWD)/scripts/benchmark.sh

CPUPROFILE=/tmp/$(shell basename $(PWD))$(shell echo $(PKG) | sed 's/[^[:alnum:]\t]//g').cpu.prof
MEMPROFILE=/tmp/$(shell basename $(PWD))$(shell echo $(PKG) | sed 's/[^[:alnum:]\t]//g').mem.prof

test-profile:
	go test $(PKG) -v -run $(RUN) -cpuprofile $(CPUPROFILE) -memprofile $(MEMPROFILE)
	-lsof -ti tcp:8888 | xargs kill -9 2> /dev/null
	-lsof -ti tcp:8989 | xargs kill -9 2> /dev/null
	go tool pprof -http=:8888 $(CPUPROFILE) &
	go tool pprof -http=:8989 $(MEMPROFILE) &

bench-profile:
	go test $(PKG) -run XXX -bench $(RUN) -cpuprofile $(CPUPROFILE) -memprofile $(MEMPROFILE)
	-lsof -ti tcp:9090 | xargs kill -9 2> /dev/null
	-lsof -ti tcp:9191 | xargs kill -9 2> /dev/null
	go tool pprof -http=:9090 $(CPUPROFILE) &
	go tool pprof -http=:9191 $(MEMPROFILE) &
