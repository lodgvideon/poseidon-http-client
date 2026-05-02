.PHONY: lint test test-race bench bench-gate fuzz-replay coverage tidy

GO ?= go
GOLANGCI_LINT ?= golangci-lint
BENCHSTAT ?= benchstat

tidy:
	$(GO) mod tidy

lint:
	$(GO) vet ./...
	$(GOLANGCI_LINT) run

test:
	$(GO) test -count=1 ./...

test-race:
	$(GO) test -race -count=1 ./...

coverage:
	$(GO) test -race -count=1 -coverprofile=cover.out ./...
	$(GO) tool cover -func=cover.out

bench:
	$(GO) test -bench=. -benchmem -benchtime=2s -count=10 -run=^$$ ./...

bench-gate:
	./scripts/bench-gate.sh
