.PHONY: lint test test-race bench bench-gate fuzz-replay coverage coverage-gate tidy

# Minimum overall and per-package statement coverage. CI fails below this.
COVERAGE_MIN ?= 80

GO ?= go
GOLANGCI_LINT ?= golangci-lint
BENCHSTAT ?= benchstat

tidy:
	$(GO) mod tidy

lint:
	$(GO) vet ./...
	$(GOLANGCI_LINT) run

# Default test timeout — client stress/E2E suite needs ~70s under -race.
TEST_TIMEOUT ?= 180s

# Stress and E2E tests are slow (~65s under -race). They are excluded
# from the fast path so CI can run a quick gate and a thorough gate.
STRESS_RUN ?= 'TestStress|TestE2E'

test:
	$(GO) test -count=1 -timeout=$(TEST_TIMEOUT) ./...

test-race:
	$(GO) test -race -count=1 -timeout=$(TEST_TIMEOUT) ./...

# Fast path: unit + integration tests only, no stress/E2E/network.
test-fast:
	$(GO) test -count=1 -timeout=60s -race ./frame/... ./hpack/... ./conn/...
	$(GO) test -count=1 -timeout=60s -race ./client/... -skip $(STRESS_RUN)

# Thorough path: includes stress, E2E against google.com/nghttp2.
test-stress:
	$(GO) test -count=1 -timeout=$(TEST_TIMEOUT) -race ./client/... -run $(STRESS_RUN)

coverage:
	$(GO) test -race -count=1 -timeout=$(TEST_TIMEOUT) -coverprofile=cover.out ./...
	$(GO) tool cover -func=cover.out

coverage-gate:
	$(GO) test -race -count=1 -timeout=$(TEST_TIMEOUT) -coverprofile=cover.out ./...
	./scripts/coverage-gate.sh $(COVERAGE_MIN)

bench:
	$(GO) test -bench=. -benchmem -benchtime=2s -count=10 -run=^$$ ./...

bench-gate:
	./scripts/bench-gate.sh
