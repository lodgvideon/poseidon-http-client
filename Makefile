.PHONY: lint test test-race test-debug bench bench-gate fuzz-replay coverage coverage-gate tidy
.PHONY: it-up it-down it-logs it-test it-test-fast it-certs

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

# Exercise the opt-in debug build (-tags poseidondebug): the leak detector and
# its finalizer-based tests. Kept out of the default `test` because the GC/
# finalizer timing is non-deterministic; run it locally when touching the
# debug tooling. CI compile-checks the tag separately (deterministic).
test-debug:
	$(GO) test -tags poseidondebug -count=1 -timeout=60s ./client/

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

# ── Docker integration test infra ────────────────────────────────
DOCKER_COMPOSE ?= docker compose
COMPOSE_FILE   ?= test/integration/docker-compose.yml
IT_DIR          = test/integration
IT_TIMEOUT     ?= 300s
IT_TAGS        = -tags=integration
IT_PKG         = ./client/integration_test/...

it-certs:
	cd $(IT_DIR) && ./scripts/gen-certs.sh

it-up: it-certs
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d --wait

it-down:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down -v

it-logs:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f

# Full integration test: bring up Docker, run tests, tear down.
it-test:
	@trap '$(MAKE) -s it-down 2>/dev/null' EXIT; \
	$(MAKE) it-up && \
	$(GO) test $(IT_TAGS) -race -count=1 -timeout=$(IT_TIMEOUT) -v $(IT_PKG)

# Fast path: only Go in-process reference server (no Docker needed).
it-test-fast:
	POSEIDON_IT_SKIP_REMOTE=true $(GO) test $(IT_TAGS) -race -count=1 -timeout=60s -v $(IT_PKG)
