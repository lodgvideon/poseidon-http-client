.PHONY: lint test test-race test-debug bench bench-alloc bench-gate fuzz-replay coverage coverage-gate tidy contrib-test
.PHONY: it-up it-down it-logs it-test it-test-fast it-certs h3-interop h3-interop-loss h3-interop-reorder h3-interop-fault h3-interop-chacha h3-soak
.PHONY: qns-image qns-image-multi

# Minimum overall and per-package statement coverage. CI fails below this.
COVERAGE_MIN ?= 80

GO ?= go
GOLANGCI_LINT ?= golangci-lint
BENCHSTAT ?= benchstat

# Nested modules. The root `./...` never reaches them, so every target that
# should cover them has to iterate explicitly.
CONTRIB_MODULES ?= contrib/prometheus
# test/interop/quic is nested for a different reason than contrib: it is a `main`
# package with no tests, and the per-package coverage gate below would fail it
# inside the root module. It still has to be vetted and linted, and above all it
# has to keep compiling against this tree — that is the whole point of its
# replace directive.
NESTED_MODULES = $(CONTRIB_MODULES) test/interop/quic

tidy:
	$(GO) mod tidy
	@for m in $(NESTED_MODULES); do $(GO) -C $$m mod tidy; done

lint:
	$(GO) vet ./...
	$(GOLANGCI_LINT) run
	@for m in $(NESTED_MODULES); do \
		$(GO) -C $$m vet ./... && (cd $$m && $(GOLANGCI_LINT) run); \
	done

# Test the contrib modules twice: once against the released parent pinned in
# their go.mod (what a consumer gets), once against this tree (catches an API
# break before it ships).
contrib-test:
	@for m in $(CONTRIB_MODULES); do \
		echo "==> $$m (pinned parent)"; \
		$(GO) -C $$m test -race -count=1 ./... || exit 1; \
		echo "==> $$m (in-tree parent)"; \
		$(GO) -C $$m mod edit -replace=github.com/lodgvideon/poseidon-http-client=$(CURDIR); \
		$(GO) -C $$m mod tidy && $(GO) -C $$m test -race -count=1 ./...; \
		status=$$?; \
		$(GO) -C $$m mod edit -dropreplace=github.com/lodgvideon/poseidon-http-client; \
		$(GO) -C $$m mod tidy; \
		[ $$status -eq 0 ] || exit $$status; \
	done

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

# Allocation-measuring benchmarks, kept behind a tag because bench-gate is an
# absolute zero-alloc gate over the codec packages and these exist to report
# non-zero numbers.
bench-alloc:
	$(GO) test -tags allocbench -bench=. -benchmem -benchtime=300x -count=1 -run=^$$ ./http3

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

# HTTP/3 interop: a real Caddy HTTP/3 server + the client run in-container on a
# shared network (Docker Desktop on Windows does not forward host UDP). The
# runner exits with the test result; teardown always runs.
H3_COMPOSE = test/integration/http3/docker-compose.yml
h3-interop:
	@trap '$(DOCKER_COMPOSE) -f $(H3_COMPOSE) down -v 2>/dev/null' EXIT; \
	$(DOCKER_COMPOSE) -f $(H3_COMPOSE) run --rm runner

# Same interop suite, but the client dials through a UDP relay that injects
# network faults. h3-interop-loss drops ~10% of datagrams each way (loss recovery,
# RFC 9002); h3-interop-reorder reorders ~20% with no loss (ack-range handling +
# receive reassembly under reorder). Override LOSS_PCT/REORDER_PCT/SEED to vary.
H3_LOSS_COMPOSE = test/integration/http3/docker-compose.loss.yml
h3-interop-loss:
	@trap '$(DOCKER_COMPOSE) -f $(H3_LOSS_COMPOSE) down -v 2>/dev/null' EXIT; \
	$(DOCKER_COMPOSE) -f $(H3_LOSS_COMPOSE) run --rm runner

h3-interop-reorder:
	@trap '$(DOCKER_COMPOSE) -f $(H3_LOSS_COMPOSE) down -v 2>/dev/null' EXIT; \
	LOSS_PCT=0 REORDER_PCT=20 $(DOCKER_COMPOSE) -f $(H3_LOSS_COMPOSE) run --rm runner

# HTTP/3 negative-path interop against a deliberately misbehaving server
# (faultserver, quic-go): checks the client's error handling — the reset fault
# surfaces a retryable StreamResetError (RFC 9114 §4.1.1).
H3_FAULT_COMPOSE = test/integration/http3/docker-compose.fault.yml
h3-interop-fault:
	@trap '$(DOCKER_COMPOSE) -f $(H3_FAULT_COMPOSE) down -v 2>/dev/null' EXIT; \
	$(DOCKER_COMPOSE) -f $(H3_FAULT_COMPOSE) run --rm runner

# HTTP/3 interop against an nginx server pinned to TLS_CHACHA20_POLY1305_SHA256,
# proving the client's ChaCha20-Poly1305 packet protection (RFC 9001 §5.3/§5.4.4)
# on the wire — the handshake cannot fall back to an AES-GCM suite.
H3_CHACHA_COMPOSE = test/integration/http3/docker-compose.chacha.yml
h3-interop-chacha:
	@trap '$(DOCKER_COMPOSE) -f $(H3_CHACHA_COMPOSE) down -v 2>/dev/null' EXIT; \
	$(DOCKER_COMPOSE) -f $(H3_CHACHA_COMPOSE) run --rm runner

# HTTP/3 soak: sustained concurrent load against the interop Caddy server for
# POSEIDON_SOAK_DURATION (default 120s) with POSEIDON_SOAK_WORKERS (default 64),
# asserting the managed client leaks no goroutines or heap (resource-exhaustion
# guard, cf. the receive-path bounds). Not part of CI (long-running).
h3-soak:
	@trap '$(DOCKER_COMPOSE) -f $(H3_COMPOSE) down -v 2>/dev/null' EXIT; \
	$(DOCKER_COMPOSE) -f $(H3_COMPOSE) run --rm \
	  -e POSEIDON_SOAK_DURATION=$${POSEIDON_SOAK_DURATION:-120s} \
	  -e POSEIDON_SOAK_WORKERS=$${POSEIDON_SOAK_WORKERS:-64} \
	  runner go test ./client/ -tags soak -run TestSoak_H3 -v -timeout 15m

# ── quic-interop-runner endpoint image ───────────────────────────
# See test/interop/quic/README.md. The build context is the repository root
# because the client is a nested module whose replace points at the parent.
QNS_IMAGE     ?= poseidon-interop:local
QNS_DOCKERFILE = test/interop/quic/Dockerfile
QNS_PLATFORMS ?= linux/amd64,linux/arm64

# One platform, loaded into the local image store, for running the interop
# runner against a locally built tag (`run.py -r poseidon=$(QNS_IMAGE)`).
qns-image:
	docker buildx build --platform linux/amd64 --load -f $(QNS_DOCKERFILE) -t $(QNS_IMAGE) .

# Both platforms the online runner requires. No --load: buildx cannot load a
# multi-platform manifest into the local store, so this proves the build and
# leaves the result in the build cache. Publishing is a separate, manual step —
# see the README.
qns-image-multi:
	docker buildx build --platform $(QNS_PLATFORMS) -f $(QNS_DOCKERFILE) -t $(QNS_IMAGE) .
