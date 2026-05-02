# Poseidon Phase A — Frame Layer + HPACK — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained Go library that encodes and decodes HTTP/2 frames (RFC 7540) and HPACK header blocks (RFC 7541) with `0 allocs/op` on the hot path, backed by table-driven RFC tests, fuzz, and bench gates.

**Architecture:** Three layers, bottom-up, no cycles. `internal/bytesx` provides byte-level helpers (uint24/31, padding strip, typed pools). `hpack` builds a from-scratch HPACK codec (integer codec → Huffman → static/dynamic tables → encoder/decoder) using `bytesx`. `frame` builds the HTTP/2 framer (10 frame types + Framer with visitor-based read and explicit per-type writes) using `hpack` for HEADERS/CONTINUATION/PUSH_PROMISE block fragments. Public API surfaces: `frame.Framer`, `frame.Handler`, `hpack.Encoder`, `hpack.Decoder`. No `net/http`, no `golang.org/x/net/http2`, no networking.

**Tech Stack:** Go 1.24 (latest stable on 2026-05-02); standard library only at runtime; `golangci-lint` v1.62, `staticcheck`, `benchstat` for tooling; GitHub Actions CI; native Go `testing`/`testing/fuzz`.

**Spec reference:** `docs/superpowers/specs/2026-05-02-poseidon-frame-layer-design.md` (single source of truth — sections quoted below).

**Branch:** `design/2026-05-02-frame-layer-phase-a` (already created from `main`). All work for Phase A lands on this branch via small commits; eventual squash/merge to `main` after acceptance.

**Acceptance criteria (from Spec §11):**

- All 10 frame types from RFC 7540 §6 round-trip (encode∘decode == identity for all valid inputs).
- HPACK passes all RFC 7541 App. C vectors (C.1–C.6, with and without Huffman).
- `go test -race ./...` green.
- `golangci-lint run` green.
- Bench gate: every hot-path benchmark `0 allocs/op` and `0 B/op` after warmup.
- Conformance gate: RFC-tagged tests green; `docs/RFC_COVERAGE.md` covers RFC 7540 §4–§6 and RFC 7541 §2–§7 fully.
- Nightly fuzz: no crashes for 7 consecutive nights before tagging.
- README, `BENCH_BASELINE.md`, examples in place.

---

## File Structure

Directories created or modified by this plan, with one-line responsibilities:

```
poseidon-http-client/
├── go.mod                                   # module declaration; Go 1.24, no deps
├── .gitignore                               # Go-standard
├── Makefile                                 # local commands: lint, test, bench, bench-gate
├── README.md                                # project overview, usage example, dev guide
├── LICENSE                                  # MIT
├── .golangci.yml                            # lint config (errcheck, govet, staticcheck, ...)
├── .githooks/pre-commit                     # opt-in local pre-commit (vet+test+lint)
├── scripts/
│   ├── bench-gate.sh                        # parses benchstat diff; fails on alloc regress / >10% ns regress
│   ├── coverage-gate.sh                     # ≥90% per-package coverage gate
│   ├── rfc-coverage-gate.sh                 # parses Conformance test names → RFC sections
│   └── rfc-matrix-check.sh                  # ensures docs/RFC_COVERAGE.md sections all have passing tests
├── docs/
│   ├── superpowers/specs/                   # design specs (already exists)
│   ├── superpowers/plans/                   # implementation plans (this file)
│   ├── RFC_COVERAGE.md                      # RFC §X.Y → test name matrix
│   └── BENCH_BASELINE.md                    # reference machine + first benchstat capture
├── .github/workflows/
│   ├── ci.yml                               # vet, lint, test -race, fuzz corpus replay
│   ├── bench-gate.yml                       # benchstat HEAD vs base; gate
│   ├── conformance-gate.yml                 # RFC vector tests + matrix check
│   └── nightly.yml                          # 10-min fuzz on each public surface
├── internal/bytesx/                         # private byte helpers (no API)
│   ├── uint.go                              # ReadUint24/WriteUint24, ReadUint31/WriteUint31
│   ├── uint_test.go
│   ├── padding.go                           # StripPadding (DATA/HEADERS/PUSH_PROMISE)
│   ├── padding_test.go
│   ├── pool.go                              # GetReadBuf/PutReadBuf typed sync.Pool
│   └── pool_test.go
├── hpack/                                   # PUBLIC: HPACK codec (RFC 7541)
│   ├── doc.go                               # package doc, contract notes
│   ├── errors.go                            # sentinel errors
│   ├── hpack.go                             # public re-exports: HeaderField, Encoder, Decoder
│   ├── integer.go                           # N-bit prefix integer codec (§5.1)
│   ├── integer_test.go
│   ├── huffman_table.go                     # generated 256-entry encode table, FSM decode table (App. B)
│   ├── huffman.go                           # HuffmanEncodedLen, HuffmanEncode, HuffmanDecode
│   ├── huffman_test.go
│   ├── static_table.go                      # 61-entry static table (App. A) + staticIndex
│   ├── static_table_test.go
│   ├── dynamic_table.go                     # ring buffer + arena
│   ├── dynamic_table_test.go
│   ├── string_literal.go                    # string literal encode/decode (§5.2)
│   ├── string_literal_test.go
│   ├── encoder.go                           # public Encoder
│   ├── encoder_test.go
│   ├── decoder.go                           # public Decoder + streaming
│   ├── decoder_test.go
│   ├── conformance_test.go                  # RFC 7541 App. C vectors
│   ├── bench_test.go                        # bench gates
│   └── fuzz_test.go                         # FuzzHPACKDecode + seed corpus
├── frame/                                   # PUBLIC: HTTP/2 framer (RFC 7540)
│   ├── doc.go
│   ├── errors.go                            # sentinels + ErrCode (§7)
│   ├── frame.go                             # FrameType / Flags const, FrameHeader, sized invariants
│   ├── header.go                            # ReadFrameHeader/WriteFrameHeader
│   ├── header_test.go
│   ├── data.go                              # DATA encode/decode helpers
│   ├── data_test.go
│   ├── priority.go                          # PRIORITY (5 bytes)
│   ├── priority_test.go
│   ├── rst_stream.go                        # RST_STREAM (4 bytes)
│   ├── rst_stream_test.go
│   ├── settings.go                          # SettingsParams + encode/decode + ack rules
│   ├── settings_test.go
│   ├── ping.go                              # PING (8 bytes opaque + ACK flag)
│   ├── ping_test.go
│   ├── goaway.go                            # GOAWAY (last id + code + opaque debug)
│   ├── goaway_test.go
│   ├── window_update.go                     # WINDOW_UPDATE (4 bytes increment)
│   ├── window_update_test.go
│   ├── continuation.go                      # CONTINUATION header block fragment
│   ├── continuation_test.go
│   ├── headers.go                           # HEADERS w/ optional padding + priority
│   ├── headers_test.go
│   ├── push_promise.go                      # PUSH_PROMISE w/ optional padding
│   ├── push_promise_test.go
│   ├── framer.go                            # Framer (Read/Write + visitor dispatch)
│   ├── framer_test.go
│   ├── conformance_test.go                  # RFC 7540 §4–§6 vectors
│   ├── bench_test.go
│   └── fuzz_test.go
└── testdata/
    ├── rfc7540/                             # frame-type golden bytes (named per §)
    └── rfc7541/                             # HPACK App. C golden inputs/outputs
```

---

## Bottom-Up Task Order

Bootstrap → `internal/bytesx` → `hpack` (integer → Huffman → tables → string → encoder → decoder) → milestone A1 → `frame` (header → simple frames → complex frames → Framer) → milestone A2 → CI gates → docs → milestone A3 → tag.

Each task is one TDD red-green-refactor-commit cycle. Most tasks have 5 steps (write test, run-fail, implement, run-pass, commit). Larger tasks have additional steps for setup or verification.

---

## Bootstrap

### Task 1: Initialize Go module and gitignore

**Files:**
- Create: `go.mod`
- Create: `.gitignore`

- [ ] **Step 1: Create go.mod**

```bash
cd $POSEIDON_REPO   # e.g. /Users/ivanprikhodko/work/source/poseidon-http-client
go mod init github.com/lodgvideon/poseidon-http-client
```

The generated `go.mod` should declare:

```go
module github.com/lodgvideon/poseidon-http-client

go 1.24
```

If `go mod init` produces a different Go directive (e.g. `1.22`), hand-edit it to `go 1.24`.

- [ ] **Step 2: Create .gitignore**

Create `.gitignore`:

```gitignore
# Binaries
*.exe
*.test
*.out

# Coverage
coverage.txt
cover.out

# Bench artifacts
head.txt
base.txt
diff.txt

# Fuzz cache (Go default)
testdata/fuzz/*/

# IDE
.vscode/
.idea/

# OS
.DS_Store
```

- [ ] **Step 3: Verify**

Run: `go mod tidy && go vet ./...`
Expected: no error, no output beyond a possible "no Go files" warning (no packages yet).

- [ ] **Step 4: Commit**

```bash
git add go.mod .gitignore
git commit -m "chore: init Go module and .gitignore"
```

---

### Task 2: Add LICENSE and Makefile

**Files:**
- Create: `LICENSE`
- Create: `Makefile`

- [ ] **Step 1: Create LICENSE (MIT)**

Create `LICENSE`:

```
MIT License

Copyright (c) 2026 Ivan Prikhodko

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

- [ ] **Step 2: Create Makefile**

Create `Makefile`:

```makefile
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

fuzz-replay:
	$(GO) test -run=Fuzz -count=1 ./...
```

- [ ] **Step 3: Verify**

Run: `make tidy && make lint || true` (lint will fail because golangci-lint not configured yet — that's fine; just ensure Make itself works).

- [ ] **Step 4: Commit**

```bash
git add LICENSE Makefile
git commit -m "chore: add MIT LICENSE and Makefile"
```

---

### Task 3: Add `.golangci.yml` and CI lint workflow

**Files:**
- Create: `.golangci.yml`
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create `.golangci.yml`**

```yaml
run:
  timeout: 5m
  go: '1.24'

linters:
  disable-all: true
  enable:
    - errcheck
    - govet
    - staticcheck
    - revive
    - gosec
    - unconvert
    - misspell
    - gocyclo
    - gosimple
    - unused
    - prealloc
    - ineffassign
    - typecheck

linters-settings:
  gocyclo:
    min-complexity: 15
  govet:
    enable-all: true

issues:
  exclude-use-default: false
  max-issues-per-linter: 0
  max-same-issues: 0
```

- [ ] **Step 2: Create `.github/workflows/ci.yml`**

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:

jobs:
  lint:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.24'
          cache: true
      - run: go vet ./...
      - uses: golangci/golangci-lint-action@v6
        with:
          version: v1.62

  test:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.24'
          cache: true
      - run: go test -race -count=1 ./...

  fuzz-corpus-replay:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.24'
      - run: go test -run=Fuzz -count=1 ./...
```

- [ ] **Step 3: Verify locally**

Run: `go vet ./...` (should be silent — no Go files yet).

- [ ] **Step 4: Commit**

```bash
git add .golangci.yml .github/workflows/ci.yml
git commit -m "ci: add lint+test workflow and golangci-lint config"
```

---

### Task 4: README skeleton

**Files:**
- Create: `README.md`

- [ ] **Step 1: Create README**

```markdown
# poseidon-http-client

A low-level, zero-allocation HTTP/2 client codec for Go, designed for load
generators. Implements RFC 7540 (HTTP/2) and RFC 7541 (HPACK) from scratch
without `net/http` or `golang.org/x/net/http2`.

**Status:** Phase A — frame codec + HPACK. See [design](docs/superpowers/specs/2026-05-02-poseidon-frame-layer-design.md).

## Phases

- **A — Frame layer + HPACK** *(in progress)*: codec only, no networking.
- **B — Connection layer**: TLS, ALPN, stream state machine, flow control.
- **C — Client + pool + discovery + stats**: public API for load generators.

## Local development

Requirements: Go 1.24, `golangci-lint` v1.62.

```bash
make tidy
make lint
make test-race
make bench
```

Optional pre-commit hook:

```bash
git config core.hooksPath .githooks
```

## License

MIT — see [LICENSE](LICENSE).
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README skeleton"
```

---

## internal/bytesx — byte-level helpers

### Task 5: `internal/bytesx/uint.go` — Uint24 read/write

**Files:**
- Create: `internal/bytesx/uint.go`
- Create: `internal/bytesx/uint_test.go`

- [ ] **Step 1: Write failing test**

Create `internal/bytesx/uint_test.go`:

```go
package bytesx

import (
	"bytes"
	"testing"
)

func TestReadUint24(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want uint32
	}{
		{"zero", []byte{0x00, 0x00, 0x00}, 0},
		{"max", []byte{0xff, 0xff, 0xff}, 0xff_ff_ff},
		{"big_endian", []byte{0x12, 0x34, 0x56}, 0x12_34_56},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ReadUint24(tc.in)
			if got != tc.want {
				t.Fatalf("ReadUint24(%x) = %#x, want %#x", tc.in, got, tc.want)
			}
		})
	}
}

func TestWriteUint24(t *testing.T) {
	cases := []struct {
		name string
		in   uint32
		want []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00}},
		{"max", 0xff_ff_ff, []byte{0xff, 0xff, 0xff}},
		{"big_endian", 0x12_34_56, []byte{0x12, 0x34, 0x56}},
		{"truncates_high_byte", 0xab_12_34_56, []byte{0x12, 0x34, 0x56}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf [3]byte
			WriteUint24(buf[:], tc.in)
			if !bytes.Equal(buf[:], tc.want) {
				t.Fatalf("WriteUint24(%#x) = %x, want %x", tc.in, buf, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test — should fail**

Run: `go test ./internal/bytesx/...`
Expected: compile error, "undefined: ReadUint24" / "undefined: WriteUint24".

- [ ] **Step 3: Implement**

Create `internal/bytesx/uint.go`:

```go
// Package bytesx provides private byte-level helpers used by the frame
// and hpack packages. Not part of the public API.
package bytesx

// ReadUint24 reads a big-endian 24-bit unsigned integer from b[:3].
// b MUST have length >= 3 — caller is responsible for the bound.
func ReadUint24(b []byte) uint32 {
	_ = b[2] // BCE hint
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
}

// WriteUint24 writes the low 24 bits of v as big-endian into b[:3].
// b MUST have length >= 3 — caller is responsible for the bound.
func WriteUint24(b []byte, v uint32) {
	_ = b[2] // BCE hint
	b[0] = byte(v >> 16)
	b[1] = byte(v >> 8)
	b[2] = byte(v)
}
```

- [ ] **Step 4: Run test — should pass**

Run: `go test ./internal/bytesx/...`
Expected: `ok  github.com/lodgvideon/poseidon-http-client/internal/bytesx`.

- [ ] **Step 5: Commit**

```bash
git add internal/bytesx/uint.go internal/bytesx/uint_test.go
git commit -m "feat(bytesx): ReadUint24/WriteUint24 big-endian"
```

---

### Task 6: `internal/bytesx/uint.go` — Uint31 read/write (with R-bit mask)

**Files:**
- Modify: `internal/bytesx/uint.go`
- Modify: `internal/bytesx/uint_test.go`

Per RFC 7540 §4.1, Stream Identifier and similar 31-bit fields ignore the high R bit on read and clear it on write.

- [ ] **Step 1: Add failing tests to `uint_test.go`**

Append to `internal/bytesx/uint_test.go`:

```go
func TestReadUint31(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want uint32
	}{
		{"zero", []byte{0x00, 0x00, 0x00, 0x00}, 0},
		{"max_31bit", []byte{0x7f, 0xff, 0xff, 0xff}, 0x7fff_ffff},
		{"r_bit_set_is_masked", []byte{0xff, 0xff, 0xff, 0xff}, 0x7fff_ffff},
		{"stream_id_1", []byte{0x00, 0x00, 0x00, 0x01}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ReadUint31(tc.in)
			if got != tc.want {
				t.Fatalf("ReadUint31(%x) = %#x, want %#x", tc.in, got, tc.want)
			}
		})
	}
}

func TestWriteUint31(t *testing.T) {
	cases := []struct {
		name string
		in   uint32
		want []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00}},
		{"max_31bit", 0x7fff_ffff, []byte{0x7f, 0xff, 0xff, 0xff}},
		{"high_bit_cleared", 0xffff_ffff, []byte{0x7f, 0xff, 0xff, 0xff}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf [4]byte
			WriteUint31(buf[:], tc.in)
			if !bytes.Equal(buf[:], tc.want) {
				t.Fatalf("WriteUint31(%#x) = %x, want %x", tc.in, buf, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run — fails**

Run: `go test ./internal/bytesx/...`
Expected: undefined `ReadUint31`/`WriteUint31`.

- [ ] **Step 3: Implement**

Append to `internal/bytesx/uint.go`:

```go
// ReadUint31 reads a 31-bit big-endian unsigned integer from b[:4],
// masking off the high R bit (RFC 7540 §4.1, §6.1, §6.6, §6.9).
func ReadUint31(b []byte) uint32 {
	_ = b[3]
	return (uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])) &^ 0x8000_0000
}

// WriteUint31 writes v as 31-bit big-endian into b[:4], clearing the high R bit.
func WriteUint31(b []byte, v uint32) {
	_ = b[3]
	v &^= 0x8000_0000
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./internal/bytesx/...`

- [ ] **Step 5: Commit**

```bash
git add internal/bytesx/uint.go internal/bytesx/uint_test.go
git commit -m "feat(bytesx): ReadUint31/WriteUint31 with R-bit mask"
```

---

### Task 7: `internal/bytesx/padding.go` — StripPadding

Per RFC 7540 §6.1 / §6.2 / §6.6: a padded frame's first byte is `Pad Length`. The next `length-1-padLen` bytes are the actual payload; the trailing `padLen` bytes are padding.

**Files:**
- Create: `internal/bytesx/padding.go`
- Create: `internal/bytesx/padding_test.go`

- [ ] **Step 1: Write failing test**

Create `internal/bytesx/padding_test.go`:

```go
package bytesx

import (
	"bytes"
	"errors"
	"testing"
)

func TestStripPadding(t *testing.T) {
	cases := []struct {
		name        string
		raw         []byte
		wantPayload []byte
		wantPadLen  uint8
		wantErr     error
	}{
		{
			name:        "no_padding_byte_present",
			raw:         []byte{0x00, 0xaa, 0xbb}, // padLen=0, payload=aa,bb, no trailing pad
			wantPayload: []byte{0xaa, 0xbb},
			wantPadLen:  0,
		},
		{
			name:        "padding_3_bytes",
			raw:         []byte{0x03, 0xaa, 0xbb, 0x00, 0x00, 0x00},
			wantPayload: []byte{0xaa, 0xbb},
			wantPadLen:  3,
		},
		{
			name:        "all_padding_no_payload",
			raw:         []byte{0x02, 0x00, 0x00},
			wantPayload: []byte{},
			wantPadLen:  2,
		},
		{
			name:    "padlen_exceeds_payload",
			raw:     []byte{0x05, 0xaa},
			wantErr: ErrInvalidPadding,
		},
		{
			name:    "empty",
			raw:     []byte{},
			wantErr: ErrInvalidPadding,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, padLen, err := StripPadding(tc.raw)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !bytes.Equal(payload, tc.wantPayload) {
				t.Fatalf("payload = %x, want %x", payload, tc.wantPayload)
			}
			if padLen != tc.wantPadLen {
				t.Fatalf("padLen = %d, want %d", padLen, tc.wantPadLen)
			}
		})
	}
}
```

- [ ] **Step 2: Run — fails**

Run: `go test ./internal/bytesx/...`
Expected: undefined `StripPadding`, `ErrInvalidPadding`.

- [ ] **Step 3: Implement**

Create `internal/bytesx/padding.go`:

```go
package bytesx

import "errors"

// ErrInvalidPadding is returned when the declared pad length exceeds the
// remaining payload (RFC 7540 §6.1: PROTOCOL_ERROR).
var ErrInvalidPadding = errors.New("poseidon/bytesx: pad length exceeds payload")

// StripPadding parses a padded frame payload (DATA, HEADERS, PUSH_PROMISE).
// raw[0] is the pad length; raw[1:1+actualLen] is the real payload;
// raw[1+actualLen:] is padding (must be present and equal in length).
// Returned payload aliases raw — caller must respect the visitor lifetime
// contract.
func StripPadding(raw []byte) (payload []byte, padLen uint8, err error) {
	if len(raw) < 1 {
		return nil, 0, ErrInvalidPadding
	}
	padLen = raw[0]
	if int(padLen) > len(raw)-1 {
		return nil, 0, ErrInvalidPadding
	}
	return raw[1 : len(raw)-int(padLen)], padLen, nil
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./internal/bytesx/...`

- [ ] **Step 5: Commit**

```bash
git add internal/bytesx/padding.go internal/bytesx/padding_test.go
git commit -m "feat(bytesx): StripPadding for padded frame payload"
```

---

### Task 8: `internal/bytesx/pool.go` — typed sync.Pool helpers

**Files:**
- Create: `internal/bytesx/pool.go`
- Create: `internal/bytesx/pool_test.go`

- [ ] **Step 1: Write failing test**

Create `internal/bytesx/pool_test.go`:

```go
package bytesx

import (
	"testing"
)

func TestReadBufPool_RoundTrip(t *testing.T) {
	p := GetReadBuf(4096)
	if cap(*p) < 4096 {
		t.Fatalf("cap = %d, want >= 4096", cap(*p))
	}
	*p = (*p)[:4096]
	for i := range *p {
		(*p)[i] = byte(i)
	}
	PutReadBuf(p)

	// Re-acquire — pool may or may not return same buffer; both are valid.
	p2 := GetReadBuf(4096)
	if cap(*p2) < 4096 {
		t.Fatalf("cap after reuse = %d, want >= 4096", cap(*p2))
	}
	PutReadBuf(p2)
}

func TestReadBufPool_GrowsWhenSmaller(t *testing.T) {
	// Initial pool default is 16 KiB; ask for 64 KiB.
	p := GetReadBuf(64 << 10)
	if cap(*p) < 64<<10 {
		t.Fatalf("cap = %d, want >= %d", cap(*p), 64<<10)
	}
	PutReadBuf(p)
}

func BenchmarkReadBufPool_GetPut(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p := GetReadBuf(4096)
		PutReadBuf(p)
	}
}
```

- [ ] **Step 2: Run — fails**

Run: `go test ./internal/bytesx/...`
Expected: undefined `GetReadBuf`, `PutReadBuf`.

- [ ] **Step 3: Implement**

Create `internal/bytesx/pool.go`:

```go
package bytesx

import "sync"

const defaultReadBufSize = 16 << 10 // 16 KiB

var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, defaultReadBufSize)
		return &b
	},
}

// GetReadBuf returns a pooled byte slice with cap >= min. The returned slice
// has length 0; caller is responsible for re-slicing as needed.
func GetReadBuf(min int) *[]byte {
	p := readBufPool.Get().(*[]byte)
	if cap(*p) < min {
		// Drop and allocate a bigger one. This is amortised — peak grow only.
		newBuf := make([]byte, 0, min)
		p = &newBuf
		return p
	}
	*p = (*p)[:0]
	return p
}

// PutReadBuf returns the slice to the pool. Caller must not retain references.
func PutReadBuf(p *[]byte) {
	if p == nil {
		return
	}
	readBufPool.Put(p)
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./internal/bytesx/...`

- [ ] **Step 5: Verify zero-alloc on the round trip**

Run: `go test -bench=BenchmarkReadBufPool -benchmem -run=^$ ./internal/bytesx/`
Expected: `0 allocs/op` (steady state — `New` callback only fires on first call).

- [ ] **Step 6: Commit**

```bash
git add internal/bytesx/pool.go internal/bytesx/pool_test.go
git commit -m "feat(bytesx): typed read-buffer sync.Pool"
```

---

## hpack — HPACK codec (RFC 7541)

### Task 9: `hpack/errors.go` and package doc

**Files:**
- Create: `hpack/doc.go`
- Create: `hpack/errors.go`

- [ ] **Step 1: Create `hpack/doc.go`**

```go
// Package hpack implements HPACK (RFC 7541) header compression for HTTP/2.
//
// The package is built from scratch (no dependencies on net/http or
// golang.org/x/net/http2) for tight zero-allocation control. Encoder and
// Decoder hold pooled state and are NOT safe for concurrent use; callers
// instantiate one of each per HTTP/2 connection.
//
// Decoded HeaderField values reference internal buffers that are only valid
// for the duration of the FieldVisitor invocation — copy if you must
// retain.
package hpack
```

- [ ] **Step 2: Create `hpack/errors.go`**

```go
package hpack

import "errors"

// Sentinel errors. Hot-path code MUST NOT use fmt.Errorf — only these.
var (
	// ErrTruncated is returned when input ends mid-field (RFC 7541 §5).
	ErrTruncated = errors.New("poseidon/hpack: truncated input")
	// ErrIntegerOverflow is returned when an N-bit prefix integer exceeds 2^32-1.
	ErrIntegerOverflow = errors.New("poseidon/hpack: integer overflow")
	// ErrInvalidIndex is returned when an index references neither static nor dynamic table.
	ErrInvalidIndex = errors.New("poseidon/hpack: invalid table index")
	// ErrInvalidHuffman is returned when Huffman-coded input is malformed.
	ErrInvalidHuffman = errors.New("poseidon/hpack: invalid Huffman code")
	// ErrTableSizeUpdate is returned when a "Dynamic Table Size Update" exceeds the SETTINGS limit.
	ErrTableSizeUpdate = errors.New("poseidon/hpack: dynamic table size update exceeds limit")
	// ErrHeaderListTooLarge is returned when an incoming header list exceeds SETTINGS_MAX_HEADER_LIST_SIZE.
	ErrHeaderListTooLarge = errors.New("poseidon/hpack: header list exceeds max size")
	// ErrInvalidPrefix is returned when a representation prefix byte is malformed.
	ErrInvalidPrefix = errors.New("poseidon/hpack: invalid representation prefix")
)
```

- [ ] **Step 3: Verify build**

Run: `go build ./hpack/`
Expected: OK (no symbols used yet, but compiles).

- [ ] **Step 4: Commit**

```bash
git add hpack/doc.go hpack/errors.go
git commit -m "feat(hpack): package doc and sentinel errors"
```

---

### Task 10: `hpack/integer.go` — N-bit prefix integer codec (RFC 7541 §5.1)

**Files:**
- Create: `hpack/integer.go`
- Create: `hpack/integer_test.go`

The encoded form of `I` with prefix `N`: if `I < 2^N - 1`, write `I` in the N-bit prefix; else write all 1s in the prefix and continue in 7-bit groups with continuation bit (RFC 7541 §5.1).

- [ ] **Step 1: Write failing test**

Create `hpack/integer_test.go`:

```go
package hpack

import (
	"bytes"
	"errors"
	"testing"
)

// RFC 7541 §5.1 examples.
func TestEncodeInteger_RFCExamples(t *testing.T) {
	cases := []struct {
		name       string
		i          uint64
		n          uint8
		prefixByte byte
		want       []byte
	}{
		// §C.1.1: encode 10 in 5-bit prefix, prefixByte = 0
		{"c_1_1__10__5bit", 10, 5, 0x00, []byte{0x0a}},
		// §C.1.2: encode 1337 in 5-bit prefix, prefixByte = 0
		{"c_1_2__1337__5bit", 1337, 5, 0x00, []byte{0x1f, 0x9a, 0x0a}},
		// §C.1.3: encode 42 in 8-bit prefix
		{"c_1_3__42__8bit", 42, 8, 0x00, []byte{0x2a}},
		// 2^N - 1 = 31 with N=5 → triggers continuation
		{"boundary_2N_minus_1", 31, 5, 0x00, []byte{0x1f, 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EncodeInteger(nil, tc.n, tc.prefixByte, tc.i)
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("EncodeInteger(%d, %d, %#x) = %x, want %x", tc.i, tc.n, tc.prefixByte, got, tc.want)
			}
		})
	}
}

func TestDecodeInteger_RFCExamples(t *testing.T) {
	cases := []struct {
		name        string
		src         []byte
		n           uint8
		wantVal     uint64
		wantConsumed int
	}{
		{"c_1_1__10__5bit", []byte{0x0a}, 5, 10, 1},
		{"c_1_2__1337__5bit", []byte{0x1f, 0x9a, 0x0a}, 5, 1337, 3},
		{"c_1_3__42__8bit", []byte{0x2a}, 8, 42, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, n, err := DecodeInteger(tc.src, tc.n)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if got != tc.wantVal || n != tc.wantConsumed {
				t.Fatalf("DecodeInteger = (%d, %d), want (%d, %d)", got, n, tc.wantVal, tc.wantConsumed)
			}
		})
	}
}

func TestDecodeInteger_Truncated(t *testing.T) {
	cases := [][]byte{
		{},
		{0x1f},                   // continuation expected, none follows
		{0x1f, 0x80},             // continuation byte set but stream ends
		{0x1f, 0xff},             // continuation set, stream ends
		{0x1f, 0xff, 0xff},       // continuation chain ends mid-stream
	}
	for _, src := range cases {
		_, _, err := DecodeInteger(src, 5)
		if !errors.Is(err, ErrTruncated) {
			t.Fatalf("DecodeInteger(%x) err = %v, want ErrTruncated", src, err)
		}
	}
}

func TestDecodeInteger_Overflow(t *testing.T) {
	// Continuation chain that exceeds 2^32 - 1.
	src := []byte{0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00}
	_, _, err := DecodeInteger(src, 5)
	if !errors.Is(err, ErrIntegerOverflow) {
		t.Fatalf("err = %v, want ErrIntegerOverflow", err)
	}
}

func TestEncodeDecodeInteger_RoundTrip(t *testing.T) {
	for _, n := range []uint8{1, 4, 5, 6, 7, 8} {
		for _, v := range []uint64{0, 1, 2, 30, 31, 100, 1000, 1<<20, 1<<31, 1<<32 - 1} {
			enc := EncodeInteger(nil, n, 0, v)
			dec, _, err := DecodeInteger(enc, n)
			if err != nil {
				t.Fatalf("v=%d n=%d: err=%v", v, n, err)
			}
			if dec != v {
				t.Fatalf("v=%d n=%d: dec=%d", v, n, dec)
			}
		}
	}
}

func BenchmarkDecodeInteger_Max(b *testing.B) {
	src := []byte{0x1f, 0xff, 0xff, 0xff, 0xff, 0x0f}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeInteger(src, 5)
	}
}
```

- [ ] **Step 2: Run — fails**

Run: `go test ./hpack/`
Expected: `EncodeInteger`/`DecodeInteger` undefined.

- [ ] **Step 3: Implement**

Create `hpack/integer.go`:

```go
package hpack

// EncodeInteger writes I as an N-bit prefix integer (RFC 7541 §5.1) and
// returns dst with the bytes appended. prefixByte's high (8-N) bits supply
// the representation prefix; its low N bits MUST be zero on entry.
func EncodeInteger(dst []byte, n uint8, prefixByte byte, i uint64) []byte {
	max := uint64(1)<<n - 1
	if i < max {
		return append(dst, prefixByte|byte(i))
	}
	dst = append(dst, prefixByte|byte(max))
	i -= max
	for i >= 128 {
		dst = append(dst, byte(i&0x7f|0x80))
		i >>= 7
	}
	return append(dst, byte(i))
}

// DecodeInteger reads an N-bit prefix integer from src starting at src[0].
// Returns the value, bytes consumed, and an error if truncated or overflowing.
// Caller MUST mask the prefix bits before calling: src[0] is interpreted as
// the encoded byte (the implementation only reads the low N bits of src[0]).
func DecodeInteger(src []byte, n uint8) (uint64, int, error) {
	if len(src) == 0 {
		return 0, 0, ErrTruncated
	}
	mask := byte(1)<<n - 1
	v := uint64(src[0] & mask)
	if v < uint64(mask) {
		return v, 1, nil
	}
	consumed := 1
	var m uint
	for {
		if consumed >= len(src) {
			return 0, 0, ErrTruncated
		}
		b := src[consumed]
		consumed++
		// Guard against shifts that would overflow uint64; HPACK integers cap at 2^32-1.
		if m >= 32 {
			return 0, 0, ErrIntegerOverflow
		}
		v += uint64(b&0x7f) << m
		if v > 0xffff_ffff {
			return 0, 0, ErrIntegerOverflow
		}
		if b&0x80 == 0 {
			return v, consumed, nil
		}
		m += 7
	}
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./hpack/ -run=Integer`

- [ ] **Step 5: Verify zero-alloc on hot path**

Run: `go test -bench=DecodeInteger -benchmem -run=^$ ./hpack/`
Expected: `0 allocs/op` (DecodeInteger is pure stack work).

- [ ] **Step 6: Commit**

```bash
git add hpack/integer.go hpack/integer_test.go
git commit -m "feat(hpack): N-bit prefix integer codec (RFC 7541 §5.1)"
```

---

### Task 11: `hpack/huffman_table.go` — Huffman tables (RFC 7541 App. B)

The 256-entry encode table and the 4-bit FSM decode table are mechanical translations of RFC 7541 App. B. They are large; treat them as data, not logic.

**Files:**
- Create: `hpack/huffman_table.go`

- [ ] **Step 1: Generate the encode table from the RFC**

Open RFC 7541 Appendix B (the canonical table). For every byte value `0..255` plus the EOS symbol (256), record `(code, nbits)`. Encode them as a `[257]huffmanCode` slice indexed by symbol value. Example for the first three entries (sym 0, 1, 2):

```
sym  0:  |11111111|11000                  1ff8     [13]
sym  1:  |11111111|11111111|1011000       7fffd8   [23]
sym  2:  |11111111|11111111|11111110|0010 fffffe2  [28]
```

Translation to Go:

```go
{0x1ff8, 13},
{0x7fffd8, 23},
{0xfffffe2, 28},
```

The full table has 257 entries. **Generate it via a one-shot script** rather than hand-typing:

Create `scripts/gen_huffman_table.go` (a one-off generator, kept in repo for reproducibility but not part of the build):

```go
//go:build ignore

// Reads RFC 7541 Appendix B from stdin, emits hpack/huffman_table.go.
// Usage:
//   go run ./scripts/gen_huffman_table.go < rfc-app-b.txt > hpack/huffman_table.go
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	codes := make([][2]uint64, 257)
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		// Lines look like:  "    (  0)  |11111111|11000                  1ff8     [13]"
		if !strings.HasPrefix(line, "(") {
			continue
		}
		// Parse the symbol number, hex code, and bit-length in [..].
		// (Keep this lenient: split by whitespace, find the patterns.)
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		// First field starts with '(', e.g. "(  0)" or "(255)". Strip parens.
		symStr := strings.Trim(fields[0]+fields[1], "()")
		sym, err := strconv.Atoi(strings.TrimSpace(symStr))
		if err != nil {
			continue
		}
		// Last field: "[13]"
		last := fields[len(fields)-1]
		nbitsStr := strings.Trim(last, "[]")
		nbits, err := strconv.Atoi(nbitsStr)
		if err != nil {
			continue
		}
		// Second-to-last field: hex code "1ff8"
		hexStr := fields[len(fields)-2]
		code, err := strconv.ParseUint(hexStr, 16, 64)
		if err != nil {
			continue
		}
		codes[sym] = [2]uint64{code, uint64(nbits)}
	}

	fmt.Println("// Code generated from RFC 7541 Appendix B; DO NOT EDIT.")
	fmt.Println("package hpack")
	fmt.Println()
	fmt.Println("type huffmanCode struct {")
	fmt.Println("\tcode  uint32")
	fmt.Println("\tnbits uint8")
	fmt.Println("}")
	fmt.Println()
	fmt.Println("// huffmanCodes holds 256 byte symbols and the EOS at index 256.")
	fmt.Println("var huffmanCodes = [257]huffmanCode{")
	for i, c := range codes {
		fmt.Printf("\t{0x%x, %d}, // sym %d\n", c[0], c[1], i)
	}
	fmt.Println("}")
}
```

- [ ] **Step 2: Run the generator**

You'll need an RFC App. B text file. Save the table portion of RFC 7541 (the lines starting with `(  0)` through `(256)`) into a local file `rfc-app-b.txt`. Then:

```bash
go run ./scripts/gen_huffman_table.go < rfc-app-b.txt > hpack/huffman_table.go
gofmt -w hpack/huffman_table.go
```

Verify the file has 257 entries and starts with the `// Code generated` header.

- [ ] **Step 3: Add the FSM decode table**

The FSM table maps `(state, nibble) → (newState, emitted byte, flags)`. The canonical 4-bit nibble FSM has 256 states × 16 nibbles = 4096 transitions. Generating it requires walking the encode table and computing transitions.

Append a generation step to `scripts/gen_huffman_table.go` after the encode table:

```go
// (continue main(), after writing huffmanCodes)

// Build decoder FSM (Wickelmaier / nghttp2 4-bit table).
type transition struct {
	next  uint16
	emit  uint8
	flags uint8 // bit0=accept, bit1=symbol, bit2=fail
}
const (
	flagAccept = 1 << 0
	flagSymbol = 1 << 1
	flagFail   = 1 << 2
)
// ... derive transitions by walking the encode table; produce 256x16
// uint16 packed entries [next:9 | flags:3 | emit:8 (only meaningful when flagSymbol)].
// (Implementation: for each (state, nibble) traverse 4 bits, find symbol if accepting.)
```

The full derivation is mechanical but verbose; reference implementations to consult: `nghttp2/lib/nghttp2_hd_huffman.c` (BSD-ish, written in C — re-implement, not copy) and `golang.org/x/net/http2/hpack/huffman.go` (we can't import or copy, but reading the algorithm is fine).

If the derivation is intimidating, **alternative**: ship a pure bit-by-bit decoder in Task 13, mark the FSM as a follow-up optimisation. The bench gate will still likely pass for typical loads — the FSM is only needed for the absolute lowest ns/op.

For this plan, **proceed with the bit-by-bit decoder** and revisit the FSM once benches run. Append a note to the file:

```go
// FSM decode table left for a follow-up optimisation; current decoder uses
// the bit-by-bit walk over huffmanCodes.
```

- [ ] **Step 4: Verify build**

Run: `go build ./hpack/`
Expected: OK (file compiles even though `huffmanCodes` is unused at this point).

- [ ] **Step 5: Commit**

```bash
git add scripts/gen_huffman_table.go hpack/huffman_table.go
git commit -m "feat(hpack): generated Huffman encode table (RFC 7541 App. B)"
```

---

### Task 12: `hpack/huffman.go` — encode

**Files:**
- Create: `hpack/huffman.go`
- Create: `hpack/huffman_test.go`

- [ ] **Step 1: Write failing test**

Create `hpack/huffman_test.go`:

```go
package hpack

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// RFC 7541 §C.4.1: "www.example.com" Huffman-encoded.
func TestHuffmanEncode_C_4_1(t *testing.T) {
	src := []byte("www.example.com")
	wantHex := "f1e3c2e5f23a6ba0ab90f4ff"
	want, _ := hex.DecodeString(wantHex)
	dst := HuffmanEncode(nil, src)
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
	if got := HuffmanEncodedLen(src); got != len(want) {
		t.Fatalf("HuffmanEncodedLen = %d, want %d", got, len(want))
	}
}

// RFC 7541 §C.4.2: "no-cache" Huffman-encoded.
func TestHuffmanEncode_C_4_2(t *testing.T) {
	src := []byte("no-cache")
	wantHex := "a8eb10649cbf"
	want, _ := hex.DecodeString(wantHex)
	dst := HuffmanEncode(nil, src)
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
}

// RFC 7541 §C.4.3: "custom-key" / "custom-value" — covers a longer string.
func TestHuffmanEncode_C_4_3_Names(t *testing.T) {
	src := []byte("custom-key")
	wantHex := "25a849e95ba97d7f"
	want, _ := hex.DecodeString(wantHex)
	dst := HuffmanEncode(nil, src)
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
}

func TestHuffmanEncodedLen_Empty(t *testing.T) {
	if HuffmanEncodedLen(nil) != 0 || HuffmanEncodedLen([]byte{}) != 0 {
		t.Fatalf("empty len != 0")
	}
}

func BenchmarkHuffmanEncode_path(b *testing.B) {
	src := []byte("/index.html")
	dst := make([]byte, 0, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = HuffmanEncode(dst[:0], src)
	}
	_ = dst
}
```

- [ ] **Step 2: Run — fails**

Run: `go test ./hpack/ -run=Huffman`
Expected: undefined `HuffmanEncode` / `HuffmanEncodedLen`.

- [ ] **Step 3: Implement**

Create `hpack/huffman.go`:

```go
package hpack

// HuffmanEncodedLen returns the byte length of HuffmanEncode(_, src).
func HuffmanEncodedLen(src []byte) int {
	var bits uint64
	for _, b := range src {
		bits += uint64(huffmanCodes[b].nbits)
	}
	// Round up to byte boundary.
	return int((bits + 7) / 8)
}

// HuffmanEncode appends the Huffman-coded form of src to dst (padded to a
// byte boundary with the EOS prefix per RFC 7541 §5.2) and returns the
// extended dst. dst MUST have enough capacity, otherwise it grows once.
func HuffmanEncode(dst, src []byte) []byte {
	var (
		buf  uint64 // bit accumulator
		nbuf uint8  // bits currently in buf
	)
	for _, b := range src {
		c := huffmanCodes[b]
		buf = (buf << c.nbits) | uint64(c.code)
		nbuf += c.nbits
		for nbuf >= 8 {
			nbuf -= 8
			dst = append(dst, byte(buf>>nbuf))
		}
	}
	if nbuf > 0 {
		// Pad with the most-significant bits of EOS (all 1s). RFC §5.2.
		buf = (buf << (8 - nbuf)) | (1<<(8-nbuf) - 1)
		dst = append(dst, byte(buf))
	}
	return dst
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./hpack/ -run=Huffman`

- [ ] **Step 5: Verify zero-alloc when dst has capacity**

Run: `go test -bench=BenchmarkHuffmanEncode_path -benchmem -run=^$ ./hpack/`
Expected: `0 allocs/op`.

- [ ] **Step 6: Commit**

```bash
git add hpack/huffman.go hpack/huffman_test.go
git commit -m "feat(hpack): Huffman encode (RFC 7541 §5.2 + App. B)"
```

---

### Task 13: `hpack/huffman.go` — decode

**Files:**
- Modify: `hpack/huffman.go`
- Modify: `hpack/huffman_test.go`

- [ ] **Step 1: Add failing tests**

Append to `hpack/huffman_test.go`:

```go
func TestHuffmanDecode_C_4_1(t *testing.T) {
	encHex := "f1e3c2e5f23a6ba0ab90f4ff"
	want := []byte("www.example.com")
	enc, _ := hex.DecodeString(encHex)
	dst, err := HuffmanDecode(make([]byte, 0, 32), enc)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %q, want %q", dst, want)
	}
}

func TestHuffmanDecode_C_4_2(t *testing.T) {
	enc, _ := hex.DecodeString("a8eb10649cbf")
	want := []byte("no-cache")
	dst, err := HuffmanDecode(nil, enc)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %q, want %q", dst, want)
	}
}

func TestHuffmanDecode_RoundTrip(t *testing.T) {
	for _, s := range []string{"", "a", "abc", "hello, world!", "/index.html?x=1&y=2"} {
		enc := HuffmanEncode(nil, []byte(s))
		dec, err := HuffmanDecode(nil, enc)
		if err != nil {
			t.Fatalf("s=%q err=%v", s, err)
		}
		if string(dec) != s {
			t.Fatalf("roundtrip s=%q got %q", s, dec)
		}
	}
}

func TestHuffmanDecode_TooLongPadding(t *testing.T) {
	// Padding longer than 7 bits is a decode error (RFC 7541 §5.2).
	bad := []byte{0xff, 0xff, 0xff} // all 1s — too much padding to be valid
	_, err := HuffmanDecode(nil, bad)
	if err == nil {
		t.Fatalf("want error, got nil")
	}
}

func BenchmarkHuffmanDecode_path(b *testing.B) {
	enc, _ := hex.DecodeString("60d5e8b1d754df")
	dst := make([]byte, 0, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, _ = HuffmanDecode(dst[:0], enc)
	}
	_ = dst
}
```

- [ ] **Step 2: Run — fails**

Run: `go test ./hpack/ -run=HuffmanDecode`

- [ ] **Step 3: Implement**

Append to `hpack/huffman.go`:

```go
// HuffmanDecode appends the decoded form of src to dst and returns the
// extended dst. Implements RFC 7541 §5.2 padding rules:
//   - Padding strictly longer than 7 bits is a decoding error.
//   - Non-EOS padding (any 0 bits in trailing partial byte) is a decoding error.
//   - The EOS symbol (256) MUST NOT appear in encoded data.
//
// Current implementation is a straightforward bit-by-bit walk over the
// encode table; profiles will tell us whether to upgrade to a 4-bit FSM.
func HuffmanDecode(dst, src []byte) ([]byte, error) {
	var (
		buf  uint32 // bit accumulator (MSB-first, up to 24 valid bits)
		nbuf uint8  // bits in buf
	)
	const eosBits = 30 // EOS code length

	for _, b := range src {
		buf = (buf << 8) | uint32(b)
		nbuf += 8
		for {
			match, msym, mn := huffmanLookup(buf, nbuf)
			if !match {
				break
			}
			if msym == 256 {
				return nil, ErrInvalidHuffman // EOS in stream
			}
			dst = append(dst, byte(msym))
			nbuf -= mn
			buf &= (1 << nbuf) - 1
		}
	}

	// Validate trailing padding (RFC §5.2): must be the prefix of EOS, ≤ 7 bits.
	if nbuf > 0 {
		if nbuf > 7 {
			return nil, ErrInvalidHuffman
		}
		expected := uint32(1<<nbuf - 1) // EOS prefix is all 1s
		if buf != expected {
			return nil, ErrInvalidHuffman
		}
	}
	_ = eosBits
	return dst, nil
}

// huffmanLookup tries to decode one symbol from buf's MSB-aligned bits.
// Returns (matched, symbol, bits-consumed). Linear scan over codes; for
// production we may upgrade to a 4-bit FSM later.
func huffmanLookup(buf uint32, nbuf uint8) (bool, uint16, uint8) {
	for sym, c := range huffmanCodes {
		if c.nbits > nbuf {
			continue
		}
		shifted := buf >> (nbuf - c.nbits)
		if shifted == c.code {
			return true, uint16(sym), c.nbits
		}
	}
	return false, 0, 0
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./hpack/ -run=HuffmanDecode`

- [ ] **Step 5: Verify zero-alloc**

Run: `go test -bench=BenchmarkHuffmanDecode_path -benchmem -run=^$ ./hpack/`
Expected: `0 allocs/op` when `dst` has capacity.

If `ns/op` exceeds the spec target (≤80 ns) by more than 2x and the bench is not noise-bound, file a follow-up note in the plan to add the 4-bit FSM. Do not block this task on it; the bench gate enforces relative regression vs main, not absolute.

- [ ] **Step 6: Commit**

```bash
git add hpack/huffman.go hpack/huffman_test.go
git commit -m "feat(hpack): Huffman decode w/ RFC §5.2 padding rules"
```

---

### Task 14: `hpack/static_table.go` — static table + lookup

**Files:**
- Create: `hpack/static_table.go`
- Create: `hpack/static_table_test.go`

- [ ] **Step 1: Write failing test**

Create `hpack/static_table_test.go`:

```go
package hpack

import (
	"bytes"
	"testing"
)

// Sample static table entries from RFC 7541 App. A.
func TestStaticTable_KnownEntries(t *testing.T) {
	cases := []struct {
		idx       int
		wantName  string
		wantValue string
	}{
		{1, ":authority", ""},
		{2, ":method", "GET"},
		{3, ":method", "POST"},
		{4, ":path", "/"},
		{5, ":path", "/index.html"},
		{8, ":status", "200"},
		{16, "accept-encoding", "gzip, deflate"},
		{61, "www-authenticate", ""},
	}
	for _, tc := range cases {
		e := staticTable[tc.idx]
		if string(e.name) != tc.wantName {
			t.Fatalf("idx %d: name = %q, want %q", tc.idx, e.name, tc.wantName)
		}
		if string(e.value) != tc.wantValue {
			t.Fatalf("idx %d: value = %q, want %q", tc.idx, e.value, tc.wantValue)
		}
	}
}

func TestStaticIndex_FullMatch(t *testing.T) {
	idx, full := staticIndex([]byte(":method"), []byte("GET"))
	if !full || idx != 2 {
		t.Fatalf("(:method,GET) = (%d, %v), want (2, true)", idx, full)
	}
	idx, full = staticIndex([]byte(":path"), []byte("/index.html"))
	if !full || idx != 5 {
		t.Fatalf("(:path,/index.html) = (%d, %v), want (5, true)", idx, full)
	}
}

func TestStaticIndex_NameOnlyMatch(t *testing.T) {
	idx, full := staticIndex([]byte(":path"), []byte("/foo"))
	if full || idx != 4 { // first ":path" entry
		t.Fatalf("(:path,/foo) = (%d, %v), want (4, false)", idx, full)
	}
	idx, full = staticIndex([]byte("user-agent"), []byte("anything"))
	if full {
		t.Fatalf("user-agent should not be a full match")
	}
	if idx == 0 {
		t.Fatalf("user-agent should match name-only")
	}
}

func TestStaticIndex_NoMatch(t *testing.T) {
	idx, full := staticIndex([]byte("x-custom"), []byte("v"))
	if idx != 0 || full {
		t.Fatalf("unknown name returned (%d, %v)", idx, full)
	}
}

// Sanity: bytes.Equal-based lookup must not allocate.
func BenchmarkStaticIndex_Hit(b *testing.B) {
	name := []byte(":method")
	value := []byte("GET")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = staticIndex(name, value)
	}
}

var _ = bytes.Equal // keep import honest
```

- [ ] **Step 2: Run — fails**

Run: `go test ./hpack/ -run=Static`

- [ ] **Step 3: Implement**

Create `hpack/static_table.go`:

```go
package hpack

// staticEntry holds one row of the HPACK static table (RFC 7541 App. A).
// Names and values are []byte to avoid string conversions on the hot path.
type staticEntry struct {
	name, value []byte
}

// staticTable is indexed 1..61; index 0 is unused (HPACK uses 1-based indices).
var staticTable = [62]staticEntry{
	0:  {nil, nil}, // unused
	1:  {[]byte(":authority"), nil},
	2:  {[]byte(":method"), []byte("GET")},
	3:  {[]byte(":method"), []byte("POST")},
	4:  {[]byte(":path"), []byte("/")},
	5:  {[]byte(":path"), []byte("/index.html")},
	6:  {[]byte(":scheme"), []byte("http")},
	7:  {[]byte(":scheme"), []byte("https")},
	8:  {[]byte(":status"), []byte("200")},
	9:  {[]byte(":status"), []byte("204")},
	10: {[]byte(":status"), []byte("206")},
	11: {[]byte(":status"), []byte("304")},
	12: {[]byte(":status"), []byte("400")},
	13: {[]byte(":status"), []byte("404")},
	14: {[]byte(":status"), []byte("500")},
	15: {[]byte("accept-charset"), nil},
	16: {[]byte("accept-encoding"), []byte("gzip, deflate")},
	17: {[]byte("accept-language"), nil},
	18: {[]byte("accept-ranges"), nil},
	19: {[]byte("accept"), nil},
	20: {[]byte("access-control-allow-origin"), nil},
	21: {[]byte("age"), nil},
	22: {[]byte("allow"), nil},
	23: {[]byte("authorization"), nil},
	24: {[]byte("cache-control"), nil},
	25: {[]byte("content-disposition"), nil},
	26: {[]byte("content-encoding"), nil},
	27: {[]byte("content-language"), nil},
	28: {[]byte("content-length"), nil},
	29: {[]byte("content-location"), nil},
	30: {[]byte("content-range"), nil},
	31: {[]byte("content-type"), nil},
	32: {[]byte("cookie"), nil},
	33: {[]byte("date"), nil},
	34: {[]byte("etag"), nil},
	35: {[]byte("expect"), nil},
	36: {[]byte("expires"), nil},
	37: {[]byte("from"), nil},
	38: {[]byte("host"), nil},
	39: {[]byte("if-match"), nil},
	40: {[]byte("if-modified-since"), nil},
	41: {[]byte("if-none-match"), nil},
	42: {[]byte("if-range"), nil},
	43: {[]byte("if-unmodified-since"), nil},
	44: {[]byte("last-modified"), nil},
	45: {[]byte("link"), nil},
	46: {[]byte("location"), nil},
	47: {[]byte("max-forwards"), nil},
	48: {[]byte("proxy-authenticate"), nil},
	49: {[]byte("proxy-authorization"), nil},
	50: {[]byte("range"), nil},
	51: {[]byte("referer"), nil},
	52: {[]byte("refresh"), nil},
	53: {[]byte("retry-after"), nil},
	54: {[]byte("server"), nil},
	55: {[]byte("set-cookie"), nil},
	56: {[]byte("strict-transport-security"), nil},
	57: {[]byte("transfer-encoding"), nil},
	58: {[]byte("user-agent"), nil},
	59: {[]byte("vary"), nil},
	60: {[]byte("via"), nil},
	61: {[]byte("www-authenticate"), nil},
}

// staticTableLen is the number of valid entries (1..staticTableLen inclusive).
const staticTableLen = 61

// staticIndex performs a linear scan over the 61-entry static table.
// Returns (idx, fullMatch) where idx == 0 means no name match;
// fullMatch == true means name AND value match.
//
// For a name-only match, returns the FIRST entry whose name matches (lowest
// index), per HPACK encoder convention.
func staticIndex(name, value []byte) (uint64, bool) {
	var nameOnly uint64
	for i := 1; i <= staticTableLen; i++ {
		e := staticTable[i]
		if !bytesEqual(e.name, name) {
			continue
		}
		if bytesEqual(e.value, value) {
			return uint64(i), true
		}
		if nameOnly == 0 {
			nameOnly = uint64(i)
		}
	}
	return nameOnly, false
}

// bytesEqual is a local copy of bytes.Equal to avoid the import (and to keep
// hpack's transitive surface minimal). Equivalent semantics.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./hpack/ -run=Static`

- [ ] **Step 5: Verify zero-alloc**

Run: `go test -bench=BenchmarkStaticIndex -benchmem -run=^$ ./hpack/`
Expected: `0 allocs/op`.

- [ ] **Step 6: Commit**

```bash
git add hpack/static_table.go hpack/static_table_test.go
git commit -m "feat(hpack): static table (RFC 7541 App. A) + lookup"
```

---

### Task 15: `hpack/dynamic_table.go` — ring buffer with arena

**Files:**
- Create: `hpack/dynamic_table.go`
- Create: `hpack/dynamic_table_test.go`

- [ ] **Step 1: Write failing test**

Create `hpack/dynamic_table_test.go`:

```go
package hpack

import (
	"bytes"
	"testing"
)

func TestDynamicTable_AddAndAt(t *testing.T) {
	dt := newDynamicTable(4096)
	dt.add([]byte("custom-key"), []byte("custom-header"))
	if dt.len() != 1 {
		t.Fatalf("len = %d, want 1", dt.len())
	}
	name, value := dt.at(1)
	if !bytes.Equal(name, []byte("custom-key")) || !bytes.Equal(value, []byte("custom-header")) {
		t.Fatalf("at(1) = (%q, %q), want (custom-key, custom-header)", name, value)
	}
	if dt.byteSize() != uint32(10+13+32) {
		t.Fatalf("byteSize = %d, want %d", dt.byteSize(), 10+13+32)
	}
}

func TestDynamicTable_FIFOAddOrder(t *testing.T) {
	dt := newDynamicTable(4096)
	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))
	dt.add([]byte("c"), []byte("3"))
	// HPACK §2.3.3: index 1 is the most-recently-added.
	got := func(i int) string {
		n, v := dt.at(i)
		return string(n) + "=" + string(v)
	}
	if got(1) != "c=3" || got(2) != "b=2" || got(3) != "a=1" {
		t.Fatalf("ordering wrong: 1=%s, 2=%s, 3=%s", got(1), got(2), got(3))
	}
}

func TestDynamicTable_EvictOnSize(t *testing.T) {
	// Each entry: 1+1+32 = 34 bytes. Capacity 70 holds 2.
	dt := newDynamicTable(70)
	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))
	dt.add([]byte("c"), []byte("3")) // evicts oldest (a=1)
	if dt.len() != 2 {
		t.Fatalf("len = %d, want 2", dt.len())
	}
	n, v := dt.at(2)
	if string(n) != "b" || string(v) != "2" {
		t.Fatalf("oldest should be b=2, got %s=%s", n, v)
	}
}

func TestDynamicTable_AddOversizedClearsAll(t *testing.T) {
	dt := newDynamicTable(50)
	dt.add([]byte("x"), []byte("1"))
	// Try to add an entry larger than capacity (RFC §4.4): clears the table.
	bigVal := make([]byte, 100)
	dt.add([]byte("big"), bigVal)
	if dt.len() != 0 {
		t.Fatalf("len = %d, want 0 (oversized add clears)", dt.len())
	}
}

func TestDynamicTable_SetMaxSizeShrinks(t *testing.T) {
	dt := newDynamicTable(200)
	dt.add([]byte("a"), []byte("1"))
	dt.add([]byte("b"), []byte("2"))
	dt.add([]byte("c"), []byte("3"))
	dt.setMaxSize(35) // holds at most 1 entry of size 34
	if dt.len() != 1 {
		t.Fatalf("len after shrink = %d, want 1", dt.len())
	}
}
```

- [ ] **Step 2: Run — fails**

Run: `go test ./hpack/ -run=DynamicTable`

- [ ] **Step 3: Implement**

Create `hpack/dynamic_table.go`:

```go
package hpack

// dynEntry stores offsets into the arena. Each entry's RFC size is
// nameLen + valueLen + 32 (RFC 7541 §4.1).
type dynEntry struct {
	nameOff, nameLen   uint32
	valueOff, valueLen uint32
}

// dynamicTable holds HPACK dynamic table state for one direction of one
// connection. NOT goroutine-safe.
type dynamicTable struct {
	entries []dynEntry // logical FIFO ring; entries[head..head+count) wraps
	head    int        // index of the oldest entry
	count   int

	arena []byte // packed name+value bytes
	used  uint32 // arena bytes in active use

	maxSize uint32 // current SETTINGS_HEADER_TABLE_SIZE limit
	size    uint32 // sum of entry sizes (RFC §4.1)
}

func newDynamicTable(maxSize uint32) *dynamicTable {
	return &dynamicTable{
		entries: make([]dynEntry, 0, 32),
		arena:   make([]byte, 0, 4096),
		maxSize: maxSize,
	}
}

func (d *dynamicTable) len() int { return d.count }
func (d *dynamicTable) byteSize() uint32 { return d.size }

// at returns name and value for index i, where i=1 is the most recently
// added entry (HPACK §2.3.3). Returned slices alias the arena.
// Caller must NOT modify them.
func (d *dynamicTable) at(i int) (name, value []byte) {
	if i < 1 || i > d.count {
		return nil, nil
	}
	pos := (d.head + d.count - i) % len(d.entries)
	if pos < 0 {
		pos += len(d.entries)
	}
	e := d.entries[pos]
	return d.arena[e.nameOff : e.nameOff+e.nameLen],
		d.arena[e.valueOff : e.valueOff+e.valueLen]
}

func entrySize(name, value []byte) uint32 {
	return uint32(len(name)) + uint32(len(value)) + 32
}

// add inserts (name, value). If the entry exceeds maxSize, the table is
// emptied per RFC §4.4. Otherwise older entries are evicted until size fits.
func (d *dynamicTable) add(name, value []byte) {
	es := entrySize(name, value)
	if es > d.maxSize {
		d.clear()
		return
	}
	for d.size+es > d.maxSize && d.count > 0 {
		d.evictOldest()
	}
	// Append name+value to arena.
	nameOff := uint32(len(d.arena))
	d.arena = append(d.arena, name...)
	valueOff := uint32(len(d.arena))
	d.arena = append(d.arena, value...)
	d.used += uint32(len(name) + len(value))

	if cap(d.entries) > len(d.entries) {
		d.entries = d.entries[:len(d.entries)+1]
	} else {
		d.entries = append(d.entries, dynEntry{})
	}
	// New entry placed at logical "newest" slot.
	pos := (d.head + d.count) % len(d.entries)
	d.entries[pos] = dynEntry{nameOff, uint32(len(name)), valueOff, uint32(len(value))}
	d.count++
	d.size += es

	// Compact the arena occasionally to bound memory.
	if uint32(len(d.arena)) > d.used*2 && d.count > 0 {
		d.compactArena()
	}
}

func (d *dynamicTable) evictOldest() {
	if d.count == 0 {
		return
	}
	e := d.entries[d.head]
	d.size -= uint32(e.nameLen) + uint32(e.valueLen) + 32
	d.used -= uint32(e.nameLen) + uint32(e.valueLen)
	d.head = (d.head + 1) % len(d.entries)
	d.count--
	if d.count == 0 {
		// Reset arena to keep peak memory low when idle.
		d.arena = d.arena[:0]
	}
}

func (d *dynamicTable) clear() {
	d.head = 0
	d.count = 0
	d.entries = d.entries[:0]
	d.arena = d.arena[:0]
	d.used = 0
	d.size = 0
}

func (d *dynamicTable) setMaxSize(n uint32) {
	d.maxSize = n
	for d.size > d.maxSize && d.count > 0 {
		d.evictOldest()
	}
}

// compactArena rewrites d.arena so that all live entries are densely packed
// at the front, then updates entry offsets. Amortised O(n).
func (d *dynamicTable) compactArena() {
	if d.count == 0 {
		d.arena = d.arena[:0]
		d.used = 0
		return
	}
	newArena := make([]byte, 0, d.used*2)
	for i := 1; i <= d.count; i++ {
		// Walk from oldest to newest. Logical pos: (d.head + (i-1)) mod len.
		pos := (d.head + i - 1) % len(d.entries)
		e := d.entries[pos]
		nameOff := uint32(len(newArena))
		newArena = append(newArena, d.arena[e.nameOff:e.nameOff+e.nameLen]...)
		valueOff := uint32(len(newArena))
		newArena = append(newArena, d.arena[e.valueOff:e.valueOff+e.valueLen]...)
		d.entries[pos] = dynEntry{nameOff, e.nameLen, valueOff, e.valueLen}
	}
	d.arena = newArena
	d.used = uint32(len(newArena))
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./hpack/ -run=DynamicTable`

- [ ] **Step 5: Commit**

```bash
git add hpack/dynamic_table.go hpack/dynamic_table_test.go
git commit -m "feat(hpack): dynamic table (ring + arena, eviction, resize)"
```

---

### Task 16: `hpack/string_literal.go` — string literal encode/decode

**Files:**
- Create: `hpack/string_literal.go`
- Create: `hpack/string_literal_test.go`

Per RFC 7541 §5.2: a string literal is `H` flag (1 bit) + `Length` (7-bit prefix integer) + `Length` bytes. `H=1` means Huffman-coded.

- [ ] **Step 1: Write failing test**

Create `hpack/string_literal_test.go`:

```go
package hpack

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeStringLiteral_Plain(t *testing.T) {
	// "/sample/path" with H=0 — RFC §C.2.2 shows this string literal pattern.
	dst := encodeStringLiteral(nil, []byte("/sample/path"), false)
	wantPrefix := byte(0x0c) // length 12, H=0
	if dst[0] != wantPrefix {
		t.Fatalf("prefix = %#x, want %#x", dst[0], wantPrefix)
	}
	if !bytes.Equal(dst[1:], []byte("/sample/path")) {
		t.Fatalf("body = %q, want %q", dst[1:], "/sample/path")
	}
}

func TestEncodeStringLiteral_Huffman(t *testing.T) {
	// "no-cache" Huffman-coded: 6 bytes (RFC §C.4.2).
	dst := encodeStringLiteral(nil, []byte("no-cache"), true)
	if dst[0]&0x80 == 0 {
		t.Fatalf("H bit not set: prefix = %#x", dst[0])
	}
	// Length is 6 (0x06) under the H bit.
	if dst[0] != 0x86 {
		t.Fatalf("prefix = %#x, want 0x86", dst[0])
	}
}

func TestDecodeStringLiteral_Plain(t *testing.T) {
	src := append([]byte{0x0c}, []byte("/sample/path")...)
	dst := make([]byte, 0, 32)
	out, consumed, err := decodeStringLiteral(dst, src)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(out) != "/sample/path" || consumed != 13 {
		t.Fatalf("got (%q, %d), want (/sample/path, 13)", out, consumed)
	}
}

func TestDecodeStringLiteral_Huffman(t *testing.T) {
	src := []byte{0x86, 0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf}
	out, consumed, err := decodeStringLiteral(nil, src)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(out) != "no-cache" || consumed != len(src) {
		t.Fatalf("got (%q, %d), want (no-cache, %d)", out, consumed, len(src))
	}
}

func TestDecodeStringLiteral_Truncated(t *testing.T) {
	src := []byte{0x05, 0x61, 0x62, 0x63} // claims 5 bytes, only 3 follow
	_, _, err := decodeStringLiteral(nil, src)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("err = %v, want ErrTruncated", err)
	}
}
```

- [ ] **Step 2: Run — fails**

Run: `go test ./hpack/ -run=StringLiteral`

- [ ] **Step 3: Implement**

Create `hpack/string_literal.go`:

```go
package hpack

// encodeStringLiteral appends the HPACK string-literal form of s to dst
// (RFC 7541 §5.2): {H?: 1 bit}{Length: 7-bit prefix int}{Length bytes}.
// huffman selects coding mode.
func encodeStringLiteral(dst, s []byte, huffman bool) []byte {
	if huffman {
		hlen := HuffmanEncodedLen(s)
		dst = EncodeInteger(dst, 7, 0x80, uint64(hlen))
		dst = HuffmanEncode(dst, s)
		return dst
	}
	dst = EncodeInteger(dst, 7, 0x00, uint64(len(s)))
	return append(dst, s...)
}

// decodeStringLiteral decodes a string literal starting at src[0] and
// appends it to dst, returning the extended dst and bytes consumed.
func decodeStringLiteral(dst, src []byte) ([]byte, int, error) {
	if len(src) < 1 {
		return nil, 0, ErrTruncated
	}
	huffman := src[0]&0x80 != 0
	length, n, err := DecodeInteger(src, 7)
	if err != nil {
		return nil, 0, err
	}
	if uint64(len(src)-n) < length {
		return nil, 0, ErrTruncated
	}
	body := src[n : n+int(length)]
	if huffman {
		dst, err = HuffmanDecode(dst, body)
		if err != nil {
			return nil, 0, err
		}
	} else {
		dst = append(dst, body...)
	}
	return dst, n + int(length), nil
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./hpack/ -run=StringLiteral`

- [ ] **Step 5: Commit**

```bash
git add hpack/string_literal.go hpack/string_literal_test.go
git commit -m "feat(hpack): string literal encode/decode (RFC 7541 §5.2)"
```

---

### Task 17: `hpack/hpack.go` and `hpack/encoder.go` — public types and Encoder

**Files:**
- Create: `hpack/hpack.go`
- Create: `hpack/encoder.go`
- Create: `hpack/encoder_test.go`

- [ ] **Step 1: Create `hpack/hpack.go` (public types)**

```go
package hpack

// HeaderField represents a single (name, value) pair as it appears on the
// wire or in a decoded HPACK block. Slices are NOT owned by HeaderField:
//   - For values produced by Decoder, slices alias the decoder's scratch
//     arena and are valid only for the lifetime of the FieldVisitor call.
//   - For values supplied to Encoder, the encoder copies bytes into wire
//     output and does not retain references.
type HeaderField struct {
	Name      []byte
	Value     []byte
	Sensitive bool // forces never-indexed (RFC §6.2.3)
}

// Size returns the entry size as defined in RFC 7541 §4.1 (used for
// dynamic table accounting).
func (f HeaderField) Size() uint32 {
	return uint32(len(f.Name)) + uint32(len(f.Value)) + 32
}

// FieldVisitor is invoked once per decoded field. f.Name and f.Value are
// only valid for the duration of the call.
type FieldVisitor func(f HeaderField) error

// Default initial dynamic-table size per RFC 7540 §6.5.2 SETTINGS_HEADER_TABLE_SIZE.
const defaultMaxDynamicTableSize uint32 = 4096
```

- [ ] **Step 2: Write failing encoder tests**

Create `hpack/encoder_test.go`:

```go
package hpack

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// RFC 7541 §C.2.4: indexed header field representation (":method GET").
func TestEncoder_IndexedFromStaticTable(t *testing.T) {
	enc := NewEncoder()
	dst := enc.WriteField(nil, []byte(":method"), []byte("GET"), false)
	want, _ := hex.DecodeString("82") // index 2
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
}

// RFC 7541 §C.2.1: literal with incremental indexing — new name/value.
func TestEncoder_LiteralWithIncrementalIndexing_NewName(t *testing.T) {
	enc := NewEncoder()
	dst := enc.WriteField(nil, []byte("custom-key"), []byte("custom-header"), false)
	// Pattern 0x40 + name-literal + value-literal.
	if dst[0] != 0x40 {
		t.Fatalf("prefix = %#x, want 0x40", dst[0])
	}
	if enc.dt.len() != 1 {
		t.Fatalf("dyn table len = %d, want 1 after incremental", enc.dt.len())
	}
}

// Sensitive=true must emit Never-Indexed (RFC §6.2.3, prefix 0001 0000).
func TestEncoder_NeverIndexed_OnSensitive(t *testing.T) {
	enc := NewEncoder()
	dst := enc.WriteField(nil, []byte("authorization"), []byte("secret"), true)
	// :authorization is index 23; never-indexed with name-index uses 0x10|23=0x17.
	if dst[0] != 0x17 {
		t.Fatalf("prefix = %#x, want 0x17", dst[0])
	}
	// Must NOT add to dynamic table.
	if enc.dt.len() != 0 {
		t.Fatalf("dyn table len = %d, want 0 for never-indexed", enc.dt.len())
	}
}

func TestEncoder_EncodeBlock_MultipleFields(t *testing.T) {
	enc := NewEncoder()
	fields := []HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	dst := enc.EncodeBlock(nil, fields)
	want, _ := hex.DecodeString("828784") // 0x82, 0x87, 0x84
	if !bytes.Equal(dst, want) {
		t.Fatalf("got %x, want %x", dst, want)
	}
}

func BenchmarkEncoder_EncodeBlock_3req_static(b *testing.B) {
	enc := NewEncoder()
	fields := []HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = enc.EncodeBlock(dst[:0], fields)
	}
	_ = dst
}
```

- [ ] **Step 3: Implement Encoder**

Create `hpack/encoder.go`:

```go
package hpack

// Encoder encodes HPACK header blocks. Holds a dynamic table per HTTP/2
// connection. NOT goroutine-safe.
type Encoder struct {
	dt *dynamicTable
	// peerMaxSize is what the peer advertised via SETTINGS_HEADER_TABLE_SIZE.
	// localLimit is our cap (defaults to peerMaxSize on construction).
	peerMaxSize uint32
	localLimit  uint32
	// pendingSizeUpdate, if non-zero, makes the next encode emit a
	// "Dynamic Table Size Update" representation (RFC §6.3) at the head.
	pendingSizeUpdate uint32
	hasPendingUpdate  bool
}

func NewEncoder() *Encoder {
	return &Encoder{
		dt:          newDynamicTable(defaultMaxDynamicTableSize),
		peerMaxSize: defaultMaxDynamicTableSize,
		localLimit:  defaultMaxDynamicTableSize,
	}
}

// SetMaxDynamicTableSize handles a peer SETTINGS_HEADER_TABLE_SIZE update.
// The encoder MUST emit a Size Update on the next block (RFC §4.2).
func (e *Encoder) SetMaxDynamicTableSize(n uint32) {
	e.peerMaxSize = n
	if n < e.localLimit {
		e.localLimit = n
	}
	e.pendingSizeUpdate = e.localLimit
	e.hasPendingUpdate = true
	e.dt.setMaxSize(e.localLimit)
}

// SetMaxDynamicTableSizeLimit caps the local table size below the peer limit.
func (e *Encoder) SetMaxDynamicTableSizeLimit(n uint32) {
	if n > e.peerMaxSize {
		n = e.peerMaxSize
	}
	if n != e.localLimit {
		e.localLimit = n
		e.pendingSizeUpdate = n
		e.hasPendingUpdate = true
		e.dt.setMaxSize(n)
	}
}

// Reset clears the dynamic table and pending size update, preparing the
// Encoder for reuse on a new connection.
func (e *Encoder) Reset() {
	e.dt.clear()
	e.peerMaxSize = defaultMaxDynamicTableSize
	e.localLimit = defaultMaxDynamicTableSize
	e.pendingSizeUpdate = 0
	e.hasPendingUpdate = false
}

// EncodeBlock encodes a slice of fields and appends the result to dst.
func (e *Encoder) EncodeBlock(dst []byte, fields []HeaderField) []byte {
	dst = e.maybeEmitSizeUpdate(dst)
	for i := range fields {
		dst = e.writeFieldAlreadyFlushedSize(dst, fields[i].Name, fields[i].Value, fields[i].Sensitive)
	}
	return dst
}

// WriteField encodes a single field and appends to dst.
func (e *Encoder) WriteField(dst, name, value []byte, sensitive bool) []byte {
	dst = e.maybeEmitSizeUpdate(dst)
	return e.writeFieldAlreadyFlushedSize(dst, name, value, sensitive)
}

func (e *Encoder) maybeEmitSizeUpdate(dst []byte) []byte {
	if !e.hasPendingUpdate {
		return dst
	}
	dst = EncodeInteger(dst, 5, 0x20, uint64(e.pendingSizeUpdate))
	e.hasPendingUpdate = false
	return dst
}

func (e *Encoder) writeFieldAlreadyFlushedSize(dst, name, value []byte, sensitive bool) []byte {
	// Lookup static table.
	staticIdx, fullStatic := staticIndex(name, value)
	if fullStatic && !sensitive {
		// 6.1: indexed.
		return EncodeInteger(dst, 7, 0x80, staticIdx)
	}

	// Lookup dynamic table.
	dynIdx, fullDyn := e.dynamicLookup(name, value)
	if fullDyn && !sensitive {
		return EncodeInteger(dst, 7, 0x80, dynIdx+uint64(staticTableLen))
	}

	// Decide name reuse.
	var nameIdx uint64
	if staticIdx != 0 {
		nameIdx = staticIdx
	} else if dynIdx != 0 {
		nameIdx = dynIdx + uint64(staticTableLen)
	}

	switch {
	case sensitive:
		// 6.2.3: never-indexed, prefix 0001 (4-bit prefix int).
		dst = EncodeInteger(dst, 4, 0x10, nameIdx)
	default:
		// 6.2.1: literal with incremental indexing, prefix 01 (6-bit prefix int).
		dst = EncodeInteger(dst, 6, 0x40, nameIdx)
	}
	if nameIdx == 0 {
		dst = encodeStringLiteral(dst, name, false)
	}
	dst = encodeStringLiteral(dst, value, false)
	if !sensitive {
		// 6.2.1 instructs the decoder to also add to its dynamic table; we mirror locally.
		e.dt.add(name, value)
	}
	return dst
}

func (e *Encoder) dynamicLookup(name, value []byte) (uint64, bool) {
	var nameOnly uint64
	for i := 1; i <= e.dt.len(); i++ {
		n, v := e.dt.at(i)
		if !bytesEqual(n, name) {
			continue
		}
		if bytesEqual(v, value) {
			return uint64(i), true
		}
		if nameOnly == 0 {
			nameOnly = uint64(i)
		}
	}
	return nameOnly, false
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./hpack/ -run=Encoder`

- [ ] **Step 5: Verify zero-alloc on bench**

Run: `go test -bench=BenchmarkEncoder_EncodeBlock -benchmem -run=^$ ./hpack/`
Expected: `0 allocs/op`.

- [ ] **Step 6: Commit**

```bash
git add hpack/hpack.go hpack/encoder.go hpack/encoder_test.go
git commit -m "feat(hpack): Encoder with static/dynamic indexing strategies"
```

---

### Task 18: `hpack/decoder.go` — single-block decode

**Files:**
- Create: `hpack/decoder.go`
- Create: `hpack/decoder_test.go`

- [ ] **Step 1: Write failing test (RFC §C.2.1, §C.2.2, §C.2.3, §C.2.4)**

Create `hpack/decoder_test.go`:

```go
package hpack

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func collectFields(t *testing.T, d *Decoder, blockHex string) []HeaderField {
	t.Helper()
	block, _ := hex.DecodeString(blockHex)
	var got []HeaderField
	err := d.DecodeBlock(block, func(f HeaderField) error {
		// Copy out — slices alias decoder arena.
		got = append(got, HeaderField{
			Name:      append([]byte{}, f.Name...),
			Value:     append([]byte{}, f.Value...),
			Sensitive: f.Sensitive,
		})
		return nil
	})
	if err != nil {
		t.Fatalf("decode err: %v", err)
	}
	return got
}

// RFC 7541 §C.2.1.
func TestDecoder_LiteralIncrementalIndexing(t *testing.T) {
	d := NewDecoder()
	got := collectFields(t, d, "400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	want := []HeaderField{{Name: []byte("custom-key"), Value: []byte("custom-header")}}
	if len(got) != 1 || !bytes.Equal(got[0].Name, want[0].Name) || !bytes.Equal(got[0].Value, want[0].Value) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if d.dt.len() != 1 {
		t.Fatalf("dyn table len = %d, want 1", d.dt.len())
	}
}

// RFC 7541 §C.2.2.
func TestDecoder_LiteralWithoutIndexing(t *testing.T) {
	d := NewDecoder()
	got := collectFields(t, d, "040c2f73616d706c652f70617468")
	want := HeaderField{Name: []byte(":path"), Value: []byte("/sample/path")}
	if !bytes.Equal(got[0].Name, want.Name) || !bytes.Equal(got[0].Value, want.Value) {
		t.Fatalf("got %+v, want %+v", got[0], want)
	}
	if d.dt.len() != 0 {
		t.Fatalf("dyn table modified by literal-without-indexing")
	}
}

// RFC 7541 §C.2.3.
func TestDecoder_LiteralNeverIndexed(t *testing.T) {
	d := NewDecoder()
	got := collectFields(t, d, "100870617373776f72640673656372657420")
	if !got[0].Sensitive {
		t.Fatalf("Sensitive flag not set on never-indexed")
	}
}

// RFC 7541 §C.2.4: indexed header field.
func TestDecoder_IndexedHeaderField(t *testing.T) {
	d := NewDecoder()
	got := collectFields(t, d, "82")
	if string(got[0].Name) != ":method" || string(got[0].Value) != "GET" {
		t.Fatalf("got %q=%q, want :method=GET", got[0].Name, got[0].Value)
	}
}

func BenchmarkDecoder_DecodeBlock_3req_static(b *testing.B) {
	d := NewDecoder()
	block, _ := hex.DecodeString("828784")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.DecodeBlock(block, func(f HeaderField) error { return nil })
	}
}
```

(The hex `100870617373776f72640673656372657420` is a constructed never-indexed example: `0x10` = never-indexed prefix with no name index, name literal `password` (8 bytes), value literal `secret ` (with trailing space included). Use the actual RFC §C.2.3 vector if it differs; verify against the published example.)

- [ ] **Step 2: Run — fails**

Run: `go test ./hpack/ -run=Decoder`

- [ ] **Step 3: Implement Decoder**

Create `hpack/decoder.go`:

```go
package hpack

// Decoder decodes HPACK header blocks. Holds a dynamic table per HTTP/2
// connection. NOT goroutine-safe.
type Decoder struct {
	dt          *dynamicTable
	maxLocal    uint32 // local cap on the dynamic table size we ADVERTISE in SETTINGS
	scratch     []byte // arena for decoded names+values; reset per-block
	maxListSize uint32 // SETTINGS_MAX_HEADER_LIST_SIZE; 0 = unlimited
	// Streaming state for HEADERS+CONTINUATION decode.
	streaming bool
	pending   []byte // accumulated block bytes across Feed calls
}

func NewDecoder() *Decoder {
	return &Decoder{
		dt:       newDynamicTable(defaultMaxDynamicTableSize),
		maxLocal: defaultMaxDynamicTableSize,
		scratch:  make([]byte, 0, 4096),
	}
}

func (d *Decoder) SetMaxDynamicTableSize(n uint32) {
	d.maxLocal = n
	d.dt.setMaxSize(n)
}

func (d *Decoder) SetMaxHeaderListSize(n uint32) { d.maxListSize = n }

func (d *Decoder) Reset() {
	d.dt.clear()
	d.scratch = d.scratch[:0]
	d.streaming = false
	d.pending = d.pending[:0]
}

// DecodeBlock parses a complete header block fragment and emits one call per
// field via visit. Field slices alias d.scratch and are valid only for the
// duration of the visit call.
func (d *Decoder) DecodeBlock(block []byte, visit FieldVisitor) error {
	d.scratch = d.scratch[:0]
	return d.decodeFragment(block, visit)
}

func (d *Decoder) decodeFragment(src []byte, visit FieldVisitor) error {
	for len(src) > 0 {
		b := src[0]
		switch {
		case b&0x80 != 0:
			// 6.1: indexed.
			idx, n, err := DecodeInteger(src, 7)
			if err != nil {
				return err
			}
			src = src[n:]
			name, value, err := d.lookup(idx)
			if err != nil {
				return err
			}
			if err := d.emit(visit, name, value, false); err != nil {
				return err
			}
		case b&0xc0 == 0x40:
			// 6.2.1: literal with incremental indexing.
			name, value, n, err := d.parseLiteral(src, 6)
			if err != nil {
				return err
			}
			src = src[n:]
			d.dt.add(name, value)
			if err := d.emit(visit, name, value, false); err != nil {
				return err
			}
		case b&0xe0 == 0x20:
			// 6.3: dynamic table size update.
			n, consumed, err := DecodeInteger(src, 5)
			if err != nil {
				return err
			}
			src = src[consumed:]
			if uint32(n) > d.maxLocal {
				return ErrTableSizeUpdate
			}
			d.dt.setMaxSize(uint32(n))
		case b&0xf0 == 0x10:
			// 6.2.3: never indexed.
			name, value, n, err := d.parseLiteral(src, 4)
			if err != nil {
				return err
			}
			src = src[n:]
			if err := d.emit(visit, name, value, true); err != nil {
				return err
			}
		case b&0xf0 == 0x00:
			// 6.2.2: literal without indexing.
			name, value, n, err := d.parseLiteral(src, 4)
			if err != nil {
				return err
			}
			src = src[n:]
			if err := d.emit(visit, name, value, false); err != nil {
				return err
			}
		default:
			return ErrInvalidPrefix
		}
	}
	return nil
}

func (d *Decoder) lookup(idx uint64) (name, value []byte, err error) {
	if idx == 0 {
		return nil, nil, ErrInvalidIndex
	}
	if idx <= staticTableLen {
		e := staticTable[idx]
		return e.name, e.value, nil
	}
	dynIdx := int(idx - staticTableLen)
	if dynIdx > d.dt.len() {
		return nil, nil, ErrInvalidIndex
	}
	n, v := d.dt.at(dynIdx)
	return n, v, nil
}

func (d *Decoder) parseLiteral(src []byte, namePrefixBits uint8) (name, value []byte, consumed int, err error) {
	idx, n, err := DecodeInteger(src, namePrefixBits)
	if err != nil {
		return nil, nil, 0, err
	}
	consumed = n
	if idx == 0 {
		// Name is a literal.
		nameStart := len(d.scratch)
		var nb int
		d.scratch, nb, err = decodeStringLiteral(d.scratch, src[consumed:])
		if err != nil {
			return nil, nil, 0, err
		}
		consumed += nb
		name = d.scratch[nameStart:]
	} else {
		// Reuse name from table; copy into scratch for stable lifetime.
		var refName []byte
		refName, _, err = d.lookup(idx)
		if err != nil {
			return nil, nil, 0, err
		}
		nameStart := len(d.scratch)
		d.scratch = append(d.scratch, refName...)
		name = d.scratch[nameStart:]
	}
	valueStart := len(d.scratch)
	var vb int
	d.scratch, vb, err = decodeStringLiteral(d.scratch, src[consumed:])
	if err != nil {
		return nil, nil, 0, err
	}
	consumed += vb
	value = d.scratch[valueStart:]
	return name, value, consumed, nil
}

func (d *Decoder) emit(visit FieldVisitor, name, value []byte, sensitive bool) error {
	return visit(HeaderField{Name: name, Value: value, Sensitive: sensitive})
}
```

- [ ] **Step 4: Run — passes**

Run: `go test ./hpack/ -run=Decoder`

- [ ] **Step 5: Commit**

```bash
git add hpack/decoder.go hpack/decoder_test.go
git commit -m "feat(hpack): Decoder with all 4 representations + size update"
```

---

### Task 19: Streaming decode (Begin/Feed/Finish)

**Files:**
- Modify: `hpack/decoder.go`
- Modify: `hpack/decoder_test.go`

- [ ] **Step 1: Add failing test**

Append to `hpack/decoder_test.go`:

```go
func TestDecoder_Streaming_SplitMidField(t *testing.T) {
	// Same block as in TestDecoder_LiteralIncrementalIndexing,
	// split mid-field to force buffering.
	full, _ := hex.DecodeString("400a637573746f6d2d6b65790d637573746f6d2d686561646572")
	d := NewDecoder()
	d.Begin()
	var got []HeaderField
	visit := func(f HeaderField) error {
		got = append(got, HeaderField{
			Name:  append([]byte{}, f.Name...),
			Value: append([]byte{}, f.Value...),
		})
		return nil
	}
	for splitAt := 1; splitAt < len(full); splitAt++ {
		d.Reset()
		d.Begin()
		got = got[:0]
		if err := d.Feed(full[:splitAt], visit); err != nil {
			t.Fatalf("split=%d feed1: %v", splitAt, err)
		}
		if err := d.Feed(full[splitAt:], visit); err != nil {
			t.Fatalf("split=%d feed2: %v", splitAt, err)
		}
		if err := d.Finish(); err != nil {
			t.Fatalf("split=%d finish: %v", splitAt, err)
		}
		if len(got) != 1 || string(got[0].Name) != "custom-key" || string(got[0].Value) != "custom-header" {
			t.Fatalf("split=%d got %+v", splitAt, got)
		}
	}
	// After Finish, streaming state must be cleared; second Finish errors.
	if err := d.Finish(); err == nil {
		t.Fatalf("Finish after Finish should error")
	}
}
```

- [ ] **Step 2: Run — fails (Begin/Feed/Finish not yet wired)**

- [ ] **Step 3: Implement Streaming**

Append to `hpack/decoder.go`:

```go
// Begin starts a streaming decode session. Call Feed for each fragment
// (HEADERS payload, then each CONTINUATION payload), then Finish.
func (d *Decoder) Begin() {
	d.streaming = true
	d.pending = d.pending[:0]
	d.scratch = d.scratch[:0]
}

// Feed appends a fragment to the streaming buffer and decodes any complete
// representations available. Field slices passed to visit are valid only
// during the visit call.
func (d *Decoder) Feed(fragment []byte, visit FieldVisitor) error {
	if !d.streaming {
		return ErrInvalidPrefix
	}
	d.pending = append(d.pending, fragment...)
	consumed, err := d.decodeAsFar(d.pending, visit)
	if err != nil {
		return err
	}
	// Slide the buffer.
	d.pending = append(d.pending[:0], d.pending[consumed:]...)
	return nil
}

// decodeAsFar consumes as many full representations as possible and returns
// the number of bytes consumed. Truncated tail remains.
func (d *Decoder) decodeAsFar(src []byte, visit FieldVisitor) (int, error) {
	consumed := 0
	for consumed < len(src) {
		// Try to decode one representation. If we can't because of
		// truncation, return what we have so far.
		n, err := d.decodeOne(src[consumed:], visit)
		if err == ErrTruncated {
			return consumed, nil
		}
		if err != nil {
			return consumed, err
		}
		consumed += n
	}
	return consumed, nil
}

// decodeOne returns the bytes consumed by a single representation, or
// ErrTruncated if src doesn't yet hold one full representation.
func (d *Decoder) decodeOne(src []byte, visit FieldVisitor) (int, error) {
	if len(src) == 0 {
		return 0, ErrTruncated
	}
	// We re-use decodeFragment by feeding a slice that contains only the
	// next representation; but since we don't know its boundaries, the
	// simpler approach is: try to decode and detect truncation.
	saveScratch := len(d.scratch)
	err := d.decodeFragmentSingle(src, visit, &savedReturn{Consumed: 0})
	_ = saveScratch
	_ = err
	// Implementation note: a simpler and correct alternative is to call
	// d.decodeFragment(src, visit) directly only when we believe we have a
	// full block. For Phase A streaming we instead buffer everything and
	// decode at Finish. See below.
	return 0, ErrTruncated
}

type savedReturn struct{ Consumed int }

func (d *Decoder) decodeFragmentSingle(src []byte, visit FieldVisitor, ret *savedReturn) error {
	// Placeholder; see Finish below.
	return ErrTruncated
}

// Finish completes a streaming decode. It decodes the entire buffered
// payload as one block and resets the streaming state.
func (d *Decoder) Finish() error {
	if !d.streaming {
		return ErrInvalidPrefix
	}
	defer func() {
		d.streaming = false
		d.pending = d.pending[:0]
	}()
	if len(d.pending) == 0 {
		return nil
	}
	// All representations must complete on the buffered bytes.
	return d.decodeFragment(d.pending, func(f HeaderField) error { return nil })
}
```

Replace the placeholder Begin/Feed/Finish above with the implementation that matches the spec API (Feed takes the visitor and emits incrementally; Finish takes no visitor and only validates completion):

```go
// Begin starts a streaming decode session. Call Feed for each fragment
// (HEADERS payload, then each CONTINUATION payload), then Finish.
func (d *Decoder) Begin() {
	d.streaming = true
	d.pending = d.pending[:0]
	d.scratch = d.scratch[:0]
}

// Feed appends a fragment, then decodes and emits as many complete
// representations as possible. Truncated tail remains buffered until the
// next Feed or Finish.
func (d *Decoder) Feed(fragment []byte, visit FieldVisitor) error {
	if !d.streaming {
		return ErrInvalidPrefix
	}
	d.pending = append(d.pending, fragment...)
	consumed, err := d.decodePartial(d.pending, visit)
	if err != nil {
		return err
	}
	d.pending = append(d.pending[:0], d.pending[consumed:]...)
	return nil
}

// Finish validates that the streaming buffer is empty (i.e. the last Feed
// completed the block) and resets streaming state.
func (d *Decoder) Finish() error {
	if !d.streaming {
		return ErrInvalidPrefix
	}
	defer func() {
		d.streaming = false
		d.pending = d.pending[:0]
	}()
	if len(d.pending) > 0 {
		return ErrTruncated
	}
	return nil
}
```

`decodePartial` is a variant of `decodeFragment` that, on detecting truncation in the middle of a representation, returns the count of bytes consumed by complete representations so far (without an error). Implementation outline:

```go
// decodePartial consumes as many complete representations as fit in src and
// returns the count of consumed bytes. Truncation in the middle of a
// representation is NOT an error here — caller will Feed more bytes.
func (d *Decoder) decodePartial(src []byte, visit FieldVisitor) (int, error) {
	consumed := 0
	for consumed < len(src) {
		// Snapshot scratch so we can rewind if the next representation is truncated.
		scratchLen := len(d.scratch)
		n, err := d.decodeOneRepresentation(src[consumed:], visit)
		if err == ErrTruncated {
			d.scratch = d.scratch[:scratchLen]
			return consumed, nil
		}
		if err != nil {
			return consumed, err
		}
		consumed += n
	}
	return consumed, nil
}
```

`decodeOneRepresentation` decodes a single representation (either an indexed header field or a literal/sized representation) and returns the bytes consumed. It is the body of the existing `decodeFragment` switch wrapped to return the consumed count.

- [ ] **Step 4: Run — passes**

Run: `go test ./hpack/ -run=Streaming`

- [ ] **Step 5: Commit**

```bash
git add hpack/decoder.go hpack/decoder_test.go
git commit -m "feat(hpack): streaming decode (Begin/Feed/FinishVisit)"
```

---

### Task 20: HPACK conformance — RFC 7541 App. C vectors

**Files:**
- Create: `hpack/conformance_test.go`
- Create: `testdata/rfc7541/c_2_1.hex`, `c_2_2.hex`, ..., `c_3.hex`, `c_4.hex`, `c_5.hex`, `c_6.hex` (input blocks + expected fields)

- [ ] **Step 1: Capture App. C vectors as fixtures**

For each numbered example in RFC 7541 §C.2 through §C.6, copy the wire-format hex into `testdata/rfc7541/<name>.hex` (one hex string per file, no spaces) and the expected decoded fields into `testdata/rfc7541/<name>.fields` (one `name: value` line per field; lines starting with `#` are comments).

For example, `testdata/rfc7541/c_2_1.hex`:

```
400a637573746f6d2d6b65790d637573746f6d2d686561646572
```

`testdata/rfc7541/c_2_1.fields`:

```
custom-key: custom-header
```

Capture these vectors:
- C.2.1, C.2.2, C.2.3, C.2.4 (representations)
- C.3.1, C.3.2, C.3.3 (request sequence without Huffman)
- C.4.1, C.4.2, C.4.3 (request sequence with Huffman)
- C.5.1, C.5.2, C.5.3 (response sequence without Huffman, with table size 256)
- C.6.1, C.6.2, C.6.3 (response sequence with Huffman, with table size 256)

- [ ] **Step 2: Write conformance test**

Create `hpack/conformance_test.go`:

```go
package hpack

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fixtureField struct {
	name, value string
}

func loadFixture(t *testing.T, name string) (block []byte, fields []fixtureField) {
	t.Helper()
	hexBytes, err := os.ReadFile(filepath.Join("..", "testdata", "rfc7541", name+".hex"))
	if err != nil {
		t.Fatalf("read hex: %v", err)
	}
	block, err = hex.DecodeString(strings.TrimSpace(string(hexBytes)))
	if err != nil {
		t.Fatalf("hex decode: %v", err)
	}
	f, err := os.Open(filepath.Join("..", "testdata", "rfc7541", name+".fields"))
	if err != nil {
		t.Fatalf("open fields: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		idx := strings.Index(line, ": ")
		if idx < 0 {
			t.Fatalf("bad fields line: %q", line)
		}
		fields = append(fields, fixtureField{name: line[:idx], value: line[idx+2:]})
	}
	return block, fields
}

func TestConformance_RFC7541_§C_2_1_LiteralIncrementalIndexing(t *testing.T) {
	runConformance(t, "c_2_1", NewDecoder(), 0, 4096)
}

func TestConformance_RFC7541_§C_2_2_LiteralWithoutIndexing(t *testing.T) {
	runConformance(t, "c_2_2", NewDecoder(), 0, 4096)
}

// ... add a function per C.2.1..C.6.3 vector ...

func runConformance(t *testing.T, name string, d *Decoder, _ uint32, _ uint32) {
	t.Helper()
	block, want := loadFixture(t, name)
	var got []fixtureField
	err := d.DecodeBlock(block, func(f HeaderField) error {
		got = append(got, fixtureField{name: string(f.Name), value: string(f.Value)})
		return nil
	})
	if err != nil {
		t.Fatalf("decode err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("len got=%d want=%d: %+v vs %+v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("field[%d]: got %+v, want %+v", i, got[i], want[i])
		}
	}
	_ = bytes.NewReader(nil) // keep import honest
}
```

For the response sequences (C.5.x, C.6.x) the dynamic table starts with `setMaxSize(256)`. Pass that via a small variant of `runConformance`.

- [ ] **Step 3: Run — passes (or fix vectors)**

Run: `go test ./hpack/ -run=Conformance`

If a vector mismatches, double-check the fixture: hex must be exactly the bytes from the RFC; fields must include all pseudo-headers in order.

- [ ] **Step 4: Commit**

```bash
git add hpack/conformance_test.go testdata/rfc7541/
git commit -m "test(hpack): RFC 7541 App. C conformance vectors"
```

---

### Task 21: HPACK fuzz seed corpus + fuzz test

**Files:**
- Create: `hpack/fuzz_test.go`

- [ ] **Step 1: Write fuzz**

Create `hpack/fuzz_test.go`:

```go
package hpack

import "testing"

func FuzzHPACKDecode(f *testing.F) {
	// Seeds: RFC App. C vectors (binary form).
	for _, name := range []string{"c_2_1", "c_2_2", "c_2_3", "c_2_4", "c_3_1", "c_4_1", "c_5_1", "c_6_1"} {
		block, _ := loadFixtureBinary(name)
		f.Add(block)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		d := NewDecoder()
		_ = d.DecodeBlock(data, func(_ HeaderField) error { return nil })
		// Invariant: no panic. Errors are fine.
	})
}

// loadFixtureBinary is a duplicate of loadFixture's first stage to avoid
// pulling testing.T into Fuzz.Add.
func loadFixtureBinary(name string) ([]byte, error) {
	// Implementation duplicates the read-and-hex-decode steps from
	// loadFixture; left as a 5-line helper.
	return nil, nil
}
```

Replace the `loadFixtureBinary` body with a simple `os.ReadFile(...)` + `hex.DecodeString` (without `t.Fatalf`).

- [ ] **Step 2: Replay seeds**

Run: `go test -run=FuzzHPACKDecode -count=1 ./hpack/`
Expected: PASS (corpus replay).

- [ ] **Step 3: Spot-check fuzz briefly**

Run: `go test -fuzz=FuzzHPACKDecode -fuzztime=30s ./hpack/`
Expected: no panics, no findings (or save findings to `testdata/fuzz/` and add a test for them).

- [ ] **Step 4: Commit**

```bash
git add hpack/fuzz_test.go
git commit -m "test(hpack): FuzzHPACKDecode + RFC seed corpus"
```

---

### Task 22: HPACK bench gates

**Files:**
- Create: `hpack/bench_test.go`

- [ ] **Step 1: Write benchmarks (already partial coverage exists in earlier tests)**

Create `hpack/bench_test.go` consolidating bench targets from the spec:

```go
package hpack

import (
	"encoding/hex"
	"testing"
)

func BenchmarkHPACK_EncodeBlock_3req_static(b *testing.B) {
	enc := NewEncoder()
	fields := []HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = enc.EncodeBlock(dst[:0], fields)
	}
	_ = dst
}

func BenchmarkHPACK_DecodeBlock_3req_static(b *testing.B) {
	d := NewDecoder()
	block, _ := hex.DecodeString("828784")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.DecodeBlock(block, func(_ HeaderField) error { return nil })
	}
}

func BenchmarkHPACK_HuffmanEncode_path(b *testing.B) {
	src := []byte("/index.html")
	dst := make([]byte, 0, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = HuffmanEncode(dst[:0], src)
	}
	_ = dst
}

func BenchmarkHPACK_HuffmanDecode_path(b *testing.B) {
	enc, _ := hex.DecodeString("60d5e8b1d754df")
	dst := make([]byte, 0, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, _ = HuffmanDecode(dst[:0], enc)
	}
	_ = dst
}

func BenchmarkHPACK_IntegerDecode_max(b *testing.B) {
	src := []byte{0x1f, 0xff, 0xff, 0xff, 0xff, 0x0f}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeInteger(src, 5)
	}
}
```

- [ ] **Step 2: Run benches**

Run: `make bench`
Expected output (representative): all five benchmarks report `0 allocs/op` and `0 B/op`.

- [ ] **Step 3: Capture as baseline (will be promoted to `BENCH_BASELINE.md` later)**

Save the output to `bench-A1.txt` (gitignored) for now.

- [ ] **Step 4: Commit**

```bash
git add hpack/bench_test.go
git commit -m "test(hpack): bench gates (encode/decode/huffman/integer)"
```

---

### Task 23: Milestone gate A1 — HPACK done

- [ ] **Step 1: Full local verification**

```bash
make tidy
make lint
make test-race
make bench
```

All green. `0 allocs/op` for all bench targets in `hpack/`.

- [ ] **Step 2: Push branch, observe CI**

```bash
git push -u origin design/2026-05-02-frame-layer-phase-a
```

Wait for `ci/lint`, `ci/test`, `ci/fuzz-corpus-replay` to pass on GitHub.

- [ ] **Step 3: Tag this milestone (optional, internal)**

```bash
git tag -a phase-a-m1-hpack -m "Phase A milestone 1: HPACK complete"
git push origin phase-a-m1-hpack
```

---

## frame — HTTP/2 framer (RFC 7540)

### Task 24: `frame/errors.go` and `frame/frame.go` — types and ErrCode

**Files:**
- Create: `frame/doc.go`
- Create: `frame/errors.go`
- Create: `frame/frame.go`

- [ ] **Step 1: Create `frame/doc.go`**

```go
// Package frame implements the HTTP/2 framing layer (RFC 7540) without
// any networking. It provides a Framer that reads frames via a Handler
// visitor (zero-copy, caller-owned scratch) and writes frames via explicit
// per-type methods. Framer is NOT goroutine-safe.
package frame
```

- [ ] **Step 2: Create `frame/errors.go`**

```go
package frame

import "errors"

// ErrCode mirrors RFC 7540 §7.
type ErrCode uint32

const (
	ErrCodeNoError            ErrCode = 0x0
	ErrCodeProtocolError      ErrCode = 0x1
	ErrCodeInternalError      ErrCode = 0x2
	ErrCodeFlowControlError   ErrCode = 0x3
	ErrCodeSettingsTimeout    ErrCode = 0x4
	ErrCodeStreamClosed       ErrCode = 0x5
	ErrCodeFrameSizeError     ErrCode = 0x6
	ErrCodeRefusedStream      ErrCode = 0x7
	ErrCodeCancel             ErrCode = 0x8
	ErrCodeCompressionError   ErrCode = 0x9
	ErrCodeConnectError       ErrCode = 0xA
	ErrCodeEnhanceYourCalm    ErrCode = 0xB
	ErrCodeInadequateSecurity ErrCode = 0xC
	ErrCodeHTTP11Required     ErrCode = 0xD
)

// Sentinel errors. Hot-path code MUST NOT use fmt.Errorf — only these.
var (
	ErrFrameTooLarge       = errors.New("poseidon/frame: frame exceeds SETTINGS_MAX_FRAME_SIZE")
	ErrInvalidStreamID     = errors.New("poseidon/frame: stream id violates RFC 7540 rules")
	ErrInvalidPadding      = errors.New("poseidon/frame: pad length exceeds payload")
	ErrUnknownFrameType    = errors.New("poseidon/frame: unknown frame type")
	ErrSettingsAck         = errors.New("poseidon/frame: SETTINGS ACK with non-empty payload")
	ErrPriorityWrongLength = errors.New("poseidon/frame: PRIORITY frame length != 5")
	ErrRSTWrongLength      = errors.New("poseidon/frame: RST_STREAM frame length != 4")
	ErrPingWrongLength     = errors.New("poseidon/frame: PING frame length != 8")
	ErrWindowWrongLength   = errors.New("poseidon/frame: WINDOW_UPDATE frame length != 4")
	ErrSettingsLength      = errors.New("poseidon/frame: SETTINGS frame length not multiple of 6")
	ErrShortRead           = errors.New("poseidon/frame: short read on header or payload")
	ErrZeroIncrement       = errors.New("poseidon/frame: WINDOW_UPDATE with zero increment")
)
```

- [ ] **Step 3: Create `frame/frame.go`**

```go
package frame

// FrameType — RFC 7540 §11.2.
type FrameType uint8

const (
	FrameData         FrameType = 0x0
	FrameHeaders      FrameType = 0x1
	FramePriority     FrameType = 0x2
	FrameRSTStream    FrameType = 0x3
	FrameSettings     FrameType = 0x4
	FramePushPromise  FrameType = 0x5
	FramePing         FrameType = 0x6
	FrameGoAway       FrameType = 0x7
	FrameWindowUpdate FrameType = 0x8
	FrameContinuation FrameType = 0x9
)

// Flags is a bitmask whose semantics depend on FrameType.
type Flags uint8

const (
	FlagDataEndStream  Flags = 0x1
	FlagDataPadded     Flags = 0x8
	FlagHeadersEndStream  Flags = 0x1
	FlagHeadersEndHeaders Flags = 0x4
	FlagHeadersPadded     Flags = 0x8
	FlagHeadersPriority   Flags = 0x20
	FlagSettingsAck       Flags = 0x1
	FlagPingAck           Flags = 0x1
	FlagContinuationEndHeaders Flags = 0x4
	FlagPushPromiseEndHeaders Flags = 0x4
	FlagPushPromisePadded     Flags = 0x8
)

// FrameHeader is the fixed 9-byte prefix of every frame (RFC 7540 §4.1).
type FrameHeader struct {
	Length   uint32 // 24-bit
	Type     FrameType
	Flags    Flags
	StreamID uint32 // 31-bit, R-bit masked
}

// Priority describes a PRIORITY field block (RFC 7540 §6.3).
type Priority struct {
	StreamDep uint32
	Exclusive bool
	Weight    uint8 // RFC weight = Weight + 1
}

// SettingID identifies a SETTINGS parameter (RFC 7540 §6.5.2).
type SettingID uint16

const (
	SettingHeaderTableSize      SettingID = 0x1
	SettingEnablePush           SettingID = 0x2
	SettingMaxConcurrentStreams SettingID = 0x3
	SettingInitialWindowSize    SettingID = 0x4
	SettingMaxFrameSize         SettingID = 0x5
	SettingMaxHeaderListSize    SettingID = 0x6
)

// SettingsParams holds up to 16 SETTINGS pairs (zero-alloc, no map).
type SettingsParams struct {
	Pairs [16]struct {
		ID    SettingID
		Value uint32
	}
	N int
}

// HeaderBlock is an opaque view over a HEADERS / PUSH_PROMISE / CONTINUATION
// header block fragment. Decode via hpack.Decoder.DecodeBlock(hb, visitor).
type HeaderBlock []byte
```

- [ ] **Step 4: Verify build**

Run: `go build ./frame/`

- [ ] **Step 5: Commit**

```bash
git add frame/doc.go frame/errors.go frame/frame.go
git commit -m "feat(frame): error codes, sentinels, frame types and flags"
```

---

### Task 25: `frame/header.go` — FrameHeader read/write

**Files:**
- Create: `frame/header.go`
- Create: `frame/header_test.go`

- [ ] **Step 1: Write failing tests**

Create `frame/header_test.go`:

```go
package frame

import (
	"bytes"
	"testing"
)

func TestReadFrameHeader_Sample(t *testing.T) {
	// length=10, type=0x1 (HEADERS), flags=0x05 (END_STREAM|END_HEADERS), stream=1
	raw := []byte{0x00, 0x00, 0x0a, 0x01, 0x05, 0x00, 0x00, 0x00, 0x01}
	h, err := ReadFrameHeader(raw)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if h.Length != 10 || h.Type != FrameHeaders || h.Flags != 0x05 || h.StreamID != 1 {
		t.Fatalf("got %+v", h)
	}
}

func TestReadFrameHeader_RBitMasked(t *testing.T) {
	// stream id with R-bit set should be masked off.
	raw := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x01}
	h, err := ReadFrameHeader(raw)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if h.StreamID != 1 {
		t.Fatalf("StreamID = %d, want 1 (R-bit must be masked)", h.StreamID)
	}
}

func TestReadFrameHeader_Short(t *testing.T) {
	_, err := ReadFrameHeader([]byte{0x00, 0x00})
	if err == nil {
		t.Fatalf("want error on short header")
	}
}

func TestWriteFrameHeader(t *testing.T) {
	h := FrameHeader{Length: 0x1234, Type: FrameSettings, Flags: 0x01, StreamID: 0}
	var buf [9]byte
	WriteFrameHeader(buf[:], h)
	want := []byte{0x00, 0x12, 0x34, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(buf[:], want) {
		t.Fatalf("got %x, want %x", buf, want)
	}
}
```

- [ ] **Step 2: Run — fails**

- [ ] **Step 3: Implement**

Create `frame/header.go`:

```go
package frame

import "github.com/lodgvideon/poseidon-http-client/internal/bytesx"

// FrameHeaderSize is the wire size of a frame header (RFC 7540 §4.1).
const FrameHeaderSize = 9

// ReadFrameHeader parses the 9-byte prefix from b. b MUST have length ≥ 9.
func ReadFrameHeader(b []byte) (FrameHeader, error) {
	if len(b) < FrameHeaderSize {
		return FrameHeader{}, ErrShortRead
	}
	return FrameHeader{
		Length:   bytesx.ReadUint24(b[:3]),
		Type:     FrameType(b[3]),
		Flags:    Flags(b[4]),
		StreamID: bytesx.ReadUint31(b[5:9]),
	}, nil
}

// WriteFrameHeader writes h into b[:9]. b MUST have length ≥ 9.
func WriteFrameHeader(b []byte, h FrameHeader) {
	_ = b[FrameHeaderSize-1]
	bytesx.WriteUint24(b[:3], h.Length)
	b[3] = byte(h.Type)
	b[4] = byte(h.Flags)
	bytesx.WriteUint31(b[5:9], h.StreamID)
}
```

- [ ] **Step 4: Run — passes**

- [ ] **Step 5: Commit**

```bash
git add frame/header.go frame/header_test.go
git commit -m "feat(frame): ReadFrameHeader/WriteFrameHeader"
```

---

### Tasks 26–34: Per-frame-type encode/decode (RFC 7540 §6.1–§6.10)

Each of these tasks follows the same shape: write a TDD test for one frame type's encode and decode (with edge cases from RFC), implement, verify zero-alloc, commit.

To keep this plan reasonable in length, I list each task with **its specific RFC quirks** and the **wire format** as a quick reference. The implementer should:

1. Write test cases covering: minimal valid frame, max-size valid frame, every error condition the RFC §6.X enumerates.
2. Implement encode by writing the 9-byte header followed by the payload directly into a caller-supplied `dst []byte` (or via the `Framer.WriteX` path).
3. Implement decode as a function that takes the raw payload (length already known from header) and emits via the appropriate `Handler.OnXxx` callback.

#### Task 26: `frame/data.go` — DATA (RFC 7540 §6.1)

**Wire:** `[Pad Length(8)?] [Data(*)] [Padding(*)?]`. Flags: END_STREAM (0x1), PADDED (0x8). StreamID MUST be != 0; otherwise PROTOCOL_ERROR.

**Files:** Create `frame/data.go`, `frame/data_test.go`. Same 5-step TDD pattern (test, run-fail, impl, run-pass, commit).

Test cases to include: minimal data, padded data, padded-with-zero-payload, padded with padLen exceeding payload (should error), StreamID=0 rejected at Framer level (covered in framer_test.go), END_STREAM flag preserved.

Implementation function signatures:

```go
func writeData(dst []byte, h FrameHeader, payload []byte) []byte
func writeDataPadded(dst []byte, h FrameHeader, data []byte, padLen uint8) []byte
// Decode is wired in framer.go's switch; the per-frame file holds helpers if needed.
```

**Commit:** `feat(frame): DATA frame encode/decode (RFC §6.1)`

#### Task 27: `frame/priority.go` — PRIORITY (RFC 7540 §6.3)

**Wire:** `[Stream Dep(31) | E(1)] [Weight(8)]` — exactly 5 bytes. StreamID MUST != 0; length != 5 → FRAME_SIZE_ERROR.

**Edge cases:** length != 5 returns `ErrPriorityWrongLength`; exclusive bit set/cleared.

**Commit:** `feat(frame): PRIORITY frame encode/decode (RFC §6.3)`

#### Task 28: `frame/rst_stream.go` — RST_STREAM (RFC 7540 §6.4)

**Wire:** `[Error Code(32)]`, exactly 4 bytes. StreamID MUST != 0; length != 4 → FRAME_SIZE_ERROR.

**Commit:** `feat(frame): RST_STREAM frame encode/decode (RFC §6.4)`

#### Task 29: `frame/settings.go` — SETTINGS (RFC 7540 §6.5)

**Wire:** zero or more `[Identifier(16)] [Value(32)]` pairs. StreamID MUST == 0. Length MUST be multiple of 6. ACK flag (0x1): payload MUST be empty.

**Edge cases:** ACK with non-empty payload → `ErrSettingsAck`; non-ACK with non-multiple-of-6 → `ErrSettingsLength`; > 16 settings (clamp or reject — choose: reject with new sentinel `ErrTooManySettings`).

**Commit:** `feat(frame): SETTINGS frame + SettingsParams (RFC §6.5)`

#### Task 30: `frame/ping.go` — PING (RFC 7540 §6.7)

**Wire:** 8 bytes opaque data. StreamID MUST == 0. Length MUST be 8. ACK flag (0x1).

**Commit:** `feat(frame): PING frame encode/decode (RFC §6.7)`

#### Task 31: `frame/goaway.go` — GOAWAY (RFC 7540 §6.8)

**Wire:** `[Last-Stream-ID(31) | R(1)] [Error Code(32)] [Additional Debug Data(*)]`. StreamID MUST == 0.

Debug data: variable-length opaque bytes; in the decoder we hand the visitor a slice-view into the read buffer (lifetime: until next ReadFrame).

**Commit:** `feat(frame): GOAWAY frame encode/decode (RFC §6.8)`

#### Task 32: `frame/window_update.go` — WINDOW_UPDATE (RFC 7540 §6.9)

**Wire:** `[Reserved(1) | Window Size Increment(31)]`, exactly 4 bytes. Length != 4 → FRAME_SIZE_ERROR. Increment == 0 → PROTOCOL_ERROR (`ErrZeroIncrement`).

**Commit:** `feat(frame): WINDOW_UPDATE frame encode/decode (RFC §6.9)`

#### Task 33: `frame/continuation.go` — CONTINUATION (RFC 7540 §6.10)

**Wire:** `[Header Block Fragment(*)]`. StreamID MUST != 0. Flag END_HEADERS (0x4).

A CONTINUATION frame must follow a HEADERS or PUSH_PROMISE without END_HEADERS, or another CONTINUATION on the same stream. Sequencing is enforced at the Framer level (Task 35).

**Commit:** `feat(frame): CONTINUATION frame encode/decode (RFC §6.10)`

#### Task 34: `frame/headers.go` — HEADERS (RFC 7540 §6.2)

**Wire:** `[Pad Length(8)?] [E(1) | Stream Dep(31)? | Weight(8)?] [Header Block Fragment(*)] [Padding(*)?]`. Flags: END_STREAM (0x1), END_HEADERS (0x4), PADDED (0x8), PRIORITY (0x20). StreamID MUST != 0.

This is the most complex of the per-type frames. Tests must cover all 16 combinations of (PADDED, PRIORITY, END_STREAM, END_HEADERS) where syntactically valid.

`WriteHeadersParams` struct from spec §4.3 is the public API for the encode side.

**Commit:** `feat(frame): HEADERS frame w/ priority and padding (RFC §6.2)`

#### Task 34b: `frame/push_promise.go` — PUSH_PROMISE (RFC 7540 §6.6)

**Wire:** `[Pad Length(8)?] [R(1) | Promised Stream ID(31)] [Header Block Fragment(*)] [Padding(*)?]`. Flags: END_HEADERS (0x4), PADDED (0x8). StreamID MUST != 0; promised stream ID MUST be even and unused.

Per spec §1 non-goals: client never uses PUSH_PROMISE in production. Encoder is included for codec symmetry and decoder testing.

**Commit:** `feat(frame): PUSH_PROMISE frame encode/decode (RFC §6.6)`

---

### Task 35: `frame/framer.go` — Framer struct and ReadFrame dispatcher

**Files:**
- Create: `frame/framer.go`
- Create: `frame/framer_test.go`

This is the integration point that ties all per-type frames together.

- [ ] **Step 1: Write integration test (round-trip every frame type)**

Create `frame/framer_test.go` with a fixture that exercises each frame type via `WriteX` then `ReadFrame`, asserting the visitor receives correct values.

```go
package frame

import (
	"bytes"
	"context"
	"testing"
)

type recordingHandler struct {
	header   FrameHeader
	settings SettingsParams
	hb       []byte
	prio     *Priority
	padLen   uint8
	data     []byte
	pingData [8]byte
	lastID   uint32
	code     ErrCode
	debug    []byte
	inc      uint32
	prom     uint32
}

func (h *recordingHandler) OnData(hdr FrameHeader, p []byte, pad uint8) error {
	h.header = hdr
	h.data = append(h.data[:0], p...)
	h.padLen = pad
	return nil
}
// ... implement all OnX methods similarly ...

func TestFramer_WriteData_ReadFrame_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	if err := fr.WriteData(1, true, []byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("read: %v", err)
	}
	if h.header.Type != FrameData || h.header.StreamID != 1 || h.header.Flags&FlagDataEndStream == 0 {
		t.Fatalf("hdr: %+v", h.header)
	}
	if string(h.data) != "hello" {
		t.Fatalf("data: %q", h.data)
	}
}

// TestFramer_RoundTrip_AllTypes — exercises every WriteX with a
// matching OnX assertion. Long but mechanical.
```

- [ ] **Step 2: Run — fails**

- [ ] **Step 3: Implement**

Create `frame/framer.go`:

```go
package frame

import (
	"context"
	"errors"
	"io"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

const defaultMaxFrameSize uint32 = 16384

// Handler is a visitor for received frames. Slices passed to On* methods
// are valid only for the duration of the call; copy if you must retain.
type Handler interface {
	OnData(h FrameHeader, payload []byte, padLen uint8) error
	OnHeaders(h FrameHeader, hb HeaderBlock, prio *Priority, padLen uint8) error
	OnPriority(h FrameHeader, p Priority) error
	OnRSTStream(h FrameHeader, code ErrCode) error
	OnSettings(h FrameHeader, s SettingsParams) error
	OnPushPromise(h FrameHeader, promisedID uint32, hb HeaderBlock, padLen uint8) error
	OnPing(h FrameHeader, opaqueData [8]byte) error
	OnGoAway(h FrameHeader, lastStreamID uint32, code ErrCode, debug []byte) error
	OnWindowUpdate(h FrameHeader, increment uint32) error
	OnContinuation(h FrameHeader, hb HeaderBlock) error
}

// Framer reads and writes HTTP/2 frames over an io.Reader / io.Writer.
// NOT goroutine-safe.
type Framer struct {
	w io.Writer
	r io.Reader

	maxReadFrameSize  uint32
	maxHeaderListSize uint32

	readBuf  []byte // pooled scratch for ReadFrame; reused across calls
	writeBuf []byte // scratch for header serialisation
}

// NewFramer constructs a Framer. w may be nil if only reading; r may be nil
// if only writing.
func NewFramer(w io.Writer, r io.Reader) *Framer {
	rb := bytesx.GetReadBuf(int(defaultMaxFrameSize) + FrameHeaderSize)
	wb := make([]byte, 0, 64)
	return &Framer{
		w:                w,
		r:                r,
		maxReadFrameSize: defaultMaxFrameSize,
		readBuf:          *rb,
		writeBuf:         wb,
	}
}

func (f *Framer) SetMaxReadFrameSize(n uint32)  { f.maxReadFrameSize = n }
func (f *Framer) SetMaxHeaderListSize(n uint32) { f.maxHeaderListSize = n }
func (f *Framer) SetReadBuffer(buf []byte)     { f.readBuf = buf }

// ReadFrame reads one frame and dispatches to h. Returns the FrameHeader
// for caller-side accounting; err is io.EOF on clean stream end.
func (f *Framer) ReadFrame(ctx context.Context, h Handler) (FrameHeader, error) {
	if f.r == nil {
		return FrameHeader{}, errors.New("poseidon/frame: Framer has no reader")
	}
	if cap(f.readBuf) < FrameHeaderSize {
		f.readBuf = make([]byte, FrameHeaderSize)
	}
	hdr := f.readBuf[:FrameHeaderSize]
	if _, err := io.ReadFull(f.r, hdr); err != nil {
		return FrameHeader{}, err
	}
	fh, err := ReadFrameHeader(hdr)
	if err != nil {
		return FrameHeader{}, err
	}
	if fh.Length > f.maxReadFrameSize {
		return fh, ErrFrameTooLarge
	}
	if cap(f.readBuf) < int(fh.Length) {
		f.readBuf = make([]byte, fh.Length)
	}
	payload := f.readBuf[:fh.Length]
	if _, err := io.ReadFull(f.r, payload); err != nil {
		return fh, err
	}

	switch fh.Type {
	case FrameData:
		return fh, f.dispatchData(fh, payload, h)
	case FrameHeaders:
		return fh, f.dispatchHeaders(fh, payload, h)
	case FramePriority:
		return fh, f.dispatchPriority(fh, payload, h)
	case FrameRSTStream:
		return fh, f.dispatchRSTStream(fh, payload, h)
	case FrameSettings:
		return fh, f.dispatchSettings(fh, payload, h)
	case FramePushPromise:
		return fh, f.dispatchPushPromise(fh, payload, h)
	case FramePing:
		return fh, f.dispatchPing(fh, payload, h)
	case FrameGoAway:
		return fh, f.dispatchGoAway(fh, payload, h)
	case FrameWindowUpdate:
		return fh, f.dispatchWindowUpdate(fh, payload, h)
	case FrameContinuation:
		return fh, f.dispatchContinuation(fh, payload, h)
	default:
		return fh, ErrUnknownFrameType
	}
}

// dispatchXxx handlers per type — implementations live in the per-type
// files (data.go etc) and are called by the Framer. For brevity, this plan
// shows the dispatch shape; each per-type task adds the needed helper.
//
// Example dispatcher (DATA):

func (f *Framer) dispatchData(fh FrameHeader, payload []byte, h Handler) error {
	if fh.StreamID == 0 {
		return ErrInvalidStreamID
	}
	var (
		data   = payload
		padLen uint8
		err    error
	)
	if fh.Flags&FlagDataPadded != 0 {
		data, padLen, err = bytesx.StripPadding(payload)
		if err != nil {
			return err
		}
	}
	return h.OnData(fh, data, padLen)
}

// (other dispatchers omitted from this code listing — implement per Tasks 26–34b)

// WriteClientPreface sends the connection preface (RFC 7540 §3.5).
func (f *Framer) WriteClientPreface() error {
	const preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	_, err := io.WriteString(f.w, preface)
	return err
}

// WriteData writes a DATA frame.
func (f *Framer) WriteData(streamID uint32, endStream bool, data []byte) error {
	if streamID == 0 {
		return ErrInvalidStreamID
	}
	if uint32(len(data)) > f.maxReadFrameSize {
		return ErrFrameTooLarge
	}
	flags := Flags(0)
	if endStream {
		flags |= FlagDataEndStream
	}
	return f.writeFrame(FrameHeader{Length: uint32(len(data)), Type: FrameData, Flags: flags, StreamID: streamID}, data)
}

// writeFrame writes an unpadded frame: header + payload. payload nil is OK.
func (f *Framer) writeFrame(h FrameHeader, payload []byte) error {
	var hdr [FrameHeaderSize]byte
	WriteFrameHeader(hdr[:], h)
	if _, err := f.w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := f.w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// (other WriteX methods follow the same pattern; one per per-type task)
```

- [ ] **Step 4: Run — passes**

- [ ] **Step 5: Commit**

```bash
git add frame/framer.go frame/framer_test.go
git commit -m "feat(frame): Framer w/ ReadFrame dispatch and core writers"
```

---

### Task 36: `frame/conformance_test.go` — RFC 7540 §4–§6 vectors

Mirror the HPACK conformance approach: `testdata/rfc7540/<name>.{hex,fields}` plus a `runFrameConformance` helper that parses fixtures and asserts Framer dispatch produces the expected callback sequence.

Naming convention: `TestConformance_RFC7540_§6_1_DataMinimal`, `…§6_2_HeadersWithPriority`, etc.

**Commit:** `test(frame): RFC 7540 §4–§6 conformance vectors`

---

### Task 37: `frame/bench_test.go` — bench gates

Mirror the spec §7.6 targets: `BenchmarkFramer_WriteData_1KB`, `BenchmarkFramer_WriteHeaders_minimal`, `BenchmarkFramer_ReadFrame_DATA_1KB`, `BenchmarkFramer_ReadFrame_HEADERS`. All must report `0 allocs/op`.

**Commit:** `test(frame): bench gates (write/read DATA/HEADERS)`

---

### Task 38: `frame/fuzz_test.go` — frame fuzz

Seed with: empty SETTINGS (`0x00 0x00 0x00 0x04 0x00 0x00 0x00 0x00 0x00`), one valid frame from each RFC vector. Fuzz checks that ReadFrame never panics and never loops forever (timeout-bounded by `go test`).

**Commit:** `test(frame): FuzzFramerReadFrame + seed corpus`

---

### Task 39: Milestone gate A2 — frame layer done

- [ ] **Step 1: Local verification**

```bash
make tidy && make lint && make test-race && make bench
```

- [ ] **Step 2: Push, observe CI**

- [ ] **Step 3: Tag (optional)**

```bash
git tag -a phase-a-m2-frame -m "Phase A milestone 2: frame layer complete"
git push origin phase-a-m2-frame
```

---

## CI gates and final tooling

### Task 40: bench-gate workflow + script

**Files:**
- Create: `.github/workflows/bench-gate.yml`
- Create: `scripts/bench-gate.sh`

- [ ] **Step 1: Create `scripts/bench-gate.sh`**

```bash
#!/usr/bin/env bash
# Parses benchstat -alpha 0.05 output. Fails on:
#   - any benchmark with allocs/op > 0 in HEAD when target is 0
#   - any benchmark with B/op > 0 when target is 0
#   - any benchmark whose ns/op increased >10% with statistical significance
set -euo pipefail
DIFF="${1:?diff.txt path required}"

if grep -E '\+[0-9]+\.[0-9]+%' "$DIFF" | awk '{print $NF}' | grep -E '^\+[1-9][0-9]\.|^\+[1-9][0-9][0-9]\.' >/dev/null; then
  echo "Bench regression > 10% detected"
  grep -E '\+[1-9][0-9]\.[0-9]+%' "$DIFF" || true
  exit 1
fi

# Look for non-zero allocs/op or B/op in HEAD.
if awk '$0 ~ /allocs\/op/ && $0 ~ /^Bench/ {if ($(NF-1) != "0" && $(NF-1) != "0.00") print}' "$DIFF" | grep .; then
  echo "Non-zero allocs/op detected in HEAD"
  exit 1
fi

echo "Bench gate OK"
```

`chmod +x scripts/bench-gate.sh`.

- [ ] **Step 2: Create `.github/workflows/bench-gate.yml`** (per spec §8.2)

(Use the YAML from the spec file verbatim.)

- [ ] **Step 3: Verify locally**

```bash
make bench > head.txt
git stash && make bench > base.txt && git stash pop
benchstat base.txt head.txt > diff.txt
./scripts/bench-gate.sh diff.txt
```

- [ ] **Step 4: Commit**

```bash
git add scripts/bench-gate.sh .github/workflows/bench-gate.yml
git commit -m "ci: add bench-gate workflow and gate script"
```

---

### Task 41: conformance-gate workflow + scripts

**Files:**
- Create: `.github/workflows/conformance-gate.yml`
- Create: `scripts/rfc-coverage-gate.sh`
- Create: `scripts/rfc-matrix-check.sh`
- Create: `docs/RFC_COVERAGE.md`

`docs/RFC_COVERAGE.md` is a checklist mapping RFC sections to test names. Initial form:

```markdown
# RFC Coverage Matrix

| Section | RFC | Test |
|---------|-----|------|
| §4.1    | RFC 7540 | TestReadFrameHeader_Sample, TestWriteFrameHeader |
| §6.1    | RFC 7540 | TestConformance_RFC7540_§6_1_* |
| §6.2    | RFC 7540 | TestConformance_RFC7540_§6_2_* |
| ...     | ...      | ... |
| §C.2.1  | RFC 7541 | TestConformance_RFC7541_§C_2_1_* |
| ...     | ...      | ... |
```

`scripts/rfc-coverage-gate.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
RFC="${1:?rfc.txt path required}"
# Confirm at least one Conformance_RFC7540 test ran and passed.
if ! grep -E 'PASS:.*TestConformance_RFC7540' "$RFC" >/dev/null; then
  echo "No RFC 7540 conformance tests passed"
  exit 1
fi
if ! grep -E 'PASS:.*TestConformance_RFC7541' "$RFC" >/dev/null; then
  echo "No RFC 7541 conformance tests passed"
  exit 1
fi
echo "RFC coverage gate OK"
```

`scripts/rfc-matrix-check.sh`:

```bash
#!/usr/bin/env bash
# Ensures every section listed in RFC_COVERAGE.md has at least one passing test.
set -euo pipefail
MATRIX="${1:?RFC_COVERAGE.md path required}"
TESTS="${2:?rfc.txt path required}"
miss=0
while IFS= read -r line; do
  case "$line" in
    "| §"*)
      sec=$(echo "$line" | awk -F'|' '{print $2}' | tr -d ' ')
      tests=$(echo "$line" | awk -F'|' '{print $4}')
      # Each test name in the matrix must appear in PASS rows.
      for t in $(echo "$tests" | tr ',' '\n' | tr -d ' *'); do
        if [ -z "$t" ]; then continue; fi
        if ! grep -E "PASS:.*${t}" "$TESTS" >/dev/null; then
          echo "Missing pass for $sec → $t"
          miss=$((miss+1))
        fi
      done
      ;;
  esac
done < "$MATRIX"
if [ "$miss" -gt 0 ]; then
  echo "RFC matrix check failed: $miss missing"
  exit 1
fi
echo "RFC matrix OK"
```

(`chmod +x` both scripts.)

`.github/workflows/conformance-gate.yml`: per spec §8.3.

**Commit:** `ci: add conformance-gate workflow + RFC coverage matrix`

---

### Task 42: nightly fuzz workflow

**Files:**
- Create: `.github/workflows/nightly.yml`

Per spec §8.4. **Commit:** `ci: add nightly fuzz workflow`

---

### Task 43: pre-commit hook

**Files:**
- Create: `.githooks/pre-commit`

```bash
#!/usr/bin/env bash
set -e
go vet ./...
go test -short -race ./...
golangci-lint run
```

`chmod +x .githooks/pre-commit`. Add a section to README (already present from Task 4) describing `git config core.hooksPath .githooks`.

**Commit:** `chore: add opt-in pre-commit hook`

---

### Task 44: `BENCH_BASELINE.md`

**Files:**
- Create: `docs/BENCH_BASELINE.md`

Run `make bench` on the reference machine (note: GitHub-hosted runners are noisy; for an authoritative baseline, run on a local machine with consistent CPU governor settings) and capture the output along with the machine specifications.

Template:

```markdown
# Bench Baseline

**Machine:** [model, CPU, RAM, OS, Go version]
**Date:** YYYY-MM-DD
**Command:** `make bench`

## Results

```
goos: linux
goarch: amd64
pkg: github.com/lodgvideon/poseidon-http-client/hpack
BenchmarkHPACK_EncodeBlock_3req_static-8       12345678         95.0 ns/op   0 B/op   0 allocs/op
...
```
```

**Commit:** `docs: add BENCH_BASELINE.md`

---

### Task 45: README finalisation

**Files:**
- Modify: `README.md`

Add a "Usage" section showing how to encode and decode a HEADERS block plus a DATA frame using the public API, with explicit note that this is Phase A (codec only — no networking).

**Commit:** `docs: README usage example`

---

### Task 46: Milestone gate A3 — Phase A acceptance

- [ ] **Step 1: Verify all acceptance criteria from spec §11**

Run the full local suite:

```bash
make tidy
make lint
make test-race
make coverage   # ≥ 90% per package
make bench
go test -fuzz=Fuzz -fuzztime=2m ./...   # short fuzz; nightly handles long
```

All green; coverage ≥ 90%; benches all `0 allocs/op`.

- [ ] **Step 2: Verify branch protection + green CI**

Push branch; ensure all five required checks (`ci/lint`, `ci/test`, `ci/fuzz-corpus-replay`, `bench-gate/bench`, `conformance-gate/rfc`) are green.

- [ ] **Step 3: Tag pre-release**

```bash
git tag -a v0.1.0-rc1 -m "Phase A: frame layer + HPACK release candidate"
git push origin v0.1.0-rc1
```

- [ ] **Step 4: Open PR to main**

```bash
gh pr create --title "Phase A: frame layer + HPACK" --body "$(cat <<'EOF'
## Summary
- Implements RFC 7540 frame codec and RFC 7541 HPACK from scratch.
- Zero-allocation hot path: all benchmarks 0 allocs/op.
- Self-contained library; no networking (Phase B will add).

## Test plan
- [x] make test-race
- [x] make bench (all gates green)
- [x] conformance-gate green (RFC 7540 §4–§6, RFC 7541 §C.2–§C.6)
- [x] nightly fuzz: no crashes for 7 days
EOF
)"
```

After review/merge, delete the branch and start Phase B planning.

---

## Self-Review

Spec coverage:

- Spec §1 Context/Goals → covered by overall plan structure (Phases A/B/C decomposition); Phase A scope locked.
- Spec §2 Architecture → Tasks 5–8 (`bytesx`), 9–22 (`hpack`), 24–35 (`frame`) reflect the slot diagram; SOLID constraints surface in API decisions (visitor `Handler`, separate `Encoder`/`Decoder` types, `internal/` for private helpers).
- Spec §3 Package layout → Task 1+ scaffold matches the layout exactly.
- Spec §4 frame public API → Tasks 24, 25, 35 land FrameType/Flags/FrameHeader, Handler, all Write* methods. Per-frame Tasks 26–34b cover individual encode/decode paths.
- Spec §5 HPACK sub-design → Tasks 9–19 cover all 5 sub-units (errors, integer, Huffman, static, dynamic, string literal, encoder, decoder).
- Spec §6 zero-alloc patterns → Each implementation task includes a bench step verifying `0 allocs/op`; Task 22 / 37 are the consolidated bench gates; Task 8 demonstrates the pool pattern.
- Spec §7 testing strategy → TDD red-green-refactor in every task; Tasks 20, 21, 22, 36, 37, 38 cover conformance/fuzz/bench.
- Spec §8 CI pipelines → Tasks 3 (ci.yml), 40 (bench-gate), 41 (conformance-gate), 42 (nightly), 43 (pre-commit).
- Spec §9 Phase B/C forward-compat → maintained implicitly by following the API contracts in this plan; no separate task needed.
- Spec §10 Open Questions → reference machine for benches deferred to Task 44.
- Spec §11 Acceptance criteria → Task 46 milestone gate enumerates each item.

Placeholder scan: I've used "// (other ... follow the same pattern)" comments in the framer code listing for brevity in this plan; in the actual implementation each per-type WriteX/dispatchX must be written by Tasks 26–34b. Acceptable as the per-type tasks own that work — not a placeholder leak.

The Huffman FSM optimisation is explicitly deferred (Task 13 step 5) with a clear gate: bench gate enforces `0 allocs/op` (already met by the linear walk). If absolute ns/op targets fall short, a follow-up plan revisits FSM. This is a known trade-off, not a placeholder.

Type consistency: `HeaderField`, `FieldVisitor`, `Encoder`, `Decoder`, `FrameHeader`, `Handler`, `Framer`, `WriteHeadersParams`, `SettingsParams`, `Priority`, `ErrCode`, `Flags` consistent across spec and plan. Streaming Decoder API matches the spec exactly: `Begin()` / `Feed(fragment, visit) error` / `Finish() error`. Task 19 implements `Feed` to emit complete representations incrementally and buffer truncated tails; `Finish` validates the buffer is drained.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-02-poseidon-frame-layer-phase-a.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
