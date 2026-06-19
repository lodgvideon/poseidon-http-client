# CONTINUATION write-path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split oversized HPACK header blocks into one HEADERS frame plus N CONTINUATION frames in the conn layer (RFC 7540 §6.2 / §6.10), so requests with large header sets transmit instead of failing with `ErrFrameTooLarge` / peer `FRAME_SIZE_ERROR`.

**Architecture:** Add a private `writeHeaderBlock` helper to `conn.Conn` that emits HEADERS + CONTINUATION under the already-held `c.wmu`. `writeHeadersWithPriority` calls it instead of inlining a single `WriteHeaders`. A second helper `maxOutFrameSize` computes the chunk bound `min(peer, our)` (floor 16384), mirroring `writeData`. No interface/signature changes; no frame-layer changes.

**Tech Stack:** Go 1.24, `frame.Framer` codec, `hpack.Encoder`, `go test -race`.

Spec: `docs/superpowers/specs/2026-06-19-continuation-write-path-design.md`

---

### Task 1: Split mechanism + unit conformance tests

**Files:**
- Create: `conn/continuation_test.go`
- Modify: `conn/conn.go` (replace the `WriteHeaders` call inside `writeHeadersWithPriority`, ~conn.go:439-450; add two helper methods after `writeHeadersWithPriority`)

**Background the implementer needs:**
- `writeHeadersWithPriority` already holds `c.wmu` (write mutex) for its whole body and already reads `c.psMu` under `wmu` (the `s.id == 0` branch), so reading peer settings under `wmu` is an established, safe lock order here.
- `frame.WriteHeadersParams` fields: `StreamID`, `BlockFragment`, `EndHeaders`, `EndStream`, `PadLength uint8`, `Priority *frame.Priority`.
- `frame.Framer.WriteContinuation(streamID uint32, endHeaders bool, blockFragment []byte) error`.
- The framer's per-payload overhead (framer.go:215-221): `+1` byte when `PadLength>0`, `+PadLength`, `+5` when `Priority!=nil`.
- `c.opts.Padding.ForHeaders()` returns the per-HEADERS pad length (`uint8`, 0 when disabled).
- `settingValue(params, id, default)` reads a setting with fallback.
- Existing wire-byte test pattern: build a `*Conn` struct literal with `fr` wired to a `bytes.Buffer` (see `conn/sendflow_test.go:173` `TestConn_WriteData_ChunksByPeerMaxFrameSize`).
- Existing frame-header parser to reuse: `parseFrameHeaders(t, b)` returns `[]frameHeaderRecord{length, ftype, flags, streamID}` (`conn/dynsettings_test.go:200`).
- Frame constants: `frame.FrameHeaders` (0x1), `frame.FrameContinuation` (0x9); flag bytes `frame.FlagHeadersEndHeaders` (0x4), `frame.FlagHeadersEndStream` (0x1), `frame.FlagHeadersPadded` (0x8), `frame.FlagHeadersPriority` (0x20), `frame.FlagContinuationEndHeaders` (0x4).

- [ ] **Step 1: Write the failing unit tests**

Create `conn/continuation_test.go`:

```go
package conn

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// newWireConn builds a *Conn whose framer writes to buf, with a fixed
// peer MAX_FRAME_SIZE, just enough to exercise writeHeadersWithPriority's
// wire output without a live peer.
func newWireConn(buf *bytes.Buffer, peerMaxFrame uint32) *Conn {
	fr := frame.NewFramer(buf, bytes.NewReader(nil))
	c := &Conn{
		opts:    ConnOptions{}.defaulted(),
		fr:      fr,
		enc:     hpack.NewEncoder(),
		streams: map[uint32]*Stream{},
		peerSettings: frame.SettingsParams{
			Pairs: [16]frame.SettingPair{
				{ID: frame.SettingMaxFrameSize, Value: peerMaxFrame},
			},
			N: 1,
		},
	}
	return c
}

// blockFrame is a parsed HEADERS/CONTINUATION frame with its payload.
type blockFrame struct {
	ftype   byte
	flags   byte
	payload []byte
}

// parseBlockFrames walks a raw frame stream and returns HEADERS (0x1)
// and CONTINUATION (0x9) frames with payloads. Other frame types fail
// the test (the write path under test emits only these).
func parseBlockFrames(t *testing.T, b []byte) []blockFrame {
	t.Helper()
	var out []blockFrame
	for len(b) >= 9 {
		length := int(b[0])<<16 | int(b[1])<<8 | int(b[2])
		ftype := b[3]
		flags := b[4]
		payload := b[9 : 9+length]
		out = append(out, blockFrame{ftype: ftype, flags: flags, payload: payload})
		b = b[9+length:]
	}
	return out
}

// bigFields returns a pseudo-header set plus n filler headers whose
// values are valSize bytes each, producing an HPACK block large enough
// to force a CONTINUATION split.
func bigFields(n, valSize int) []hpack.HeaderField {
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	val := bytes.Repeat([]byte("x"), valSize)
	for i := 0; i < n; i++ {
		name := []byte("x-big-" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
		fields = append(fields, hpack.HeaderField{Name: name, Value: append([]byte(nil), val...)})
	}
	return fields
}

func TestConformance_RFC7540_Sec6_2_HeadersSplitIntoContinuation(t *testing.T) {
	var buf bytes.Buffer
	c := newWireConn(&buf, 256) // small peer frame → force split
	s := newStream(0, 8, c, 65535)
	c.nextID = 1

	fields := bigFields(20, 60) // encoded block well over 256 bytes
	if err := c.writeHeadersWithPriority(context.Background(), s, fields, true, nil); err != nil {
		t.Fatalf("writeHeadersWithPriority: %v", err)
	}

	frames := parseBlockFrames(t, buf.Bytes())
	if len(frames) < 2 {
		t.Fatalf("got %d frames, want HEADERS + >=1 CONTINUATION", len(frames))
	}
	// Frame 1: HEADERS, END_HEADERS unset, END_STREAM set.
	if frames[0].ftype != byte(frame.FrameHeaders) {
		t.Fatalf("frame 0 type = %d, want HEADERS", frames[0].ftype)
	}
	if frames[0].flags&byte(frame.FlagHeadersEndHeaders) != 0 {
		t.Fatalf("frame 0 must NOT set END_HEADERS when split")
	}
	if frames[0].flags&byte(frame.FlagHeadersEndStream) == 0 {
		t.Fatalf("frame 0 must carry END_STREAM")
	}
	// Frames 2..n: CONTINUATION; only the last sets END_HEADERS.
	for i := 1; i < len(frames); i++ {
		if frames[i].ftype != byte(frame.FrameContinuation) {
			t.Fatalf("frame %d type = %d, want CONTINUATION", i, frames[i].ftype)
		}
		endH := frames[i].flags&byte(frame.FlagContinuationEndHeaders) != 0
		if i == len(frames)-1 && !endH {
			t.Fatalf("last CONTINUATION must set END_HEADERS")
		}
		if i != len(frames)-1 && endH {
			t.Fatalf("non-final CONTINUATION %d must NOT set END_HEADERS", i)
		}
	}
	// Reassembled block must decode back to the original fields.
	var block []byte
	for _, f := range frames {
		block = append(block, f.payload...)
	}
	dec := hpack.NewDecoder()
	count := 0
	if err := dec.DecodeBlock(block, func(hpack.HeaderField) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("DecodeBlock reassembled block: %v", err)
	}
	if count != len(fields) {
		t.Fatalf("decoded %d fields, want %d", count, len(fields))
	}
}

func TestConformance_RFC7540_Sec6_10_ContinuationFlagsAndPadding(t *testing.T) {
	var buf bytes.Buffer
	c := newWireConn(&buf, 256)
	c.opts.Padding = fixedPadding(8) // padding only legal on HEADERS
	s := newStream(0, 8, c, 65535)
	c.nextID = 1

	prio := &frame.Priority{StreamDep: 0, Weight: 15}
	fields := bigFields(20, 60)
	if err := c.writeHeadersWithPriority(context.Background(), s, fields, false, prio); err != nil {
		t.Fatalf("writeHeadersWithPriority: %v", err)
	}

	frames := parseBlockFrames(t, buf.Bytes())
	if len(frames) < 2 {
		t.Fatalf("got %d frames, want split", len(frames))
	}
	if frames[0].flags&byte(frame.FlagHeadersPadded) == 0 {
		t.Fatalf("HEADERS frame must carry PADDED")
	}
	if frames[0].flags&byte(frame.FlagHeadersPriority) == 0 {
		t.Fatalf("HEADERS frame must carry PRIORITY")
	}
	for i := 1; i < len(frames); i++ {
		// CONTINUATION has no PADDED/PRIORITY flag bits defined; assert
		// the HEADERS-specific bits are clear on every CONTINUATION.
		if frames[i].flags&byte(frame.FlagHeadersPadded) != 0 {
			t.Fatalf("CONTINUATION %d must not be padded", i)
		}
		if frames[i].flags&byte(frame.FlagHeadersPriority) != 0 {
			t.Fatalf("CONTINUATION %d must not carry priority", i)
		}
	}
}

func TestConn_WriteHeaders_BlockFits_SingleFrame(t *testing.T) {
	var buf bytes.Buffer
	c := newWireConn(&buf, 16384) // default-sized frame
	s := newStream(0, 8, c, 65535)
	c.nextID = 1

	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	if err := c.writeHeadersWithPriority(context.Background(), s, fields, true, nil); err != nil {
		t.Fatalf("writeHeadersWithPriority: %v", err)
	}
	frames := parseBlockFrames(t, buf.Bytes())
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1 (no split for small block)", len(frames))
	}
	if frames[0].ftype != byte(frame.FrameHeaders) {
		t.Fatalf("frame type = %d, want HEADERS", frames[0].ftype)
	}
	if frames[0].flags&byte(frame.FlagHeadersEndHeaders) == 0 {
		t.Fatalf("single HEADERS frame must set END_HEADERS")
	}
}

// fixedPadding returns a PaddingStrategy that always pads HEADERS by n.
// Min==Max==n makes ForHeaders() deterministic (PadBytes returns n).
func fixedPadding(n uint8) PaddingStrategy {
	return PaddingStrategy{Min: n, Max: n}
}
```

(Types verified against `conn/padding.go` and `hpack/decoder.go`: `PaddingStrategy{Min,Max uint8, DataOnly bool}`, `ForHeaders()` returns `n` when `Min==Max==n`; the decoder API is `DecodeBlock(block []byte, func(hpack.HeaderField) error) error`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./conn/ -run 'TestConformance_RFC7540_Sec6_2_HeadersSplitIntoContinuation|TestConformance_RFC7540_Sec6_10_ContinuationFlagsAndPadding|TestConn_WriteHeaders_BlockFits_SingleFrame' -v`

Expected: the two split tests FAIL — current code emits a single HEADERS frame (`len(frames) < 2`), so the "want split" assertions fail. `TestConn_WriteHeaders_BlockFits_SingleFrame` PASSES already (fast path matches current behavior).

- [ ] **Step 3: Implement the split in `conn/conn.go`**

Replace the body of `writeHeadersWithPriority` from the `buf := encBufPool.Get()...` block through the `WriteHeaders` call (conn.go ~439-450) so it delegates to a new helper. The final shape of the tail of `writeHeadersWithPriority`:

```go
	buf := encBufPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	block := c.enc.EncodeBlock(*buf, fields)

	err := c.writeHeaderBlock(s.id, block, endStream, prio)

	*buf = block[:0]
	encBufPool.Put(buf)
	if err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}
```

Add these two methods immediately after `writeHeadersWithPriority`:

```go
// maxOutFrameSize returns the largest frame payload we may emit: the
// minimum of the peer's advertised SETTINGS_MAX_FRAME_SIZE and our own,
// floored at the RFC default (16384). Mirrors the bound used by writeData.
// Caller may hold c.wmu; this takes c.psMu (the established wmu→psMu order).
func (c *Conn) maxOutFrameSize() int {
	c.psMu.RLock()
	peerMax := settingValue(c.peerSettings, frame.SettingMaxFrameSize, 16384)
	c.psMu.RUnlock()
	maxFrame := int(peerMax)
	if ourMax := int(c.opts.Settings.MaxFrameSize); ourMax < maxFrame {
		maxFrame = ourMax
	}
	if maxFrame <= 0 {
		maxFrame = 16384
	}
	return maxFrame
}

// writeHeaderBlock emits the encoded HPACK block as a HEADERS frame
// followed by zero or more CONTINUATION frames when it exceeds one frame's
// payload budget (RFC 7540 §6.2 / §6.10). The caller MUST hold c.wmu so the
// HEADERS+CONTINUATION run is contiguous (RFC §6.10: no interleaving).
// END_STREAM and padding/priority ride the HEADERS frame only; END_HEADERS
// rides the final frame.
func (c *Conn) writeHeaderBlock(streamID uint32, block []byte, endStream bool, prio *frame.Priority) error {
	maxFrame := c.maxOutFrameSize()
	padLen := c.opts.Padding.ForHeaders()

	budget0 := maxFrame
	if padLen > 0 {
		budget0 -= 1 + int(padLen)
	}
	if prio != nil {
		budget0 -= 5
	}
	if budget0 <= 0 {
		budget0 = 1
	}

	// Single-frame fast path: byte-identical to the pre-split behavior.
	if len(block) <= budget0 {
		return c.fr.WriteHeaders(frame.WriteHeadersParams{
			StreamID:      streamID,
			BlockFragment: block,
			EndHeaders:    true,
			EndStream:     endStream,
			PadLength:     padLen,
			Priority:      prio,
		})
	}

	// Frame 1: HEADERS with END_HEADERS unset; padding/priority/END_STREAM
	// ride here only.
	if err := c.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      streamID,
		BlockFragment: block[:budget0],
		EndHeaders:    false,
		EndStream:     endStream,
		PadLength:     padLen,
		Priority:      prio,
	}); err != nil {
		return err
	}

	// Frames 2..n: CONTINUATION; only the last sets END_HEADERS.
	rest := block[budget0:]
	for len(rest) > 0 {
		n := len(rest)
		if n > maxFrame {
			n = maxFrame
		}
		last := n == len(rest)
		if err := c.fr.WriteContinuation(streamID, last, rest[:n]); err != nil {
			return err
		}
		rest = rest[n:]
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./conn/ -run 'TestConformance_RFC7540_Sec6_2_HeadersSplitIntoContinuation|TestConformance_RFC7540_Sec6_10_ContinuationFlagsAndPadding|TestConn_WriteHeaders_BlockFits_SingleFrame' -race -v`

Expected: all three PASS.

- [ ] **Step 5: Run the full conn + frame suite to confirm no regression**

Run: `go test ./conn/ ./frame/ -race -count=1`

Expected: `ok` for both. (Existing HEADERS-emitting tests still see single frames because their blocks are small.)

- [ ] **Step 6: Commit**

```bash
git add conn/conn.go conn/continuation_test.go
git commit -m "feat(conn): split oversized HEADERS into CONTINUATION"
```

---

### Task 2: h2 integration test — large headers round-trip

**Files:**
- Modify: `conn/continuation_test.go` (append the integration test)

**Background the implementer needs:**
- Helpers in `conn/integration_test.go`: `startH2TestServer(t, http.Handler) (*httptest.Server, *tls.Config)` and `dialServer(t, srv, cfg) *Conn`.
- Go's `net/http2.Server` advertises `SETTINGS_MAX_FRAME_SIZE = 16384` by default, so a request block > 16384 forces the client to split; the server reassembles CONTINUATION frames transparently.
- Stream API: `c.NewStream(ctx)`, `s.SendHeaders(ctx, fields, endStream)`, `s.Recv(ctx)` returning a `StreamEvent` with `.Type == EventHeaders` and `.Headers []hpack.HeaderField`.

- [ ] **Step 1: Write the integration test**

Append to `conn/continuation_test.go` (add `"net/http"`, `"strconv"`, `"time"` to the import block):

```go
func TestIntegration_LargeHeaders_SplitAcrossContinuation(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := 0
		for name := range r.Header {
			if len(name) >= 6 && name[:6] == "X-Big-" {
				count++
			}
		}
		w.Header().Set("X-Recv-Count", strconv.Itoa(count))
		w.WriteHeader(200)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	// ~40 headers of ~600 bytes each → block well over one 16384 frame.
	fields := bigFields(40, 600)
	if err := s.SendHeaders(ctx, fields, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}

	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("event type = %v, want EventHeaders", ev.Type)
	}
	var status, recv string
	for _, f := range ev.Headers {
		switch string(f.Name) {
		case ":status":
			status = string(f.Value)
		case "x-recv-count":
			recv = string(f.Value)
		}
	}
	if status != "200" {
		t.Fatalf("status = %q, want 200 (CONTINUATION reassembly failed?)", status)
	}
	if recv != "40" {
		t.Fatalf("server received x-recv-count=%q big headers, want 40", recv)
	}
}
```

- [ ] **Step 2: Run the integration test**

Run: `go test ./conn/ -run TestIntegration_LargeHeaders_SplitAcrossContinuation -race -v`

Expected: PASS. If `x-recv-count` mismatches because Go canonicalizes header names differently, adjust the prefix check to compare case-insensitively (`strings.HasPrefix(strings.ToLower(name), "x-big-")`) and re-run.

- [ ] **Step 3: Commit**

```bash
git add conn/continuation_test.go
git commit -m "test(conn): large-header CONTINUATION round-trip"
```

---

### Task 3: RFC trace rows + final verification

**Files:**
- Modify: `docs/RFC_COVERAGE.md` (add two rows in the conn integration matrix, after the §8.1.x rows near line 95)

- [ ] **Step 1: Add the conformance rows**

In `docs/RFC_COVERAGE.md`, under the `### B.1 / B.2.1 ...` connection-layer matrix (the table whose header is `| Section | Type | Test |`), add these two rows (keep the existing column alignment style):

```
| §6.2    | Conformance | TestConformance_RFC7540_Sec6_2_HeadersSplitIntoContinuation (conn) — oversized HEADERS block split into HEADERS+CONTINUATION |
| §6.10   | Conformance | TestConformance_RFC7540_Sec6_10_ContinuationFlagsAndPadding (conn) — padding/priority only on HEADERS, END_HEADERS only on final CONTINUATION |
```

- [ ] **Step 2: Verify the conformance gate still passes**

Run: `bash scripts/rfc-coverage-gate.sh` (if present) — else: `go test ./conn/ -run 'TestConformance_RFC7540' -count=1`

Expected: PASS (at least one `TestConformance_RFC7540_*` and `TestConformance_RFC7541_*` green; no conformance failures).

- [ ] **Step 3: Full race suite + lint + bench gate**

Run: `go test ./... -race -count=1`
Expected: all packages `ok`.

Run: `make lint`
Expected: 0 issues. (If `golangci-lint` is unavailable in the environment, note it and skip — do not fake the result.)

Run: `go test ./frame/ ./hpack/ -run '^$' -bench=. -benchmem -count=1`
Expected: frame + hpack hot-path benches still report `0 B/op  0 allocs/op` (the codec was not touched, so this must be unchanged).

- [ ] **Step 4: Commit**

```bash
git add docs/RFC_COVERAGE.md
git commit -m "docs: RFC 7540 6.2/6.10 CONTINUATION coverage rows"
```

---

## Self-Review

**1. Spec coverage:**
- Mechanism (HEADERS + CONTINUATION, flag rules) → Task 1 Step 3. ✓
- Budget `min(peer,our)` floor 16384, frame-1 overhead subtraction → `maxOutFrameSize` + `budget0` in Task 1 Step 3. ✓
- `budget0 <= 0` fallback → Task 1 Step 3. ✓
- Single-frame zero-change fast path → Task 1 Step 3 + `TestConn_WriteHeaders_BlockFits_SingleFrame`. ✓
- Unit wire-byte tests (sequence, flags, boundary) → Task 1. ✓
- h2 integration round-trip → Task 2. ✓
- RFC_COVERAGE rows §6.2/§6.10 → Task 3. ✓
- 0-alloc codec gate unchanged → Task 3 Step 3. ✓
- No interface/signature change → confirmed: only a private helper added, `writeHeadersWithPriority` signature unchanged. ✓

**2. Placeholder scan:** Steps 1a and Task 2 Step 2 flag *verification* points (PaddingStrategy field names, decoder method name, header canonicalization) with concrete fallback instructions — these are guarded adaptations, not placeholders. All code blocks are complete.

**3. Type consistency:** `writeHeaderBlock(streamID uint32, block []byte, endStream bool, prio *frame.Priority)` and `maxOutFrameSize() int` are referenced identically in the call site and definition. `bigFields`, `parseBlockFrames`, `blockFrame`, `newWireConn`, `fixedPadding` are each defined once in Task 1 and reused in Task 2. Test names match exactly between tasks and the RFC_COVERAGE rows.

**Known adaptation risks (call out, don't hide):**
- `PaddingStrategy` literal in `fixedPadding` is a best-guess; Step 1a verifies and corrects it against `conn/padding.go`.
- `hpack` decode method name (`DecodeFull`) verified in Step 1a.
- Header-name canonicalization in the integration assertion has a documented fallback (Task 2 Step 2).
