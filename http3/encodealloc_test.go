//go:build !race

package http3

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// encodeHeadersAllocCeiling is what one static-only request-header encode costs,
// and both directions are errors. Above it, a per-request allocation came back;
// below it, the path improved and the win is not locked in until this drops.
//
// This is the path EVERY request takes against a server advertising
// SETTINGS_QPACK_MAX_TABLE_CAPACITY: 0, and every request takes until the server's
// SETTINGS arrive, on a client whose target user is a load generator.
//
// It cannot be a benchmark: ./http3 is one of the seven packages under the
// absolute zero-alloc bench-gate (.github/workflows/bench-gate.yml), which fails
// any Benchmark line with non-zero allocs/op. A Test emits no B/op columns, so
// this is the shape that fits — the same reason conn/client/grpc/http1 use
// AllocsPerRun gates. BenchmarkEncodeRequestHeaders_StaticOnly measures the same
// path for timing and is env-guarded behind POSEIDON_BENCH_ENCODE.
//
// What the remaining ten are, so nobody hunts for them twice — attributed with
// -memprofile, which is the only tool that answers this (-gcflags=-m answers
// whether escape analysis inlined something, not whether a line allocates):
//
//	~3.1  EncodeHeaders itself: the make([]header.Field, 0, 4+len(Headers)) field
//	      slice, plus []byte(r.Method)/Scheme/Authority/Path — converting a string
//	      to []byte copies by definition of the language, and these escape into
//	      the field slice. Removing them means header.Field holding strings.
//	~1.15 hpack.HuffmanEncode
//	~1.06 hpack.EncodeInteger
//	~1.06 AppendHeaders growing the frame from the nil dst it is handed
//	~0.3  appendV + EncodeFieldSection growth
//
// The four that are already gone were the pseudo-header NAMES: []byte(":method")
// and friends are compile-time constants that were rebuilt on every encode, and
// are now package-level (pseudoMethod etc. in request.go).
//
// The frame-growth ones (AppendHeaders, appendV) are NOT free to fix by pooling:
// sendRequest appends the DATA frame header to the returned buffer and documents
// that it may do so because "frame comes from EncodeHeaders(enc, nil, ...) fresh
// per request, so appending to it aliases nothing". Any pooling has to replace
// that argument, not quietly falsify it. Tracked in #626.
const encodeHeadersAllocCeiling = 10

func TestEncodeRequestHeaders_AllocsPerCall(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, defaultSettings)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	req := &Request{
		Method: "GET", Scheme: "https", Authority: "example.com", Path: "/",
		Headers: []header.Field{{Name: []byte("user-agent"), Value: []byte("poseidon/1")}},
	}

	// Encode once outside the count so any one-time lazy initialisation inside the
	// encoder is not charged to the steady state.
	if _, err := client.encodeRequestHeaders(req); err != nil {
		t.Fatalf("encodeRequestHeaders: %v", err)
	}
	if enc := client.qpackEncoder.Load(); enc != nil {
		t.Fatal("fixture is wrong: this gate must measure the static-only path, " +
			"but a dynamic encoder is installed")
	}

	got := testing.AllocsPerRun(2000, func() {
		if _, err := client.encodeRequestHeaders(req); err != nil {
			t.Fatalf("encodeRequestHeaders: %v", err)
		}
	})

	if int(got) > encodeHeadersAllocCeiling {
		t.Errorf("static header encode allocates %.0f/call, ceiling %d: a per-request "+
			"allocation came back on the path every request takes against a "+
			"capacity-0 server", got, encodeHeadersAllocCeiling)
	}
	if int(got) < encodeHeadersAllocCeiling {
		t.Errorf("static header encode allocates %.0f/call, below the ceiling of %d: "+
			"the path improved — lower encodeHeadersAllocCeiling to %.0f to lock the win in",
			got, encodeHeadersAllocCeiling, got)
	}
}
