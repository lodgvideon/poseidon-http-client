package grpc

import (
	"context"
	"testing"
)

// unaryTransportWrites is how many Write calls one unary RPC currently makes on
// the transport:
//
//	1. HEADERS          — the request header block
//	2. DATA             — the request message
//	3. DATA(END_STREAM) — the empty half-close frame CloseSend sends
//
// Each is a separate syscall, a separate TLS record (~22 bytes of record header
// and AEAD tag on top of the payload), and, since Go enables TCP_NODELAY,
// usually a separate segment. For a small RPC the record overhead alone is
// comparable to the payload, which is why this is gated rather than left to a
// profiler.
//
// Lower it when the send path is changed to emit fewer — the constant is the
// record of the win, and this test failing low is the reminder to update it.
const unaryTransportWrites = 3

// writeCountSlack absorbs the occasional WINDOW_UPDATE. unaryWriteCountRPCs is
// kept small enough that the connection-level refund threshold (32 KiB) is
// never reached by the echoed payload, so in practice none fires.
const writeCountSlack = 2

// unaryWriteCountRPCs is the sample size. Small on purpose — see
// writeCountSlack.
const unaryWriteCountRPCs = 200

// TestUnaryTransportWriteCount pins how many transport writes a unary RPC
// costs. It is a benchmark metric expressed as a test so it runs in CI: the
// benchmarks report ns/op and allocs, neither of which moves much when three
// small syscalls become one, because the cost lands on the wire and on the
// kernel rather than in the Go heap.
func TestUnaryTransportWriteCount(t *testing.T) {
	var wc writeCounter
	cc := dialMockPeer(t, newMockGRPCPeer(t), &wc)
	ctx := context.Background()

	// Warm up past the handshake and the cold HPACK table, then zero the
	// counter so only steady-state RPCs are measured.
	if _, err := cc.Invoke(ctx, "/bench.Svc/Echo", []byte("hello"), nil); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	wc.writes.Store(0)
	wc.bytes.Store(0)

	for i := 0; i < unaryWriteCountRPCs; i++ {
		if _, err := cc.Invoke(ctx, "/bench.Svc/Echo", []byte("hello"), nil); err != nil {
			t.Fatalf("Invoke %d: %v", i, err)
		}
	}

	writes := wc.writes.Load()
	perRPC := float64(writes) / unaryWriteCountRPCs
	t.Logf("unary RPC: %.3f transport writes, %.1f bytes (n=%d)",
		perRPC, float64(wc.bytes.Load())/unaryWriteCountRPCs, unaryWriteCountRPCs)

	if want := int64(unaryTransportWrites) * unaryWriteCountRPCs; writes > want+writeCountSlack {
		t.Errorf("unary RPC costs %.3f transport writes, want at most %d: the send path "+
			"regressed, or a frame is being flushed that used to ride along with another",
			perRPC, unaryTransportWrites)
	} else if writes < want-writeCountSlack {
		t.Errorf("unary RPC costs %.3f transport writes, fewer than the recorded %d: "+
			"the send path improved — lower unaryTransportWrites to %.0f to lock the win in",
			perRPC, unaryTransportWrites, perRPC)
	}
}
