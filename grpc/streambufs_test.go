package grpc

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// TestStreamBufs_NoContentLeaksBetweenRPCs is the reuse test the issue asks for,
// and the one the repository's history says to write: a buffer handed from one
// call to the next must never let the first call's bytes surface in the second.
//
// The first call is made to fail partway, which is the shape that leaves the
// most residue behind — a half-written send buffer and a decoder holding an
// incomplete message.
func TestStreamBufs_NoContentLeaksBetweenRPCs(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()

	// Call 1: send a distinctive payload, then abandon the stream early so its
	// buffers go back to the pool mid-conversation.
	first := bytes.Repeat([]byte{'A'}, 4096)
	s1, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil)
	if err != nil {
		t.Fatalf("NewStream 1: %v", err)
	}
	if err := s1.SendLast(ctx, first); err != nil && !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendLast 1: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Call 2 draws the same buffers.
	second := []byte("second")
	got, err := cc.Invoke(ctx, "/bench.Svc/Echo", second, nil)
	if err != nil {
		t.Fatalf("Invoke 2: %v", err)
	}
	if !bytes.Equal(got, second) {
		t.Fatalf("second call returned %q, want %q", got, second)
	}
	if bytes.ContainsRune(got, 'A') {
		t.Error("the first call's payload surfaced in the second call's response")
	}
}

// TestStreamBufs_ReusedAcrossManyCalls pins that the pool actually recycles:
// after a warm-up the send buffer stops being regrown from zero.
func TestStreamBufs_ReusedAcrossManyCalls(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	req := bytes.Repeat([]byte{'q'}, 2048)

	for i := 0; i < 8; i++ {
		if _, err := cc.Invoke(ctx, "/bench.Svc/Echo", req, nil); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	// A stream opened now must come up with capacity already in hand, which is
	// only true if a previous call returned it.
	s, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if cap(s.sendBuf) == 0 && cap(s.dec.buf) == 0 {
		t.Error("a fresh stream got no pooled capacity: nothing is being recycled")
	}
}

// TestStreamBufs_CloseIsTheOnlyReturn pins that the buffers go back exactly
// once. Close is sync.Once-guarded, so a second Close must not hand the same
// pair to the pool twice — two owners writing one buffer is the worst outcome
// this pool can produce.
func TestStreamBufs_CloseIsTheOnlyReturn(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s, err := cc.NewStream(context.Background(), "/bench.Svc/Echo", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if s.bufs == nil {
		t.Fatal("stream came up with no pooled buffers")
	}
	_ = s.Close()
	if s.bufs != nil {
		t.Error("Close left the pooled pair attached")
	}
	_ = s.Close() // must be inert
	if s.sendBuf != nil || s.dec.buf != nil {
		t.Error("Close left this stream pointing at buffers it no longer owns")
	}
}

// TestStreamBufs_StaleStreamCannotReachThem is why the Stream struct itself is
// NOT pooled.
//
// A pooled Stream would have to re-arm closed for its next owner, and a caller
// holding one from a finished RPC would then pass every guard and operate on the
// next call's stream — the shape of #370 one layer down. Leaving the struct
// alone makes closed a permanent latch, so a stale reference is refused forever
// and can never reach buffers the pool has since handed to someone else.
func TestStreamBufs_StaleStreamCannotReachThem(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()

	stale, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	_ = stale.Close()

	// Another RPC now owns whatever the pool handed on.
	if _, err := cc.Invoke(ctx, "/bench.Svc/Echo", []byte("live"), nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	for _, c := range []struct {
		name string
		call func() error
	}{
		{"Send", func() error { return stale.Send(ctx, []byte("x")) }},
		{"SendLast", func() error { return stale.SendLast(ctx, []byte("x")) }},
		{"CloseSend", func() error { return stale.CloseSend(ctx) }},
		{"Recv", func() error { _, err := stale.Recv(ctx); return err }},
		{"RecvInto", func() error { _, err := stale.RecvInto(ctx, nil); return err }},
		{"Header", func() error { _, err := stale.Header(ctx); return err }},
	} {
		if err := c.call(); !errors.Is(err, ErrStreamClosed) {
			t.Errorf("%s on a closed stream = %v, want ErrStreamClosed", c.name, err)
		}
	}
}

// TestStreamBufs_OversizeIsNotPooled pins the cap: one outlier response must not
// park its buffer in the pool for the life of the process.
func TestStreamBufs_OversizeIsNotPooled(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s, err := cc.NewStream(context.Background(), "/bench.Svc/Echo", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	// Simulate the outlier directly: grow both buffers past the cap, then close.
	s.sendBuf = make([]byte, 0, maxPooledStreamBuf+1)
	s.dec.buf = make([]byte, 0, maxPooledStreamBuf+1)
	b := s.bufs
	_ = s.Close()
	if b == nil {
		t.Fatal("no pooled pair to inspect")
	}
	if cap(b.send) > maxPooledStreamBuf || cap(b.dec) > maxPooledStreamBuf {
		t.Errorf("an oversize buffer was pooled: send cap %d, dec cap %d, limit %d",
			cap(b.send), cap(b.dec), maxPooledStreamBuf)
	}
}
