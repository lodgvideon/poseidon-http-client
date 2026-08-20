package grpc

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "NewStream 1")
	sendErr := s1.SendLast(ctx, first)
	require.Truef(t, sendErr == nil || errors.Is(sendErr, ErrStreamClosed), "SendLast 1: %v", sendErr)
	require.NoError(t, s1.Close(), "Close 1")

	// Call 2 draws the same buffers.
	second := []byte("second")
	got, err := cc.Invoke(ctx, "/bench.Svc/Echo", second, nil)

	require.NoError(t, err, "Invoke 2")
	require.Truef(t, bytes.Equal(got, second), "second call returned %q, want %q", got, second)
	assert.NotContains(t, string(got), "A",
		"the first call's payload surfaced in the second call's response")
}

// TestStreamBufs_ReleaseKeepsCapacity pins the mechanism the pool depends on:
// a released pair carries its capacity back, truncated to zero length.
//
// It deliberately does NOT assert that a later stream draws that same pair.
// sync.Pool does not promise Get returns anything previously Put — it may hand
// back a fresh one from New, and under -race its per-P caching behaves
// differently again. A test asserting otherwise passes on a good day and fails
// on a loaded machine, which is what the first version of this test did. The
// evidence that recycling actually happens is the allocation gate
// (TestInvokeInto_AllocsPerCall, 13 -> 10), which measures the outcome rather
// than guessing at the mechanism.
func TestStreamBufs_ReleaseKeepsCapacity(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	s, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil)
	require.NoError(t, err, "NewStream")
	// Grow both buffers the way a real call does, staying under the pool cap.
	// The decoder grows through Push rather than by assigning its field: which
	// field carries the pooled capacity is an internal detail that has already
	// moved once (dec.buf -> dec.own when the borrow path landed), and a test
	// that reaches for it breaks on the refactor instead of on the behaviour.
	s.sendBuf = append(s.sendBuf[:0], bytes.Repeat([]byte{'x'}, 2048)...)
	s.dec.Push(bytes.Repeat([]byte{'y'}, 4096))
	wantSend, wantDec := cap(s.sendBuf), cap(s.dec.own)
	b := s.bufs

	_ = s.Close()

	assert.Equalf(t, wantSend, cap(b.send),
		"released send buffer has capacity %d, want %d", cap(b.send), wantSend)
	assert.Equalf(t, wantDec, cap(b.dec),
		"released decoder buffer has capacity %d, want %d", cap(b.dec), wantDec)
	assert.Truef(t, len(b.send) == 0 && len(b.dec) == 0,
		"released buffers carry length %d/%d, want 0 — the next owner would read stale bytes",
		len(b.send), len(b.dec))
}

// TestStreamBufs_AcquireAttachesUsableBuffers pins the other half: a stream
// comes up with both buffers attached and empty, whether the pool recycled a
// pair or minted one.
func TestStreamBufs_AcquireAttachesUsableBuffers(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)

	s, err := cc.NewStream(context.Background(), "/bench.Svc/Echo", nil)

	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	require.NotNil(t, s.bufs, "stream came up with no pooled pair attached")
	assert.Truef(t, len(s.sendBuf) == 0 && len(s.dec.buf) == 0,
		"stream came up with %d/%d bytes already in its buffers",
		len(s.sendBuf), len(s.dec.buf))
}

// TestStreamBufs_CloseIsTheOnlyReturn pins that the buffers go back exactly
// once. Close is sync.Once-guarded, so a second Close must not hand the same
// pair to the pool twice — two owners writing one buffer is the worst outcome
// this pool can produce.
func TestStreamBufs_CloseIsTheOnlyReturn(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s, err := cc.NewStream(context.Background(), "/bench.Svc/Echo", nil)
	require.NoError(t, err, "NewStream")
	require.NotNil(t, s.bufs, "stream came up with no pooled buffers")
	// Both buffers have to be non-nil going in, or the checks after Close hold
	// whether or not the code nils anything. This test used to open a stream and
	// close it having sent and received nothing: acquireBufs then takes its
	// slices from a pooled pair whose own buffers may themselves be nil, so both
	// fields were already nil before releaseBufs ran and stayed nil either way.
	// Which pair sync.Pool hands back is not deterministic either, so the
	// assertion was not merely weak, it was weak by luck. Growing them is what
	// makes it an assertion. The decoder grows through Push, for the reason
	// TestStreamBufs_ReleaseKeepsCapacity gives.
	s.sendBuf = append(s.sendBuf[:0], bytes.Repeat([]byte{'x'}, 2048)...)
	s.dec.Push(bytes.Repeat([]byte{'y'}, 4096))
	require.NotEmpty(t, s.sendBuf, "the fixture did not grow the send buffer")
	require.NotEmpty(t, s.dec.buf, "the fixture did not grow the decoder buffer")

	_ = s.Close()
	attachedAfterClose := s.bufs
	_ = s.Close() // must be inert

	assert.Nil(t, attachedAfterClose, "Close left the pooled pair attached")
	assert.Truef(t, s.sendBuf == nil && s.dec.buf == nil && s.dec.own == nil,
		"Close left this stream pointing at buffers it no longer owns: sendBuf %d, "+
			"dec.buf %d, dec.own %d bytes — the pool has handed them on by now",
		len(s.sendBuf), len(s.dec.buf), len(s.dec.own))
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
	require.NoError(t, err, "NewStream")
	_ = stale.Close()
	// Another RPC now owns whatever the pool handed on.
	_, err = cc.Invoke(ctx, "/bench.Svc/Echo", []byte("live"), nil)
	require.NoError(t, err, "Invoke")

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
		err := c.call()

		assert.ErrorIsf(t, err, ErrStreamClosed,
			"%s on a closed stream = %v, want ErrStreamClosed", c.name, err)
	}
}

// TestStreamBufs_OversizeIsNotPooled pins the cap: one outlier response must not
// park its buffer in the pool for the life of the process.
func TestStreamBufs_OversizeIsNotPooled(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s, err := cc.NewStream(context.Background(), "/bench.Svc/Echo", nil)
	require.NoError(t, err, "NewStream")
	// Simulate the outlier directly: grow both buffers past the cap, then close.
	//
	// The decoder is grown through Push rather than by assigning dec.buf.
	// releaseBufs hands the pool dec.OWN, and dec.buf is the field that moved
	// when the borrow path landed — assigning it left this test measuring a
	// buffer the cap check never sees, so the dec arm of the assertion below
	// could not fail and `if cap(b.dec) > maxPooledStreamBuf` could be deleted
	// outright with the suite still green. The send arm never had the problem.
	// TestStreamBufs_ReleaseKeepsCapacity states the rule; this is the test that
	// did not get the memo.
	s.sendBuf = make([]byte, 0, maxPooledStreamBuf+1)
	s.dec.Push(make([]byte, maxPooledStreamBuf+1))
	require.Greaterf(t, cap(s.dec.own), maxPooledStreamBuf,
		"the fixture grew dec.own to cap %d, want more than %d — the guard under "+
			"test is never reached below that", cap(s.dec.own), maxPooledStreamBuf)
	b := s.bufs

	_ = s.Close()

	require.NotNil(t, b, "no pooled pair to inspect")
	assert.Truef(t, cap(b.send) <= maxPooledStreamBuf && cap(b.dec) <= maxPooledStreamBuf,
		"an oversize buffer was pooled: send cap %d, dec cap %d, limit %d",
		cap(b.send), cap(b.dec), maxPooledStreamBuf)
}
