package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unaryTransportWrites is how many Write calls one unary RPC currently makes on
// the transport:
//
//  1. HEADERS                — the request header block
//  2. DATA(msg, END_STREAM)  — the request message, half-closing as it goes
//
// Each is a separate syscall, a separate TLS record (~22 bytes of record header
// and AEAD tag on top of the payload), and, since Go enables TCP_NODELAY,
// usually a separate segment. For a small RPC the record overhead alone is
// comparable to the payload, which is why this is gated rather than left to a
// profiler.
//
// It was 3 until Invoke switched from Send + CloseSend to SendLast, which
// folded the empty END_STREAM frame into the message's own.
//
// Lower it when the send path is changed to emit fewer — the constant is the
// record of the win, and this test failing low is the reminder to update it.
const unaryTransportWrites = 1

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
	_, err := cc.Invoke(ctx, "/bench.Svc/Echo", []byte("hello"), nil)
	require.NoError(t, err, "warmup")
	wc.writes.Store(0)
	wc.bytes.Store(0)

	for i := 0; i < unaryWriteCountRPCs; i++ {
		_, err := cc.Invoke(ctx, "/bench.Svc/Echo", []byte("hello"), nil)
		require.NoErrorf(t, err, "Invoke %d", i)
	}

	writes := wc.writes.Load()
	perRPC := float64(writes) / unaryWriteCountRPCs
	t.Logf("unary RPC: %.3f transport writes, %.1f bytes (n=%d)",
		perRPC, float64(wc.bytes.Load())/unaryWriteCountRPCs, unaryWriteCountRPCs)
	want := int64(unaryTransportWrites) * unaryWriteCountRPCs
	assert.LessOrEqualf(t, writes, want+writeCountSlack,
		"unary RPC costs %.3f transport writes, want at most %d: the send path "+
			"regressed, or a frame is being flushed that used to ride along with another",
		perRPC, unaryTransportWrites)
	assert.GreaterOrEqualf(t, writes, want-writeCountSlack,
		"unary RPC costs %.3f transport writes, fewer than the recorded %d: "+
			"the send path improved — lower unaryTransportWrites to %.0f to lock the win in",
		perRPC, unaryTransportWrites, perRPC)
}

// TestUnaryEndStreamRidesTheMessage pins the shape behind the write count: one
// DATA frame per unary RPC, carrying both the message and END_STREAM. The write
// counter alone cannot see this — the frame it replaced had no payload, so
// nothing about the byte totals would change if the two frames came back and
// merely shared a flush.
func TestUnaryEndStreamRidesTheMessage(t *testing.T) {
	p := newMockGRPCPeer(t)
	cc := dialMockPeer(t, p, nil)
	ctx := context.Background()
	_, err := cc.Invoke(ctx, "/bench.Svc/Echo", []byte("hello"), nil)
	require.NoError(t, err, "warmup")
	p.dataFrames.Store(0)
	const rpcs = 20

	for i := 0; i < rpcs; i++ {
		_, err := cc.Invoke(ctx, "/bench.Svc/Echo", []byte("hello"), nil)
		require.NoErrorf(t, err, "Invoke %d", i)
	}

	assert.Equalf(t, int64(rpcs), p.dataFrames.Load(),
		"server saw %d DATA frames for %d unary RPCs, want %d: the message and "+
			"the half-close are no longer sharing a frame", p.dataFrames.Load(), rpcs, rpcs)
}

// TestSendLastRejectsASecondSend pins that SendLast really half-closes: a Send
// after it must fail rather than write DATA onto a stream the peer has been
// told is over.
func TestSendLastRejectsASecondSend(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	s, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	require.NoError(t, s.SendLast(ctx, []byte("one")), "SendLast")
	sendErr := s.Send(ctx, []byte("two"))
	sendLastErr := s.SendLast(ctx, []byte("two"))
	closeSendErr := s.CloseSend(ctx)

	assert.ErrorIsf(t, sendErr, ErrSendClosed, "Send after SendLast = %v, want ErrSendClosed", sendErr)
	assert.ErrorIsf(t, sendLastErr, ErrSendClosed, "second SendLast = %v, want ErrSendClosed", sendLastErr)
	// CloseSend stays idempotent: SendLast already did what it would do.
	assert.NoErrorf(t, closeSendErr, "CloseSend after SendLast = %v, want nil", closeSendErr)
}
