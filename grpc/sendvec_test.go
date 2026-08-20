package grpc

import (
	"bytes"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The send path no longer copies a message to put its five-byte header in front
// of it. The benchmark that shows the win (BenchmarkGRPC_Unary_64KB, ~293 kB/op
// down to ~218 kB/op) is real but indirect: it moves because a copied 64 KiB
// message pushed the pooled send buffer past maxPooledStreamBuf, so the buffer
// was thrown away and reallocated on every call. Raising that constant would
// produce the same drop with the copy still happening.
//
// So the copy is gated here directly instead, on the thing that is true if and
// only if the message is not copied: the stream's send buffer stays tiny no
// matter how large the message is.

// prefixBufCeiling bounds what the send buffer may hold. It is not
// messagePrefixLen exactly: appending five bytes to a nil slice yields capacity
// 8, and a future size class could round differently. What matters is that the
// capacity is a small constant rather than a function of the message, so any
// value in this range means the message did not go through it and any
// message-sized value means it did.
const prefixBufCeiling = 64

// TestSendVec_LargeMessageIsNotCopied is the gate. It fails if the send buffer
// grows to hold the message, which is what AppendMessage did.
func TestSendVec_LargeMessageIsNotCopied(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srvBeginResponse(w)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx := t.Context()
	s, err := cc.NewStream(ctx, "/t.S/M", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	const msgSize = 1 << 20

	sendErr := s.SendLast(ctx, make([]byte, msgSize))

	require.Truef(t, sendErr == nil || benignSendLastErr(sendErr), "SendLast: %v", sendErr)
	assert.LessOrEqualf(t, cap(s.sendBuf), prefixBufCeiling,
		"after sending %d bytes the stream's send buffer has capacity %d, want at "+
			"most %d — the message was copied into it, which is the whole cost this "+
			"change removes", msgSize, cap(s.sendBuf), prefixBufCeiling)
}

// TestSendVec_StreamingMessagesAreNotCopied covers Send as well as SendLast, and
// several messages in a row, so a buffer that grows on the second send is caught
// too.
func TestSendVec_StreamingMessagesAreNotCopied(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srvBeginResponse(w)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx := t.Context()
	s, err := cc.NewStream(ctx, "/t.S/M", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	for i, size := range []int{1 << 10, 1 << 16, 1 << 20, 8} {
		err := s.Send(ctx, make([]byte, size))

		if err != nil {
			if errors.Is(err, ErrStreamClosed) || benignSendLastErr(err) {
				break // the server finished early; the caps below are still meaningful
			}
			require.NoErrorf(t, err, "Send %d (%d bytes)", i, size)
		}
		require.LessOrEqualf(t, cap(s.sendBuf), prefixBufCeiling,
			"send %d of %d bytes grew the send buffer to %d, want at most %d",
			i, size, cap(s.sendBuf), prefixBufCeiling)
	}
}

// TestSendVec_DoesNotRetainTheMessage pins the other half of not copying. The
// vector lives in the pooled scratch, so a stream that is done sending — or one
// parked between sends — must not still be pointing at the caller's message, or
// the change would trade a copy for a leak the benchmarks cannot see.
func TestSendVec_DoesNotRetainTheMessage(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srvBeginResponse(w)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx := t.Context()
	s, err := cc.NewStream(ctx, "/t.S/M", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	sendErr := s.Send(ctx, make([]byte, 1<<20))

	require.Truef(t, sendErr == nil || benignSendLastErr(sendErr) || errors.Is(sendErr, ErrStreamClosed),
		"Send: %v", sendErr)
	if s.bufs == nil {
		t.Skip("no pooled scratch on this stream")
	}
	for i, v := range s.bufs.vec {
		assert.Nilf(t, v, "vec[%d] still points at %d bytes after the send returned", i, len(v))
	}
}

// TestSendVec_MessagesStillArriveIntact is the correctness half: the wire must
// carry exactly what the copying form carried. A cursor that dropped or
// duplicated a boundary would still produce a plausible-looking DATA stream, so
// this drives a real round trip against the echo peer, which writes the client's
// own DATA bytes back verbatim. Getting the message back intact means the prefix
// and the payload both crossed in the right order.
//
// The sizes straddle the 16 KiB frame boundary, which is where a vectored write
// has to cut across a buffer rather than between two.
func TestSendVec_MessagesStillArriveIntact(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := t.Context()

	for _, size := range []int{0, 1, 5, 16378, 16379, 16380, 16384, 32768, 1 << 20} {
		msg := make([]byte, size)
		for i := range msg {
			msg[i] = byte(i*7 + size)
		}

		got, err := cc.Invoke(ctx, "/bench.Svc/Echo", msg, nil)

		require.NoErrorf(t, err, "size %d: Invoke", size)
		if !assert.Lenf(t, got, size, "size %d: echoed %d bytes back", size, len(got)) {
			continue
		}
		for i := range msg {
			if !assert.Equalf(t, msg[i], got[i], "size %d: echo differs at byte %d", size, i) {
				break
			}
		}
	}
}

// TestSendVec_FusedOpenDoesNotRetainTheRequest is
// TestSendVec_DoesNotRetainTheMessage's missing sibling. The unary path never
// goes through writeMessage — openStream seats the vector itself, so the fused
// HEADERS+DATA write is a second, independent site that has to drop the
// reference again, and nothing covered it. Every unary call in the process goes
// through this one.
func TestSendVec_FusedOpenDoesNotRetainTheRequest(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	co := callOptions{maxRecvMessageSize: cc.opts.MaxRecvMessageSize}
	req := bytes.Repeat([]byte{'q'}, 1<<20)

	s, err := cc.openStream(t.Context(), "/bench.Svc/Echo", nil, co, req, true)

	require.NoError(t, err, "openStream(fuse)")
	defer func() { _ = s.Close() }()
	require.NotNil(t, s.bufs, "the fused open left no pooled scratch to inspect")
	for i, v := range s.bufs.vec {
		assert.Truef(t, v == nil,
			"vec[%d] still points at %d bytes once the fused open returned — the "+
				"scratch outlives the call and is pooled, so a stream parked between "+
				"the open and the first Recv keeps the caller's whole request alive",
			i, len(v))
	}
}

// TestSendVec_ReleaseClearsTheVector covers the third and last site, and the
// one that matters most: releaseBufs is what stops a finished RPC's message
// staying reachable from a struct sitting in streamBufPool for the life of the
// process.
//
// The fixture seats the vector by hand and leaves it seated, because the two
// sites that clear it on the way out are exactly what this clear is a backstop
// for — reach releaseBufs through a normal send and the field is already nil,
// and the guard could be deleted with nothing to show for it. Same shape as
// TestStreamBufs_OversizeIsNotPooled, which stages its outlier the same way.
func TestSendVec_ReleaseClearsTheVector(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s, err := cc.NewStream(t.Context(), "/bench.Svc/Echo", nil)
	require.NoError(t, err, "NewStream")
	b := s.bufs
	require.NotNil(t, b, "stream came up with no pooled scratch")
	msg := bytes.Repeat([]byte{'m'}, 4096)
	b.vec[0], b.vec[1] = []byte{0, 0, 0, 0x10, 0x00}, msg

	_ = s.Close()

	assert.Truef(t, b.vec[0] == nil && b.vec[1] == nil,
		"Close parked a scratch in the pool still pointing at %d bytes of the "+
			"caller's message — it stays reachable, and alive, for as long as that "+
			"struct sits unclaimed", len(b.vec[1]))
}
