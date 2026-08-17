package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fused unary path sends the request WITH the headers and half-closes in the
// same write, so the stream it returns is already send-complete. That fact lives
// in one flag, sentEnd, and InvokeInto closes the stream immediately afterwards
// — so nothing observable breaks if the flag is missed, and a mutation dropping
// it survived the whole suite.
//
// It still has to be right: the moment anything but InvokeInto uses the fused
// open, a missing sentEnd lets a caller send DATA after END_STREAM, which is a
// protocol violation the peer answers with a stream error. This is the gate.

// TestFusedOpen_LeavesTheStreamSendComplete drives openStream directly, since
// that is the only way to hold a fused stream open long enough to look at it.
func TestFusedOpen_LeavesTheStreamSendComplete(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	co := callOptions{maxRecvMessageSize: cc.opts.MaxRecvMessageSize}

	s, err := cc.openStream(context.Background(), "/bench.Svc/Echo", nil, co, []byte("hello"), true)

	require.NoError(t, err, "openStream(fuse)")
	defer func() { _ = s.Close() }()
	assert.True(t, s.sentEnd,
		"a fused open left sentEnd false — the request was half-closed on the wire, "+
			"so a later Send would put DATA after END_STREAM")
	// The flag's whole purpose: further sends are refused.
	sendErr := s.Send(context.Background(), []byte("more"))
	sendLastErr := s.SendLast(context.Background(), []byte("more"))
	assert.ErrorIsf(t, sendErr, ErrSendClosed,
		"Send after a fused open = %v, want ErrSendClosed", sendErr)
	assert.ErrorIsf(t, sendLastErr, ErrSendClosed,
		"SendLast after a fused open = %v, want ErrSendClosed", sendLastErr)
}

// TestUnfusedOpen_LeavesTheStreamOpen is the other half: the streaming path must
// NOT be half-closed by opening, or a client-streaming call could never send.
func TestUnfusedOpen_LeavesTheStreamOpen(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	co := callOptions{maxRecvMessageSize: cc.opts.MaxRecvMessageSize}

	s, err := cc.openStream(context.Background(), "/bench.Svc/Echo", nil, co, nil, false)

	require.NoError(t, err, "openStream(no fuse)")
	defer func() { _ = s.Close() }()
	require.False(t, s.sentEnd, "opening a streaming call half-closed the request side")
	assert.NoError(t, s.Send(context.Background(), []byte("first")),
		"Send on a freshly opened streaming call must succeed")
}

// TestFusedOpen_EmptyRequestIsStillAMessage pins that a nil request body is a
// real zero-length gRPC message, not "no message" — which is why openStream takes
// an explicit flag instead of inferring the fusion from a nil slice.
func TestFusedOpen_EmptyRequestIsStillAMessage(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)

	got, err := cc.Invoke(context.Background(), "/bench.Svc/Echo", nil, nil)

	require.NoError(t, err, "Invoke with an empty request")
	assert.Emptyf(t, got, "echo of an empty request returned %d bytes, want 0", len(got))
}
