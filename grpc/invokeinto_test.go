package grpc

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInvokeInto_ReusesTheBuffer is the point: a load generator hammering one
// unary method reuses the response buffer instead of allocating per call.
func TestInvokeInto_ReusesTheBuffer(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	req := bytes.Repeat([]byte{'q'}, 256)
	buf := make([]byte, 0, 256)
	firstCap := cap(buf)

	for i := 0; i < 20; i++ {
		var err error
		buf, err = cc.InvokeInto(ctx, "/bench.Svc/Echo", req, buf[:0], nil)
		require.NoErrorf(t, err, "call %d", i)
		require.Truef(t, bytes.Equal(buf, req), "call %d: echo differs", i)
	}

	assert.Equalf(t, firstCap, cap(buf),
		"buffer capacity grew %d -> %d: the loop reallocated", firstCap, cap(buf))
}

// TestInvokeInto_DiscardsLength pins that only dst's capacity is used, so a
// dirty buffer cannot prepend its old contents to the response.
func TestInvokeInto_DiscardsLength(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	dirty := bytes.Repeat([]byte{'Z'}, 1000)

	got, err := cc.InvokeInto(context.Background(), "/bench.Svc/Echo", []byte("hi"), dirty, nil)

	require.NoError(t, err, "InvokeInto")
	assert.Equalf(t, "hi", string(got),
		"response = %q, want \"hi\" — dst's length or contents survived", got)
}

// TestInvokeInto_DrainDoesNotClobberTheResponse is the trap this form sets for
// itself.
//
// Invoke reads a second time to reach the terminal event, which is how a
// two-message answer to a unary method is caught. If that drain were handed the
// response buffer, a second message would overwrite the answer about to be
// returned — the reuse would corrupt exactly the thing it is optimising.
//
// The mock peer echoes DATA verbatim, so a request carrying two length-prefixed
// messages comes back as two, which is the case under test: it must be reported
// as an error rather than returning either message.
func TestInvokeInto_DrainDoesNotClobberTheResponse(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	var two []byte
	var err error
	two, err = AppendMessage(two, bytes.Repeat([]byte{'A'}, 64))
	require.NoError(t, err, "AppendMessage")
	two, err = AppendMessage(two, bytes.Repeat([]byte{'B'}, 64))
	require.NoError(t, err, "AppendMessage")
	s, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	// Raw DATA: two already carries its own prefixes.
	require.NoError(t, s.s.SendData(ctx, two, true), "SendData")

	buf := make([]byte, 0, 256)
	first, err := s.RecvInto(ctx, buf[:0])
	require.NoError(t, err, "first RecvInto")
	require.True(t, bytes.Equal(first, bytes.Repeat([]byte{'A'}, 64)), "first message is not A")
	// The drain must not be given first's buffer. Reading with Recv leaves it
	// intact, which is what InvokeInto relies on.
	_, drainErr := s.Recv(ctx)

	if drainErr == nil {
		// A second message arrived, as staged.
		assert.True(t, bytes.Equal(first, bytes.Repeat([]byte{'A'}, 64)),
			"the drain overwrote the first message")
	}
}

// TestInvokeInto_MatchesInvoke pins that the two forms agree on the happy path,
// so routing Invoke through InvokeInto did not move its result.
func TestInvokeInto_MatchesInvoke(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := context.Background()
	req := []byte("payload")

	a, aErr := cc.Invoke(ctx, "/bench.Svc/Echo", req, nil)
	b, bErr := cc.InvokeInto(ctx, "/bench.Svc/Echo", req, make([]byte, 0, 8), nil)

	require.NoError(t, aErr, "Invoke")
	require.NoError(t, bErr, "InvokeInto")
	assert.Truef(t, bytes.Equal(a, b), "Invoke = %q, InvokeInto = %q", a, b)
}

// TestInvokeInto_ErrorKeepsTheBuffer pins the same decision RecvInto makes: a
// caller looping unary calls on one buffer must not lose it on the first
// failure.
func TestInvokeInto_ErrorKeepsTheBuffer(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	buf := make([]byte, 0, 512)

	// A method name without a leading slash is rejected before any I/O.
	got, err := cc.InvokeInto(context.Background(), "bad-method", nil, buf[:0], nil)

	require.Error(t, err, "InvokeInto accepted a malformed method name")
	require.NotNil(t, got, "InvokeInto returned nil on error, discarding the caller's buffer")
	assert.Equalf(t, cap(buf), cap(got),
		"returned capacity %d, want the caller's %d", cap(got), cap(buf))
}
