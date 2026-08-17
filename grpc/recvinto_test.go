package grpc

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// streamOf opens a call whose request carries n length-prefixed messages of
// size bytes. The mock peer echoes DATA verbatim, so the same n messages come
// back — a server-streaming shape without needing a streaming server.
func streamOf(t *testing.T, cc *ClientConn, n, size int) *Stream {
	t.Helper()
	var req []byte
	for i := 0; i < n; i++ {
		msg := bytes.Repeat([]byte{byte('a' + i%26)}, size)
		var err error
		req, err = AppendMessage(req, msg)
		require.NoError(t, err, "AppendMessage")
	}
	s, err := cc.NewStream(context.Background(), "/bench.Svc/EchoStream", nil)
	require.NoError(t, err, "NewStream")
	t.Cleanup(func() { _ = s.Close() })
	// req already carries its own length prefixes, so it goes out as raw DATA
	// rather than through Send, which would prefix it a second time.
	require.NoError(t, s.s.SendData(context.Background(), req, true), "SendData")
	return s
}

// TestRecvInto_ReusesTheBuffer is the point of the method: a loop that hands
// back the same buffer must stop allocating per message.
func TestRecvInto_ReusesTheBuffer(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	const msgs, size = 32, 512
	s := streamOf(t, cc, msgs, size)
	buf := make([]byte, 0, size)
	firstCap := cap(buf)
	ctx := context.Background()

	for i := 0; i < msgs; i++ {
		var err error
		buf, err = s.RecvInto(ctx, buf[:0])
		require.NoErrorf(t, err, "RecvInto %d", i)
		require.Lenf(t, buf, size, "message %d has %d bytes, want %d", i, len(buf), size)
		require.Truef(t, bytes.Equal(buf, bytes.Repeat([]byte{byte('a' + i%26)}, size)),
			"message %d differs", i)
	}

	assert.Equalf(t, firstCap, cap(buf),
		"buffer capacity grew %d -> %d: the loop reallocated", firstCap, cap(buf))
}

// TestRecvInto_TerminalErrorKeepsTheBuffer pins the decision that makes the loop
// idiom work. The last iteration of every stream is an error — io.EOF for a
// clean call — and returning nil there would hand the caller's buffer to the
// garbage collector once per stream, which is the allocation this method exists
// to remove.
func TestRecvInto_TerminalErrorKeepsTheBuffer(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s := streamOf(t, cc, 1, 128)
	ctx := context.Background()
	buf := make([]byte, 0, 4096)
	wantCap := cap(buf)

	var err error
	buf, err = s.RecvInto(ctx, buf[:0])
	require.NoError(t, err, "first RecvInto")
	buf, err = s.RecvInto(ctx, buf[:0])

	require.ErrorIsf(t, err, io.EOF, "terminal RecvInto = %v, want io.EOF", err)
	require.NotNil(t, buf, "terminal RecvInto returned nil, discarding the caller's buffer")
	assert.Emptyf(t, buf, "terminal RecvInto returned %d bytes, want 0", len(buf))
	assert.Equalf(t, wantCap, cap(buf),
		"terminal RecvInto returned capacity %d, want the caller's %d", cap(buf), wantCap)
}

// TestRecv_ContractUnchanged pins that routing Recv through RecvInto did not
// move its edges: a fresh slice per message, and nil on the terminal error.
func TestRecv_ContractUnchanged(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s := streamOf(t, cc, 2, 64)
	ctx := context.Background()

	first, firstErr := s.Recv(ctx)
	second, secondErr := s.Recv(ctx)
	got, terminalErr := s.Recv(ctx)

	require.NoError(t, firstErr, "first Recv")
	require.NoError(t, secondErr, "second Recv")
	// Two messages, two distinct backing arrays: the first must not have been
	// overwritten by the second.
	if len(first) > 0 && len(second) > 0 {
		assert.NotSame(t, &first[0], &second[0], "Recv handed out the same backing array twice")
	}
	assert.True(t, bytes.Equal(first, bytes.Repeat([]byte{'a'}, 64)),
		"the first message was overwritten by the second")
	assert.ErrorIsf(t, terminalErr, io.EOF, "terminal Recv = (%v, %v), want (nil, io.EOF)", got, terminalErr)
	assert.Nilf(t, got, "terminal Recv = (%v, %v), want (nil, io.EOF)", got, terminalErr)
}

// TestRecvInto_DiscardsLength pins that only dst's capacity is used, so a caller
// passing a non-empty buffer does not get its old contents prepended.
func TestRecvInto_DiscardsLength(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s := streamOf(t, cc, 1, 32)
	dirty := bytes.Repeat([]byte{'Z'}, 1000) // length AND content that must not survive

	got, err := s.RecvInto(context.Background(), dirty)

	require.NoError(t, err, "RecvInto")
	require.Lenf(t, got, 32, "got %d bytes, want 32 — dst's length was not discarded", len(got))
	assert.NotContains(t, string(got), "Z", "dst's previous contents leaked into the message")
}
