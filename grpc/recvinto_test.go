package grpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
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
		if req, err = AppendMessage(req, msg); err != nil {
			t.Fatalf("AppendMessage: %v", err)
		}
	}
	s, err := cc.NewStream(context.Background(), "/bench.Svc/EchoStream", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	// req already carries its own length prefixes, so it goes out as raw DATA
	// rather than through Send, which would prefix it a second time.
	if err := s.s.SendData(context.Background(), req, true); err != nil {
		t.Fatalf("SendData: %v", err)
	}
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
		if err != nil {
			t.Fatalf("RecvInto %d: %v", i, err)
		}
		if len(buf) != size {
			t.Fatalf("message %d has %d bytes, want %d", i, len(buf), size)
		}
		want := bytes.Repeat([]byte{byte('a' + i%26)}, size)
		if !bytes.Equal(buf, want) {
			t.Fatalf("message %d differs", i)
		}
	}
	if cap(buf) != firstCap {
		t.Errorf("buffer capacity grew %d -> %d: the loop reallocated", firstCap, cap(buf))
	}
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
	if buf, err = s.RecvInto(ctx, buf[:0]); err != nil {
		t.Fatalf("first RecvInto: %v", err)
	}
	buf, err = s.RecvInto(ctx, buf[:0])
	if !errors.Is(err, io.EOF) {
		t.Fatalf("terminal RecvInto = %v, want io.EOF", err)
	}
	if buf == nil {
		t.Fatal("terminal RecvInto returned nil, discarding the caller's buffer")
	}
	if len(buf) != 0 {
		t.Errorf("terminal RecvInto returned %d bytes, want 0", len(buf))
	}
	if cap(buf) != wantCap {
		t.Errorf("terminal RecvInto returned capacity %d, want the caller's %d", cap(buf), wantCap)
	}
}

// TestRecv_ContractUnchanged pins that routing Recv through RecvInto did not
// move its edges: a fresh slice per message, and nil on the terminal error.
func TestRecv_ContractUnchanged(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s := streamOf(t, cc, 2, 64)
	ctx := context.Background()

	first, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	second, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("second Recv: %v", err)
	}
	// Two messages, two distinct backing arrays: the first must not have been
	// overwritten by the second.
	if len(first) > 0 && len(second) > 0 && &first[0] == &second[0] {
		t.Error("Recv handed out the same backing array twice")
	}
	if !bytes.Equal(first, bytes.Repeat([]byte{'a'}, 64)) {
		t.Error("the first message was overwritten by the second")
	}
	if got, err := s.Recv(ctx); !errors.Is(err, io.EOF) || got != nil {
		t.Errorf("terminal Recv = (%v, %v), want (nil, io.EOF)", got, err)
	}
}

// TestRecvInto_DiscardsLength pins that only dst's capacity is used, so a caller
// passing a non-empty buffer does not get its old contents prepended.
func TestRecvInto_DiscardsLength(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	s := streamOf(t, cc, 1, 32)

	dirty := bytes.Repeat([]byte{'Z'}, 1000) // length AND content that must not survive
	got, err := s.RecvInto(context.Background(), dirty)
	if err != nil {
		t.Fatalf("RecvInto: %v", err)
	}
	if len(got) != 32 {
		t.Fatalf("got %d bytes, want 32 — dst's length was not discarded", len(got))
	}
	if bytes.ContainsRune(got, 'Z') {
		t.Error("dst's previous contents leaked into the message")
	}
}
