package grpc

import (
	"errors"
	"net/http"
	"testing"
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
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()

	const msgSize = 1 << 20
	if err := s.SendLast(ctx, make([]byte, msgSize)); err != nil && !benignSendLastErr(err) {
		t.Fatalf("SendLast: %v", err)
	}

	if got := cap(s.sendBuf); got > prefixBufCeiling {
		t.Errorf("after sending %d bytes the stream's send buffer has capacity %d, want at "+
			"most %d — the message was copied into it, which is the whole cost this "+
			"change removes", msgSize, got, prefixBufCeiling)
	}
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
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()

	for i, size := range []int{1 << 10, 1 << 16, 1 << 20, 8} {
		if err := s.Send(ctx, make([]byte, size)); err != nil {
			if errors.Is(err, ErrStreamClosed) || benignSendLastErr(err) {
				break // the server finished early; the caps below are still meaningful
			}
			t.Fatalf("Send %d (%d bytes): %v", i, size, err)
		}
		if got := cap(s.sendBuf); got > prefixBufCeiling {
			t.Fatalf("send %d of %d bytes grew the send buffer to %d, want at most %d",
				i, size, got, prefixBufCeiling)
		}
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
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Send(ctx, make([]byte, 1<<20)); err != nil && !benignSendLastErr(err) &&
		!errors.Is(err, ErrStreamClosed) {
		t.Fatalf("Send: %v", err)
	}
	if s.bufs == nil {
		t.Skip("no pooled scratch on this stream")
	}
	for i, v := range s.bufs.vec {
		if v != nil {
			t.Errorf("vec[%d] still points at %d bytes after the send returned", i, len(v))
		}
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
		if err != nil {
			t.Fatalf("size %d: Invoke: %v", size, err)
		}
		if len(got) != size {
			t.Errorf("size %d: echoed %d bytes back", size, len(got))
			continue
		}
		for i := range msg {
			if got[i] != msg[i] {
				t.Errorf("size %d: echo differs at byte %d", size, i)
				break
			}
		}
	}
}
