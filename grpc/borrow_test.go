package grpc

import (
	"bytes"
	"testing"
)

// PushBorrowed makes the decoder alias a caller's DATA chunk instead of copying
// it. Getting the ownership wrong here does not crash — it silently returns the
// wrong bytes, or hands a pooled buffer to two owners at once. These pin the
// rule stated on PushBorrowed.

// TestBorrow_RefusedWhileBytesArePending is the precondition. A chunk that
// continues a partly-received message must be appended to what is pending, not
// aliased — aliasing would throw the earlier bytes away.
func TestBorrow_RefusedWhileBytesArePending(t *testing.T) {
	whole, err := AppendMessage(nil, bytes.Repeat([]byte{'a'}, 100))
	if err != nil {
		t.Fatal(err)
	}
	var d decoder
	d.Push(whole[:20]) // a partial message is now pending

	if d.PushBorrowed(whole[20:], nil) {
		t.Fatal("borrowed while bytes were pending — the pending prefix would be lost")
	}
	d.Push(whole[20:])

	msg, ok, err := d.Next()
	if err != nil || !ok {
		t.Fatalf("Next after the fallback = (ok=%v, err=%v)", ok, err)
	}
	if !bytes.Equal(msg, bytes.Repeat([]byte{'a'}, 100)) {
		t.Error("the message reassembled from two chunks is wrong")
	}
}

// TestBorrow_EndsOnTheNextPush pins the release rule: the undelivered remainder
// of a borrowed chunk must survive into the decoder's own buffer, because the
// caller is free to reuse the chunk the moment the borrow ends.
func TestBorrow_EndsOnTheNextPush(t *testing.T) {
	first, err := AppendMessage(nil, []byte("first-message"))
	if err != nil {
		t.Fatal(err)
	}
	// A chunk holding one whole message plus the head of a second.
	second, err := AppendMessage(nil, []byte("second-message"))
	if err != nil {
		t.Fatal(err)
	}
	chunk := append(append([]byte(nil), first...), second[:6]...)

	var d decoder
	if !d.PushBorrowed(chunk, nil) {
		t.Fatal("an empty decoder refused to borrow")
	}
	msg, ok, err := d.Next()
	if err != nil || !ok {
		t.Fatalf("first Next = (ok=%v, err=%v)", ok, err)
	}
	if string(msg) != "first-message" {
		t.Fatalf("first message = %q", msg)
	}

	// The next Push ends the borrow. Scribble over the borrowed chunk first:
	// if the remainder were still aliasing it, the second message would come
	// out corrupted.
	rest := append([]byte(nil), second[6:]...)
	d.Push(rest)
	for i := range chunk {
		chunk[i] = 0xFF
	}

	msg, ok, err = d.Next()
	if err != nil || !ok {
		t.Fatalf("second Next = (ok=%v, err=%v)", ok, err)
	}
	if string(msg) != "second-message" {
		t.Errorf("second message = %q, want \"second-message\" — the borrowed remainder "+
			"was not copied out before the borrow ended", msg)
	}
}

// TestBorrow_ReturnsTheSlabExactlyOnce is the pooling half. The decoder owns the
// slab while borrowing, so it must return it when the borrow ends — and must not
// return it a second time when the stream is closed, which would put one buffer
// in the pool twice and hand it to two owners.
func TestBorrow_ReturnsTheSlabExactlyOnce(t *testing.T) {
	whole, err := AppendMessage(nil, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	slab := new([]byte)
	*slab = make([]byte, 0, 64)

	var d decoder
	if !d.PushBorrowed(whole, slab) {
		t.Fatal("empty decoder refused to borrow")
	}
	if d.borrowed != slab {
		t.Fatal("the decoder did not take the slab")
	}
	d.Push([]byte{0}) // ends the borrow
	if d.borrowed != nil || d.borrowing {
		t.Error("the borrow did not end on Push")
	}
	// A second end must be inert.
	d.release()
	if d.borrowed != nil || d.borrowing {
		t.Error("release after the borrow already ended re-armed it")
	}
}

// TestBorrow_ReleaseEndsAHeldBorrow pins what Close depends on: a decoder still
// aliasing a chunk when the stream goes away must give the slab back, or the
// buffer never returns to the pool.
func TestBorrow_ReleaseEndsAHeldBorrow(t *testing.T) {
	whole, err := AppendMessage(nil, []byte("held"))
	if err != nil {
		t.Fatal(err)
	}
	slab := new([]byte)
	var d decoder
	if !d.PushBorrowed(whole, slab) {
		t.Fatal("empty decoder refused to borrow")
	}
	d.release()
	if d.borrowing || d.borrowed != nil {
		t.Error("release left the borrow in place")
	}
}

// TestBorrow_CloseReleasesAHeldBorrow is the leak gate, and it exists because a
// mutation removing release() from Close broke nothing else: the borrow simply
// stays held, the pooled slab never returns, and every test still passes. It has
// to be checked through Close rather than by calling release directly, which is
// the mistake the first version of these tests made.
func TestBorrow_CloseReleasesAHeldBorrow(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := t.Context()

	s, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendLast(ctx, bytes.Repeat([]byte{'q'}, 64)); err != nil {
		t.Fatalf("SendLast: %v", err)
	}
	if _, err := s.Recv(ctx); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	// The response arrived in one DATA frame, so the decoder is aliasing it and
	// nothing has ended the borrow yet.
	if !s.dec.borrowing {
		t.Skip("the response did not arrive as a single borrowed chunk; nothing to gate here")
	}

	_ = s.Close()
	if s.dec.borrowing || s.dec.borrowed != nil {
		t.Error("Close left a borrow held — the pooled DATA slab never returns to the pool")
	}
}

// TestBorrow_EndToEndOverAReusedConn is the integration check: the borrow path
// runs inside Stream.pump, where the chunk comes from conn's pooled DATA slab.
// Several calls on one connection make the pool actually recycle, so a
// double-return or a stale alias surfaces as one call seeing another's bytes.
func TestBorrow_EndToEndOverAReusedConn(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := t.Context()

	var dst []byte
	for i := 0; i < 40; i++ {
		want := bytes.Repeat([]byte{byte('a' + i%26)}, 512+i)
		got, err := cc.InvokeInto(ctx, "/bench.Svc/Echo", want, dst, nil)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("call %d returned %d bytes that do not match the request — a borrowed "+
				"chunk was reused under the decoder", i, len(got))
		}
		dst = got
	}
}
