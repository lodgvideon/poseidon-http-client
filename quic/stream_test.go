package quic

import (
	"bytes"
	"testing"
)

// feed applies a sequence of (offset, data, fin) frames to a fresh recvStream
// and returns it.
func feed(frames ...streamFrameInput) *recvStream {
	r := &recvStream{}
	for _, f := range frames {
		r.receive(f.offset, f.data, f.fin)
	}
	return r
}

type streamFrameInput struct {
	offset uint64
	data   []byte
	fin    bool
}

func TestRecvStream_InOrder(t *testing.T) {
	r := feed(
		streamFrameInput{0, []byte("hello "), false},
		streamFrameInput{6, []byte("world"), true},
	)
	if got := r.bytes(); !bytes.Equal(got, []byte("hello world")) {
		t.Fatalf("bytes = %q", got)
	}
	if !r.complete() {
		t.Fatal("expected complete after FIN")
	}
}

// TestConformance_RFC9000_Sec2_StreamReassembly verifies that STREAM data
// arriving out of order is reassembled into the correct byte stream, and that
// the stream is only complete once the FIN and every preceding byte is present
// (RFC 9000 §2.2).
func TestConformance_RFC9000_Sec2_StreamReassembly(t *testing.T) {
	// FIN arrives (offset 6) before the bytes that precede it (offset 0).
	r := feed(
		streamFrameInput{6, []byte("world"), true},
		streamFrameInput{0, []byte("hello "), false},
	)
	if r.finalSize != 11 {
		t.Fatalf("finalSize = %d, want 11", r.finalSize)
	}
	if got := r.bytes(); !bytes.Equal(got, []byte("hello world")) {
		t.Fatalf("bytes = %q, want %q", got, "hello world")
	}
	if !r.complete() {
		t.Fatal("expected complete once the gap before the FIN is filled")
	}
}

func TestRecvStream_GapNotComplete(t *testing.T) {
	// Tail arrives with FIN but the middle is still missing.
	r := feed(
		streamFrameInput{0, []byte("AB"), false},
		streamFrameInput{4, []byte("EF"), true},
	)
	if r.complete() {
		t.Fatal("must not be complete while [2,4) is missing")
	}
	if got := r.bytes(); !bytes.Equal(got, []byte("AB")) {
		t.Fatalf("bytes = %q, want %q", got, "AB")
	}
	// Fill the gap; now the buffered tail should fold in and complete.
	r.receive(2, []byte("CD"), false)
	if got := r.bytes(); !bytes.Equal(got, []byte("ABCDEF")) {
		t.Fatalf("bytes = %q, want %q", got, "ABCDEF")
	}
	if !r.complete() {
		t.Fatal("expected complete after gap filled")
	}
}

func TestRecvStream_DuplicateAndOverlap(t *testing.T) {
	r := feed(
		streamFrameInput{0, []byte("hello"), false},
		streamFrameInput{0, []byte("hel"), false},      // wholly duplicate
		streamFrameInput{3, []byte("lo world"), false}, // overlaps [3,5) then extends
	)
	if got := r.bytes(); !bytes.Equal(got, []byte("hello world")) {
		t.Fatalf("bytes = %q, want %q", got, "hello world")
	}
}

func TestRecvStream_MultipleBufferedChunks(t *testing.T) {
	// Deliver strictly in reverse; only the last frame unblocks all of them.
	r := feed(
		streamFrameInput{8, []byte("IJKL"), true},
		streamFrameInput{4, []byte("EFGH"), false},
		streamFrameInput{0, []byte("ABCD"), false},
	)
	if got := r.bytes(); !bytes.Equal(got, []byte("ABCDEFGHIJKL")) {
		t.Fatalf("bytes = %q, want %q", got, "ABCDEFGHIJKL")
	}
	if !r.complete() {
		t.Fatal("expected complete")
	}
}

func TestConn_OnStream_DeliversToOpenStream(t *testing.T) {
	c := &Conn{}
	s := c.OpenStream() // stream 0
	h := &connFrameHandler{c: c}
	if err := h.OnStream(s.ID(), 0, true, []byte("response body")); err != nil {
		t.Fatal(err)
	}
	if !h.ackEliciting {
		t.Fatal("STREAM frame must mark the packet ack-eliciting")
	}
	if got := s.recv.bytes(); !bytes.Equal(got, []byte("response body")) {
		t.Fatalf("delivered %q, want %q", got, "response body")
	}
	if !s.recv.complete() {
		t.Fatal("stream should be complete after FIN")
	}
	// A frame for an unknown stream is ignored, not an error.
	if err := h.OnStream(999, 0, false, []byte("x")); err != nil {
		t.Fatalf("unknown stream: %v", err)
	}
}

func TestConn_OpenStream_IDs(t *testing.T) {
	c := &Conn{}
	for want := uint64(0); want < 16; want += 4 {
		s := c.OpenStream()
		if s.ID() != want {
			t.Fatalf("stream ID = %d, want %d", s.ID(), want)
		}
		if c.streams[want] != s {
			t.Fatalf("stream %d not registered", want)
		}
	}
}
