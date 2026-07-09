package quic

import (
	"bytes"
	"testing"
)

// TestConformance_RFC9000_Sec21_AcceptServerUniStream checks that a
// server-initiated unidirectional stream (id&3==3) is accepted, its bytes are
// delivered, and it is queued exactly once for AcceptUniStream.
func TestConformance_RFC9000_Sec21_AcceptServerUniStream(t *testing.T) {
	c := &Conn{localMaxStreamsUni: 3}
	h := &connFrameHandler{c: c}
	if err := h.OnStream(3, 0, false, []byte{0x00, 0x04}); err != nil {
		t.Fatalf("accept server uni: %v", err)
	}
	s := c.AcceptUniStream()
	if s == nil || s.ID() != 3 {
		t.Fatalf("AcceptUniStream = %v, want stream 3", s)
	}
	if got := s.recv.bytes(); !bytes.Equal(got, []byte{0x00, 0x04}) {
		t.Fatalf("delivered %x, want 0004", got)
	}
	if c.AcceptUniStream() != nil {
		t.Fatal("only one uni stream should be queued")
	}
	// A later frame on the now-known stream is delivered without re-accepting.
	if err := h.OnStream(3, 2, false, []byte{0x07}); err != nil {
		t.Fatal(err)
	}
	if c.AcceptUniStream() != nil {
		t.Fatal("an already-accepted stream must not be re-queued")
	}
	if got := s.recv.bytes(); !bytes.Equal(got, []byte{0x00, 0x04, 0x07}) {
		t.Fatalf("reassembled %x, want 000407", got)
	}
}

// TestConformance_RFC9000_Sec46_UniStreamLimit checks that a server uni stream
// beyond our advertised limit is a STREAM_LIMIT_ERROR connection error.
func TestConformance_RFC9000_Sec46_UniStreamLimit(t *testing.T) {
	c := &Conn{localMaxStreamsUni: 3}
	h := &connFrameHandler{c: c}
	// ids 3,7,11 are within the limit (id>>2 = 0,1,2); id 15 (>>2 = 3) exceeds it.
	if err := h.OnStream(15, 0, false, []byte{0x00}); err != ErrTooManyUniStreams {
		t.Fatalf("over-limit uni = %v, want ErrTooManyUniStreams", err)
	}
	if code, ok := closeCodeFor(ErrTooManyUniStreams); !ok || code != ErrCodeStreamLimitError {
		t.Fatalf("closeCodeFor(ErrTooManyUniStreams) = %#x,%v, want STREAM_LIMIT_ERROR", code, ok)
	}
}

// TestConformance_RFC9000_Sec21_ServerBidiRejected checks that a server-initiated
// bidirectional stream (id&3==1) is a connection error for an HTTP/3 client.
func TestConformance_RFC9000_Sec21_ServerBidiRejected(t *testing.T) {
	c := &Conn{localMaxStreamsUni: 3}
	h := &connFrameHandler{c: c}
	if err := h.OnStream(1, 0, false, []byte("x")); err != ErrServerBidiStream {
		t.Fatalf("server bidi = %v, want ErrServerBidiStream", err)
	}
	if code, ok := closeCodeFor(ErrServerBidiStream); !ok || code != ErrCodeStreamLimitError {
		t.Fatalf("closeCodeFor(ErrServerBidiStream) = %#x,%v, want STREAM_LIMIT_ERROR", code, ok)
	}
}

// TestConn_AcceptUniStream_Order checks that accepted streams are returned in the
// order they were accepted.
func TestConn_AcceptUniStream_Order(t *testing.T) {
	c := &Conn{localMaxStreamsUni: 3}
	h := &connFrameHandler{c: c}
	for _, id := range []uint64{3, 7, 11} {
		if err := h.OnStream(id, 0, false, []byte{byte(id)}); err != nil {
			t.Fatalf("accept %d: %v", id, err)
		}
	}
	for _, want := range []uint64{3, 7, 11} {
		if s := c.AcceptUniStream(); s == nil || s.ID() != want {
			t.Fatalf("AcceptUniStream = %v, want %d", s, want)
		}
	}
	if c.AcceptUniStream() != nil {
		t.Fatal("no more accepted streams expected")
	}
}
