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
		_ = r.receive(f.offset, f.data, f.fin)
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
	_ = r.receive(2, []byte("CD"), false)
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
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 4}}
	s, err := c.OpenStream() // stream 0
	if err != nil {
		t.Fatal(err)
	}
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
	// A frame for an unopened client-initiated stream (id&3==0, we never opened
	// stream 8) is ignored, not an error. Server-initiated streams are classified
	// and accepted or rejected separately (see accept_uni_test.go).
	if err := h.OnStream(8, 0, false, []byte("x")); err != nil {
		t.Fatalf("unopened client stream: %v", err)
	}
}

// TestConnFrameHandler_OnCrypto_ReassemblesByOffset pins the fix for the real
// bug that broke live interop: a server's handshake CRYPTO stream spans many
// frames that can arrive out of order, and must be reassembled by offset before
// being fed to TLS (RFC 9000 §19.6). Appending in arrival order garbled the
// server's ServerHello/Certificate flight.
func TestConnFrameHandler_OnCrypto_ReassemblesByOffset(t *testing.T) {
	c := &Conn{}
	h := &connFrameHandler{c: c, space: spaceInitial}

	// A later CRYPTO frame arrives before the bytes preceding it.
	if err := h.OnCrypto(6, []byte("world")); err != nil {
		t.Fatal(err)
	}
	if got := c.cryptoRecv[spaceInitial].read(); len(got) != 0 {
		t.Fatalf("gapped CRYPTO must not be readable yet, got %q", got)
	}
	// The gap fills; the whole prefix becomes available in order.
	if err := h.OnCrypto(0, []byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if got := string(c.cryptoRecv[spaceInitial].read()); got != "hello world" {
		t.Fatalf("reassembled CRYPTO = %q, want %q", got, "hello world")
	}
}

func TestConn_OpenUniStream_IDs(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsUni: 3, InitialMaxStreamDataUni: 5000}}
	for _, want := range []uint64{2, 6, 10} {
		s, err := c.OpenUniStream()
		if err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		if s.ID() != want {
			t.Fatalf("uni stream ID = %d, want %d", s.ID(), want)
		}
		if s.sendMax != 5000 {
			t.Fatalf("uni sendMax = %d, want 5000 (initial_max_stream_data_uni)", s.sendMax)
		}
	}
	if _, err := c.OpenUniStream(); err != ErrTooManyStreams {
		t.Fatalf("4th uni stream err = %v, want ErrTooManyStreams", err)
	}
}

// TestStream_RecvAndFinished feeds a stream in two chunks (the second with FIN)
// and checks Recv returns only the newly contiguous bytes each call, and
// Finished flips once the FIN and all bytes are present.
func TestStream_RecvAndFinished(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	h := &connFrameHandler{c: c}

	if err := h.OnStream(s.ID(), 0, false, []byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if got := string(s.Recv()); got != "hello " {
		t.Fatalf("Recv 1 = %q, want %q", got, "hello ")
	}
	if got := string(s.Recv()); got != "" {
		t.Fatalf("Recv with nothing new = %q, want empty", got)
	}
	if s.Finished() {
		t.Fatal("stream must not be finished before FIN")
	}

	if err := h.OnStream(s.ID(), 6, true, []byte("world")); err != nil {
		t.Fatal(err)
	}
	if got := string(s.Recv()); got != "world" {
		t.Fatalf("Recv 2 = %q, want %q", got, "world")
	}
	if !s.Finished() {
		t.Fatal("stream should be finished after FIN with all bytes present")
	}
}

func TestConn_OpenStream_IDs(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 4}}
	for want := uint64(0); want < 16; want += 4 {
		s, err := c.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		if s.ID() != want {
			t.Fatalf("stream ID = %d, want %d", s.ID(), want)
		}
		if c.streams[want] != s {
			t.Fatalf("stream %d not registered", want)
		}
	}
}

// TestConformance_RFC9000_Sec46_StreamLimit verifies that OpenStream refuses to
// open more bidirectional streams than the peer's advertised
// initial_max_streams_bidi limit (RFC 9000 §4.6): the (limit+1)th open returns
// ErrTooManyStreams.
func TestConformance_RFC9000_Sec46_StreamLimit(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 2}}
	if _, err := c.OpenStream(); err != nil {
		t.Fatalf("stream 1: %v", err)
	}
	if _, err := c.OpenStream(); err != nil {
		t.Fatalf("stream 2: %v", err)
	}
	if _, err := c.OpenStream(); err != ErrTooManyStreams {
		t.Fatalf("stream 3 err = %v, want ErrTooManyStreams", err)
	}

	// A peer that advertises no bidi streams forbids even the first (§18.2).
	zero := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 0}}
	if _, err := zero.OpenStream(); err != ErrTooManyStreams {
		t.Fatalf("zero-limit err = %v, want ErrTooManyStreams", err)
	}
}
