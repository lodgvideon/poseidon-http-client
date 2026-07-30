package quic

import "testing"

// TestConformance_RFC9000_Sec1321_BlockedFramesAckEliciting checks that
// DATA_BLOCKED, STREAM_DATA_BLOCKED, and STREAMS_BLOCKED are ack-eliciting, so a
// packet carrying only such a frame is acknowledged rather than left unacked and
// retransmitted by the peer (RFC 9000 §13.2.1; only PADDING, ACK, and
// CONNECTION_CLOSE are non-ack-eliciting, §12.4).
func TestConformance_RFC9000_Sec1321_BlockedFramesAckEliciting(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
	}{
		{"data-blocked", AppendDataBlocked(nil, 100)},
		{"stream-data-blocked", AppendStreamDataBlocked(nil, 0, 100)},
		{"streams-blocked-bidi", AppendStreamsBlocked(nil, false, 5)},
		{"streams-blocked-uni", AppendStreamsBlocked(nil, true, 5)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// nextBidiStreamID past 0 so the STREAM_DATA_BLOCKED on stream 0 is for a
			// created bidi stream (§19.13), not a not-yet-created one.
			h := &connFrameHandler{c: &Conn{nextBidiStreamID: 4}, space: spaceApp} // BLOCKED frames ride 1-RTT
			if err := ParseFrames(tc.frame, h); err != nil {
				t.Fatalf("ParseFrames: %v", err)
			}
			if !h.ackEliciting {
				t.Fatal("a BLOCKED frame must be ack-eliciting (§13.2.1)")
			}
		})
	}
}

// TestConformance_RFC9000_Sec1913_StreamDataBlockedStreamState checks that a
// received STREAM_DATA_BLOCKED — sent only by a stream's sender — is a
// STREAM_STATE_ERROR for a send-only stream (client-initiated unidirectional, the
// peer has no send side) or a locally initiated stream not yet created, while one
// for a stream the peer can send on is accepted (RFC 9000 §19.13).
func TestConformance_RFC9000_Sec1913_StreamDataBlockedStreamState(t *testing.T) {
	// Cursors: client bidi opened through ID 0 (next is 4); client uni through 2.
	// localMaxStreamsUni lets the server open uni streams up to ID 3+4·N (§4.6).
	newConn := func() *Conn { return &Conn{nextBidiStreamID: 4, openedUni: 1, localMaxStreamsUni: 10} }

	if err := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(2, 0); err != ErrStreamState {
		t.Fatalf("STREAM_DATA_BLOCKED on a send-only client-uni stream = %v, want ErrStreamState", err)
	}
	if err := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(4, 0); err != ErrStreamState {
		t.Fatalf("STREAM_DATA_BLOCKED on a not-yet-created client-bidi stream = %v, want ErrStreamState", err)
	}
	if err := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(0, 0); err != nil {
		t.Fatalf("STREAM_DATA_BLOCKED on a created client-bidi stream = %v, want nil", err)
	}
	if err := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(3, 0); err != nil {
		t.Fatalf("STREAM_DATA_BLOCKED on a server-uni (peer-sender) stream = %v, want nil", err)
	}
}

// TestConformance_RFC9000_Sec1914_StreamsBlockedOverLimit checks that a
// STREAMS_BLOCKED frame whose Maximum Streams exceeds 2^60 is a
// FRAME_ENCODING_ERROR — a larger value implies a stream ID past the 2^62-1 varint
// space (RFC 9000 §19.14) — while exactly 2^60 is accepted, for both types.
func TestConformance_RFC9000_Sec1914_StreamsBlockedOverLimit(t *testing.T) {
	const limit = uint64(1) << 60
	for _, uni := range []bool{false, true} {
		if err := (&connFrameHandler{c: &Conn{}}).OnStreamsBlocked(uni, limit); err != nil {
			t.Fatalf("STREAMS_BLOCKED uni=%v maximum=2^60 = %v, want nil", uni, err)
		}
		if err := (&connFrameHandler{c: &Conn{}}).OnStreamsBlocked(uni, limit+1); err != ErrFrameEncoding {
			t.Fatalf("STREAMS_BLOCKED uni=%v maximum=2^60+1 = %v, want ErrFrameEncoding", uni, err)
		}
	}
}

// TestConformance_RFC9000_Sec1914_StreamsBlockedOnRefusedOpen pins the §19.14
// SHOULD that an endpoint signal the peer when its cumulative stream limit refuses
// a new stream — otherwise a server that under-grants never learns the client is
// stalled, so it has no trigger to send MAX_STREAMS. Latched per distinct limit
// value like the other *_BLOCKED frames, so a retry loop cannot flood the peer.
func TestConformance_RFC9000_Sec1914_StreamsBlockedOnRefusedOpen(t *testing.T) {
	newConn := func(bidi, uni uint64) (*Conn, *countingPC) {
		dcid := []byte("blocked0")
		keys, _ := InitialKeys(dcid)
		sealer, err := NewSealer(keys)
		if err != nil {
			t.Fatal(err)
		}
		pc := &countingPC{}
		return &Conn{
			pc: pc, dcid: dcid, oneRTTSealer: sealer,
			peer: TransportParams{InitialMaxStreamsBidi: bidi, InitialMaxStreamsUni: uni},
		}, pc
	}

	t.Run("bidi", func(t *testing.T) {
		c, pc := newConn(0, 0)
		if _, err := c.OpenStream(); err != ErrTooManyStreams {
			t.Fatalf("OpenStream at limit 0 = %v, want ErrTooManyStreams", err)
		}
		if got := pc.datagrams.Load(); got != 1 {
			t.Fatalf("datagrams after the first refusal = %d, want 1 (STREAMS_BLOCKED)", got)
		}
		// A second refusal at the same limit must not re-emit.
		if _, err := c.OpenStream(); err != ErrTooManyStreams {
			t.Fatal("second OpenStream should still be refused")
		}
		if got := pc.datagrams.Load(); got != 1 {
			t.Fatalf("datagrams after a repeat refusal = %d, want still 1", got)
		}
		// A raised-then-exhausted limit is a new value, so it is signalled again.
		c.peer.InitialMaxStreamsBidi = 1
		if _, err := c.OpenStream(); err != nil {
			t.Fatalf("OpenStream after the grant: %v", err)
		}
		if _, err := c.OpenStream(); err != ErrTooManyStreams {
			t.Fatal("OpenStream past the raised limit should be refused")
		}
		if got := pc.datagrams.Load(); got != 2 {
			t.Fatalf("datagrams after refusal at a new limit = %d, want 2", got)
		}
	})

	t.Run("uni", func(t *testing.T) {
		c, pc := newConn(0, 0)
		if _, err := c.OpenUniStream(); err != ErrTooManyStreams {
			t.Fatalf("OpenUniStream at limit 0 = %v, want ErrTooManyStreams", err)
		}
		if got := pc.datagrams.Load(); got != 1 {
			t.Fatalf("datagrams after the first refusal = %d, want 1 (STREAMS_BLOCKED uni)", got)
		}
	})
}
