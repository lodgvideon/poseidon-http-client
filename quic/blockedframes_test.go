package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		{"data-blocked", appendDataBlocked(nil, 100)},
		{"stream-data-blocked", appendStreamDataBlocked(nil, 0, 100)},
		{"streams-blocked-bidi", appendStreamsBlocked(nil, false, 5)},
		{"streams-blocked-uni", appendStreamsBlocked(nil, true, 5)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// nextBidiStreamID past 0 so the STREAM_DATA_BLOCKED on stream 0 is for a
			// created bidi stream (§19.13), not a not-yet-created one.
			h := &connFrameHandler{c: &Conn{nextBidiStreamID: 4}, space: spaceApp} // BLOCKED frames ride 1-RTT

			err := ParseFrames(tc.frame, h)

			require.NoError(t, err, "ParseFrames")
			assert.True(t, h.ackEliciting, "a BLOCKED frame must be ack-eliciting (§13.2.1)")
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

	sendOnlyUni := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(2, 0)
	notYetCreatedBidi := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(4, 0)
	createdBidi := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(0, 0)
	peerUni := (&connFrameHandler{c: newConn()}).OnStreamDataBlocked(3, 0)

	assert.ErrorIsf(t, sendOnlyUni, ErrStreamState,
		"STREAM_DATA_BLOCKED on a send-only client-uni stream = %v, want ErrStreamState", sendOnlyUni)
	assert.ErrorIsf(t, notYetCreatedBidi, ErrStreamState,
		"STREAM_DATA_BLOCKED on a not-yet-created client-bidi stream = %v, want ErrStreamState",
		notYetCreatedBidi)
	assert.NoErrorf(t, createdBidi,
		"STREAM_DATA_BLOCKED on a created client-bidi stream = %v, want nil", createdBidi)
	assert.NoErrorf(t, peerUni,
		"STREAM_DATA_BLOCKED on a server-uni (peer-sender) stream = %v, want nil", peerUni)
}

// TestConformance_RFC9000_Sec1914_StreamsBlockedOverLimit checks that a
// STREAMS_BLOCKED frame whose Maximum Streams exceeds 2^60 is a
// FRAME_ENCODING_ERROR — a larger value implies a stream ID past the 2^62-1 varint
// space (RFC 9000 §19.14) — while exactly 2^60 is accepted, for both types.
func TestConformance_RFC9000_Sec1914_StreamsBlockedOverLimit(t *testing.T) {
	const limit = uint64(1) << 60

	for _, uni := range []bool{false, true} {
		atLimit := (&connFrameHandler{c: &Conn{}}).OnStreamsBlocked(uni, limit)
		pastLimit := (&connFrameHandler{c: &Conn{}}).OnStreamsBlocked(uni, limit+1)

		assert.NoErrorf(t, atLimit, "STREAMS_BLOCKED uni=%v maximum=2^60 = %v, want nil", uni, atLimit)
		assert.ErrorIsf(t, pastLimit, ErrFrameEncoding,
			"STREAMS_BLOCKED uni=%v maximum=2^60+1 = %v, want ErrFrameEncoding", uni, pastLimit)
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
		require.NoError(t, err)
		pc := &countingPC{}
		return &Conn{
			pc: pc, dcid: dcid, oneRTTSealer: sealer,
			peer: TransportParams{InitialMaxStreamsBidi: bidi, InitialMaxStreamsUni: uni},
		}, pc
	}

	t.Run("bidi", func(t *testing.T) {
		c, pc := newConn(0, 0)

		_, err := c.OpenStream()

		require.ErrorIsf(t, err, ErrTooManyStreams, "OpenStream at limit 0 = %v, want ErrTooManyStreams", err)
		require.EqualValuesf(t, 1, pc.datagrams.Load(),
			"datagrams after the first refusal = %d, want 1 (STREAMS_BLOCKED)", pc.datagrams.Load())

		// A second refusal at the same limit must not re-emit.
		_, err = c.OpenStream()

		require.Error(t, err, "second OpenStream should still be refused")
		require.EqualValuesf(t, 1, pc.datagrams.Load(),
			"datagrams after a repeat refusal = %d, want still 1", pc.datagrams.Load())

		// A raised-then-exhausted limit is a new value, so it is signalled again.
		c.peer.InitialMaxStreamsBidi = 1
		_, errGrant := c.OpenStream()
		_, errPast := c.OpenStream()

		require.NoErrorf(t, errGrant, "OpenStream after the grant: %v", errGrant)
		require.Error(t, errPast, "OpenStream past the raised limit should be refused")
		assert.EqualValuesf(t, 2, pc.datagrams.Load(),
			"datagrams after refusal at a new limit = %d, want 2", pc.datagrams.Load())
	})

	t.Run("uni", func(t *testing.T) {
		c, pc := newConn(0, 0)

		_, err := c.OpenUniStream()

		require.ErrorIsf(t, err, ErrTooManyStreams,
			"OpenUniStream at limit 0 = %v, want ErrTooManyStreams", err)
		assert.EqualValuesf(t, 1, pc.datagrams.Load(),
			"datagrams after the first refusal = %d, want 1 (STREAMS_BLOCKED uni)", pc.datagrams.Load())
	})
}

// streamsBlockedSpy records the STREAMS_BLOCKED frames decoded from a packet.
type streamsBlockedSpy struct {
	nopFrameHandler
	uni    []bool
	limits []uint64
}

func (h *streamsBlockedSpy) OnStreamsBlocked(uni bool, maxStreams uint64) error {
	h.uni = append(h.uni, uni)
	h.limits = append(h.limits, maxStreams)
	return nil
}

// decodeStreamsBlocked opens a 1-RTT datagram this connection sealed and returns the
// STREAMS_BLOCKED frames it actually carries.
func decodeStreamsBlocked(t *testing.T, opener *Opener, dcid, pkt []byte) *streamsBlockedSpy {
	t.Helper()
	hdr, err := ParseHeader(pkt, len(dcid))
	require.NoError(t, err, "ParseHeader")
	_, _, payload, err := opener.Open(pkt, hdr.PNOffset, 0)
	require.NoError(t, err, "Open")
	spy := &streamsBlockedSpy{}
	require.NoError(t, ParseFrames(payload, spy), "ParseFrames")
	return spy
}

// TestConformance_RFC9000_Sec133_StreamsBlockedRetransmitRestatesLimit pins the two
// trailing clauses of the §13.3 blocked-signal rule, which a frozen retransmit
// descriptor silently drops: re-send "only while the endpoint is blocked on the
// corresponding limit", and "these frames always include the limit that is causing
// blocking at the time that they are transmitted".
//
// The second arm decodes the datagram flushRetransmits actually wrote. It used to
// re-encode the frame itself from streamsBlockedNow's return value and compare that
// against appendStreamsBlocked — which is the test rebuilding its own subject:
// deleting `rf.offset = limit` (quic/conn_seal.go:65), the single line that carries
// the §13.3 restate, left the whole 692-test quic suite green (SURVIVOR 0/2). With
// the wire decode below the same mutation is caught.
func TestConformance_RFC9000_Sec133_StreamsBlockedRetransmitRestatesLimit(t *testing.T) {
	dcid := []byte("sbretran")
	newConn := func(limit uint64) (*Conn, *capturePC, *Opener) {
		keys, _ := InitialKeys(dcid)
		sealer, err := NewSealer(keys)
		require.NoError(t, err)
		opener, err := NewOpener(keys)
		require.NoError(t, err)
		pc := &capturePC{}
		return &Conn{
			pc: pc, dcid: dcid, oneRTTSealer: sealer,
			peer: TransportParams{InitialMaxStreamsBidi: limit},
		}, pc, opener
	}

	t.Run("no-longer-blocked-drops-the-stale-signal", func(t *testing.T) {
		// The peer raised the limit before the loss was detected: the stall is over, so
		// the stale signal must not go out at all.
		c, pc, _ := newConn(0)
		_, err := c.OpenStream()
		require.ErrorIs(t, err, ErrTooManyStreams, "setup: the open should have been refused")
		before := len(pc.pkts) // the original STREAMS_BLOCKED(0)
		c.retransQueue[spaceApp] = append(c.retransQueue[spaceApp],
			retransFrame{kind: retransStreamsBlocked, offset: 0, fin: false})
		c.peer.InitialMaxStreamsBidi = 8 // MAX_STREAMS arrived

		err = c.flushRetransmits(spaceApp)

		require.NoErrorf(t, err, "flushRetransmits: %v", err)
		assert.Equalf(t, before, len(pc.pkts),
			"sent %d datagram(s) while no longer blocked, want 0", len(pc.pkts)-before)
		assert.Emptyf(t, c.retransQueue[spaceApp],
			"descriptor left queued: %d", len(c.retransQueue[spaceApp]))
	})

	t.Run("still-blocked-restates-todays-limit", func(t *testing.T) {
		// Still blocked, but at a limit that has moved since the lost frame was built:
		// the re-sent frame must carry today's limit, not the frozen one.
		c, pc, opener := newConn(4)
		c.openedBidi = 4
		c.retransQueue[spaceApp] = append(c.retransQueue[spaceApp],
			retransFrame{kind: retransStreamsBlocked, offset: 1, fin: false}) // stale limit 1

		err := c.flushRetransmits(spaceApp)

		require.NoErrorf(t, err, "flushRetransmits: %v", err)
		require.Lenf(t, pc.pkts, 1,
			"flushRetransmits wrote %d datagrams, want 1 while still blocked", len(pc.pkts))
		spy := decodeStreamsBlocked(t, opener, dcid, pc.pkts[0])
		require.Lenf(t, spy.limits, 1,
			"the re-sent datagram carried %d STREAMS_BLOCKED frames, want 1", len(spy.limits))
		assert.Equalf(t, []bool{false}, spy.uni,
			"the re-sent frame is for the wrong stream type: uni=%v", spy.uni)
		assert.Equalf(t, []uint64{4}, spy.limits,
			"the re-sent STREAMS_BLOCKED carries limit %v, want [4] — §13.3 requires the "+
				"limit causing blocking AT TRANSMIT TIME, not the frozen 1 in the descriptor",
			spy.limits)
	})
}
