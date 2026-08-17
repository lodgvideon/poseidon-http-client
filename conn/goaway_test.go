package conn

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOnGoAway_BlocksNewStream verifies that after a GOAWAY frame is
// received, NewStream returns ErrGoAway (RFC 7540 §6.8).
//
// errors.Is rather than ==: NewStream now returns a *GoAwayError carrying the
// peer's reason, and that type reports itself as ErrGoAway. The sentinel is
// still the contract, the equality is not.
func TestOnGoAway_BlocksNewStream(t *testing.T) {
	c := newGoAwayConn()
	h := newConnHandler(c, hpack.NewDecoder())

	err := h.OnGoAway(frame.FrameHeader{}, 0, frame.ErrCodeNoError, nil)

	require.NoErrorf(t, err, "OnGoAway")
	require.True(t, c.goAwayReceived.Load(), "goAwayReceived flag not set")
	_, nsErr := c.NewStream(context.Background())
	require.ErrorIsf(t, nsErr, ErrGoAway, "NewStream err = %v, want ErrGoAway", nsErr)
}

// TestOnGoAway_SurfacesPeerCodeAndLastStreamID is the gate on #570: the peer's
// reason must reach the caller.
//
// It used to be dropped at onGoAwayReceived's signature, so a graceful drain
// (NO_ERROR), a demand to back off (ENHANCE_YOUR_CALM) and an outright
// rejection all arrived as one sentinel — and those three call for opposite
// responses from a load generator, which is this library's stated user.
//
// The table drives distinct codes on purpose: a fix that hard-coded any single
// value, or that stored the code but reported a zero one, passes a one-code
// test.
func TestOnGoAway_SurfacesPeerCodeAndLastStreamID(t *testing.T) {
	for _, tc := range []struct {
		name string
		code frame.ErrCode
		last uint32
	}{
		{"graceful drain", frame.ErrCodeNoError, 0},
		{"back off", frame.ErrCodeEnhanceYourCalm, 7},
		{"peer rejects us", frame.ErrCodeProtocolError, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGoAwayConn()
			h := newConnHandler(c, hpack.NewDecoder())
			require.NoErrorf(t, h.OnGoAway(frame.FrameHeader{}, tc.last, tc.code, nil), "OnGoAway")

			_, err := c.NewStream(context.Background())

			// The sentinel is still the contract for callers that only ask
			// "was this a GOAWAY" — client/retry.go is one of them.
			require.ErrorIsf(t, err, ErrGoAway, "NewStream err = %v, want it to match ErrGoAway", err)
			var ge *GoAwayError
			require.ErrorAsf(t, err, &ge,
				"NewStream err = %T (%v), want a *GoAwayError carrying the peer's reason", err, err)
			assert.Equalf(t, tc.code, ge.Code,
				"Code = %v, want %v — the peer's reason was discarded", ge.Code, tc.code)
			assert.Equalf(t, tc.last, ge.LastStreamID,
				"LastStreamID = %d, want %d", ge.LastStreamID, tc.last)
		})
	}
}

// TestOnGoAway_StreamsAtOrBelowLastID_Survive confirms streams whose
// id is ≤ lastStreamID stay live (peer is processing them); streams
// above lastStreamID are reset with REFUSED_STREAM and evicted.
func TestOnGoAway_StreamsAtOrBelowLastID_Survive(t *testing.T) {
	c := newGoAwayConn()
	keep := newStream(3, 8, c, 65535)
	keep.id = 3
	c.streams[3] = keep
	c.inflight++
	drop := newStream(5, 8, c, 65535)
	drop.id = 5
	c.streams[5] = drop
	c.inflight++

	c.onGoAwayReceived(3, frame.ErrCodeNoError)

	_, keptOK := c.streams[3]
	require.True(t, keptOK, "stream 3 evicted but should survive (id ≤ 3)")
	_, droppedOK := c.streams[5]
	require.False(t, droppedOK, "stream 5 not evicted but should be (id > 3)")
	require.EqualValuesf(t, 1, c.inflight, "inflight = %d, want 1", c.inflight)
	// Drop received an EventReset.
	select {
	case ev := <-drop.events:
		assert.Equalf(t, EventReset, ev.Type, "drop got %+v, want EventReset(REFUSED_STREAM)", ev)
		assert.Equalf(t, frame.ErrCodeRefusedStream, ev.RSTCode,
			"drop got %+v, want EventReset(REFUSED_STREAM)", ev)
	case <-time.After(time.Second):
		require.FailNow(t, "drop never got reset event")
	}
}

// TestOnGoAway_WakesAcquireSendCredits asserts a writer blocked on send credit
// observes the GOAWAY-driven cond.Broadcast (B.2.6).
//
// Two things make this attributable, and an earlier version of this test had
// neither. It PASSED with the GOAWAY-side fcOutCond.Broadcast deleted — verified
// by mutation — because it gave the writer a 2s context and then waited 3s for
// it to return. The context was doing the waking, via the AfterFunc watchdog
// acquireSendCredits registers on its first block, and the 2.00s runtime was the
// tell.
//
//  1. The context outlives the assertion window by 30x, so a timeout cannot be
//     what ends the wait.
//  2. The GOAWAY makes this stream a REFUSED victim (lastStreamID below its id),
//     so the writer has a reason to return rather than re-park on a window that
//     is still zero. It returns ErrStreamClosed, which names the cause — a bare
//     "it returned" would still be satisfied by any other wake.
func TestOnGoAway_WakesAcquireSendCredits(t *testing.T) {
	c := newGoAwayConn()
	c.peerConnSendWindow = 0 // force the wait
	s := newStream(1, 8, c, 65535)
	s.id = 1
	s.sendWindow = 65535
	c.streams[1] = s
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := c.acquireSendCredits(ctx, s, s.gen.Load(), 100, 0)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)

	// lastStreamID 0 < stream 1: the peer never processed our HEADERS, so this
	// stream is refused and the writer must stop, not wait for credit.
	c.onGoAwayReceived(0, frame.ErrCodeNoError)

	select {
	case err := <-done:
		require.ErrorIsf(t, err, ErrStreamClosed,
			"acquireSendCredits returned %v, want ErrStreamClosed;\n"+
				"the writer left for some reason other than the GOAWAY that refused its stream", err)
	case <-time.After(time.Second):
		require.FailNow(t, "acquireSendCredits still parked 1s after a GOAWAY refused its stream;\n"+
			"the GOAWAY path did not broadcast on fcOutCond, so a blocked writer never re-checks (B.2.6)")
	}
}

// TestOnPing_AckFrame_IsNoop confirms an inbound PING with ACK=1 is
// routed to deliverPingAck (D.4). With no waiter registered the call
// is a no-op: no echo frame is written back to the peer.
func TestOnPing_AckFrame_IsNoop(t *testing.T) {
	var buf bytes.Buffer
	c := newGoAwayConn()
	c.fr = frame.NewFramer(&buf, bytes.NewReader([]byte{}))
	h := newConnHandler(c, hpack.NewDecoder())

	err := h.OnPing(frame.FrameHeader{Flags: frame.FlagPingAck}, [8]byte{1, 2, 3, 4, 5, 6, 7, 8})

	require.NoErrorf(t, err, "OnPing(ACK)")
	require.Zerof(t, buf.Len(), "ACK echoed for ACK input: %d bytes", buf.Len())
}

// TestOnPing_NonAck_EchoesPayloadWithAckFlag verifies the RFC §6.7
// rule: receive PING (no ACK) → send PING (ACK=1) with identical
// 8-byte opaque data.
func TestOnPing_NonAck_EchoesPayloadWithAckFlag(t *testing.T) {
	var buf bytes.Buffer
	c := newGoAwayConn()
	c.fr = frame.NewFramer(&buf, bytes.NewReader([]byte{}))
	h := newConnHandler(c, hpack.NewDecoder())
	payload := [8]byte{0xDE, 0xAD, 0xBE, 0xEF, 0xFA, 0xCE, 0xCA, 0xFE}

	err := h.OnPing(frame.FrameHeader{}, payload)

	require.NoErrorf(t, err, "OnPing")
	got := parseFrameHeaders(t, buf.Bytes())
	require.Lenf(t, got, 1, "frame count = %d, want 1", len(got))
	require.Equalf(t, byte(0x6), got[0].ftype, "ftype = 0x%x, want 0x6 (PING)", got[0].ftype)
	require.NotZero(t, got[0].flags&0x1, "ACK flag not set")
	require.EqualValuesf(t, 8, got[0].length, "PING length = %d, want 8", got[0].length)
	gotPayload := buf.Bytes()[9:17]
	assert.Truef(t, bytes.Equal(gotPayload, payload[:]),
		"payload = %x, want %x", gotPayload, payload)
}

// newGoAwayConn builds a *Conn just enough for OnGoAway / OnPing /
// NewStream unit tests.
func newGoAwayConn() *Conn {
	c := &Conn{
		opts: ConnOptions{
			Settings: AdvertisedSettings{MaxConcurrentStreams: 100},
		}.defaulted(),
		streams:            map[uint32]*Stream{},
		readerDone:         make(chan struct{}),
		pingWaiters:        make(map[[8]byte]chan struct{}),
		peerConnSendWindow: 65535,
	}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	return c
}
