package conn

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
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
	if err := h.OnGoAway(frame.FrameHeader{}, 0, frame.ErrCodeNoError, nil); err != nil {
		t.Fatalf("OnGoAway: %v", err)
	}
	if !c.goAwayReceived.Load() {
		t.Fatalf("goAwayReceived flag not set")
	}
	if _, err := c.NewStream(context.Background()); !errors.Is(err, ErrGoAway) {
		t.Fatalf("NewStream err = %v, want ErrGoAway", err)
	}
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
			if err := h.OnGoAway(frame.FrameHeader{}, tc.last, tc.code, nil); err != nil {
				t.Fatalf("OnGoAway: %v", err)
			}

			_, err := c.NewStream(context.Background())
			// The sentinel is still the contract for callers that only ask
			// "was this a GOAWAY" — client/retry.go is one of them.
			if !errors.Is(err, ErrGoAway) {
				t.Fatalf("NewStream err = %v, want it to match ErrGoAway", err)
			}
			var ge *GoAwayError
			if !errors.As(err, &ge) {
				t.Fatalf("NewStream err = %T (%v), want a *GoAwayError carrying the peer's reason", err, err)
			}
			if ge.Code != tc.code {
				t.Errorf("Code = %v, want %v — the peer's reason was discarded", ge.Code, tc.code)
			}
			if ge.LastStreamID != tc.last {
				t.Errorf("LastStreamID = %d, want %d", ge.LastStreamID, tc.last)
			}
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

	// keep stays
	if _, ok := c.streams[3]; !ok {
		t.Fatalf("stream 3 evicted but should survive (id ≤ 3)")
	}
	if _, ok := c.streams[5]; ok {
		t.Fatalf("stream 5 not evicted but should be (id > 3)")
	}
	if c.inflight != 1 {
		t.Fatalf("inflight = %d, want 1", c.inflight)
	}

	// Drop received an EventReset.
	select {
	case ev := <-drop.events:
		if ev.Type != EventReset || ev.RSTCode != frame.ErrCodeRefusedStream {
			t.Fatalf("drop got %+v, want EventReset(REFUSED_STREAM)", ev)
		}
	case <-time.After(time.Second):
		t.Fatalf("drop never got reset event")
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

	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := c.acquireSendCredits(ctx, s, s.gen.Load(), 100, 0)
		done <- result{err}
	}()

	time.Sleep(50 * time.Millisecond)
	// lastStreamID 0 < stream 1: the peer never processed our HEADERS, so this
	// stream is refused and the writer must stop, not wait for credit.
	c.onGoAwayReceived(0, frame.ErrCodeNoError)

	select {
	case r := <-done:
		if !errors.Is(r.err, ErrStreamClosed) {
			t.Fatalf("acquireSendCredits returned %v, want ErrStreamClosed;\n"+
				"the writer left for some reason other than the GOAWAY that refused its stream", r.err)
		}
	case <-time.After(time.Second):
		t.Fatalf("acquireSendCredits still parked 1s after a GOAWAY refused its stream;\n" +
			"the GOAWAY path did not broadcast on fcOutCond, so a blocked writer never re-checks (B.2.6)")
	}
}

// TestOnPing_AckFrame_IsNoop confirms an inbound PING with ACK=1 is
// routed to deliverPingAck (D.4). With no waiter registered the call
// is a no-op: no echo frame is written back to the peer.
func TestOnPing_AckFrame_IsNoop(t *testing.T) {
	var buf bytes.Buffer
	fr := frame.NewFramer(&buf, bytes.NewReader([]byte{}))
	c := newGoAwayConn()
	c.fr = fr
	h := newConnHandler(c, hpack.NewDecoder())
	if err := h.OnPing(frame.FrameHeader{Flags: frame.FlagPingAck}, [8]byte{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatalf("OnPing(ACK): %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("ACK echoed for ACK input: %d bytes", buf.Len())
	}
}

// TestOnPing_NonAck_EchoesPayloadWithAckFlag verifies the RFC §6.7
// rule: receive PING (no ACK) → send PING (ACK=1) with identical
// 8-byte opaque data.
func TestOnPing_NonAck_EchoesPayloadWithAckFlag(t *testing.T) {
	var buf bytes.Buffer
	fr := frame.NewFramer(&buf, bytes.NewReader([]byte{}))
	c := newGoAwayConn()
	c.fr = fr
	h := newConnHandler(c, hpack.NewDecoder())
	payload := [8]byte{0xDE, 0xAD, 0xBE, 0xEF, 0xFA, 0xCE, 0xCA, 0xFE}
	if err := h.OnPing(frame.FrameHeader{}, payload); err != nil {
		t.Fatalf("OnPing: %v", err)
	}
	got := parseFrameHeaders(t, buf.Bytes())
	if len(got) != 1 {
		t.Fatalf("frame count = %d, want 1", len(got))
	}
	if got[0].ftype != 0x6 { // PING
		t.Fatalf("ftype = 0x%x, want 0x6 (PING)", got[0].ftype)
	}
	if got[0].flags&0x1 == 0 {
		t.Fatalf("ACK flag not set")
	}
	if got[0].length != 8 {
		t.Fatalf("PING length = %d, want 8", got[0].length)
	}
	gotPayload := buf.Bytes()[9:17]
	if !bytes.Equal(gotPayload, payload[:]) {
		t.Fatalf("payload = %x, want %x", gotPayload, payload)
	}
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
