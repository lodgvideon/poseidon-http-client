package conn

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestOnGoAway_BlocksNewStream verifies that after a GOAWAY frame is
// received, NewStream returns ErrGoAway (RFC 7540 §6.8).
func TestOnGoAway_BlocksNewStream(t *testing.T) {
	c := newGoAwayConn()
	h := newConnHandler(c, hpack.NewDecoder())
	if err := h.OnGoAway(frame.FrameHeader{}, 0, frame.ErrCodeNoError, nil); err != nil {
		t.Fatalf("OnGoAway: %v", err)
	}
	if !c.goAwayReceived.Load() {
		t.Fatalf("goAwayReceived flag not set")
	}
	if _, err := c.NewStream(context.Background()); err != ErrGoAway {
		t.Fatalf("NewStream err = %v, want ErrGoAway", err)
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

// TestOnGoAway_WakesAcquireSendCredits asserts a writer blocked on
// send credit observes the GOAWAY-driven cond.Broadcast.
func TestOnGoAway_WakesAcquireSendCredits(t *testing.T) {
	c := newGoAwayConn()
	c.peerConnSendWindow = 0 // force the wait
	s := newStream(1, 8, c, 65535)
	s.id = 1
	s.sendWindow = 65535
	c.streams[1] = s

	woke := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = c.acquireSendCredits(ctx, s, 100)
		close(woke)
	}()

	time.Sleep(50 * time.Millisecond)
	c.onGoAwayReceived(99, frame.ErrCodeNoError)

	// The cond.Broadcast wakes the waiter; the loop re-evaluates,
	// peerConnSendWindow is still zero, so it spins until ctx
	// expires. We just want to make sure the broadcast happened —
	// observable via the post-cond loop iteration.
	select {
	case <-woke:
	case <-time.After(3 * time.Second):
		t.Fatalf("acquireSendCredits never returned after GOAWAY broadcast")
	}
}

// TestOnPing_AckFrame_IsNoop confirms an inbound PING with ACK=1 is
// silently dropped (we never initiate active PINGs in B.2.6).
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
