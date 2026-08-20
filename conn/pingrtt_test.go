package conn

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// awaitPing returns the payload of the first PING frame written to s. A PING is
// a 9-byte header followed by exactly 8 opaque bytes (RFC 9113 §6.7). It reports
// false rather than failing the test, because it runs on a peer goroutine and
// t.FailNow is only valid on the goroutine running the test.
func (s *syncSink) awaitPing(deadline time.Time) ([8]byte, bool) {
	for time.Now().Before(deadline) {
		s.mu.Lock()
		b := s.buf.Bytes()
		ok := len(b) >= 17 && b[3] == byte(frame.FramePing)
		var out [8]byte
		if ok {
			copy(out[:], b[9:17])
		}
		s.mu.Unlock()
		if ok {
			return out, true
		}
		time.Sleep(time.Millisecond)
	}
	return [8]byte{}, false
}

// TestConn_Ping_MeasuresTheRoundTrip is the assertion TestConn_Ping_RTT cannot
// carry.
//
// That test runs against a loopback httptest peer and accepts 0 <= rtt < 1s, for
// a good reason its comment states: on a coarse clock a loopback round trip
// genuinely rounds to zero. The consequence is that a Ping which never measures
// anything at all passes too — replacing time.Since(start) with a literal 0 left
// the whole package green (#825).
//
// Giving the peer a known delay puts a floor under the measurement without
// reintroducing the clock-resolution flake: the ACK cannot arrive sooner than
// the sleep, so an RTT below it is not a fast clock, it is no clock.
func TestConn_Ping_MeasuresTheRoundTrip(t *testing.T) {
	const ackDelay = 50 * time.Millisecond

	sink := &syncSink{}
	c := newGoAwayConn()
	c.fr = frame.NewFramer(sink, bytes.NewReader(nil)) // writer first
	acked := make(chan [8]byte, 1)
	go func() {
		payload, ok := sink.awaitPing(time.Now().Add(5 * time.Second))
		if !ok {
			return
		}
		time.Sleep(ackDelay)
		c.deliverPingAck(payload)
		acked <- payload
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rtt, err := c.Ping(ctx)

	require.NoError(t, err, "Ping against a peer that answers after a fixed delay")
	assert.GreaterOrEqualf(t, rtt, ackDelay,
		"RTT = %v for an ACK the peer withheld for %v — the round trip cannot have been "+
			"shorter than the delay, so this is a Ping that reports a number it never measured",
		rtt, ackDelay)
	var seen [8]byte
	select {
	case seen = <-acked:
	case <-time.After(time.Second):
		require.FailNow(t, "the peer goroutine never saw a PING to acknowledge; "+
			"the fixture never produced the round trip this test measures")
	}
	assert.Equalf(t, binary.BigEndian.Uint64(seen[:]), uint64(1),
		"the ACK closed the waiter for payload %v; Ping numbers its payloads from 1, so a "+
			"different value means the RTT above belongs to some other frame", seen)
}
