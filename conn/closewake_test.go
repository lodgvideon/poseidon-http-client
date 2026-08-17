package conn

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestStream_CloseWakesASenderParkedOnCredit pins that abandoning a stream
// actually abandons its writer.
//
// acquireSendCredits already bails with ErrStreamClosed when it observes
// s.closed — but only on wake, and it is parked in cond.Wait(). That check and
// the broadcast that makes it observable were written together for the peer
// RST_STREAM path (RFC 9113 §6.4). Close sets the same flag and inherited
// neither, so a Send blocked on flow-control credit slept through the Close
// meant to abandon it and woke only when its own context expired — which for a
// long-lived request context means a stuck goroutine, on the one API documented
// as "how a client abandons an RPC early".
func TestStream_CloseWakesASenderParkedOnCredit(t *testing.T) {
	var buf bytes.Buffer
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.fr = frame.NewFramer(&buf, nil) // writer first
	c.peerConnSendWindow = 0          // no credit, and none is coming

	s := newStream(1, 8, c, 65535)
	s.sendWindow = 0
	c.streams[1] = s

	// A context far longer than the test, so passing cannot come from it.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.ref().SendData(ctx, []byte("payload"), false) }()

	// Let the writer reach cond.Wait rather than racing Close against its entry.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.fcOutMu.Lock()
		parked := c.peerConnSendWindow == 0
		c.fcOutMu.Unlock()
		if parked {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	_ = s.ref().Close()

	select {
	case err := <-done:
		require.Truef(t, errors.Is(err, ErrStreamClosed), "Send = %v, want ErrStreamClosed", err)
	case <-time.After(3 * time.Second):
		require.FailNow(t, "Send still parked 3s after Close; only its own context would free it")
	}
}
