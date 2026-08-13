package quic

import "testing"

// Draining pendingCtrl has two halves that must happen together: the frames go out,
// and the queue is emptied. Only the first half was covered — mutating
// takePendingCtrl to emit the frames WITHOUT emptying the queue left the whole quic
// suite green, twice.
//
// That mutation is not cosmetic. pendingCtrl non-empty is what makes flush decide
// there is app-space work to do (hasCtrl in conn_seal.go), so a queue that never
// empties re-sends the same MAX_DATA / MAX_STREAM_DATA grants on every subsequent
// flush, forever, and keeps a packet owed at all times. The peer is not harmed —
// credit grants are idempotent — which is exactly why nothing noticed.
//
// The second flush is the assertion that catches it; asserting only "the first
// flush wrote one datagram" is what the suite already did.

// drainConn builds a 1-RTT-ready connection whose writes are captured, matching the
// local fixture TestConformance_RFC9000_Sec822_PathResponsePaddedTo1200 uses.
func drainConn() (*Conn, *capturePC) {
	dcid := []byte("draintst")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	pc := &capturePC{}
	c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer, handshakeComplete: true}
	c.keys.OneRTT, _ = NewOpener(keys)
	return c, pc
}

// TestFlush_DrainsPendingCtrl pins the queue-emptying half for the reader-side
// flush (conn_seal.go).
func TestFlush_DrainsPendingCtrl(t *testing.T) {
	c, pc := drainConn()
	c.pendingCtrl = AppendMaxData(nil, 1000)

	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if len(pc.pkts) != 1 {
		t.Fatalf("first flush wrote %d datagrams, want 1", len(pc.pkts))
	}
	if len(c.pendingCtrl) != 0 {
		t.Errorf("pendingCtrl still holds %d bytes after being flushed", len(c.pendingCtrl))
	}

	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if len(pc.pkts) != 1 {
		t.Fatalf("a second flush with nothing new queued wrote %d datagrams, want 1: "+
			"the grants were re-sent because the queue was never emptied", len(pc.pkts))
	}
}

// TestFlushControl_DrainsPendingCtrl pins the same half for the consumer-side
// flushControl (conn_recv.go), which drains the same queue on a different
// goroutine. Both call takePendingCtrl; before it they each had their own copy of
// the drain, so both need the guard.
func TestFlushControl_DrainsPendingCtrl(t *testing.T) {
	c, pc := drainConn()
	c.pendingCtrl = AppendMaxData(nil, 1000)

	c.mu.Lock()
	err := c.flushControl()
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(pc.pkts) != 1 {
		t.Fatalf("first flushControl wrote %d datagrams, want 1", len(pc.pkts))
	}
	if len(c.pendingCtrl) != 0 {
		t.Errorf("pendingCtrl still holds %d bytes after being flushed", len(c.pendingCtrl))
	}

	c.mu.Lock()
	err = c.flushControl()
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(pc.pkts) != 1 {
		t.Fatalf("a second flushControl with nothing queued wrote %d datagrams, want 1: "+
			"flushControl returns early only because the queue is empty", len(pc.pkts))
	}
}
