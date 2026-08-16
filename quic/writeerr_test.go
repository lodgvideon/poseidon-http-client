package quic

import (
	"errors"
	"testing"
)

// Every send path ends in a pc.Write whose error must reach the caller: a datagram
// that never left the host is not a lost packet, and nothing recovers it. Loss is
// self-healing — the peer's missing ACK arms a PTO and the frame is resent — so a
// swallowed local write error is the one failure the transport cannot repair, and
// it presents as a connection that simply waits.
//
// #674 gated the first of these (conn_seal.go, TestConn_SendPath_WriteErrorReachesCaller)
// and left failWritePC behind; the mutation battery that found it deferred the rest
// under a one-round budget, which is #676. The three below had no test at all, so
// deleting the error check at any of them left the whole quic and http3 suites green.
//
// They are gated where each one is actually observable, which is not uniform:
// flushBatch surfaces through Stream.Send, writeAppFrames through Stream.Reset, and
// flushControl through nothing — its only non-test caller (stream.go, granting
// receive credit on the consumer's goroutine, INV-3) discards the error deliberately,
// so the honest gate is on flushControl itself and the caller's choice is left alone.

// failWriteConn builds a 1-RTT-ready connection whose failOn-th datagram write fails,
// matching drainConn's shape (pendingctrl_drain_test.go) with the capturing transport
// swapped for the failing one.
func failWriteConn(failOn int) (*Conn, *failWritePC) {
	dcid := []byte("wrerrtst")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	pc := &failWritePC{failOn: failOn}
	c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer, handshakeComplete: true}
	c.keys.OneRTT, _ = NewOpener(keys)
	return c, pc
}

// TestConn_FlushControl_WriteErrorIsReturned gates quic/conn_recv.go's pc.Write.
//
// flushControl emits the queued MAX_DATA / MAX_STREAM_DATA grants on the consumer's
// goroutine so a flow-control-blocked peer is unblocked without a round trip. Its one
// caller discards the result on purpose, so this asserts the error is *returned* —
// the property that keeps that discard a decision rather than an accident, and the
// only thing a future caller wanting to act on it would have to rely on.
func TestConn_FlushControl_WriteErrorIsReturned(t *testing.T) {
	c, pc := failWriteConn(1)
	c.pendingCtrl = AppendMaxData(nil, 1000)

	c.mu.Lock()
	err := c.flushControl()
	c.mu.Unlock()

	if !errors.Is(err, errSendFailed) {
		t.Fatalf("flushControl = %v, want the transport's send error %v: the credit grant "+
			"never left the host, and reporting success means a peer that is still blocked "+
			"looks like a peer that has been unblocked", err, errSendFailed)
	}
	if pc.writes != 1 {
		t.Fatalf("transport saw %d writes, want 1 — the failing write is the one being gated, "+
			"so a different count means this test proved something else", pc.writes)
	}
}

// TestStream_Reset_WriteErrorReachesCaller gates quic/send.go's pc.Write, through
// writeAppFrames' most direct public caller.
//
// Reset is the strongest case for propagation on this path: it has already marked the
// send side aborted and dropped the stream's buffered data from the retransmit sources
// (§13.3) before the write, so a swallowed error leaves a stream that this endpoint
// considers reset and the peer has never been told about — and nothing will retry,
// because the frame's retransmission depends on the packet having been sent.
func TestStream_Reset_WriteErrorReachesCaller(t *testing.T) {
	c, pc := failWriteConn(1)
	s := &Stream{id: 0, conn: c, sendMax: 1 << 30}

	err := s.Reset(0x10)

	if !errors.Is(err, errSendFailed) {
		t.Fatalf("Stream.Reset = %v, want the transport's send error %v: the RESET_STREAM "+
			"never left the host while the send side was already marked aborted locally, so "+
			"the caller believes the peer was told", err, errSendFailed)
	}
	if pc.writes != 1 {
		t.Fatalf("transport saw %d writes, want 1", pc.writes)
	}
}

// TestConn_FlushBatch_WriteErrorReachesCaller gates quic/gso.go's lone-datagram
// pc.Write — the n == 1 arm, which skips GSO because a single datagram gains nothing
// from a cmsg. It is the arm every small send takes.
func TestConn_FlushBatch_WriteErrorReachesCaller(t *testing.T) {
	c, pc := failWriteConn(1)
	b := c.newBatch()
	if err := c.addToBatch(&b, make([]byte, 1200)); err != nil {
		t.Fatal(err)
	}

	err := c.flushBatch(&b)

	if !errors.Is(err, errSendFailed) {
		t.Fatalf("flushBatch = %v, want the transport's send error %v: the datagram was "+
			"sealed and recorded in flight, so swallowing this reports a packet as sent that "+
			"the host never handed to the network", err, errSendFailed)
	}
	if pc.writes != 1 {
		t.Fatalf("transport saw %d writes, want 1", pc.writes)
	}
}

// TestConn_FlushBatch_WriteErrorReachesCaller_Fallback is the same gate on the
// per-datagram fallback loop, which is a different Write call: failWritePC implements
// no WriteGSO, so a multi-datagram batch goes through writeSegmentsTo. The injection
// is aimed at the SECOND datagram so the loop is proven to abort mid-batch rather than
// only on its first iteration — a check that only ever fails first is satisfied by a
// loop that ignores every later error.
func TestConn_FlushBatch_WriteErrorReachesCaller_Fallback(t *testing.T) {
	c, pc := failWriteConn(2)
	b := c.newBatch()
	for i := 0; i < 3; i++ {
		if err := c.addToBatch(&b, make([]byte, 1200)); err != nil {
			t.Fatal(err)
		}
	}

	err := c.flushBatch(&b)

	if !errors.Is(err, errSendFailed) {
		t.Fatalf("flushBatch = %v, want the transport's send error %v", err, errSendFailed)
	}
	if pc.writes != 2 {
		t.Fatalf("transport saw %d writes, want 2 — the loop must stop at the failing "+
			"datagram, and a different count means the abort happened somewhere else", pc.writes)
	}
	if len(pc.written) != 1 {
		t.Fatalf("%d datagrams were delivered, want the 1 that preceded the failure", len(pc.written))
	}
}
