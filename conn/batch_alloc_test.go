//go:build !race

package conn

import (
	"bytes"
	"context"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// SendBatch is the API a load generator calls on every tick, with a []BatchEntry
// it reuses. If the call allocates per entry, the generator pays that allocation
// at its full request rate — which is the cost the whole feature exists to
// remove, so an allocating batch path would be self-defeating.
//
// Behind !race for the reason the other allocation gates in this repo are: the
// detector allocates as it instruments and swamps a difference of one. That
// makes this test invisible to the -race job, so it is carried by the named
// allocation-gate step in ci.yml instead — which matches on "DoesNotAllocate",
// hence the name.
//
// Note bench-gate does NOT cover ./conn: its absolute zero-alloc scope is frame,
// hpack, internal/bytesx, internal/bufx, qpack, quic and http3. This test is the
// only thing holding the contract here.

// batchAllocConn is discardConn's batch sibling: a Conn with an encoder, a sink
// framer and enough credit that no entry is ever refused.
func batchAllocConn(tb testing.TB, entries int) (*Conn, []BatchEntry) {
	tb.Helper()
	c := newGoAwayConn()
	c.fr = frame.NewFramer(discardWriter{}, bytes.NewReader(nil))
	c.enc = hpack.NewEncoder()
	c.opts = ConnOptions{}.defaulted()
	c.nextID = 1
	c.peerConnSendWindow = 1 << 30

	batch := make([]BatchEntry, entries)
	body := make([]byte, 256)
	for i := range batch {
		// Streams stay open across runs — an entry that ended its stream would
		// need a fresh one per iteration, and allocating THAT is the caller's
		// cost, not the batch path's.
		s := newStream(uint32(1+2*i), 8, c, 1<<24)
		s.sendWindow = 1 << 24
		c.streams[s.id] = s
		batch[i] = BatchEntry{Stream: s.ref(), Fields: batchFields, Body: body}
	}
	return c, batch
}

// TestSendBatch_DoesNotAllocate is the gate.
func TestSendBatch_DoesNotAllocate(t *testing.T) {
	c, batch := batchAllocConn(t, 8)
	ctx := context.Background()

	n := testing.AllocsPerRun(200, func() {
		if err := c.SendBatch(ctx, batch); err != nil {
			t.Fatalf("SendBatch: %v", err)
		}
		for i := range batch {
			if batch[i].Err != nil {
				t.Fatalf("entry %d: %v", i, batch[i].Err)
			}
		}
	})
	if n != 0 {
		t.Errorf("SendBatch allocates %.1f per call over %d entries, want 0 — a dataVec "+
			"or a segment scratch built per entry would do exactly this", n, len(batch))
	}
}

// TestSendBatchV_DoesNotAllocate pins the same for a vectored body, whose
// caller supplies the [][]byte.
func TestSendBatchV_DoesNotAllocate(t *testing.T) {
	c, batch := batchAllocConn(t, 8)
	ctx := context.Background()
	prefix := []byte{0, 0, 0, 0, 5}
	body := make([]byte, 256)
	for i := range batch {
		batch[i].Body = nil
		batch[i].BodyV = [][]byte{prefix, body}
	}

	n := testing.AllocsPerRun(200, func() {
		if err := c.SendBatch(ctx, batch); err != nil {
			t.Fatalf("SendBatch: %v", err)
		}
	})
	if n != 0 {
		t.Errorf("SendBatch with a vectored body allocates %.1f per call, want 0", n)
	}
}
