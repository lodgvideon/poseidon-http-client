//go:build !race

package conn

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// writeData is on the hot path of every request with a body, and the vectored
// twin it now delegates to takes a [][]byte. A one-element vector built per call
// would put a slice header array on the heap for every DATA frame — trading a
// duplicated state machine for an allocation, which on this path is the worse of
// the two.
//
// Behind !race for the reason the other allocation gates in this repo are: the
// detector allocates as it instruments and swamps a difference of one.
//
// Note bench-gate does NOT cover ./conn — its absolute zero-alloc scope is
// frame, hpack, internal/bytesx, qpack, quic and http3 — so this test is the
// only thing holding the contract here.

// discardConn builds a Conn whose framer writes into a sink, with enough credit
// for the payloads below.
func discardConn(tb testing.TB) (*Conn, *Stream) {
	tb.Helper()
	c := newGoAwayConn()
	c.fr = frame.NewFramer(discardWriter{}, bytes.NewReader(nil))
	c.opts.Settings.MaxFrameSize = 16384
	s := newStream(1, 8, c, 1<<24)
	s.id = 1
	s.sendWindow = 1 << 24
	c.peerConnSendWindow = 1 << 24
	return c, s
}

// discardWriter keeps nothing, so the gate measures the send path rather than a
// growing buffer.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestWriteData_DoesNotAllocate is the gate.
func TestWriteData_DoesNotAllocate(t *testing.T) {
	c, s := discardConn(t)
	ctx := context.Background()
	payload := make([]byte, 4096)
	gen := s.gen.Load()

	// Plain t.Fatalf inside the closure, never require: testify reflects and
	// allocates, and AllocsPerRun counts the whole process. The count assertion
	// below is outside the measured body.
	n := testing.AllocsPerRun(200, func() {
		if err := c.writeData(ctx, s, gen, payload, false); err != nil {
			t.Fatalf("writeData: %v", err)
		}
	})

	assert.Zerof(t, n, "writeData allocates %.1f per call, want 0 — a one-element vector built "+
		"per call would do exactly this", n)
}

// TestWriteDataV_DoesNotAllocate pins the same for the vectored entry point,
// whose caller supplies the slice.
func TestWriteDataV_DoesNotAllocate(t *testing.T) {
	c, s := discardConn(t)
	ctx := context.Background()
	prefix := []byte{0, 0, 0, 0, 5}
	body := make([]byte, 4096)
	bufs := [][]byte{prefix, body}
	gen := s.gen.Load()

	n := testing.AllocsPerRun(200, func() {
		if err := c.writeDataV(ctx, s, gen, bufs, false); err != nil {
			t.Fatalf("writeDataV: %v", err)
		}
	})

	assert.Zerof(t, n, "writeDataV allocates %.1f per call, want 0", n)
}
