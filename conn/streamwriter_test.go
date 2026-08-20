package conn

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// streamWriter is meant to be "the narrow surface a *Stream needs from its owner
// Conn". It was not: Stream reached markStreamDone and writeRSTStreamBestEffort
// by downcasting to *Conn, so against a fake those calls silently vanished and
// every test built on one certified a lifecycle production does not run — no
// slot retired, no best-effort reset.
//
// The compiler enforces the surface now. These pin that the calls actually
// arrive, because "it compiles" only proves the method exists.

// TestStreamWriter_EndStreamRetiresThroughTheInterface is the gate. It ends the
// local side through the ordinary send path and requires the retire to reach the
// writer.
func TestStreamWriter_EndStreamRetiresThroughTheInterface(t *testing.T) {
	w := &fakeStreamWriter{}
	s := newStream(1, 8, w, 65535)
	s.id = 1
	s.sendWindow = 65535

	err := s.sendData(context.Background(), s.gen.Load(), []byte("x"), true)

	require.NoError(t, err, "sendData")
	w.mu.Lock()
	defer w.mu.Unlock()
	assert.Equalf(t, 1, w.doneCalls,
		"markStreamDone reached the writer %d times, want 1 — the retire used to "+
			"be a downcast, so a fake never saw it and the slot was never released in any "+
			"test built on one", w.doneCalls)
	assert.Equalf(t, uint32(1), w.lastDoneID, "markStreamDone got id %d, want 1", w.lastDoneID)
}

// TestStreamWriter_NoRetireWithoutEndStream is the negative half: a send that
// does not end the stream must not retire it.
func TestStreamWriter_NoRetireWithoutEndStream(t *testing.T) {
	w := &fakeStreamWriter{}
	s := newStream(1, 8, w, 65535)
	s.id = 1
	s.sendWindow = 65535

	err := s.sendData(context.Background(), s.gen.Load(), []byte("x"), false)

	require.NoError(t, err, "sendData")
	w.mu.Lock()
	defer w.mu.Unlock()
	assert.Zerof(t, w.doneCalls,
		"markStreamDone fired %d times for a non-final DATA frame, want 0", w.doneCalls)
}

// TestEndLocalAndRetire_ReadsIDUnderTheLock pins the discipline the helper
// exists for: the id handed to markStreamDone is the one read in the same s.mu
// section that set localEnded, not a second bare read afterwards. A struct whose
// local and remote sides have both ended can be recycled between those two
// reads and handed to another request.
func TestEndLocalAndRetire_ReadsIDUnderTheLock(t *testing.T) {
	w := &fakeStreamWriter{}
	s := newStream(7, 8, w, 65535)
	s.id = 7

	s.endLocalAndRetire()

	s.mu.Lock()
	ended := s.localEnded
	s.mu.Unlock()
	assert.True(t, ended, "endLocalAndRetire did not latch localEnded")
	w.mu.Lock()
	defer w.mu.Unlock()
	assert.Equalf(t, uint32(7), w.lastDoneID,
		"retired id %d, want 7 — the id must come from the same locked section "+
			"that set localEnded", w.lastDoneID)
}

// TestWriteRSTStreamID_NeedsNoStream pins the inverted primitive. rstStream used
// to fabricate a &Stream{id: id} purely to satisfy a signature — a value that
// looks like a live registered stream, is not one, and allocates on the
// push-refusal path.
func TestWriteRSTStreamID_NeedsNoStream(t *testing.T) {
	c := newGoAwayConn()
	var buf bytes.Buffer
	c.fr = frame.NewFramer(&buf, nil)

	err := c.writeRSTStreamID(9, frame.ErrCodeRefusedStream)

	require.NoError(t, err, "writeRSTStreamID")
	assert.NotZero(t, buf.Len(), "no RST_STREAM reached the wire")
}
