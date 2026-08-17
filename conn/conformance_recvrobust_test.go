package conn

// Receiver-robustness conformance: three MUST/MUST-NOT rules the client already
// honors by construction but that had no regression test. Each pins behavior
// against a peer that exercises a corner the happy path never reaches.

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConformance_RFC9113_Sec4_3_HeaderDecodeError_ConnCompressionError pins
// RFC 9113 §4.3: "A decoding error in a field block MUST be treated as a
// connection error (Section 5.4.1) of type COMPRESSION_ERROR." A corrupt HPACK
// stream desyncs the one decoder shared by every stream, so the remedy is a
// whole-connection teardown, never a single stream reset.
func TestConformance_RFC9113_Sec4_3_HeaderDecodeError_ConnCompressionError(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	m.addStream(1)
	// RFC 7541 §6.1: "The index value of 0 is not used. It MUST be treated as a decoding error..."
	// 0x80 is an Indexed Header Field representation with index 0.
	bad := []byte{0x80}

	err := h.OnHeaders(frame.FrameHeader{
		Type: frame.FrameHeaders, Length: uint32(len(bad)),
		Flags: frame.FlagHeadersEndHeaders, StreamID: 1,
	}, bad, nil, 0)

	var ce *ConnError
	require.Truef(t, errors.As(err, &ce),
		"err = %v (%T), want *ConnError — a decode error is connection-scoped (§4.3), not a stream reset", err, err)
	assert.Equalf(t, frame.ErrCodeCompressionError, ce.Code, "code = %v, want COMPRESSION_ERROR (§4.3)", ce.Code)
}

// TestConformance_RFC9113_Sec6_9_1_WindowUpdateOnClosedStream_Tolerated pins
// RFC 9113 §6.9.1: a peer may send WINDOW_UPDATE after its own END_STREAM, so a
// receiver can see one on a "half-closed (remote)" or "closed" stream — "A
// receiver MUST NOT treat this as an error." Neither a fully-closed (evicted)
// stream nor a half-closed(remote) one may turn a late WINDOW_UPDATE into an
// error.
func TestConformance_RFC9113_Sec6_9_1_WindowUpdateOnClosedStream_Tolerated(t *testing.T) {
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.nextID = 5 // client streams 1 and 3 were opened then closed — 3 is closed, not idle

	// (b) A WINDOW_UPDATE on a half-closed(remote) stream still registered because
	// our request upload has not finished; it credits the send window, no error.
	s := newStream(1, 8, &fakeStreamWriter{}, 65535)
	s.remoteEnded = true
	c.streams[1] = s

	// (a) A WINDOW_UPDATE on a fully-closed (evicted, no longer registered) stream.
	closedErr := c.onWindowUpdate(3, 100)
	halfClosedErr := c.onWindowUpdate(1, 100)

	assert.NoErrorf(t, closedErr,
		"WINDOW_UPDATE on closed stream 3: err = %v, want nil (§6.9.1 MUST NOT treat as error)", closedErr)
	assert.NoErrorf(t, halfClosedErr,
		"WINDOW_UPDATE on half-closed(remote) stream 1: err = %v, want nil (§6.9.1)", halfClosedErr)
}

// TestConformance_RFC9113_Sec8_1_CompleteResponseNotDiscardedByTrailingRSTNoError
// pins RFC 9113 §8.1: a server that has sent a complete response MAY ask the
// client to abort its still-open upload with RST_STREAM(NO_ERROR) — "Clients
// MUST NOT discard responses as a result of receiving such a RST_STREAM." The
// reset must be tolerated (no error) and must arrive after the already-delivered
// response, so a client that stopped reading at END_STREAM keeps it.
func TestConformance_RFC9113_Sec8_1_CompleteResponseNotDiscardedByTrailingRSTNoError(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	// A complete response (END_STREAM) arrives while our upload is still open.
	require.NoError(t, deliverBlock(t, h, 1, status("200"), true), "response HEADERS")
	ev := <-s.events
	require.Equalf(t, EventHeaders, ev.Type, "response event = %+v, want a complete 200 response", ev)
	require.Truef(t, ev.EndStream, "response event = %+v, want a complete 200 response", ev)
	require.Equalf(t, "200", statusOf(ev.Headers), "response event = %+v, want a complete 200 response", ev)

	// The server then asks us to abort the upload with RST_STREAM(NO_ERROR).
	err := h.OnRSTStream(frame.FrameHeader{Type: frame.FrameRSTStream, StreamID: 1}, frame.ErrCodeNoError)

	require.NoErrorf(t, err, "trailing RST(NO_ERROR): err = %v, want nil — must be tolerated (§8.1)", err)
	// The reset is enqueued AFTER the complete response, never in place of it.
	ev2 := <-s.events
	assert.Equalf(t, EventReset, ev2.Type,
		"post-response event = %+v, want EventReset(NO_ERROR) delivered after the intact response", ev2)
	assert.Equalf(t, frame.ErrCodeNoError, ev2.RSTCode,
		"post-response event = %+v, want EventReset(NO_ERROR) delivered after the intact response", ev2)
}
