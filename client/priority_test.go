package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ————————————————————————————————————————————————————————————————
// These tests used to drive a real net/http h2 server and assert only that the
// response was 200. That server ignores PRIORITY entirely, so every one of them
// passed with client.go's `s.SendHeadersWithPriority(ctx, hdrs, endStream,
// req.Priority)` mutated to pass a literal nil — the field could stop reaching
// the wire and nothing here noticed. The observable had to move from the status
// code to the HEADERS frame the peer actually receives, which is where
// RFC 7540 §6.2 puts the priority: a PRIORITY flag plus a 5-byte prefix, both
// surfaced by Framer as the *frame.Priority argument to OnHeaders.
// ————————————————————————————————————————————————————————————————

// priorityRecorder is a frame.Handler that records the priority carried by the
// first HEADERS frame it sees. Embedding nopHandler keeps this to the one method
// that matters; everything else stays a no-op.
//
// Framer sets prio non-nil exactly when FlagHeadersPriority is set, so a nil
// recording is a direct read of "no PRIORITY flag on the wire" — the negative
// arm needs no separate flag plumbing.
type priorityRecorder struct {
	nopHandler
	mu   sync.Mutex
	seen bool
	prio *frame.Priority
}

func (h *priorityRecorder) OnHeaders(_ frame.FrameHeader, _ frame.HeaderBlock, prio *frame.Priority, _ uint8) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.seen {
		h.seen = true
		if prio != nil {
			cp := *prio
			h.prio = &cp
		}
	}
	return nil
}

// result reports what the peer saw on the request HEADERS: whether a HEADERS
// frame arrived at all, and the priority it carried (nil for "no PRIORITY
// flag").
func (h *priorityRecorder) result() (seen bool, prio *frame.Priority) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen, h.prio
}

// priorityCapturingGETServer is minimalGETServer with the request HEADERS routed
// through rec on the way past, so the test can assert on the frame the client
// really wrote while the request still completes with a 200.
func priorityCapturingGETServer(rec *priorityRecorder) func(srvFr *frame.Framer) {
	return func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), frame.Handler(multiHandler{rec, capH})); err != nil {
				return
			}
			sid, ok := capH.firstHeadersStreamID()
			if !ok {
				continue
			}
			enc := hpack.NewEncoder()
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("200")},
			})
			_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      sid,
				BlockFragment: block,
				EndHeaders:    true,
				EndStream:     true,
			})
			return
		}
	}
}

// doWithPriority runs one GET carrying prio against a fake H2 peer and returns
// what that peer observed on the request HEADERS, plus the response status.
func doWithPriority(t *testing.T, prio *frame.Priority) (rec *priorityRecorder, status int) {
	t.Helper()

	rec = &priorityRecorder{}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{srvAfter: priorityCapturingGETServer(rec)}},
	})
	require.NoError(t, err, "NewClient against the in-memory peer")
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp Response
	require.NoError(t, c.Do(ctx, &Request{Method: "GET", Path: "/", Priority: prio}, &resp),
		"Do with Priority=%+v", prio)

	return rec, resp.Status
}

// TestPriority_HeaderCarriesRequestPriority pins that Request.Priority is what
// the peer receives on the HEADERS frame — dependency, exclusive bit and weight
// all of them. Asserting only that the request succeeded cannot tell a delivered
// priority from a dropped one, because no HTTP/2 server is required to act on it.
func TestPriority_HeaderCarriesRequestPriority(t *testing.T) {
	want := &frame.Priority{StreamDep: 0, Exclusive: false, Weight: 200}

	rec, status := doWithPriority(t, want)

	seen, got := rec.result()
	require.True(t, seen, "the peer received no HEADERS frame, so nothing was observed at all")
	require.NotNil(t, got, "HEADERS arrived without the PRIORITY flag: Request.Priority never reached the wire")
	assert.Equalf(t, want.StreamDep, got.StreamDep,
		"stream dependency = %d, want %d — a caller's priority tree is built from this field", got.StreamDep, want.StreamDep)
	assert.Equalf(t, want.Exclusive, got.Exclusive,
		"exclusive = %v, want %v", got.Exclusive, want.Exclusive)
	assert.Equalf(t, want.Weight, got.Weight,
		"weight = %d, want %d — RFC 7540 §6.3 weight is the wire value, not weight+1", got.Weight, want.Weight)
	assert.Equal(t, 200, status, "the request must still complete normally with a priority attached")
}

// TestPriority_NilMeansNoFlag is the other direction of the same decision:
// without Request.Priority the HEADERS frame must carry no PRIORITY flag at all.
// A one-sided test is satisfied by a client that always emits one.
func TestPriority_NilMeansNoFlag(t *testing.T) {
	rec, status := doWithPriority(t, nil)

	seen, got := rec.result()
	require.True(t, seen, "the peer received no HEADERS frame, so nothing was observed at all")
	assert.Truef(t, got == nil,
		"HEADERS carried PRIORITY %+v with no Request.Priority set: the client spends 5 bytes "+
			"and a flag per request on a priority the caller never asked for", got)
	assert.Equal(t, 200, status, "status = %d, want 200", status)
}

// TestPriority_ExclusiveWithDep covers the combination the plain case does not:
// a non-zero parent stream together with the exclusive bit, at the maximum
// weight. StreamDep is the field an off-by-one in the 31-bit dependency encoding
// would corrupt, and the exclusive bit shares its first byte — packing them
// wrongly is invisible while the dependency is 0.
func TestPriority_ExclusiveWithDep(t *testing.T) {
	want := &frame.Priority{StreamDep: 1, Exclusive: true, Weight: 255}

	rec, status := doWithPriority(t, want)

	seen, got := rec.result()
	require.True(t, seen, "the peer received no HEADERS frame, so nothing was observed at all")
	require.NotNil(t, got, "HEADERS arrived without the PRIORITY flag: Request.Priority never reached the wire")
	assert.Equalf(t, want.StreamDep, got.StreamDep,
		"stream dependency = %d, want %d — the exclusive bit shares this field's first byte, "+
			"so a bad pack shows up here", got.StreamDep, want.StreamDep)
	assert.Truef(t, got.Exclusive,
		"exclusive bit lost in transit; the peer would not reparent the dependency's other children")
	assert.Equalf(t, want.Weight, got.Weight, "weight = %d, want %d (max)", got.Weight, want.Weight)
	assert.Equal(t, 200, status, "status = %d, want 200", status)
}

// multiHandler fans one frame out to several handlers, so the recorder can watch
// the stream without displacing the captureHandler that drives the reply.
type multiHandler []frame.Handler

func (m multiHandler) OnData(fh frame.FrameHeader, p []byte, pad uint8) error {
	for _, h := range m {
		if err := h.OnData(fh, p, pad); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnHeaders(fh frame.FrameHeader, hb frame.HeaderBlock, prio *frame.Priority, pad uint8) error {
	for _, h := range m {
		if err := h.OnHeaders(fh, hb, prio, pad); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnPriority(fh frame.FrameHeader, p frame.Priority) error {
	for _, h := range m {
		if err := h.OnPriority(fh, p); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnRSTStream(fh frame.FrameHeader, c frame.ErrCode) error {
	for _, h := range m {
		if err := h.OnRSTStream(fh, c); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnSettings(fh frame.FrameHeader, p frame.SettingsParams) error {
	for _, h := range m {
		if err := h.OnSettings(fh, p); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnPushPromise(fh frame.FrameHeader, id uint32, hb frame.HeaderBlock, pad uint8) error {
	for _, h := range m {
		if err := h.OnPushPromise(fh, id, hb, pad); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnPing(fh frame.FrameHeader, d [8]byte) error {
	for _, h := range m {
		if err := h.OnPing(fh, d); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnGoAway(fh frame.FrameHeader, last uint32, c frame.ErrCode, d []byte) error {
	for _, h := range m {
		if err := h.OnGoAway(fh, last, c, d); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnWindowUpdate(fh frame.FrameHeader, inc uint32) error {
	for _, h := range m {
		if err := h.OnWindowUpdate(fh, inc); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnContinuation(fh frame.FrameHeader, hb frame.HeaderBlock) error {
	for _, h := range m {
		if err := h.OnContinuation(fh, hb); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnAltSvc(fh frame.FrameHeader, e []frame.AltSvcEntry) error {
	for _, h := range m {
		if err := h.OnAltSvc(fh, e); err != nil {
			return err
		}
	}
	return nil
}

func (m multiHandler) OnOrigin(fh frame.FrameHeader, o []string) error {
	for _, h := range m {
		if err := h.OnOrigin(fh, o); err != nil {
			return err
		}
	}
	return nil
}
