package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// rstRecordingStreams wraps fakeStreamMap to capture each rstStream(id, code) the
// handler makes. fakeStreamMap.rstStream is a no-op stub, so a push test that
// needs to observe the client's reset uses this instead.
type rstRecordingStreams struct {
	*fakeStreamMap
	rsts []rstCall
}

type rstCall struct {
	id   uint32
	code frame.ErrCode
}

func (r *rstRecordingStreams) rstStream(id uint32, code frame.ErrCode) error {
	r.rsts = append(r.rsts, rstCall{id, code})
	return nil
}

// TestConformance_RFC9113_Sec6_6_PushPromiseAfterParentRST_CancelsPromised pins
// the RST-then-PP race carve-out of §6.6: "an endpoint that has sent RST_STREAM
// on the associated stream MUST handle PUSH_PROMISE frames that might have been
// created before the RST_STREAM frame is received and processed." A PUSH_PROMISE
// whose associated (parent) stream the client already reset — so it is closed,
// not idle — must NOT be the §6.6 connection error; instead the promised stream
// is reset with CANCEL and the connection survives. This is the sibling of
// TestConformance_RFC9113_Sec6_5_2_PushPromiseOnIdleParent_ConnError, which takes
// the connection-error branch for a genuinely idle parent.
func TestConformance_RFC9113_Sec6_6_PushPromiseAfterParentRST_CancelsPromised(t *testing.T) {
	base := newFakeStreamMap()
	base.pushEnabled = true // opt-in EnablePush path
	rec := &rstRecordingStreams{fakeStreamMap: base}
	h := newConnHandler(rec, hpack.NewDecoder())

	// Parent stream 1 is NOT registered — the client reset it, so lookupStream
	// returns nil and fakeStreamMap.isIdleStream returns false: a closed, not
	// idle, associated stream (the RST-then-PP race).
	block := encodeBlock(t, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	})
	fh := frame.FrameHeader{
		Type:     frame.FramePushPromise,
		StreamID: 1,
		Flags:    frame.FlagPushPromiseEndHeaders,
	}

	if err := h.OnPushPromise(fh, 2, block, 0); err != nil {
		t.Fatalf("PUSH_PROMISE after parent RST: %v — §6.6 requires handling it as a stream-level "+
			"CANCEL, not a connection error", err)
	}
	if len(rec.rsts) != 1 {
		t.Fatalf("rstStream called %d times, want 1 (a single CANCEL on the promised stream)", len(rec.rsts))
	}
	if rec.rsts[0].id != 2 || rec.rsts[0].code != frame.ErrCodeCancel {
		t.Errorf("rstStream(%d, %v), want (2, CANCEL)", rec.rsts[0].id, rec.rsts[0].code)
	}
}
