package conn

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestStreamClose_IdempotentAfterRecycle is a regression test for a nil-pointer
// panic: on the both-ended path Close() recycles the stream (resetting closed,
// w, localEnded, remoteEnded), so a second Close() used to skip the idempotency
// guard and dereference the nil-ed w in writeRSTStream. The released guard now
// makes a repeat Close a safe no-op.
func TestStreamClose_IdempotentAfterRecycle(t *testing.T) {
	c := &Conn{}
	s := newStream(1, 8, c, 65535)
	s.mu.Lock()
	s.localEnded = true
	s.remoteEnded = true
	s.mu.Unlock()

	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	var panicked bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		if err := s.Close(); err != nil {
			t.Fatalf("second Close returned err = %v, want nil", err)
		}
	}()
	if panicked {
		t.Fatalf("second Close() panicked; Close must be idempotent after recycle")
	}
}

// TestApplyPeerSettings_OversizedInitialWindow_ConnError is a regression test
// for a flow-control hang: a peer SETTINGS_INITIAL_WINDOW_SIZE above 2^31-1
// delivered with no open streams used to be stored verbatim (the delta guard
// only iterates open streams) and later seed a negative int32 send window,
// wedging SendData. RFC 7540 §6.5.2 requires FLOW_CONTROL_ERROR.
func TestApplyPeerSettings_OversizedInitialWindow_ConnError(t *testing.T) {
	c := newDynSettingsConn()

	var p frame.SettingsParams
	setPeerSetting(&p, frame.SettingInitialWindowSize, 0x80000000) // 2^31, one past max

	err := c.applyPeerSettings(p)

	var ce *ConnError
	if !errors.As(err, &ce) || ce.Code != frame.ErrCodeFlowControlError {
		t.Fatalf("applyPeerSettings(0x80000000) = %v, want *ConnError FLOW_CONTROL_ERROR", err)
	}
}

// TestOnContinuation_FloodCapped is a regression test for the CONTINUATION-flood
// memory-exhaustion DoS (CVE-2024-27316 class): connHandler used to append every
// CONTINUATION fragment into pendingBuf with no cap. It now returns a ConnError
// once the accumulated block exceeds maxHeaderBytes.
func TestOnContinuation_FloodCapped(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	h.maxHeaderBytes = 4096 // small cap for a fast, deterministic test
	m.addStream(1)

	// Initial HEADERS without END_HEADERS opens the pending buffer.
	startFH := frame.FrameHeader{Type: frame.FrameHeaders, Flags: 0, StreamID: 1}
	if err := h.OnHeaders(startFH, frame.HeaderBlock(nil), nil, 0); err != nil {
		t.Fatalf("OnHeaders: %v", err)
	}

	frag := make([]byte, 1024)
	contFH := frame.FrameHeader{Type: frame.FrameContinuation, Length: 1024, Flags: 0, StreamID: 1}

	var got error
	for i := 0; i < 64; i++ { // 64 KiB worth — must trip the 4 KiB cap well before
		if err := h.OnContinuation(contFH, frame.HeaderBlock(frag)); err != nil {
			got = err
			break
		}
	}

	var ce *ConnError
	if !errors.As(got, &ce) || ce.Code != frame.ErrCodeEnhanceYourCalm {
		t.Fatalf("OnContinuation flood = %v (bufLen=%d), want *ConnError ENHANCE_YOUR_CALM", got, len(h.pendingBuf))
	}
}
