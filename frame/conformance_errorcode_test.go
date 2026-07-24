package frame

// RFC 9113 §7: "Unknown or unsupported error codes MUST NOT trigger any special
// behavior." The Framer parses the 32-bit error code of RST_STREAM and GOAWAY
// and surfaces it to the handler unchanged, even when it lies outside the
// defined 0x00-0x0d range — no rejection, no remapping at the codec layer.

import (
	"bytes"
	"context"
	"testing"
)

// TestConformance_RFC9113_Sec7_UnknownErrorCodeSurfacedUnchanged pins that an
// unknown error code on RST_STREAM and on GOAWAY reaches the handler verbatim.
func TestConformance_RFC9113_Sec7_UnknownErrorCodeSurfacedUnchanged(t *testing.T) {
	const unknown = ErrCode(0xdeadbeef) // far outside the defined 0x00-0x0d range

	t.Run("RST_STREAM", func(t *testing.T) {
		raw := frameBytes(4, FrameRSTStream, 0, 1, []byte{0xde, 0xad, 0xbe, 0xef})
		fr := NewFramer(nil, bytes.NewReader(raw))
		h := &recordingHandler{}
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			t.Fatalf("RST_STREAM with unknown code: %v — §7: unknown codes must not trigger special behavior", err)
		}
		if h.rstCode != unknown {
			t.Errorf("OnRSTStream code = 0x%x, want 0x%x (surfaced unchanged)", uint32(h.rstCode), uint32(unknown))
		}
	})

	t.Run("GOAWAY", func(t *testing.T) {
		// GOAWAY payload: 4-byte last-stream-id (0) + 4-byte error code.
		raw := frameBytes(8, FrameGoAway, 0, 0, []byte{0, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef})
		fr := NewFramer(nil, bytes.NewReader(raw))
		h := &recordingHandler{}
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			t.Fatalf("GOAWAY with unknown code: %v — §7: unknown codes must not trigger special behavior", err)
		}
		if h.goCode != unknown {
			t.Errorf("OnGoAway code = 0x%x, want 0x%x (surfaced unchanged)", uint32(h.goCode), uint32(unknown))
		}
	})
}
