package quic

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

// TestConformance_RFC9000_Sec196_CryptoOffsetExceedsMax checks that a CRYPTO frame
// whose offset plus length exceeds 2^62-1 is a FRAME_ENCODING_ERROR, while a frame
// at exactly the limit is accepted (RFC 9000 §19.6).
func TestConformance_RFC9000_Sec196_CryptoOffsetExceedsMax(t *testing.T) {
	// offset = MaxVarint with one byte of data → offset+length = 2^62 > 2^62-1.
	h := &connFrameHandler{c: &Conn{}}
	if err := h.OnCrypto(bytesx.MaxVarint, []byte{0x00}); err != ErrFrameEncoding {
		t.Fatalf("OnCrypto(offset=MaxVarint, 1 byte) = %v, want ErrFrameEncoding", err)
	}

	// Boundary: offset+length == 2^62-1 exactly is accepted.
	h2 := &connFrameHandler{c: &Conn{}}
	if err := h2.OnCrypto(bytesx.MaxVarint-1, []byte{0x00}); err != nil {
		t.Fatalf("OnCrypto at the exact limit = %v, want nil", err)
	}

	// A normal offset is accepted.
	h3 := &connFrameHandler{c: &Conn{}}
	if err := h3.OnCrypto(0, []byte("hello")); err != nil {
		t.Fatalf("OnCrypto(offset=0) = %v, want nil", err)
	}
}

// TestConformance_RFC9000_Sec196_CryptoOverflowViaParser checks the oversized
// CRYPTO frame is rejected end to end through the frame parser (RFC 9000 §19.6).
func TestConformance_RFC9000_Sec196_CryptoOverflowViaParser(t *testing.T) {
	bad := AppendCrypto(nil, bytesx.MaxVarint, []byte{0x01})
	if err := ParseFrames(bad, &connFrameHandler{c: &Conn{}}); err != ErrFrameEncoding {
		t.Fatalf("ParseFrames(oversized crypto) = %v, want ErrFrameEncoding", err)
	}

	good := AppendCrypto(nil, 0, []byte("clienthello"))
	if err := ParseFrames(good, &connFrameHandler{c: &Conn{}}); err != nil {
		t.Fatalf("ParseFrames(valid crypto) = %v, want nil", err)
	}
}
