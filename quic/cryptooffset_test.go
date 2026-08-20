package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

// TestConformance_RFC9000_Sec196_CryptoOffsetExceedsMax checks that a CRYPTO frame
// whose offset plus length exceeds 2^62-1 is a FRAME_ENCODING_ERROR (RFC 9000
// §19.6). At exactly the limit the encoding check does not fire; such an extreme
// offset is instead refused by the separate CRYPTO buffer cap (§7.5) — no real
// handshake reaches offset 2^62, so this only confirms the varint boundary is
// inclusive, not an encoding error.
func TestConformance_RFC9000_Sec196_CryptoOffsetExceedsMax(t *testing.T) {
	// offset = MaxVarint with one byte of data → offset+length = 2^62 > 2^62-1.
	h := &connFrameHandler{c: &Conn{}}
	h2 := &connFrameHandler{c: &Conn{}}
	h3 := &connFrameHandler{c: &Conn{}}

	overMax := h.OnCrypto(bytesx.MaxVarint, []byte{0x00})
	atMax := h2.OnCrypto(bytesx.MaxVarint-1, []byte{0x00})
	normal := h3.OnCrypto(0, []byte("hello"))

	assert.ErrorIsf(t, overMax, ErrFrameEncoding,
		"OnCrypto(offset=MaxVarint, 1 byte) = %v, want ErrFrameEncoding", overMax)
	// Boundary: offset+length == 2^62-1 exactly is not an encoding error; the
	// buffer cap refuses it instead (CRYPTO_BUFFER_EXCEEDED, not FRAME_ENCODING).
	assert.ErrorIsf(t, atMax, ErrCryptoBufferExceeded,
		"OnCrypto at the exact varint limit = %v, want ErrCryptoBufferExceeded", atMax)
	// A normal offset is accepted.
	assert.NoErrorf(t, normal, "OnCrypto(offset=0) = %v, want nil", normal)
}

// TestConformance_RFC9000_Sec196_CryptoOverflowViaParser checks the oversized
// CRYPTO frame is rejected end to end through the frame parser (RFC 9000 §19.6).
func TestConformance_RFC9000_Sec196_CryptoOverflowViaParser(t *testing.T) {
	bad := AppendCrypto(nil, bytesx.MaxVarint, []byte{0x01})
	good := AppendCrypto(nil, 0, []byte("clienthello"))

	badErr := ParseFrames(bad, &connFrameHandler{c: &Conn{}})
	goodErr := ParseFrames(good, &connFrameHandler{c: &Conn{}})

	assert.ErrorIsf(t, badErr, ErrFrameEncoding,
		"ParseFrames(oversized crypto) = %v, want ErrFrameEncoding", badErr)
	assert.NoErrorf(t, goodErr, "ParseFrames(valid crypto) = %v, want nil", goodErr)
}
