package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec7_5_CryptoTooManyGaps_ProtocolViolation pins that the
// out-of-order range cap that protects STREAM reassembly also applies to the
// CRYPTO stream. CRYPTO has no flow-control window (RFC 9000 §7.5), so before this
// the only bound was maxCryptoBuffer bytes — which still admits ~32K one-byte
// gapped chunks, each triggering an O(len(pending)) bufferGap pass: an O(n^2)
// reader-goroutine denial of service. OnCrypto discarded recvStream.receive's
// return, dropping the ErrProtocolViolation the cap raises; it now propagates it.
func TestConformance_RFC9000_Sec7_5_CryptoTooManyGaps_ProtocolViolation(t *testing.T) {
	h := &connFrameHandler{c: &Conn{}}
	var err error

	// 1-byte CRYPTO frames at increasing even offsets; offset 0 is never sent, so
	// nothing becomes contiguous and every frame adds a fresh retained range.
	for i := 0; i <= maxRecvGapChunks+2; i++ {
		if err = h.OnCrypto(uint64(2*i+2), []byte{'x'}); err != nil {
			break
		}
	}

	assert.ErrorIsf(t, err, ErrProtocolViolation,
		"OnCrypto err = %v, want ErrProtocolViolation once retained CRYPTO ranges "+
			"exceed %d — the cap must protect the unwindowed CRYPTO stream too", err, maxRecvGapChunks)
}

// TestConformance_RFC9000_Sec7_5_CryptoGapCapAtTheLimit is the CRYPTO twin of
// TestConformance_RFC9000_Sec11_1_GapCapAtTheLimit: the same cap, pinned at its
// value and in both directions, on the stream that has no flow-control window to
// fall back on (RFC 9000 §7.5). The test above it accepts any cap below ~515;
// this one accepts only maxRecvGapChunks. #828.
func TestConformance_RFC9000_Sec7_5_CryptoGapCapAtTheLimit(t *testing.T) {
	h := &connFrameHandler{c: &Conn{}}

	firstErrAt, firstErr := -1, error(nil)
	for i := 0; i < maxRecvGapChunks+1; i++ {
		if err := h.OnCrypto(uint64(2*i+2), []byte{'x'}); err != nil {
			firstErrAt, firstErr = i, err
			break
		}
	}

	require.Equalf(t, maxRecvGapChunks, firstErrAt,
		"the first refused CRYPTO frame was #%d (err %v), want #%d — CRYPTO has no "+
			"window bounding the bytes, so this count is the only bound there is",
		firstErrAt, firstErr, maxRecvGapChunks)
	assert.ErrorIsf(t, firstErr, ErrProtocolViolation,
		"CRYPTO frame #%d = %v, want ErrProtocolViolation", firstErrAt, firstErr)
}
