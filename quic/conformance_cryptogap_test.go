package quic

import (
	"errors"
	"testing"
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
	if !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("OnCrypto err = %v, want ErrProtocolViolation once retained CRYPTO ranges "+
			"exceed %d — the cap must protect the unwindowed CRYPTO stream too", err, maxRecvGapChunks)
	}
}
