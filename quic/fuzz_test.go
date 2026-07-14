package quic

import "testing"

// fuzzFrameHandler records the frames ParseFrames dispatches and touches every
// aliased byte slice it is handed, so a parser that produced an out-of-bounds or
// mis-sized slice surfaces as a panic under the fuzzer rather than passing
// silently. It embeds nopFrameHandler so it satisfies the full FrameHandler
// interface with no space restriction (every frame type is dispatched).
type fuzzFrameHandler struct {
	nopFrameHandler
	sink int
}

func (h *fuzzFrameHandler) OnCrypto(_ uint64, data []byte) error {
	h.sink += len(data)
	for _, b := range data { // walk the aliased slice to exercise its bounds
		h.sink += int(b)
	}
	return nil
}

func (h *fuzzFrameHandler) OnStream(_, _ uint64, _ bool, data []byte) error {
	h.sink += len(data)
	for _, b := range data {
		h.sink += int(b)
	}
	return nil
}

func (h *fuzzFrameHandler) OnNewToken(token []byte) error { h.sink += len(token); return nil }

func (h *fuzzFrameHandler) OnConnectionClose(_ bool, _, _ uint64, reason []byte) error {
	h.sink += len(reason)
	return nil
}

func (h *fuzzFrameHandler) OnNewConnectionID(_, _ uint64, connID []byte, _ *[16]byte) error {
	h.sink += len(connID)
	return nil
}

// FuzzParseFrames feeds arbitrary bytes to the QUIC frame parser (RFC 9000 §12.4,
// §19). The parser must reject any malformed packet payload with an error and
// never panic, hang, or allocate unboundedly — a decrypted payload is fully
// attacker-controlled once the AEAD is opened, so this is the top adversarial
// surface.
func FuzzParseFrames(f *testing.F) {
	f.Add([]byte{})               // empty payload
	f.Add([]byte{0x01})           // PING
	f.Add([]byte{0x00, 0x00, 0x00}) // a PADDING run
	f.Add(AppendStream(nil, 4, 0, true, []byte("hello")))
	f.Add(AppendStream(nil, 8, 100, false, nil))
	f.Add(AppendCrypto(nil, 0, []byte("crypto-bytes")))
	f.Add(AppendPing(nil))
	f.Add(AppendAck(nil, 10, 3, 4, []AckRange{{Gap: 1, Length: 2}}))
	f.Add(AppendPathResponse(nil, [8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	f.Add([]byte{0x40})       // truncated 2-byte varint frame type
	f.Add([]byte{0x08, 0x40}) // STREAM with a truncated stream-id varint
	f.Add([]byte{0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // ACK: huge largest, no more bytes
	f.Add([]byte{0x02, 0x00, 0x00, 0xc0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // ACK: huge range count

	f.Fuzz(func(_ *testing.T, payload []byte) {
		var h fuzzFrameHandler
		// Contract: returns (an error or nil) without panicking, hanging, or
		// unbounded allocation. Progress is guaranteed — every branch advances the
		// cursor or returns — so the loop inside ParseFrames terminates on len(payload).
		_ = ParseFrames(payload, &h)
	})
}

// FuzzParseHeader feeds arbitrary bytes and a fuzzed local-DCID length to the
// QUIC packet-header parser (RFC 9000 §17). An on-path attacker controls every
// header byte before decryption, so the parser must never panic on a truncated,
// oversized, or nonsensical header — only ever return ErrPacketEncoding.
func FuzzParseHeader(f *testing.F) {
	// A short-header packet with an 8-byte DCID (fixed bit set).
	f.Add([]byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 0xab}, 8)
	// A long-header shell (fixed bit set, v1).
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}, 0)
	// A Version Negotiation shell (version == 0).
	f.Add([]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0)
	f.Add([]byte{}, 0)
	f.Add([]byte{0x00}, 8)
	f.Add([]byte{0x80}, 0)   // long-header first byte, nothing else
	f.Add([]byte{0x40}, 21)  // short header, out-of-range dcidLen
	f.Add([]byte{0x40}, -1)  // short header, negative dcidLen

	f.Fuzz(func(_ *testing.T, pkt []byte, dcidLen int) {
		// Contract: returns (Header, error) without panicking for any input,
		// including a negative or absurdly large dcidLen (both are range-guarded).
		_, _ = ParseHeader(pkt, dcidLen)
	})
}
