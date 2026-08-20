package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// appendTableSizeUpdate encodes an RFC 7541 §6.3 dynamic table size update:
// pattern 001 with a 5-bit prefix integer.
func appendTableSizeUpdate(dst []byte, n uint32) []byte {
	const maxPrefix = 1<<5 - 1
	if n < maxPrefix {
		return append(dst, 0x20|byte(n))
	}
	dst = append(dst, 0x20|maxPrefix)
	n -= maxPrefix
	for n >= 128 {
		dst = append(dst, byte(n%128|128))
		n /= 128
	}
	return append(dst, byte(n))
}

// decoderFor builds a decoder exactly as Conn does, through the same call. The
// tests below drive applyDecoderSettings — the real wiring — rather than
// repeating it, so removing the wiring from the product makes them fail.
func decoderFor(advertised uint32) (*hpack.Decoder, AdvertisedSettings) {
	s := AdvertisedSettings{HeaderTableSize: advertised}.defaulted()
	d := hpack.NewDecoder()
	applyDecoderSettings(d, s)
	return d, s
}

// TestConformance_RFC7541_Sec63_DecoderHonorsAdvertisedTableSize pins the
// decoder's dynamic-table limit to what we advertise, not to hpack's default.
//
// SETTINGS_HEADER_TABLE_SIZE tells the peer how large a dynamic table it may use
// for the blocks it sends us (RFC 7540 §6.5.2). A peer that takes us at our word
// announces the new size with a dynamic table size update, and §6.3 makes an
// update above the SETTINGS limit a decoding error. Conn wired the advertised
// value into the *encoder* only, leaving the decoder at hpack's 4096 default —
// so advertising anything larger killed the connection the moment a conformant
// peer used exactly what was offered.
//
// Go's HTTP/2 server does not expose this: its hpack encoder clamps to its own
// 4096 limit and never emits the update, so an httptest peer passes either way.
// That is why this drives the wiring directly instead of a live server.
func TestConformance_RFC7541_Sec63_DecoderHonorsAdvertisedTableSize(t *testing.T) {
	const advertised = 8192
	dec, s := decoderFor(advertised)

	upd := appendTableSizeUpdate(nil, advertised)

	err := dec.DecodeBlock(upd, func(hpack.HeaderField) error { return nil })

	assert.NoErrorf(t, err,
		"a peer honouring our advertised SETTINGS_HEADER_TABLE_SIZE=%d was rejected", s.HeaderTableSize)
}

// TestConformance_RFC7541_Sec63_DecoderRejectsAboveAdvertised is the other half.
// Without it, deleting the limit check entirely would satisfy the test above.
func TestConformance_RFC7541_Sec63_DecoderRejectsAboveAdvertised(t *testing.T) {
	dec, s := decoderFor(4096)

	upd := appendTableSizeUpdate(nil, s.HeaderTableSize+1)

	err := dec.DecodeBlock(upd, func(hpack.HeaderField) error { return nil })

	assert.Errorf(t, err, "an update to %d was accepted against an advertised limit of %d",
		s.HeaderTableSize+1, s.HeaderTableSize)
}

// TestConn_DecoderTableSizeMatchesAdvertised covers every value a caller can
// configure, including the zero that defaults to 4096.
func TestConn_DecoderTableSizeMatchesAdvertised(t *testing.T) {
	for _, advertised := range []uint32{0, 4096, 8192, 65536} {
		dec, s := decoderFor(advertised)
		upd := appendTableSizeUpdate(nil, s.HeaderTableSize)

		err := dec.DecodeBlock(upd, func(hpack.HeaderField) error { return nil })

		assert.NoErrorf(t, err, "advertised=%d (defaulted to %d): peer update rejected",
			advertised, s.HeaderTableSize)
	}
}
