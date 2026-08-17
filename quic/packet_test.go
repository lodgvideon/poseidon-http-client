package quic

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustParse(t *testing.T, pkt []byte, dcidLen int) Header {
	t.Helper()
	h, err := ParseHeader(pkt, dcidLen)
	require.NoErrorf(t, err, "ParseHeader(%x)", pkt)
	return h
}

// TestConformance_RFC9000_Sec172_InitialHeader parses a hand-built Initial
// packet header (§17.2.2): first byte, version, CIDs, empty token, Length.
func TestConformance_RFC9000_Sec172_InitialHeader(t *testing.T) {
	pkt := []byte{
		0xc0,                   // long form + fixed, type Initial, pn-len bits (protected)
		0x00, 0x00, 0x00, 0x01, // version 1
		0x04, 0xaa, 0xbb, 0xcc, 0xdd, // DCID len 4 + DCID
		0x00,                   // SCID len 0
		0x00,                   // Token len 0
		0x08,                   // Length = 8
		1, 2, 3, 4, 5, 6, 7, 8, // packet number + payload (8 bytes)
	}

	h := mustParse(t, pkt, 0)

	assert.Truef(t, h.Type == PacketInitial && h.Version == QUICVersion1,
		"type=%d version=%d, want Initial/QUIC v1", h.Type, h.Version)
	assert.Truef(t,
		bytes.Equal(h.DCID, []byte{0xaa, 0xbb, 0xcc, 0xdd}) && len(h.SCID) == 0 && len(h.Token) == 0,
		"dcid=%x scid=%x token=%x", h.DCID, h.SCID, h.Token)
	assert.Truef(t, h.Length == 8 && h.PNOffset == 13 && h.PacketLen == 21,
		"length=%d pnOffset=%d packetLen=%d, want 8/13/21", h.Length, h.PNOffset, h.PacketLen)
}

// TestConformance_RFC9000_Sec173_ShortHeader parses short headers with both a
// zero-length and a 4-byte destination connection ID (§17.3).
func TestConformance_RFC9000_Sec173_ShortHeader(t *testing.T) {
	zeroCID := []byte{0x40, 1, 2, 3}
	fourCID := []byte{0x40, 0x11, 0x22, 0x33, 0x44, 9, 9}

	h := mustParse(t, zeroCID, 0)
	h4 := mustParse(t, fourCID, 4)

	assert.Truef(t, h.Type == PacketShort && len(h.DCID) == 0 && h.PNOffset == 1 && h.PacketLen == 4,
		"zero-cid: %+v", h)
	assert.Truef(t, bytes.Equal(h4.DCID, []byte{0x11, 0x22, 0x33, 0x44}) && h4.PNOffset == 5,
		"4-cid: dcid=%x pnOffset=%d", h4.DCID, h4.PNOffset)
}

// TestConformance_RFC9000_Sec1725_RetryHeader parses a Retry packet (§17.2.5):
// Retry Token then a 16-byte integrity tag, no Length or packet number.
func TestConformance_RFC9000_Sec1725_RetryHeader(t *testing.T) {
	pkt := []byte{0xf0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0xaa}
	pkt = append(pkt, 't', 'o', 'k') // retry token
	for i := 0; i < 16; i++ {
		pkt = append(pkt, 0xcc) // integrity tag
	}

	h := mustParse(t, pkt, 0)

	assert.Truef(t,
		h.Type == PacketRetry && bytes.Equal(h.SCID, []byte{0xaa}) && string(h.Token) == "tok" && h.PNOffset == 0,
		"retry: %+v token=%q", h, h.Token)
}

// TestConformance_RFC9000_Sec171_VersionNegotiation detects a Version
// Negotiation packet by its zero Version (§17.2.1).
func TestConformance_RFC9000_Sec171_VersionNegotiation(t *testing.T) {
	pkt := []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x01, 0xbb, 0x00, 0x00, 0x00, 0x00, 0x01}

	h := mustParse(t, pkt, 0)

	assert.Truef(t,
		h.Type == PacketVersionNegotiation && h.Version == 0 && bytes.Equal(h.DCID, []byte{0xbb}),
		"vn: %+v", h)
}

// TestParseHeader_Coalesced walks two packets coalesced in one datagram using
// PacketLen (RFC 9000 §12.2).
func TestParseHeader_Coalesced(t *testing.T) {
	first := []byte{
		0xe0, 0x00, 0x00, 0x00, 0x01, // Handshake, version 1
		0x00, 0x00, // DCID len 0, SCID len 0
		0x04, 1, 2, 3, 4, // Length 4 + 4 bytes
	}
	second := []byte{0x40, 9, 9} // short header
	datagram := append(append([]byte{}, first...), second...)

	h1 := mustParse(t, datagram, 0)
	h2 := mustParse(t, datagram[h1.PacketLen:], 0)

	assert.Truef(t, h1.Type == PacketHandshake && h1.PacketLen == len(first),
		"first: type=%d packetLen=%d want %d", h1.Type, h1.PacketLen, len(first))
	assert.Equalf(t, PacketShort, h2.Type, "second: type=%d, want short", h2.Type)
}

// TestPacketHeader_RoundTrip writes headers with the writers and parses them
// back.
func TestPacketHeader_RoundTrip(t *testing.T) {
	dcid := []byte{0xde, 0xad, 0xbe, 0xef}
	scid := []byte{0x01, 0x02}

	hdr, pnOff := AppendLongHeader(nil, PacketInitial, QUICVersion1, dcid, scid, nil, 4, 10)
	shdr, spnOff := AppendShortHeader(nil, dcid, 2, true)

	require.Equalf(t, len(hdr), pnOff, "pnOff %d != len(hdr) %d", pnOff, len(hdr))
	pkt := append(append([]byte{}, hdr...), make([]byte, 10)...) // 10 bytes PN+payload
	h := mustParse(t, pkt, 0)
	assert.Truef(t, h.Type == PacketInitial && bytes.Equal(h.DCID, dcid) &&
		bytes.Equal(h.SCID, scid) && h.Length == 10 && h.PNOffset == pnOff,
		"initial round-trip: %+v", h)
	spkt := append(append([]byte{}, shdr...), 7, 7)
	sh := mustParse(t, spkt, len(dcid))
	assert.Truef(t, sh.Type == PacketShort && bytes.Equal(sh.DCID, dcid) && sh.PNOffset == spnOff,
		"short round-trip: %+v", sh)
}

func TestParseHeader_Malformed(t *testing.T) {
	cases := map[string]struct {
		pkt     []byte
		dcidLen int
	}{
		"empty":              {nil, 0},
		"long_truncated_ver": {[]byte{0xc0, 0x00, 0x00}, 0},
		"dcid_len_too_big":   {[]byte{0xc0, 0, 0, 0, 1, 0x15}, 0}, // DCID len 21 > 20
		"dcid_past_end":      {[]byte{0xc0, 0, 0, 0, 1, 0x08, 0x00}, 0},
		"initial_len_past":   {[]byte{0xc0, 0, 0, 0, 1, 0x00, 0x00, 0x00, 0x40}, 0}, // Length huge
		"fixed_bit_clear":    {[]byte{0x80, 0, 0, 0, 1, 0x00, 0x00, 0x00, 0x00}, 0}, // long, ver!=0, fixed=0
		"short_dcid_too_big": {[]byte{0x40, 1, 2}, 8},
		"retry_no_tag":       {[]byte{0xf0, 0, 0, 0, 1, 0, 0, 0x01}, 0}, // <16 bytes after CIDs
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseHeader(tc.pkt, tc.dcidLen)

			assert.Truef(t, errors.Is(err, ErrPacketEncoding),
				"ParseHeader(%x) = %v, want ErrPacketEncoding", tc.pkt, err)
		})
	}
}

func BenchmarkParseHeader(b *testing.B) {
	pkt, _ := AppendLongHeader(nil, PacketInitial, QUICVersion1, []byte{1, 2, 3, 4}, nil, nil, 4, 20)
	pkt = append(pkt, make([]byte, 20)...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ParseHeader(pkt, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendLongHeader(b *testing.B) {
	dst := make([]byte, 0, 64)
	dcid := []byte{1, 2, 3, 4}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = AppendLongHeader(dst[:0], PacketInitial, QUICVersion1, dcid, nil, nil, 4, 20)
	}
}
