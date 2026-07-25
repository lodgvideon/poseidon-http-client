package frame

// Wire-level conformance for padding octets (§6.1/§6.2) and unused flag bits
// (§4.1). All four are implemented correctly by construction — padding is drawn
// from a never-mutated zero array, flags are computed from named constants — but
// no test inspected the produced bytes. A padding leak is an information
// disclosure, exactly what the "MUST be set to zero" rule prevents.

import (
	"bytes"
	"context"
	"testing"
)

// TestConformance_RFC9113_Sec6_1_PaddedDataPaddingOctetsZero pins §6.1: "Padding
// octets MUST be set to zero when sending." It inspects the produced DATA frame
// bytes, not a round-trip that strips padding before checking.
func TestConformance_RFC9113_Sec6_1_PaddedDataPaddingOctetsZero(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	const padLen = 9
	if err := fr.WriteDataPadded(1, false, []byte("hello"), padLen); err != nil {
		t.Fatalf("WriteDataPadded: %v", err)
	}
	raw := buf.Bytes()
	// wire: 9-byte header, pad-length byte, data, then padLen padding octets (tail).
	if raw[9] != padLen {
		t.Fatalf("pad-length byte = %d, want %d", raw[9], padLen)
	}
	for i, b := range raw[len(raw)-padLen:] {
		if b != 0 {
			t.Errorf("DATA padding octet %d = 0x%02x, want 0x00 (§6.1 Padding octets MUST be set to zero when sending)", i, b)
		}
	}
}

// TestConformance_RFC9113_Sec6_2_PaddedHeadersPaddingOctetsZero pins the same
// rule for HEADERS (§6.2 carries the identical padding clause).
func TestConformance_RFC9113_Sec6_2_PaddedHeadersPaddingOctetsZero(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	const padLen = 9
	if err := fr.WriteHeaders(WriteHeadersParams{
		StreamID: 1, BlockFragment: []byte{0x82}, EndHeaders: true, PadLength: padLen,
	}); err != nil {
		t.Fatalf("WriteHeaders: %v", err)
	}
	raw := buf.Bytes()
	if raw[9] != padLen {
		t.Fatalf("pad-length byte = %d, want %d", raw[9], padLen)
	}
	for i, b := range raw[len(raw)-padLen:] {
		if b != 0 {
			t.Errorf("HEADERS padding octet %d = 0x%02x, want 0x00 (§6.2 Padding octets MUST be set to zero when sending)", i, b)
		}
	}
}

// TestConformance_RFC9113_Sec4_1_UnusedFlagsIgnoredOnReceipt pins §4.1: "Unused
// flags MUST be ignored on receipt". DATA defines END_STREAM (0x1) and PADDED
// (0x8); 0x4 is unused. A DATA frame carrying only the unused bit must be
// delivered as an ordinary data frame — ignored, not misread as PADDED (which
// would strip the first payload byte as a pad length and corrupt the body).
func TestConformance_RFC9113_Sec4_1_UnusedFlagsIgnoredOnReceipt(t *testing.T) {
	const unused Flags = 0x04
	raw := frameBytes(5, FrameData, unused, 1, []byte("hello"))
	fr := NewFramer(nil, bytes.NewReader(raw))
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("ReadFrame: %v — an unused flag bit must be ignored, not an error (§4.1)", err)
	}
	if !bytes.Equal(h.dataPayload, []byte("hello")) {
		t.Errorf("data = %q, want \"hello\" — the unused bit was not ignored (misparsed as PADDED?)", h.dataPayload)
	}
	if h.dataPad != 0 {
		t.Errorf("dataPad = %d, want 0 — the unused bit must not be read as PADDED", h.dataPad)
	}
}

// TestConformance_RFC9113_Sec4_1_UnusedFlagsUnsetWhenSending pins §4.1: "Unused
// flags MUST be ... left unset (0x00) when sending". A non-END_STREAM, non-padded
// DATA frame must set no flag bits; an END_STREAM DATA frame sets only 0x1.
func TestConformance_RFC9113_Sec4_1_UnusedFlagsUnsetWhenSending(t *testing.T) {
	for _, tc := range []struct {
		name      string
		endStream bool
		want      byte
	}{
		{"plain", false, 0x00},
		{"end_stream", true, byte(FlagDataEndStream)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			fr := NewFramer(&buf, nil)
			if err := fr.WriteData(1, tc.endStream, []byte("x")); err != nil {
				t.Fatalf("WriteData: %v", err)
			}
			// Flags byte is at offset 4 of the 9-byte frame header.
			if flags := buf.Bytes()[4]; flags != tc.want {
				t.Errorf("DATA flags = 0x%02x, want 0x%02x — no unused flag bits when sending (§4.1)", flags, tc.want)
			}
		})
	}
}
