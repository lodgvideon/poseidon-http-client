package frame

import (
	"testing"
)

// WriteSettings sizes a scratch array from maxSettingsPairs * settingsPairWireSize
// and bounds N by maxSettingsPairs. Those used to be the literals 16 and
// [96]byte in two files with nothing saying 96 was 16*6, so raising one without
// the other is an index-out-of-range in a wire writer (#517).
//
// The arithmetic is now structural, but a test still earns its place: it is what
// fails if someone "simplifies" the scratch back to a literal, and it pins the
// per-pair wire size against RFC 7540 §6.5.1 rather than against itself.

// TestWriteSettings_ScratchFitsAFullTable writes a full table and checks the
// bytes that come out — the case the scratch is sized for, and one no other test
// exercises, since the codec's own tests write two or three settings.
//
// It asserts the WRITER's output rather than a round trip. Reading the frame back
// returns N=7 for the 16 written, which is correct and not a bug: the decoder
// keeps one slot per DEFINED identifier and RFC 7540 §6.5.2 requires unknown ones
// to be ignored. A round-trip assertion here would be testing the reader's filter
// while claiming to test the writer's capacity.
func TestWriteSettings_ScratchFitsAFullTable(t *testing.T) {
	fr, buf := newFramerWithBuffer()
	var s SettingsParams
	s.N = maxSettingsPairs
	for i := 0; i < maxSettingsPairs; i++ {
		s.Pairs[i] = SettingPair{ID: SettingID(i + 1), Value: uint32(i) * 1000}
	}
	if err := fr.WriteSettings(s); err != nil {
		t.Fatalf("WriteSettings with a full table: %v", err)
	}

	// 9-byte frame header, then one entry per pair.
	const hdr = 9
	wantPayload := maxSettingsPairs * settingsPairWireSize
	if got := buf.Len() - hdr; got != wantPayload {
		t.Fatalf("payload = %d bytes, want %d (%d pairs x %d) — the scratch did not "+
			"hold a full table", got, wantPayload, maxSettingsPairs, settingsPairWireSize)
	}

	// The last entry is the one a too-small scratch loses first, so it is the one
	// worth decoding by hand.
	last := buf.Bytes()[hdr+(maxSettingsPairs-1)*settingsPairWireSize:]
	wantID := SettingID(maxSettingsPairs)
	wantVal := uint32(maxSettingsPairs-1) * 1000
	gotID := SettingID(last[0])<<8 | SettingID(last[1])
	gotVal := uint32(last[2])<<24 | uint32(last[3])<<16 | uint32(last[4])<<8 | uint32(last[5])
	if gotID != wantID || gotVal != wantVal {
		// uint16 conversions, not bare %#x on the SettingID. fmt routes x/X/q
		// through a Stringer just as it does v/s, so once SettingID names
		// itself, %#x on one hex-encodes the NAME — id 0x5 prints as
		// 0x53455454494e47535f... and the diagnostic becomes unreadable at
		// exactly the moment it is needed.
		t.Errorf("last entry on the wire = id %#x value %d, want id %#x value %d",
			uint16(gotID), gotVal, uint16(wantID), wantVal)
	}
}

// TestWriteSettings_RefusesMoreThanTheTable is the bound itself: N past the array
// is rejected rather than read out of range.
func TestWriteSettings_RefusesMoreThanTheTable(t *testing.T) {
	fr, _ := newFramerWithBuffer()
	var s SettingsParams
	s.N = maxSettingsPairs + 1
	if err := fr.WriteSettings(s); err != ErrSettingsLength {
		t.Errorf("WriteSettings with N=%d returned %v, want ErrSettingsLength", s.N, err)
	}
}

// TestSettingsPairWireSize is the RFC anchor: §6.5.1 gives each entry a 16-bit
// identifier and a 32-bit value. Asserted against the literal rather than
// against the constant, so the constant cannot drift and take the scratch with
// it.
func TestSettingsPairWireSize(t *testing.T) {
	if settingsPairWireSize != 2+4 {
		t.Errorf("settingsPairWireSize = %d, want 6 (a 16-bit identifier plus a "+
			"32-bit value, RFC 7540 §6.5.1)", settingsPairWireSize)
	}
}
