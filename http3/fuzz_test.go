package http3

import "testing"

// FuzzFrameReader streams arbitrary bytes through the HTTP/3 frame reader
// (RFC 9114 §7.1) via Feed + a ReadFrame loop, the shape a real stream reader
// drives. A peer controls every stream byte, so the reader must never panic or
// hang and must bound its buffering: with a frame-length cap set, a header
// announcing a huge length is rejected with ErrH3FrameTooLarge rather than
// buffered. Every successful ReadFrame consumes at least two bytes (the type and
// length varints), so the loop always terminates.
func FuzzFrameReader(f *testing.F) {
	f.Add(AppendData(nil, []byte("body")))
	f.Add(AppendHeaders(nil, []byte{0x00, 0x00, 0xd1})) // wraps a QPACK field section
	f.Add(AppendSettings(nil, []Setting{{ID: SettingQPACKMaxTableCapacity, Value: 4096}}))
	f.Add(AppendGoaway(nil, 0))
	// Two frames back to back, to exercise the consume-and-advance path.
	f.Add(AppendData(AppendData(nil, []byte("a")), []byte("bb")))
	f.Add([]byte{})                                                             // empty
	f.Add([]byte{0x00})                                                         // type varint only, no length
	f.Add([]byte{0x00, 0x40})                                                   // truncated length varint
	f.Add([]byte{0x00, 0xc0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})         // header declaring a huge length
	f.Add([]byte{0x40})                                                         // truncated type varint

	f.Fuzz(func(_ *testing.T, data []byte) {
		var r FrameReader
		r.SetMaxFrameLen(1 << 20) // bound buffering so a huge declared length cannot OOM
		r.Feed(data)
		// Drain until a frame is not yet complete (ErrNeedMore) or is rejected
		// (ErrH3FrameTooLarge). The iteration cap is a belt-and-suspenders guard on
		// top of the guaranteed >=2-byte-per-frame progress.
		for i := 0; i < 4096; i++ {
			if _, _, err := r.ReadFrame(); err != nil {
				return
			}
		}
	})
}

// FuzzParseSettings feeds arbitrary bytes to the HTTP/3 SETTINGS payload parser
// (RFC 9114 §7.2.4). A malformed or hostile SETTINGS payload — truncated pair,
// repeated or reserved identifier — must be rejected with an error, never a
// panic. The huge-count defense is implicit: the loop advances by whole varints
// and terminates when the payload is exhausted.
func FuzzParseSettings(f *testing.F) {
	f.Add([]byte{})                   // empty (valid: no settings)
	f.Add([]byte{0x01, 0x40, 0x00})   // id=1, value=0 (2-byte value)
	f.Add([]byte{0x06, 0x44, 0x00})   // MAX_FIELD_SECTION_SIZE
	f.Add([]byte{0x40})               // truncated identifier varint
	f.Add([]byte{0x01})               // identifier present, value missing
	f.Add([]byte{0x02, 0x00})         // reserved h2 identifier -> H3_SETTINGS_ERROR
	f.Add([]byte{0x01, 0x00, 0x01, 0x00}) // duplicate identifier

	f.Fuzz(func(_ *testing.T, payload []byte) {
		_, _ = ParseSettings(payload)
	})
}
