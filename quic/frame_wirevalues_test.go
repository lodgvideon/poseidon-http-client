package quic

import "testing"

// The numeric frame types of RFC 9000 §19, pinned against literal bytes.
//
// TestFrames_RoundTrip was the only well-formed coverage of several of these,
// and a round-trip cannot see a wrong constant: the encoder and the parser read
// the same identifier, so the value survives the trip whatever it is. Verified
// by mutation — swapping FrameMaxStreamsBidi/Uni (0x12/0x13), or the
// STREAMS_BLOCKED pair (0x16/0x17), passed the entire quic suite. The uni and
// bidi variants carry identical operands, so nothing downstream noticed either.
//
// A peer would notice immediately: it would read the client's uni-stream limit
// as a bidi one.
//
// The types that were already literal-pinned elsewhere are deliberately not
// repeated here — STREAM (§19.8), ACK (§19.3), CONNECTION_CLOSE (§19.19),
// NEW_CONNECTION_ID (§19.15), PATH_CHALLENGE (§19.17), PADDING/PING (§19.1-2)
// and CRYPTO (via the RFC 9001 A.2 vector) all have hand-built fixtures.

// TestConformance_RFC9000_Sec19_FrameTypeDecodesFromLiteralBytes decodes each
// remaining frame type from bytes written out by hand against §19, so a wrong
// constant in the PARSER is visible.
func TestConformance_RFC9000_Sec19_FrameTypeDecodesFromLiteralBytes(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		// §19.4: RESET_STREAM is 0x04 — id=4, app error 270 (0x410e), final 12.
		{"RESET_STREAM", []byte{0x04, 0x04, 0x41, 0x0e, 0x0c}, "reset id=4 err=270 final=12"},
		// §19.5: STOP_SENDING is 0x05 — id=8, app error 257 (0x4101).
		{"STOP_SENDING", []byte{0x05, 0x08, 0x41, 0x01}, "stopsending id=8 err=257"},
		// §19.9: MAX_DATA is 0x10 — 100 encodes as the two-byte varint 0x4064.
		{"MAX_DATA", []byte{0x10, 0x40, 0x64}, "maxdata 100"},
		// §19.10: MAX_STREAM_DATA is 0x11.
		{"MAX_STREAM_DATA", []byte{0x11, 0x04, 0x40, 0x64}, "maxstreamdata id=4 max=100"},
		// §19.11: MAX_STREAMS is 0x12 for bidirectional, 0x13 for unidirectional.
		// This pair is the interop-critical one — see the file comment.
		{"MAX_STREAMS bidi", []byte{0x12, 0x40, 0x64}, "maxstreams uni=false max=100"},
		{"MAX_STREAMS uni", []byte{0x13, 0x40, 0x64}, "maxstreams uni=true max=100"},
		// §19.12: DATA_BLOCKED is 0x14.
		{"DATA_BLOCKED", []byte{0x14, 0x2a}, "datablocked 42"},
		// §19.13: STREAM_DATA_BLOCKED is 0x15.
		{"STREAM_DATA_BLOCKED", []byte{0x15, 0x04, 0x07}, "streamdatablocked id=4 lim=7"},
		// §19.14: STREAMS_BLOCKED is 0x16 bidirectional, 0x17 unidirectional.
		{"STREAMS_BLOCKED bidi", []byte{0x16, 0x05}, "streamsblocked uni=false lim=5"},
		{"STREAMS_BLOCKED uni", []byte{0x17, 0x05}, "streamsblocked uni=true lim=5"},
		// §19.16: RETIRE_CONNECTION_ID is 0x19.
		{"RETIRE_CONNECTION_ID", []byte{0x19, 0x03}, "retirecid 3"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			eq(t, parse(t, c.in), []string{c.want})
		})
	}
}

// TestConformance_RFC9000_Sec19_FrameTypeEncodesToLiteralByte pins the same
// values on the ENCODER, so a wrong constant is caught on the side a peer reads
// even if the parser is changed to match.
func TestConformance_RFC9000_Sec19_FrameTypeEncodesToLiteralByte(t *testing.T) {
	cases := []struct {
		name string
		got  []byte
		want byte
	}{
		{"RESET_STREAM", AppendResetStream(nil, 4, 0x10e, 12), 0x04},
		{"STOP_SENDING", appendStopSending(nil, 8, 0x101), 0x05},
		{"MAX_DATA", AppendMaxData(nil, 100), 0x10},
		{"MAX_STREAM_DATA", AppendMaxStreamData(nil, 4, 100), 0x11},
		{"MAX_STREAMS bidi", AppendMaxStreams(nil, false, 100), 0x12},
		{"MAX_STREAMS uni", AppendMaxStreams(nil, true, 100), 0x13},
		{"DATA_BLOCKED", appendDataBlocked(nil, 42), 0x14},
		{"STREAM_DATA_BLOCKED", appendStreamDataBlocked(nil, 4, 7), 0x15},
		{"STREAMS_BLOCKED bidi", appendStreamsBlocked(nil, false, 5), 0x16},
		{"STREAMS_BLOCKED uni", appendStreamsBlocked(nil, true, 5), 0x17},
		{"RETIRE_CONNECTION_ID", appendRetireConnectionID(nil, 3), 0x19},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if len(c.got) == 0 {
				t.Fatalf("%s encoded to nothing", c.name)
			}
			if c.got[0] != c.want {
				t.Fatalf("%s frame type = 0x%02x, want 0x%02x (RFC 9000 §19);\n"+
					"a peer reads this octet to decide what the frame IS — a round-trip "+
					"through our own parser cannot see this", c.name, c.got[0], c.want)
			}
		})
	}
}
