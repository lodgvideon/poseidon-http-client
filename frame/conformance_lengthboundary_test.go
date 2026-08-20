package frame

// The two read-side length guards this codec has, tested AT their boundary
// rather than comfortably past it (#780).
//
// Both were exercised only by frames far over the limit — 100 octets against a
// 50-octet cap, 200 against 64, a 4-octet GOAWAY against a minimum of 8 — so an
// off-by-one in either guard was free: `> maxFrameSize` widened to
// `> maxFrameSize+1`, and `< 8` narrowed to `< 7`, both survived the whole
// package suite twice. The boundary IS the rule here, so the boundary is what
// has to be sent.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9113_Sec4_2_ReadFrameSizeBoundary pins the receive-side
// SETTINGS_MAX_FRAME_SIZE check at exactly the advertised value and one octet
// past it.
//
// RFC 9113 §4.2: "An endpoint MUST send an error code of FRAME_SIZE_ERROR if a
// frame exceeds the size defined in SETTINGS_MAX_FRAME_SIZE, exceeds any limit
// defined for the frame type, or is too small to contain mandatory frame data."
// "Exceeds" is the whole rule: a frame of exactly the advertised size is legal
// and MUST be processed, so a guard that also refused it would break every peer
// that fills its frames — the failure mode a one-sided over-limit test cannot
// see.
//
// The fixture builds the wire bytes directly instead of writing them through a
// Framer with a temporarily raised limit, because SetMaxReadFrameSize drives the
// same field the WRITE guard reads: the test this replaces set the limit first
// and then tried to write an oversized frame, so the write failed, the function
// returned early, and ReadFrame was never called at all.
func TestConformance_RFC9113_Sec4_2_ReadFrameSizeBoundary(t *testing.T) {
	const limit = 64

	for _, tc := range []struct {
		name    string
		length  uint32
		wantErr error
	}{
		{"exactly the advertised size is accepted", limit, nil},
		{"one octet past it is FRAME_SIZE_ERROR", limit + 1, ErrFrameTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := frameBytes(tc.length, FrameData, 0, 1, make([]byte, tc.length))
			fr := NewFramer(nil, bytes.NewReader(raw))
			fr.SetMaxReadFrameSize(limit)

			_, err := fr.ReadFrame(context.Background(), &recordingHandler{})

			if tc.wantErr == nil {
				require.NoErrorf(t, err,
					"a %d-octet frame against an advertised limit of %d was refused (%v); "+
						"§4.2 only makes a frame that EXCEEDS the limit an error, so refusing "+
						"one at the limit rejects every peer that fills its frames",
					tc.length, limit, err)
				return
			}
			require.ErrorIsf(t, err, tc.wantErr,
				"a %d-octet frame against an advertised limit of %d gave %v, want "+
					"ErrFrameTooLarge — the first octet past the limit is where §4.2 bites, "+
					"and a guard off by one there admits a frame the connection sized its "+
					"buffers against", tc.length, limit, err)
		})
	}
}

// TestConformance_RFC9113_Sec4_2_GoAwayTooSmallForMandatoryData pins the other
// direction of the same clause — "or is too small to contain mandatory frame
// data" — at the boundary.
//
// A GOAWAY's mandatory data is the 4-octet last-stream-id plus the 4-octet error
// code; everything after that is optional debug data. So 8 is the smallest legal
// GOAWAY and 7 is one short. Only 4 was ever sent, which left `< 8` free to
// become `< 7` — and under that mutation a seven-octet GOAWAY from a hostile
// peer indexes payload[7] and payload[8:] on a seven-octet slice, i.e. panics
// the frame parser on peer-chosen input.
func TestConformance_RFC9113_Sec4_2_GoAwayTooSmallForMandatoryData(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
		wantErr error
	}{
		{"one octet short of the mandatory fields", make([]byte, 7), ErrShortRead},
		{"exactly the mandatory fields, no debug data", []byte{0, 0, 0, 1, 0, 0, 0, 0}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := frameBytes(uint32(len(tc.payload)), FrameGoAway, 0, 0, tc.payload)
			fr := NewFramer(nil, bytes.NewReader(raw))

			_, err := fr.ReadFrame(context.Background(), &recordingHandler{})

			if tc.wantErr == nil {
				require.NoErrorf(t, err,
					"an 8-octet GOAWAY was refused (%v); that is exactly the last-stream-id "+
						"plus the error code, with the debug data §6.8 makes optional simply "+
						"absent, so it is the shortest CONFORMANT GOAWAY a peer can send", err)
				return
			}
			require.ErrorIsf(t, err, tc.wantErr,
				"a %d-octet GOAWAY gave %v, want ErrShortRead — one octet short of the "+
					"mandatory fields is what the guard exists for, and reading past it is "+
					"an index-out-of-range on peer-chosen input",
				len(tc.payload), err)
		})
	}
}
