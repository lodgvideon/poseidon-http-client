package conn

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Four flow-control decisions in this package are "at the limit is legal, one
// past it is a FLOW_CONTROL_ERROR", and every test that drove them approached
// from a distance: a window of 100 overflowed with 200 bytes, a retroactive
// delta parked well past 2^31-1. A boundary only ever approached from far away
// is one no test can tell from a boundary placed a byte to either side — each of
// the four mutations these kill survived the whole package twice before they
// existed (#800).
//
// flowwindow_ceiling_test.go pins the neighbouring pair: the SEND windows via
// onWindowUpdate, and the SETTINGS value itself. These are the other direction —
// the RECEIVE windows the peer spends — plus the one loop inside
// applyPeerSettings that its at-the-ceiling test cannot reach, because that test
// runs on a conn with no open streams and the loop body never executes.

// TestApplyPeerSettings_RetroactiveDeltaBoundaryIsExact drives the per-stream
// loop RFC 7540 §6.9.2 requires: a new SETTINGS_INITIAL_WINDOW_SIZE is a delta
// applied to every open stream's send window. §6.9.1 puts the ceiling at 2^31-1
// INCLUSIVE, so a stream whose window lands exactly there is legal and must be
// kept; only a sum strictly past it is the connection error.
func TestApplyPeerSettings_RetroactiveDeltaBoundaryIsExact(t *testing.T) {
	// The peer has advertised no INITIAL_WINDOW_SIZE yet, so it starts at the
	// RFC default and a SETTINGS naming the ceiling moves every open stream by
	// maxFlowWindow-65535. Seeding sendWindow at the default therefore lands the
	// stream on exactly the ceiling.
	for _, tc := range []struct {
		name       string
		sendWindow int32
		wantWindow int32
		wantErr    bool
	}{
		{"one below the ceiling", connInitialRecvWindow - 1, int32(maxFlowWindow - 1), false},
		{"exactly the ceiling", connInitialRecvWindow, int32(maxFlowWindow), false},
		{"one past the ceiling", connInitialRecvWindow + 1, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGoAwayConn()
			s := &Stream{id: 1, sendWindow: tc.sendWindow}
			c.streams[1] = s
			params := frame.SettingsParams{N: 1}
			params.Pairs[0] = frame.SettingPair{
				ID: frame.SettingInitialWindowSize, Value: uint32(maxFlowWindow),
			}

			err := c.applyPeerSettings(params)

			if tc.wantErr {
				var ce *ConnError
				require.ErrorAsf(t, err, &ce,
					"a delta putting an open stream's send window past 2^31-1 gave %v; want a "+
						"ConnError, because §6.9.2 makes that overflow connection-scoped and a "+
						"stream reset would leave the two ends disagreeing about every window", err)
				assert.Equalf(t, frame.ErrCodeFlowControlError, ce.Code,
					"code = %v, want FLOW_CONTROL_ERROR (RFC 7540 §6.9.1)", ce.Code)
				return
			}
			require.NoErrorf(t, err,
				"a delta landing on exactly 2^31-1 was rejected: %v — the ceiling is inclusive, "+
					"so refusing it kills a connection the peer configured legally", err)
			assert.Equalf(t, tc.wantWindow, s.sendWindow,
				"stream send window = %d, want %d — the delta has to be applied to the open "+
					"stream, not merely accepted at the connection level",
				s.sendWindow, tc.wantWindow)
		})
	}
}

// TestConn_DebitConnRecv_OverflowBoundaryIsExact pins the connection receive
// window at its edge. RFC 7540 §6.9.1 lets a peer spend the window down to
// exactly zero; only the byte after that is the FLOW_CONTROL_ERROR that kills
// the connection. The existing overflow test drives a window of 100 with 200
// bytes, which cannot tell "< 0" from "< -1".
func TestConn_DebitConnRecv_OverflowBoundaryIsExact(t *testing.T) {
	for _, tc := range []struct {
		name    string
		window  int32
		length  uint32
		wantErr bool
	}{
		{"one short of the window", 100, 99, false},
		{"exactly the window", 100, 100, false},
		{"one byte past the window", 100, 101, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGoAwayConn()
			c.connRecvWindow = tc.window

			refund, err := c.debitConnRecv(tc.length)

			if tc.wantErr {
				var ce *ConnError
				require.ErrorAsf(t, err, &ce,
					"spending one byte past the connection window gave %v; want a ConnError — "+
						"a peer that overruns the connection window has desynced from us and "+
						"every later frame is accounted against a window we no longer agree on", err)
				assert.Equalf(t, frame.ErrCodeFlowControlError, ce.Code,
					"code = %v, want FLOW_CONTROL_ERROR (RFC 7540 §6.9.1)", ce.Code)
				return
			}
			require.NoErrorf(t, err,
				"spending %d of a %d-byte window was refused: %v — refusing at the limit "+
					"tears down a connection whose peer stayed inside its credit",
				tc.length, tc.window, err)
			assert.Zerof(t, refund,
				"refund = %d for %d bytes; nothing under the %d-byte batching threshold may "+
					"emit a WINDOW_UPDATE", refund, tc.length, recvWindowRefundThreshold)
		})
	}
}

// TestConn_DebitConnRecv_RefundBatchesAtTheThreshold pins the OTHER side of the
// batching decision. The existing test feeds exactly recvWindowRefundThreshold
// and asserts a WINDOW_UPDATE, which pins ">= threshold". Nothing pinned "and
// not before", so the granularity could collapse to per-frame chatter — one
// control frame per DATA frame — with the whole package green.
func TestConn_DebitConnRecv_RefundBatchesAtTheThreshold(t *testing.T) {
	for _, tc := range []struct {
		name    string
		lengths []uint32
		want    []uint32
	}{
		{"one byte below the threshold refunds nothing",
			[]uint32{recvWindowRefundThreshold - 1}, []uint32{0}},
		{"exactly the threshold refunds",
			[]uint32{recvWindowRefundThreshold}, []uint32{recvWindowRefundThreshold}},
		{"the pending count carries across frames",
			[]uint32{recvWindowRefundThreshold - 1, 1}, []uint32{0, recvWindowRefundThreshold}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newGoAwayConn()
			c.connRecvWindow = 1 << 20

			refunds := make([]uint32, 0, len(tc.lengths))
			errs := make([]error, 0, len(tc.lengths))
			for _, n := range tc.lengths {
				r, err := c.debitConnRecv(n)
				refunds = append(refunds, r)
				errs = append(errs, err)
			}

			for i, err := range errs {
				require.NoErrorf(t, err, "debitConnRecv(%d) at index %d", tc.lengths[i], i)
			}
			assert.Equalf(t, tc.want, refunds,
				"refunds = %v, want %v — a WINDOW_UPDATE before the threshold is the "+
					"per-frame control chatter the batching exists to avoid, and one after it "+
					"is a peer stalled against a window we never gave back", refunds, tc.want)
		})
	}
}

// TestConn_OnDataReceived_StreamOverflowBoundaryIsExact is the per-stream half
// of the same decision, and the same one-past-the-edge case. A stream that
// overruns its window is reset (§6.9.1 stream error), while the connection —
// already accounted for — survives; the existing test uses a window of 50 and
// 100 bytes, far enough out that the guard could sit a byte to either side.
func TestConn_OnDataReceived_StreamOverflowBoundaryIsExact(t *testing.T) {
	for _, tc := range []struct {
		name    string
		window  int32
		length  uint32
		wantErr bool
	}{
		{"one short of the window", 50, 49, false},
		{"exactly the window", 50, 50, false},
		{"one byte past the window", 50, 51, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := newGoAwayConn()
			c.fr = frame.NewFramer(&buf, bytes.NewReader(nil))
			c.connRecvWindow = 1 << 20
			s := newStream(1, 8, c, tc.window)
			s.id = 1
			c.streams[1] = s

			err := c.onDataReceived(s, tc.length)

			if tc.wantErr {
				var se *StreamError
				require.ErrorAsf(t, err, &se,
					"spending one byte past the stream window gave %v; want a StreamError — "+
						"a per-stream overrun is a reset of that stream, not a connection teardown", err)
				assert.Equalf(t, frame.ErrCodeFlowControlError, se.Code,
					"code = %v, want FLOW_CONTROL_ERROR (RFC 7540 §6.9.1)", se.Code)
				return
			}
			require.NoErrorf(t, err,
				"a DATA frame of %d bytes against a %d-byte stream window was refused: %v — "+
					"resetting at the limit kills a stream whose peer stayed inside its credit",
				tc.length, tc.window, err)
		})
	}
}
