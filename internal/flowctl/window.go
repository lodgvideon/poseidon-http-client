// Package flowctl implements HTTP/2 flow-control accounting (RFC 7540
// §5.2 / §6.9). Lock-free atomic windows + an adaptive sizer.
package flowctl

import "sync/atomic"

// FlowWindow tracks one direction's window in bytes. Negative values
// indicate peer overshoot (RFC §6.9 violation; caller decides
// FLOW_CONTROL_ERROR).
type FlowWindow struct {
	available atomic.Int64
}

// New constructs a FlowWindow with the given initial quota.
func New(initial int32) *FlowWindow {
	w := &FlowWindow{}
	w.available.Store(int64(initial))
	return w
}

// TryConsume reserves n bytes from the window if available. Returns true
// on success, false if the window does not have n bytes.
func (w *FlowWindow) TryConsume(n int32) bool {
	for {
		cur := w.available.Load()
		if cur < int64(n) {
			return false
		}
		if w.available.CompareAndSwap(cur, cur-int64(n)) {
			return true
		}
	}
}

// Replenish adds n bytes (peer WINDOW_UPDATE on send-side, local consumer
// drain on recv-side). Returns the new value.
func (w *FlowWindow) Replenish(n int32) int64 {
	return w.available.Add(int64(n))
}

// Available returns a snapshot of the window value.
func (w *FlowWindow) Available() int64 {
	return w.available.Load()
}

// AdjustInitial applies a delta after a peer SETTINGS_INITIAL_WINDOW_SIZE
// change (RFC §6.9.2). May produce a negative value (legitimate per RFC:
// senders must wait until window becomes positive again).
func (w *FlowWindow) AdjustInitial(delta int32) {
	w.available.Add(int64(delta))
}
