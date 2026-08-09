package conn

import "sync/atomic"

// HTTP/2 caps how much a peer may have in flight at the receive window, and RFC
// 9113 §6.5.2 fixes that window at 65535 bytes until an endpoint says otherwise.
// A client that never says otherwise is limited to one window per round trip —
// about 6.5 MB/s for the whole connection at 10 ms RTT, however fast the link or
// the CPU. That ceiling is invisible on loopback and dominates everywhere else.
//
// recvWindowTuner lifts it by measuring what the connection can actually carry
// and growing the windows to match, which is the bandwidth-delay product: the
// bytes a peer can deliver in one round trip. Both terms are measured at once by
// sending a PING and counting the DATA that arrives before its ACK. The sample
// is bytes-per-round-trip by construction, so it needs no clock and no smoothed
// RTT estimate — the peer's ACK is the clock.
//
// The rule is: if a round trip delivered S bytes, the window should be at least
// 2S, because a window of S is exactly what a peer stalls against. Windows only
// grow — WINDOW_UPDATE can only add, and shrinking a receive window mid-flight
// is a hazard with no upside here — so the estimate converges upward and stops.
// A sample that does not move the target means the window was not the binding
// constraint, and probing backs off exponentially so a steady connection stops
// paying for PINGs it learns nothing from.
//
// # Bounds
//
// The ceiling is derived, not guessed: a stream's window never exceeds what that
// stream's event channel can hold (StreamEventBuffer x MaxFrameSize). That is
// the memory the connection has already committed to buffering for the stream,
// so growing to it cannot make the channel-overflow reset any more likely than
// the configured buffer already does. A caller who wants a different bound sets
// ConnOptions.MaxRecvWindow.
//
// # Concurrency
//
// Every field here is touched only by the reader goroutine: onData runs from
// connHandler.OnData and onAck from connHandler.OnPing, both inside
// Framer.ReadFrame. The two published targets live on the Conn as atomics
// because the refund path reads them while holding other locks, and an atomic
// scalar composes with any of them.

// bdpPingPayload identifies the tuner's own PING among the ACKs the reader sees.
// Conn.Ping numbers its payloads from a counter starting at 1, so no application
// ping can collide with this until 2^63 of them have been sent.
var bdpPingPayload = [8]byte{'p', 'o', 's', 'e', 'i', 'd', 'o', 'n'}

// minProbeBytes is how much DATA must arrive before the tuner spends another
// PING. It bounds probe traffic on a connection that trickles.
const minProbeBytes = 16 << 10

// maxProbeBytes is the ceiling of the exponential back-off applied after a
// sample that changed nothing, so a long-running connection whose window is
// already large enough eventually stops probing altogether in practice.
const maxProbeBytes = 8 << 20

// maxAutoRecvWindow is the absolute ceiling on a tuned window, whatever the
// derived or configured bound says. It is well under the 2^31-1 the protocol
// allows so the int32 window arithmetic cannot overflow.
const maxAutoRecvWindow = 64 << 20

// recvWindowTuner holds the sampling state for one connection.
type recvWindowTuner struct {
	// max is the ceiling both targets grow toward.
	max uint32
	// sampling reports that a PING is outstanding and sampleBytes is counting.
	sampling bool
	// sampleBytes is the DATA received since the outstanding PING was written.
	sampleBytes uint64
	// probeBytes is the DATA received since the last sample ended.
	probeBytes uint64
	// probeThreshold is the probeBytes needed to start the next sample. It
	// resets on growth and doubles on a sample that taught us nothing.
	probeThreshold uint64
}

// newRecvWindowTuner builds a tuner with the ceiling derived from opts, or nil
// when auto-tuning is off. floor is the window size already in effect, so the
// ceiling can never ask for less than the connection already has.
func newRecvWindowTuner(opts ConnOptions, floor uint32) *recvWindowTuner {
	if !opts.AutoTuneRecvWindow {
		return nil
	}
	return &recvWindowTuner{
		max:            recvWindowCeiling(opts, floor),
		probeThreshold: minProbeBytes,
	}
}

// recvWindowCeiling resolves the largest window the tuner may ask for. An
// explicit MaxRecvWindow wins; otherwise the bound is what one stream's event
// channel can hold, which is the memory the connection has already committed.
func recvWindowCeiling(opts ConnOptions, floor uint32) uint32 {
	max := opts.MaxRecvWindow
	if max == 0 {
		// StreamEventBuffer and MaxFrameSize are both defaulted and bounded
		// (frameSizeCeil is 2^24-1, the buffer is a caller-set int), so this is
		// computed in uint64 and clamped rather than trusted to fit.
		budget := uint64(opts.StreamEventBuffer) * uint64(opts.Settings.MaxFrameSize)
		if budget > maxAutoRecvWindow {
			budget = maxAutoRecvWindow
		}
		max = uint32(budget)
	}
	if max > maxAutoRecvWindow {
		max = maxAutoRecvWindow
	}
	if max < floor {
		max = floor
	}
	return max
}

// onData folds n received DATA bytes into the current sample and reports
// whether the caller should now write a BDP PING to open the next one.
func (t *recvWindowTuner) onData(n uint32) bool {
	if t.sampling {
		t.sampleBytes += uint64(n)
		return false
	}
	t.probeBytes += uint64(n)
	if t.probeBytes < t.probeThreshold {
		return false
	}
	t.sampling = true
	t.sampleBytes = 0
	t.probeBytes = 0
	return true
}

// probeFailed abandons an outstanding sample when its PING could not be
// written, so a transient write failure does not park the tuner forever.
func (t *recvWindowTuner) probeFailed() {
	t.sampling = false
	t.sampleBytes = 0
}

// onAck closes the outstanding sample and publishes the new targets on c.
//
// A peer cannot drive this anywhere dangerous. An unsolicited ACK carrying the
// tuner's payload ends the sample early, which makes it smaller and so can only
// withhold growth, never manufacture it; the sample itself counts bytes the peer
// actually delivered and paid flow control for; and every result is clamped to
// t.max regardless.
func (t *recvWindowTuner) onAck(c *Conn) {
	if !t.sampling {
		return
	}
	t.sampling = false
	// A window of exactly S is what a peer delivering S bytes per round trip
	// stalls against, so the target is twice the sample.
	want := t.sampleBytes * 2
	// Both raises always run: they publish independent targets, and short
	// circuiting would leave one of them stale.
	grewConn := t.raise(&c.connRecvTarget, want)
	grewStream := t.raise(&c.streamRecvTarget, want)
	if grewConn || grewStream {
		t.probeThreshold = minProbeBytes
		return
	}
	// The window was not what limited this round trip, so the next sample would
	// most likely say the same thing. Ask for more evidence before spending
	// another PING.
	t.probeThreshold *= 2
	if t.probeThreshold > maxProbeBytes {
		t.probeThreshold = maxProbeBytes
	}
}

// raise stores want into target when it is larger than what is there, clamped
// to the tuner's ceiling, and reports whether it moved.
func (t *recvWindowTuner) raise(target *atomic.Uint32, want uint64) bool {
	cur := target.Load()
	if want > uint64(t.max) {
		want = uint64(t.max)
	}
	if uint32(want) <= cur {
		return false
	}
	target.Store(uint32(want))
	return true
}
