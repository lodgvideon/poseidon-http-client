package quic

// Receive-window sizes the client advertises and auto-tunes. They are the
// values NewConn seeds and that http3.Dial encodes in its transport parameters,
// keeping the advertised limits and the granting logic in one place.
const (
	// DefaultStreamRecvWindow is the per-stream receive window (initial
	// max_stream_data the client advertises, RFC 9000 §18.2).
	DefaultStreamRecvWindow uint64 = 256 << 10
	// DefaultConnRecvWindow is the connection-level receive window (initial
	// max_data).
	DefaultConnRecvWindow uint64 = 1 << 20
)

// onStreamConsumed accounts for n bytes the application has read from stream s
// and, as consumption frees receive window, grants the peer more credit by
// queueing MAX_STREAM_DATA and MAX_DATA frames (RFC 9000 §4.1). Grants are
// batched — sent only when the limit would advance by at least half a window —
// so a large response does not emit a control frame per read. Assumes c.mu is
// held (invoked from the Recv path, which takes c.mu).
func (c *Conn) onStreamConsumed(s *Stream, n uint64) {
	c.connRecvConsumed += n

	// connRecvMax == 0 is the receive-flow-control-disabled sentinel (the gate
	// OnStream uses to skip enforcement): with no enforcement there is no credit to
	// grant, so queue nothing. Before the Recv-path flushControl (INV-3) a spurious
	// grant here sat unsent and was harmless; now it would be flushed, so gate it —
	// and a real connection always seeds connRecvMax (NewConn / DefaultConnRecvWindow).
	if c.connRecvMax == 0 {
		return
	}

	// Per-stream: keep the advertised limit a full window ahead of what has
	// been consumed on this stream — but stop once the receiving part is in Size
	// Known or Reset Recvd (RFC 9000 §3.2, §13.3): the final size is settled, so no
	// further data can arrive and more credit would buy the peer nothing. The
	// connection-level grant below is unaffected; §4.1 accounting still runs.
	if !s.recv.fin && !s.recvReset {
		if want := s.recv.base + DefaultStreamRecvWindow; want >= s.recvMax+DefaultStreamRecvWindow/2 {
			s.recvMax = want
			c.pendingCtrl = AppendMaxStreamData(c.pendingCtrl, s.id, want)
			if c.grantedStreams == nil {
				c.grantedStreams = map[uint64]struct{}{}
			}
			c.grantedStreams[s.id] = struct{}{}
		}
	}
	// Connection-level: a window ahead of total bytes consumed across streams.
	if want := c.connRecvConsumed + DefaultConnRecvWindow; want >= c.connRecvMax+DefaultConnRecvWindow/2 {
		c.connRecvMax = want
		c.pendingCtrl = AppendMaxData(c.pendingCtrl, want)
		c.grantedConn = true
	}
}

// A grant above is queued exactly once and never retransmitted, and the limit it
// carries is applied to local state as it is queued. That is safe on its own only
// because a later grant supersedes a lost one — and the two functions below exist
// because a later grant is not guaranteed to come.
//
// Grants are emitted only as the application consumes, at half-window steps. If
// the packet carrying one is lost, the peer stays at its previous limit and stops
// sending; with nothing arriving, nothing is consumed, so the next grant is never
// produced. Both sides then wait for the other. It needs a response larger than a
// window to reach the first grant, which is why every test below 256 KiB passes
// through the same lossy path unaffected.
//
// The peer's own way out is DATA_BLOCKED / STREAM_DATA_BLOCKED (RFC 9000 §4.1),
// which carries the limit it is stuck at. Answering it with the CURRENT limit
// breaks the deadlock, and comparing against that field is what keeps the answer
// bounded: a peer that already knows the current limit learns nothing from being
// told again, so it gets nothing.
//
// That answer is necessary but not sufficient, and measurement is what showed it:
// with only the two functions below, a 1 MiB response through 10% loss still hung
// on the second run of thirty. The peer's signal travels the same lossy path as
// the grant did, and a peer sends it once per limit, so the rescue is dropped
// exactly as often as the thing it was rescuing. Recovery cannot depend on a
// packet the peer sends.
//
// regrantAfterLoss is the half that does not: RFC 9000 §13.3 has the receiver
// re-send its CURRENT limit when the packet carrying the last one is declared
// lost. Both obligations are catalogued (13.3-13.4-13, 13.3-13.4-14 in
// docs/rfc-analysis/RFC9000_QUIC_TRANSPORT_FACTS.md) and both were classed
// informative, which is why neither was implemented and why a deadlock reachable
// with one dropped datagram survived to v1.

// regrantConnLimit re-sends the connection-level receive limit after the peer
// reported itself blocked at blockedAt (RFC 9000 §19.12). It is a no-op unless the
// peer is behind — i.e. the grant that would have moved it never landed.
func (c *Conn) regrantConnLimit(blockedAt uint64) {
	if c.connRecvMax == 0 || blockedAt >= c.connRecvMax || len(c.pendingCtrl) >= maxPendingCtrl {
		return
	}
	c.pendingCtrl = AppendMaxData(c.pendingCtrl, c.connRecvMax)
}

// regrantStreamLimit re-sends one stream's receive limit after the peer reported
// itself blocked at blockedAt (RFC 9000 §19.13). Nothing is sent once the final
// size is known, matching onStreamConsumed: no further data can arrive, so more
// credit would buy the peer nothing.
func (c *Conn) regrantStreamLimit(id, blockedAt uint64) {
	if len(c.pendingCtrl) >= maxPendingCtrl {
		return
	}
	s := c.streams[id]
	if s == nil || s.recv.fin || s.recvReset || blockedAt >= s.recvMax {
		return
	}
	c.pendingCtrl = AppendMaxStreamData(c.pendingCtrl, id, s.recvMax)
}

// regrantAfterLoss re-sends every receive limit that has been granted and could
// still matter, carrying the CURRENT value (RFC 9000 §13.3). detectLost calls it
// once per loss episode, which is the closest trigger available without recording
// per packet which grants rode in it — a grant is a handful of bytes and an
// episode is rare, so re-sending one that was not in fact lost costs less than the
// bookkeeping to tell.
//
// The set only holds streams that crossed a half-window of consumption, so a
// connection full of small responses re-sends nothing. Entries stay until the
// stream's final size is known: a re-grant can be lost too, and the next loss
// episode is then the retry.
func (c *Conn) regrantAfterLoss() {
	if c.grantedConn && len(c.pendingCtrl) < maxPendingCtrl {
		c.pendingCtrl = AppendMaxData(c.pendingCtrl, c.connRecvMax)
	}
	for id := range c.grantedStreams {
		s := c.streams[id]
		if s == nil || s.recv.fin || s.recvReset {
			delete(c.grantedStreams, id) // nothing more can arrive; stop carrying it
			continue
		}
		if len(c.pendingCtrl) >= maxPendingCtrl {
			return
		}
		c.pendingCtrl = AppendMaxStreamData(c.pendingCtrl, id, s.recvMax)
	}
}
