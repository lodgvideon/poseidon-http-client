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
		}
	}
	// Connection-level: a window ahead of total bytes consumed across streams.
	if want := c.connRecvConsumed + DefaultConnRecvWindow; want >= c.connRecvMax+DefaultConnRecvWindow/2 {
		c.connRecvMax = want
		c.pendingCtrl = AppendMaxData(c.pendingCtrl, want)
	}
}
