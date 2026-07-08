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
// so a large response does not emit a control frame per read.
func (c *Conn) onStreamConsumed(s *Stream, n uint64) {
	c.connRecvConsumed += n

	// Per-stream: keep the advertised limit a full window ahead of what has
	// been consumed on this stream.
	if want := uint64(s.recv.readOff) + DefaultStreamRecvWindow; want >= s.recvMax+DefaultStreamRecvWindow/2 {
		s.recvMax = want
		c.pendingCtrl = AppendMaxStreamData(c.pendingCtrl, s.id, want)
	}
	// Connection-level: a window ahead of total bytes consumed across streams.
	if want := c.connRecvConsumed + DefaultConnRecvWindow; want >= c.connRecvMax+DefaultConnRecvWindow/2 {
		c.connRecvMax = want
		c.pendingCtrl = AppendMaxData(c.pendingCtrl, want)
	}
}
