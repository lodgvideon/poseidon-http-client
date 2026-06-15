package conn

import (
	"errors"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Sentinel errors. All are stable across releases; callers may use
// errors.Is to identify them.
var (
	// ErrALPNFailed is returned when the TLS handshake completed but
	// the negotiated ALPN protocol is not "h2".
	ErrALPNFailed = errors.New("conn: ALPN did not negotiate h2")
	// ErrTooManyStreams is returned by NewStream when the in-flight
	// count already equals min(local advertised, peer-advertised)
	// MaxConcurrentStreams.
	ErrTooManyStreams = errors.New("conn: in-flight stream cap reached")
	// ErrConnClosed is returned by every public method once the *Conn
	// has been Close'd or its reader loop has exited.
	ErrConnClosed = errors.New("conn: connection closed")
	// ErrStreamClosed is returned by SendHeaders / SendData / Recv
	// once the stream has been reset locally or by the peer.
	ErrStreamClosed = errors.New("conn: stream already closed")
	// ErrFlowControlExhausted is reserved for future explicit
	// non-blocking write paths; B.2.3 always blocks instead.
	ErrFlowControlExhausted = errors.New("conn: send window too small for payload")
	// ErrUnexpectedPushPromise is surfaced when the peer sends a
	// PUSH_PROMISE despite our handshake advertising ENABLE_PUSH=0.
	ErrUnexpectedPushPromise = errors.New("conn: peer sent PUSH_PROMISE while ENABLE_PUSH=0")
	// ErrGoAway is returned by NewStream once the peer has sent a
	// GOAWAY frame: existing streams whose ID is ≤ the GOAWAY's
	// last-stream-id continue, but no new streams may be opened on
	// this connection (RFC 7540 §6.8).
	ErrGoAway = errors.New("conn: peer sent GOAWAY; no new streams")
	// ErrConnDraining is returned by NewStream once Shutdown has
	// been called locally. Existing streams continue, but no new
	// streams may be opened. Mirrors ErrGoAway semantics for the
	// outbound (client-initiated) shutdown path.
	ErrConnDraining = errors.New("conn: connection draining; no new streams")
)

// ConnError is connection-fatal. After it is returned the Conn is dead
// and all Streams created from it return ErrConnClosed.
type ConnError struct {
	Code   frame.ErrCode
	Reason string
	Last   uint32 // last-stream-id from the GOAWAY (0 if originated locally)
}

// Error returns a string describing the connection-fatal error.
func (e *ConnError) Error() string {
	return fmt.Sprintf("conn: connection error code=%v last=%d reason=%q",
		e.Code, e.Last, e.Reason)
}

// StreamError is non-fatal — the stream is reset, the Conn keeps going.
type StreamError struct {
	StreamID uint32
	Code     frame.ErrCode
}

// Error returns a string describing the stream reset.
func (e *StreamError) Error() string {
	return fmt.Sprintf("conn: stream %d reset code=%v", e.StreamID, e.Code)
}
