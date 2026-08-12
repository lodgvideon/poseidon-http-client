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
	// ErrALPNNotHTTP11 is returned by H1TLSDialer when the TLS handshake
	// completed but the peer selected an ALPN protocol other than
	// "http/1.1" (an empty selection — the peer does not speak ALPN — is
	// accepted, since HTTP/1.1 is what an ALPN-less TLS peer implies).
	ErrALPNNotHTTP11 = errors.New("conn: ALPN did not negotiate http/1.1")
	// ErrALPNConflict is returned before any dial when a dialer's ALPN
	// assertion contradicts the caller's explicit Config.NextProtos — e.g.
	// TLSDialer (which asserts "h2") given NextProtos=["http/1.1"], or
	// H1TLSDialer given a list containing "h2". Overriding the caller's
	// list would negotiate a protocol the caller did not ask for and hand
	// it to a codec that cannot speak it, so the mismatch is refused at the
	// dial call instead.
	ErrALPNConflict = errors.New("conn: dialer ALPN assertion conflicts with Config.NextProtos")
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

	// ErrStaleStream is returned when a StreamRef from a finished request is
	// used after its Stream has been recycled. It is a caller bug — a handle
	// retained past Close — not a stream that ended.
	//
	// Deliberately NOT wrapped around ErrStreamClosed. Three shipped call sites
	// treat ErrStreamClosed as benign-and-continue: grpc's benignHalfClose
	// reports a half-close that never reached the wire as success, grpc.Invoke
	// swallows it and then blocks in Recv until the context expires, and
	// client's send path records it as a cut and hands the stream to
	// drainResponse. Laundering a use-after-recycle through any of those turns a
	// programming error into a hang or a silently wrong result.
	ErrStaleStream = errors.New("conn: stale stream handle used after the stream was recycled")
	// ErrUnexpectedPushPromise is surfaced when the peer sends a
	// PUSH_PROMISE despite our handshake advertising ENABLE_PUSH=0.
	ErrUnexpectedPushPromise = errors.New("conn: peer sent PUSH_PROMISE while ENABLE_PUSH=0")
	// ErrIllegalPromisedID is surfaced when a PUSH_PROMISE promises a
	// stream id that is not a legal choice for the server's next stream:
	// zero, odd (the client-initiated space), or not greater than an id
	// the server already reserved (RFC 7540 §5.1.1). §6.6 makes this a
	// connection error of type PROTOCOL_ERROR.
	ErrIllegalPromisedID = errors.New("conn: PUSH_PROMISE promised an illegal stream id")
	// ErrPushRefused reports that a promised stream was rejected because
	// accepting it would exceed the concurrent server-initiated stream
	// count we advertised in SETTINGS_MAX_CONCURRENT_STREAMS. Internal to
	// the reader path: it becomes an RST_STREAM(REFUSED_STREAM) on the
	// promised id (RFC 7540 §6.6), never a caller-visible error.
	ErrPushRefused = errors.New("conn: promised stream refused; push cap reached")
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
	// ErrPushedStreamReadOnly is returned when the caller tries to send request
	// frames (HEADERS or DATA) on a server-pushed stream obtained via
	// LookupStream. A pushed stream is receive-only for the client: RFC 9113 §5.1
	// forbids sending any frame other than RST_STREAM, WINDOW_UPDATE, or PRIORITY
	// in the "reserved (remote)" state, and the client only ever receives the
	// promised response — it never sends application data on a pushed stream.
	ErrPushedStreamReadOnly = errors.New("conn: cannot send on a server-pushed stream (receive-only)")
)

// GoAwayError reports that the peer sent GOAWAY, and carries the two things the
// frame actually says: how much of our work the peer accepted, and why it is
// going away.
//
// The reason used to be discarded at the boundary, so every GOAWAY reached the
// caller as the bare ErrGoAway sentinel. That collapses three situations which
// call for opposite responses — NO_ERROR is a peer draining gracefully, so
// redial elsewhere and keep the load profile; ENHANCE_YOUR_CALM is a demand to
// back off, where redialling immediately makes it worse; PROTOCOL_ERROR and
// friends mean the peer is rejecting this client, so a redial fails identically
// and the right move is to report it. RFC 9113 §6.8 puts the code in the frame
// precisely so the receiver can tell them apart.
//
// It is returned by NewStream in place of ErrGoAway, which it still matches
// under errors.Is, so callers that only ask "is this a GOAWAY" are unaffected.
// Callers that want the detail use errors.As.
type GoAwayError struct {
	// LastStreamID is the highest stream id the peer says it processed or may
	// yet process. Streams above it were never accepted and are safe to retry
	// on a fresh connection; the Conn has already reset them with
	// REFUSED_STREAM.
	LastStreamID uint32
	// Code is the peer's stated reason (RFC 9113 §6.8). When a peer sends more
	// than one GOAWAY this is the newest reason, which §6.8 allows to become
	// more specific than an initial NO_ERROR.
	Code frame.ErrCode
}

// Error returns a string describing the peer's GOAWAY.
func (e *GoAwayError) Error() string {
	return fmt.Sprintf("conn: peer sent GOAWAY code=%v last=%d; no new streams",
		e.Code, e.LastStreamID)
}

// Is reports whether target is ErrGoAway, so that errors.Is(err, ErrGoAway)
// keeps holding now that the sentinel has been replaced by this type. The
// retry path depends on it.
func (e *GoAwayError) Is(target error) bool { return target == ErrGoAway }

// ConnError is connection-fatal. After it is returned the Conn is dead
// and all Streams created from it return ErrConnClosed.
type ConnError struct {
	Code   frame.ErrCode
	Reason string
	// Last is the last-stream-id advertised in the GOAWAY sent for this error.
	//
	// It is set only on the path that actually sends one — the reader loop
	// diagnosing a connection error, which is by definition local. The comment
	// here used to say "0 if originated locally", which had it backwards and
	// described a case no code path could produce: a GOAWAY *received* from the
	// peer never builds a ConnError at all, it produces a GoAwayError. Nothing
	// assigned this field, so every connection error printed last=0.
	Last uint32
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
