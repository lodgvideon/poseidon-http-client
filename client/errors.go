package client

import (
	"errors"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Sentinel errors returned (or wrapped via %w) from the client.
var (
	// ErrInvalidRequest indicates a Request failed up-front validation.
	// Wrapped errors carry a human-readable detail.
	ErrInvalidRequest = errors.New("client: invalid request")

	// ErrClosed is returned from Do, DoStream, or transport.acquire
	// after Client.Close has been called.
	ErrClosed = errors.New("client: closed")

	// ErrRedialBackoff is returned when a previous dial attempt
	// failed and the configured DialBackoff window has not elapsed.
	ErrRedialBackoff = errors.New("client: redial in backoff window")

	// ErrResidueOnAcquire is returned when every connection the pool could
	// offer had unread octets on it — a peer that keeps writing unsolicited
	// responses (RFC 9112 §6.3). Failing the request is the only safe answer:
	// such a connection cannot be framed, so handing one back would mean
	// serving the peer's octets as this request's response.
	ErrResidueOnAcquire = errors.New("client: no connection free of unsolicited data")

	// ErrEmptyResponse is returned when the response HEADERS frame
	// did not contain a :status pseudo-header at all.
	ErrEmptyResponse = errors.New("client: response missing :status")

	// ErrInvalidStatus is returned when the :status pseudo-header is
	// present but does not parse as a non-negative integer. Distinct
	// from ErrEmptyResponse so retry logic can treat the two separately.
	ErrInvalidStatus = errors.New("client: response :status is not a valid integer")

	// ErrConflictingContentEncoding is returned when a Request sets both
	// Request.CompressBody and its own content-encoding header. The two say
	// contradictory things about the body — "encode this" versus "this is
	// already encoded" — and either reading corrupts it, so the request is
	// refused rather than guessed at. Wraps ErrInvalidRequest, so it is a hard
	// stop for the Retryer.
	ErrConflictingContentEncoding = errors.New("client: content-encoding header conflicts with Request.CompressBody")

	// ErrUnsupportedContentEncoding is returned when Request.CompressBody names
	// a coding this client cannot produce. Failing is deliberate: silently
	// sending the body uncompressed would leave the caller believing it was
	// compressed. Wraps ErrInvalidRequest, so it is a hard stop for the Retryer.
	ErrUnsupportedContentEncoding = errors.New("client: unsupported Request.CompressBody encoding")
)

// StreamResetError is returned from Do (or surfaced via DoStream's
// EventReset) when the stream is reset while a response is being read.
//
// Code is not always the peer's. conn resets its own stream with a synthesised
// RST_STREAM(CANCEL) when a response outruns the per-stream event buffer, and
// that reaches the caller here too — the code says the client stopped wanting
// the stream, which is exactly what happened, and the retry classifier
// correctly does not retry it. It used to be REFUSED_STREAM, whose RFC 9113
// §8.7 meaning is a promise that the server never processed the request; that
// made the classifier replay work the server had already done.
//
// A reset that follows a complete response is not an error at all: RFC 9113
// §8.1 requires the response be kept, and neither Do nor Response.BodyReader
// reports one.
type StreamResetError struct {
	Code conn.ErrCode
}

// Error implements the error interface.
func (e *StreamResetError) Error() string {
	return fmt.Sprintf("client: stream reset by peer: %v", e.Code)
}

// Unwrap returns nil. Provided for structural consistency with [DialError]
// so error-handling code can uniformly call errors.Is/As on client errors.
func (e *StreamResetError) Unwrap() error { return nil }

// DialError wraps the underlying dial error and the address that
// failed. Returned from Do/DoStream when the lazy dial fails.
type DialError struct {
	Addr string
	Err  error
}

// Error implements the error interface.
func (e *DialError) Error() string {
	return fmt.Sprintf("client: dial %s: %v", e.Addr, e.Err)
}

// Unwrap exposes the underlying error for errors.Is / errors.As.
func (e *DialError) Unwrap() error { return e.Err }

// Pool-related errors. Used by the TransportPool transport.
var (
	// ErrPoolClosed is returned by Pool operations after Close.
	ErrPoolClosed = errors.New("client: pool closed")

	// ErrAcquireTimeout is returned when PoolOptions.AcquireTimeout
	// elapses before capacity becomes available.
	ErrAcquireTimeout = errors.New("client: acquire timeout")

	// ErrDialBackoff is returned when a recent dial failure on the
	// pool is still within the DialBackoff window.
	ErrDialBackoff = errors.New("client: dial backoff active")

	// ErrInvalidPoolOptions is returned by NewClient when Transport
	// and Pool are inconsistent.
	ErrInvalidPoolOptions = errors.New("client: invalid pool options")

	// ErrInvalidTransportKind is returned by NewClient when
	// ClientOptions.Transport is not a defined TransportKind.
	ErrInvalidTransportKind = errors.New("client: invalid transport kind")

	// ErrInvalidOptions is returned by NewClient when ClientOptions are
	// internally inconsistent (e.g. both Addr and Resolver supplied) or are
	// missing a field the chosen transport requires (Addr, ConnOpts.Dialer,
	// or TLSConfig for an HTTP/3 transport).
	//
	// It is not the only sentinel NewClient rejects options with: a Pool that
	// does not match the transport carries ErrInvalidPoolOptions, a dialer
	// asserting an unusable ALPN carries ErrALPNProtocolMismatch, and an
	// undefined kind carries ErrInvalidTransportKind. A caller wanting "any
	// option was rejected" should test for all four; the split exists so the
	// common cases can be told apart without parsing the message.
	ErrInvalidOptions = errors.New("client: invalid ClientOptions")

	// ErrBodyTooLarge is returned when the response body (compressed or
	// decompressed) exceeds the configured maximum size, preventing
	// memory-exhaustion attacks such as gzip bombs.
	ErrBodyTooLarge = errors.New("client: response body exceeds maximum size")

	// ErrTrailersUnsupportedH1 is returned when a request carrying trailers
	// is sent over an HTTP/1.1 connection. HTTP/1.1 request trailers require
	// chunked transfer-coding with a trailer section, which this fallback
	// transport does not implement; the request is rejected rather than
	// corrupting the connection with a second request line.
	ErrTrailersUnsupportedH1 = errors.New("client: HTTP/1.1 does not support request trailers")

	// ErrTrailersUnsupportedH3 is returned when a request carrying trailers
	// is sent over the buffered HTTP/3 transport. http3.Request has no request-
	// trailer field, so the request is rejected rather than silently dropping
	// the trailer section.
	ErrTrailersUnsupportedH3 = errors.New("client: buffered HTTP/3 transport does not support request trailers")

	// ErrStreamingUnsupported is returned by DoStream and Do(BodyMode=BodyStream)
	// when the underlying transport has no incremental response-body path. All
	// three shipped transports have one — HTTP/2 (*conn.Stream), HTTP/3 (via
	// http3.Client.DoStream) and HTTP/1.1 (h1Exchange, which reads a chunk per
	// Recv) — so this is now reachable only through a transport outside the tree.
	ErrStreamingUnsupported = errors.New("client: streaming responses are not supported by this transport")

	// ErrALPNProtocolMismatch reports that a connection's application protocol
	// is not the one the transport speaks. It is returned in two places:
	//
	//   - by NewClient, when ConnOpts.Dialer asserts an ALPN protocol
	//     (conn.ALPNAsserter) the chosen transport cannot use — an HTTP/1.1
	//     transport with conn.TLSDialer, or an HTTP/2 transport with
	//     conn.H1TLSDialer;
	//   - by the HTTP/1.1 transports, when a freshly dialled connection reports
	//     a negotiated ALPN protocol other than "http/1.1".
	//
	// The second check is the backstop for a custom dialer: writing an HTTP/1.1
	// request into a connection the peer frames as HTTP/2 fails as an unhelpful
	// "read status line: EOF" on every exchange, with nothing pointing at ALPN.
	ErrALPNProtocolMismatch = errors.New("client: negotiated ALPN protocol does not match transport")
)
