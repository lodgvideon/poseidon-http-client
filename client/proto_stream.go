package client

import (
	"context"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// protoStream is the protocol-agnostic unit of one HTTP request/response
// exchange. It is satisfied by:
//   - *conn.Stream (HTTP/2) — returned by conn.Conn.NewStream
//   - *h1Exchange  (HTTP/1.1) — defined in h1_transport.go
//
// The interface mirrors the *conn.Stream surface used by sendRequest and
// drainResponse, so the H2 hot path has zero additional indirection cost
// (the concrete *conn.Stream is stored in an interface only when it crosses
// the transport boundary, which happens once per request).
type protoStream interface {
	// SendHeaders sends the request headers. For H2 this writes a HEADERS
	// frame; for H1.1 this writes the request line + header block.
	// endStream=true means no body will follow.
	SendHeaders(ctx context.Context, fields []conn.HeaderField, endStream bool) error

	// SendHeadersWithPriority is like SendHeaders but carries optional H2
	// PRIORITY data. H1.1 implementations ignore prio.
	SendHeadersWithPriority(ctx context.Context, fields []conn.HeaderField, endStream bool, prio *frame.Priority) error

	// SendData sends a body chunk. endStream=true marks the final chunk.
	SendData(ctx context.Context, p []byte, endStream bool) error

	// Recv returns the next protocol event. For H2 this reads a StreamEvent
	// from the stream's channel; for H1.1 it synthesises EventHeaders on the
	// first call, then EventData for each body chunk.
	Recv(ctx context.Context) (conn.StreamEvent, error)

	// Close cancels an in-flight exchange. Idempotent.
	Close() error
}

// respStream is the minimal streaming-read surface the response body reader
// (responseBodyReader) and StreamResponse drive to deliver DATA / trailers /
// reset events incrementally. It is exactly the tail of protoStream — Recv +
// Close — and is satisfied by:
//   - *conn.Stream (HTTP/2) — the concrete stream reads events from its channel;
//   - *h3RespStream (HTTP/3) — wraps an http3.BodyReader (defined in h3_transport.go).
//
// It is deliberately narrower than protoStream so DoStream/BodyStream can hold an
// interface that both protocols satisfy without the Send half. On the H2 hot path
// a *conn.Stream (a pointer) is boxed into this interface at most once per request,
// which costs no allocation — the pointer is stored inline in the interface word.
type respStream interface {
	Recv(ctx context.Context) (conn.StreamEvent, error)
	Close() error
}
