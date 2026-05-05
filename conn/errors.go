package conn

import (
	"errors"
	"fmt"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Sentinel errors. All are stable across releases; callers may use
// errors.Is to identify them.
var (
	ErrALPNFailed            = errors.New("conn: ALPN did not negotiate h2")
	ErrTooManyStreams        = errors.New("conn: B.1 supports one in-flight stream per Conn")
	ErrConnClosed            = errors.New("conn: connection closed")
	ErrStreamClosed          = errors.New("conn: stream already closed")
	ErrFlowControlExhausted  = errors.New("conn: send window too small for payload")
	ErrUnexpectedPushPromise = errors.New("conn: peer sent PUSH_PROMISE while ENABLE_PUSH=0")
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
