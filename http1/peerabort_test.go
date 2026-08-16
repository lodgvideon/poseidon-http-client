package http1

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// A server reaping an idle keep-alive is a platform-dependent event at the socket
// layer, because HTTP/1.1 has no protocol signal for it. Linux delivers the queued
// FIN ahead of the RST our next write provokes, so the read ends in io.EOF; Windows
// reports the abort and no EOF ever arrives. Only the first shape was recognised, so
// on Windows the request failed instead of being replayed.
//
// The two integration tests in client/h1_fin_reuse_test.go drive the real event, but
// they cannot cover this: on Linux they take the io.EOF branch, so isPeerAbort could
// be deleted outright and CI — which is Linux — would stay green. These pin the
// abort branch on whichever platform they run on, using that platform's own errno.

// abortConn answers one request and then reports the local stack's connection-aborted
// error, which is what a reaped keep-alive looks like on a platform that does not
// deliver the FIN first. The error is wrapped exactly as the net package wraps it, so
// the classifier is exercised through the same errors.Is chain a real socket produces.
type abortConn struct{ errno error }

func (c *abortConn) Read([]byte) (int, error) {
	return 0, &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: &os.SyscallError{Syscall: "read", Err: c.errno},
	}
}

func (c *abortConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *abortConn) Close() error                     { return nil }
func (c *abortConn) LocalAddr() net.Addr              { return abortAddr{} }
func (c *abortConn) RemoteAddr() net.Addr             { return abortAddr{} }
func (c *abortConn) SetDeadline(time.Time) error      { return nil }
func (c *abortConn) SetReadDeadline(time.Time) error  { return nil }
func (c *abortConn) SetWriteDeadline(time.Time) error { return nil }

type abortAddr struct{}

func (abortAddr) Network() string { return "fake" }
func (abortAddr) String() string  { return "fake" }

// readResponseOver drives one request/response over nc and returns the read error.
func readResponseOver(t *testing.T, nc net.Conn) error {
	t.Helper()
	ex := NewConn(nc).NewExchange()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.test")},
	}
	if err := ex.WriteRequest(context.Background(), fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	_, _, err := ex.ReadResponse(context.Background())
	return err
}

// TestErrServerClosedIdle_PeerAbortBeforeAnyByte is the platform sibling of
// TestErrServerClosedIdle_ClosedBeforeAnyByte: the same event, reported by a stack
// that aborts rather than delivering EOF, must reach the retry classifier as the same
// replayable failure.
func TestErrServerClosedIdle_PeerAbortBeforeAnyByte(t *testing.T) {
	err := readResponseOver(t, &abortConn{errno: testAbortErrno})

	if err == nil {
		t.Fatal("ReadResponse returned no error after the peer aborted the connection")
	}
	if !errors.Is(err, ErrServerClosedIdle) {
		t.Errorf("error is %v, want ErrServerClosedIdle — %v is how this platform reports a "+
			"keep-alive the peer has already destroyed, and without the type the retry "+
			"classifier cannot tell it from a failure that may have been processed, so the "+
			"one safely replayable H1 failure goes unretried here", err, testAbortErrno)
	}
	if !errors.Is(err, testAbortErrno) {
		t.Errorf("error is %v; it should still wrap the underlying %v for diagnosis", err, testAbortErrno)
	}
}

// TestErrServerClosedIdle_NotAfterAPartialResponse_PeerAbort is the boundary, and it
// is the half that decides correctness: an abort arriving after the server began
// answering is no evidence at all that the request went unprocessed, so replaying it
// would duplicate work the peer may already have done.
//
// The status line is cut mid-token rather than ended, which is what puts this on the
// guard's own path: a complete line is consumed successfully and the abort then lands
// on the header-block read, which the guard does not cover, so such a fixture asserts
// nothing about it. Here the failing read IS the status-line read, with firstRead
// still true — readConsumedNothing is the only conjunct left to reject it. That makes
// this the one arm which fails if the abort branch is ever hoisted out of the guard
// rather than added inside it; no test built on io.EOF can express it, since none of
// them produce an abort.
func TestErrServerClosedIdle_NotAfterAPartialResponse_PeerAbort(t *testing.T) {
	nc := &partialThenAbortConn{sent: []byte("HTTP/1.1 20"), errno: testAbortErrno}
	err := readResponseOver(t, nc)

	if err == nil {
		t.Fatal("ReadResponse returned no error on a response truncated by an abort")
	}
	if errors.Is(err, ErrServerClosedIdle) {
		t.Errorf("a status line truncated by %v was classified as ErrServerClosedIdle — the "+
			"server had started answering, so it may have processed the request, and the "+
			"caller would replay it", testAbortErrno)
	}
}

// partialThenAbortConn writes part of a response and only then aborts.
type partialThenAbortConn struct {
	sent  []byte
	errno error
}

func (c *partialThenAbortConn) Read(p []byte) (int, error) {
	if len(c.sent) > 0 {
		n := copy(p, c.sent)
		c.sent = c.sent[n:]
		return n, nil
	}
	return 0, &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: &os.SyscallError{Syscall: "read", Err: c.errno},
	}
}

func (c *partialThenAbortConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *partialThenAbortConn) Close() error                     { return nil }
func (c *partialThenAbortConn) LocalAddr() net.Addr              { return abortAddr{} }
func (c *partialThenAbortConn) RemoteAddr() net.Addr             { return abortAddr{} }
func (c *partialThenAbortConn) SetDeadline(time.Time) error      { return nil }
func (c *partialThenAbortConn) SetReadDeadline(time.Time) error  { return nil }
func (c *partialThenAbortConn) SetWriteDeadline(time.Time) error { return nil }
