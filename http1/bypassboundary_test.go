package http1

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// bufio.Read short-circuits when its buffer is empty AND the caller's slice is
// AT LEAST buffer-sized: it reads straight into that slice, so the buffer stays
// empty and anything the peer appended after the response never enters the
// reader. ReadBodyChunk notes that as `bypassed` and, once the body is complete,
// asks the socket instead — because the completion guard's Buffered() check is
// blind on exactly this path.
//
// `>=` is therefore the boundary, not `>`: at a slice of exactly readBufSize the
// bypass happens. Nothing pinned that. Every other test on this path reads with
// a comfortably larger slice, and a comfortably larger slice bypasses under
// either operator — so the off-by-one at the boundary was invisible, and the
// connection it lets through carries an unsolicited response the next request
// parses as its own status line (RFC 9112 §6.3 MUST NOT).
//
// Hence a scripted transport rather than a socket. The boundary is only reached
// when the whole body arrives in the one read that still carries a full-sized
// slice: a short read leaves a remainder, ReadBodyChunk clamps the next slice to
// it, and len(buf) drops below readBufSize before completion. A real socket
// returns whatever the kernel happens to hold at that instant and cannot promise
// that; scriptedConn can, and the premise is asserted below rather than assumed.

// scriptedConn serves each chunk in a single Read, in order, and then reports a
// timeout — the quiet-socket answer, which is what a probe under a deadline gets
// from a peer that has sent nothing more.
type scriptedConn struct {
	chunks [][]byte
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	for len(c.chunks) > 0 && len(c.chunks[0]) == 0 {
		c.chunks = c.chunks[1:]
	}
	if len(c.chunks) == 0 {
		return 0, scriptTimeout{}
	}
	n := copy(p, c.chunks[0])
	c.chunks[0] = c.chunks[0][n:]
	return n, nil
}

func (c *scriptedConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return scriptAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return scriptAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

type scriptAddr struct{}

func (scriptAddr) Network() string { return "scripted" }
func (scriptAddr) String() string  { return "scripted" }

// scriptTimeout is what a read past the end of the script returns: a net.Error
// that timed out, the same shape a real socket gives a probe whose deadline
// elapsed with nothing to read.
type scriptTimeout struct{}

func (scriptTimeout) Error() string   { return "scripted: i/o timeout" }
func (scriptTimeout) Timeout() bool   { return true }
func (scriptTimeout) Temporary() bool { return true }

// exactBufferExchange drives one exchange whose body is exactly readBufSize
// bytes and whose caller slice is exactly readBufSize bytes, and reports whether
// the connection came out poolable. tail is whatever the peer appends after the
// response.
func exactBufferExchange(t *testing.T, tail string) bool {
	t.Helper()

	body := make([]byte, readBufSize)
	for i := range body {
		body[i] = 'x'
	}
	head := []byte("HTTP/1.1 200 OK\r\nContent-Length: " +
		strconv.Itoa(readBufSize) + "\r\n\r\n")

	// Head and body in separate reads: the head alone leaves the reader empty,
	// which is the other half of the bypass condition.
	script := &scriptedConn{chunks: [][]byte{head, body}}
	if tail != "" {
		script.chunks = append(script.chunks, []byte(tail))
	}

	ex := NewConn(script).NewExchange()
	ctx := context.Background()
	if err := ex.WriteRequest(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if _, _, err := ex.ReadResponse(ctx); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	// EXACTLY readBufSize. One byte more and the mutation this pins survives:
	// a larger slice bypasses under `>` as well.
	buf := make([]byte, readBufSize)
	n, done, err := ex.ReadBodyChunk(buf)
	if err != nil {
		t.Fatalf("ReadBodyChunk: %v", err)
	}
	if n != readBufSize || !done {
		t.Fatalf("premise not exercised: first ReadBodyChunk returned n=%d done=%v, "+
			"want n=%d done=true — the body must complete in the one read that still "+
			"carries a full-sized slice, or the bypass boundary is never reached",
			n, done, readBufSize)
	}
	return ex.KeepAlive()
}

// TestReadBodyChunk_BypassAtExactBufferSize_NotPoolable is the gate: at a slice
// of exactly readBufSize the read bypasses the reader, so the appended response
// is invisible to Buffered() and only the socket can be asked.
func TestReadBodyChunk_BypassAtExactBufferSize_NotPoolable(t *testing.T) {
	if exactBufferExchange(t, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\npwn") {
		t.Error("KeepAlive() = true after a body read of exactly readBufSize bytes left " +
			"an unsolicited response on the socket — the read bypassed the reader, so " +
			"Buffered() sees nothing, and the connection goes back to the pool with a " +
			"peer-chosen response the next request parses as its own status line " +
			"(RFC 9112 §6.3)")
	}
}

// TestReadBodyChunk_BypassAtExactBufferSize_CleanStaysPoolable is the
// over-rejection guard. The boundary must not condemn a connection merely for
// having been read with a full-sized slice; without this, widening the check to
// "always condemn on the bypass path" would pass the gate above and cost a
// connection per request at exactly the buffer size the fast path uses.
func TestReadBodyChunk_BypassAtExactBufferSize_CleanStaysPoolable(t *testing.T) {
	if !exactBufferExchange(t, "") {
		t.Error("KeepAlive() = false after a body read of exactly readBufSize bytes with " +
			"nothing left on the socket, want true")
	}
}
