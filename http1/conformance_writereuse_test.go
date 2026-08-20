package http1_test

// Reuse safety on the SEND side. The read side established the invariant "any
// error means this connection is not poolable" with a blanket defer in
// ReadResponse and ReadBodyChunk; the write side never had it, and a failed or
// abandoned write leaves the peer counting octets just as surely.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// halfDeadConn writes okBytes octets and then fails every write, while serving
// resp on the read side. It stands in for a socket that dies mid-request: the
// peer has a partial message and this client is about to decide whether to hand
// the connection to the next one.
type halfDeadConn struct {
	net.Conn
	okBytes int
	resp    []byte
	off     int
}

func (c *halfDeadConn) Write(p []byte) (int, error) {
	if c.okBytes <= 0 {
		return 0, errors.New("connection reset by peer")
	}
	n := len(p)
	if n > c.okBytes {
		n = c.okBytes
	}
	c.okBytes -= n
	if n < len(p) {
		return n, errors.New("connection reset by peer")
	}
	return n, nil
}

func (c *halfDeadConn) Read(p []byte) (int, error) {
	if c.off >= len(c.resp) {
		return 0, net.ErrClosed
	}
	n := copy(p, c.resp[c.off:])
	c.off += n
	return n, nil
}

func (c *halfDeadConn) Close() error                     { return nil }
func (c *halfDeadConn) SetReadDeadline(time.Time) error  { return nil }
func (c *halfDeadConn) SetWriteDeadline(time.Time) error { return nil }
func (c *halfDeadConn) SetDeadline(time.Time) error      { return nil }

// fullWriteErrorConn accepts every octet and, once failing is set, reports an
// error anyway — the (len(p), err) return io.Writer explicitly permits and that
// a *tls.Conn produces when the record reached the kernel and the connection
// then failed. It is the one shape halfDeadConn cannot make: that one returns a
// nil error whenever it accepted the whole slice, so a caller counting octets
// can always tell something went wrong.
type fullWriteErrorConn struct {
	net.Conn
	failing bool
	resp    []byte
	off     int
}

func (c *fullWriteErrorConn) Write(p []byte) (int, error) {
	if c.failing {
		return len(p), errors.New("connection reset by peer")
	}
	return len(p), nil
}

func (c *fullWriteErrorConn) Read(p []byte) (int, error) {
	if c.off >= len(c.resp) {
		return 0, net.ErrClosed
	}
	n := copy(p, c.resp[c.off:])
	c.off += n
	return n, nil
}

func (c *fullWriteErrorConn) Close() error                     { return nil }
func (c *fullWriteErrorConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fullWriteErrorConn) SetWriteDeadline(time.Time) error { return nil }
func (c *fullWriteErrorConn) SetDeadline(time.Time) error      { return nil }

// TestConformance_RFC9112_Sec9_3_PartialWriteNotPoolable pins that a socket
// write failure condemns the connection.
//
// A partial head is a partial request-line or a half-written field: the peer is
// mid-message and cannot resynchronise. ReadResponse computed
// `keepAlive = respMinor >= 1 && !ex.condemned`, and nothing on the write path
// ever set condemned — so a truncated request followed by a readable response
// came back poolable, and the next request's octets landed on top of the
// unfinished one.
func TestConformance_RFC9112_Sec9_3_PartialWriteNotPoolable(t *testing.T) {
	const resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"

	t.Run("head truncated", func(t *testing.T) {
		nc := &halfDeadConn{okBytes: 12, resp: []byte(resp)}
		ex := http1.NewConn(nc).NewExchange()

		err := ex.WriteRequest(context.Background(), reqCL("GET"), true)

		require.Error(t, err, "WriteRequest = nil on a socket that failed mid-write")
		_, _, rerr := ex.ReadResponse(context.Background())
		assert.Falsef(t, rerr == nil && ex.KeepAlive(),
			"KeepAlive() = true after a truncated head; the peer has half a request "+
				"and the next one would be appended to it")
		assert.False(t, ex.KeepAlive(), "KeepAlive() = true after a truncated head")
	})

	// The short-write case. It is NOT what pins WriteBody's own condemn, and the
	// distinction is worth stating because the name suggests otherwise: three
	// independent mechanisms make this assertion true. ex.keepAlive is still its
	// zero value false (nothing calls ReadResponse here, and that is the only
	// place it is set true); KeepAlive's abandoned-upload guard fires on its own
	// because reqBodyWritten is 2 against a declared 5; and the condemn. Delete
	// the condemn and the first two still hold it up (#820). What this row
	// genuinely pins is that a short body write is refused and reported — the
	// subtest below is the one that isolates the condemn.
	t.Run("body truncated", func(t *testing.T) {
		// Enough for the whole head, not enough for the body.
		nc := &halfDeadConn{okBytes: 4096, resp: []byte(resp)}
		c := http1.NewConn(nc)
		ex := c.NewExchange()
		fields := reqCL("POST", header.Field{
			Name: []byte("content-length"), Value: []byte("5"),
		})
		require.NoError(t, ex.WriteRequest(context.Background(), fields, false), "WriteRequest")
		nc.okBytes = 2 // the body write now fails part-way

		err := ex.WriteBody(context.Background(), []byte("HELLO"), true)

		require.Error(t, err, "WriteBody = nil on a socket that failed mid-write")
		assert.False(t, ex.KeepAlive(),
			"KeepAlive() = true after a truncated body; the peer is still counting "+
				"octets against the Content-Length this client declared")
	})

	// The case where WriteBody's condemn is the ONLY thing standing between the
	// socket and the pool, and the one the suite never sent (#820).
	//
	// net.Conn permits a Write to return (len(p), err): every octet accepted and
	// an error reported anyway. That is what a *tls.Conn reports when the record
	// reached the kernel and the connection then failed, and halfDeadConn cannot
	// produce it — it returns a nil error whenever n == len(p). On that path
	// reqBodyWritten equals the declared Content-Length, so the abandoned-upload
	// guard does not fire; the peer then answers, so ReadResponse sets keepAlive
	// from respMinor and !condemned, so the zero-value route does not either.
	//
	// The error is genuinely ambiguous — the peer may or may not have the octets —
	// and that ambiguity is the reason the connection cannot be reused: a pooled
	// socket whose peer might be mid-body desynchronises the next request.
	t.Run("body write errors after accepting every octet", func(t *testing.T) {
		nc := &fullWriteErrorConn{resp: []byte(resp)}
		ex := http1.NewConn(nc).NewExchange()
		fields := reqCL("POST", header.Field{
			Name: []byte("content-length"), Value: []byte("5"),
		})
		require.NoError(t, ex.WriteRequest(context.Background(), fields, false), "WriteRequest")
		nc.failing = true // every octet still accepted, but the error is reported

		err := ex.WriteBody(context.Background(), []byte("HELLO"), true)

		require.Error(t, err, "WriteBody = nil on a socket that reported a write failure")
		_, _, rerr := ex.ReadResponse(context.Background())
		require.NoError(t, rerr, "ReadResponse — the peer answered, which is the point")
		assert.False(t, ex.KeepAlive(),
			"KeepAlive() = true after the socket reported an error on the body write. "+
				"Every octet was accepted, so the under-run guard cannot see it, and the "+
				"peer answered, so keepAlive was set from the response — the condemn on "+
				"the write error is the only thing left, and without it this socket goes "+
				"back into the pool")
	})
}

// TestConformance_RFC9112_Sec9_3_AbandonedUploadNotPoolable pins the case no
// error reports at all: the caller declared a Content-Length, wrote part of it,
// and stopped — a cancelled request, a body source that failed. Every write
// SUCCEEDED, so nothing on the error paths fires.
//
// The under-run check inside WriteBody only runs on the final chunk, which by
// definition never comes here. KeepAlive is where the question belongs: it is
// the caller's decision to stop that makes the body short, and this is the
// moment the caller asks whether the connection survived it.
func TestConformance_RFC9112_Sec9_3_AbandonedUploadNotPoolable(t *testing.T) {
	srv, ex := bodyExchange(t, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	defer srv.Close()
	fields := reqCL("POST", header.Field{
		Name: []byte("content-length"), Value: []byte("5"),
	})
	require.NoError(t, ex.WriteRequest(context.Background(), fields, false), "WriteRequest")

	// Three of the five declared octets, then the caller gives up.
	require.NoError(t, ex.WriteBody(context.Background(), []byte("HEL"), false), "WriteBody")

	// Reading the response is what makes this test say anything. A server may
	// answer before it has consumed the body — 413, 401, a redirect — and that
	// success is what sets keepAlive true. Without it KeepAlive() is false for
	// the trivial reason that no response has been read, and the assertion below
	// would hold with the under-run guard deleted.
	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")
	assert.False(t, ex.KeepAlive(),
		"KeepAlive() = true with 3 of 5 declared octets written — the peer is "+
			"waiting for two more and would read the next request's line as them")
}

// TestConformance_RFC9112_Sec9_3_CompleteUploadStillPoolable is the control. A
// predicate that simply refused every request carrying a body would pass the
// test above; this fails unless the full-body case is left alone.
func TestConformance_RFC9112_Sec9_3_CompleteUploadStillPoolable(t *testing.T) {
	srv, ex := bodyExchange(t, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	defer srv.Close()
	fields := reqCL("POST", header.Field{
		Name: []byte("content-length"), Value: []byte("5"),
	})
	require.NoError(t, ex.WriteRequest(context.Background(), fields, false), "WriteRequest")

	require.NoError(t, ex.WriteBody(context.Background(), []byte("HELLO"), true), "WriteBody")

	_, _, err := ex.ReadResponse(context.Background())
	require.NoError(t, err, "ReadResponse")
	assert.True(t, ex.KeepAlive(),
		"KeepAlive() = false after a complete, well-framed exchange — the under-run "+
			"guard is firing on a body that was fully written")
}

// bodyExchange gives an Exchange over a real socket whose peer drains whatever
// is sent and answers with resp. A real socket rather than net.Pipe: the client
// writes a head and a body before reading anything, which an unbuffered
// synchronous pipe deadlocks on.
func bodyExchange(t *testing.T, resp string) (net.Conn, *http1.Exchange) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer nc.Close()
		buf := make([]byte, 4096)
		_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = nc.Read(buf)
		_, _ = nc.Write([]byte(resp))
		_, _ = nc.Read(buf)
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err, "Dial")
	t.Cleanup(func() { _ = cli.Close() })
	return cli, http1.NewConn(cli).NewExchange()
}
