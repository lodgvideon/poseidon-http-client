package http1_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// TestReadBodyChunk_CoalescedEOF_KeepAlive_NotReusable is a regression test for
// the coalesced-EOF fix: when the final Content-Length body bytes arrive in the
// same Read as io.EOF (the peer closed the socket), the exchange must mark
// itself non-keep-alive even on an HTTP/1.1 response WITHOUT "Connection:
// close" — otherwise the now-dead socket is pooled and the next request reuses
// a closed connection.
func TestReadBodyChunk_CoalescedEOF_KeepAlive_NotReusable(t *testing.T) {
	const bodyLen = 16384 // matches http1 bufio size so the final Read yields (n, io.EOF)
	body := make([]byte, bodyLen)
	for i := range body {
		body[i] = 'x'
	}
	// HTTP/1.1 with no "Connection: close": header-derived keepAlive is true,
	// so only the coalesced-EOF handling can flip it to false.
	head := []byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", bodyLen))
	nc := &coalescedEOFConn{head: head, body: body}
	ex := http1.NewConn(nc).NewExchange()
	ctx := context.Background()
	require.NoError(t, ex.WriteRequest(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true), "WriteRequest")
	_, _, err := ex.ReadResponse(ctx)
	require.NoError(t, err, "ReadResponse")

	buf := make([]byte, bodyLen)
	total := 0
	for {
		n, done, rerr := ex.ReadBodyChunk(buf)
		total += n
		require.NoErrorf(t, rerr, "ReadBodyChunk: unexpected err=%v (after %d bytes)", rerr, total)
		if done {
			break
		}
	}

	require.Equalf(t, bodyLen, total, "body length = %d, want %d", total, bodyLen)
	require.True(t, nc.coalesced,
		"test premise not exercised: final bytes were not delivered coalesced with io.EOF")
	require.False(t, ex.KeepAlive(),
		"KeepAlive() = true after coalesced EOF; the socket is closed and must not be pooled")
}

// coalescedEOFConn serves the response head, then the whole body together with
// io.EOF in a single Read.
type coalescedEOFConn struct {
	head      []byte
	body      []byte
	stage     int
	coalesced bool
}

func (m *coalescedEOFConn) Read(p []byte) (int, error) {
	switch m.stage {
	case 0:
		n := copy(p, m.head)
		m.head = m.head[n:]
		if len(m.head) == 0 {
			m.stage = 1
		}
		return n, nil
	case 1:
		n := copy(p, m.body)
		m.body = m.body[n:]
		if len(m.body) == 0 {
			m.stage = 2
			if n > 0 {
				m.coalesced = true
			}
			return n, io.EOF
		}
		return n, nil
	default:
		return 0, io.EOF
	}
}

func (m *coalescedEOFConn) Write(p []byte) (int, error)      { return len(p), nil }
func (m *coalescedEOFConn) Close() error                     { return nil }
func (m *coalescedEOFConn) LocalAddr() net.Addr              { return coalAddr{} }
func (m *coalescedEOFConn) RemoteAddr() net.Addr             { return coalAddr{} }
func (m *coalescedEOFConn) SetDeadline(time.Time) error      { return nil }
func (m *coalescedEOFConn) SetReadDeadline(time.Time) error  { return nil }
func (m *coalescedEOFConn) SetWriteDeadline(time.Time) error { return nil }

type coalAddr struct{}

func (coalAddr) Network() string { return "fake" }
func (coalAddr) String() string  { return "fake" }
