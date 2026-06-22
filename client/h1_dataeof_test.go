package client_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestH1_ContentLength_DataEOFCoalesced is a regression test for silent body
// truncation: a Content-Length response whose final bytes arrive in a single
// underlying Read returning (n>0, io.EOF) used to surface io.EOF as a fatal
// error, causing the h1 adapter to discard the final (up to bufio-sized) bytes
// and fail the request. The fix treats EOF coinciding with body completion as
// a clean end, delivering the bytes with a nil error.
func TestH1_ContentLength_DataEOFCoalesced(t *testing.T) {
	// 16384 == http1's bufio.NewReaderSize (http1/conn.go) and the h1 adapter's
	// scratch buffer (client/h1_transport.go), so the final Read fills the
	// buffer and bufio's direct-read path passes the underlying (n, io.EOF)
	// through unchanged — the coalesced condition under test. If either buffer
	// size changes this must change in lockstep or the test stops exercising it.
	const bodyLen = 16384

	body := make([]byte, bodyLen)
	for i := range body {
		body[i] = byte('A' + (i % 26))
	}
	head := []byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", bodyLen))

	mc := &h1eofConn{head: head, body: body}

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: h1eofDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return mc, nil
		})},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	derr := c.Do(ctx, &client.Request{Method: "GET", Path: "/", WantBody: true}, &resp)

	if !mc.eofRidden() {
		t.Fatalf("test premise not exercised: server did not deliver final bytes coalesced with io.EOF")
	}
	if derr != nil {
		t.Fatalf("Do returned %v; want nil (final bytes + io.EOF must not be dropped)", derr)
	}
	if len(resp.Body) != bodyLen {
		t.Fatalf("body truncated: got %d bytes, want %d", len(resp.Body), bodyLen)
	}
}

type h1eofDialer func(ctx context.Context, addr string) (net.Conn, error)

func (f h1eofDialer) Dial(ctx context.Context, addr string) (net.Conn, error) { return f(ctx, addr) }

// h1eofConn returns the response head, then the entire body together with
// io.EOF in one Read call, deterministically reproducing the coalesced-EOF
// condition that the bufio direct-read path passes through unchanged.
type h1eofConn struct {
	mu       sync.Mutex
	head     []byte
	body     []byte
	stage    int
	gotEOFOK bool
}

func (m *h1eofConn) Read(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
				m.gotEOFOK = true
			}
			return n, io.EOF
		}
		return n, nil
	default:
		return 0, io.EOF
	}
}

func (m *h1eofConn) eofRidden() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gotEOFOK
}

func (m *h1eofConn) Write(p []byte) (int, error)        { return len(p), nil }
func (m *h1eofConn) Close() error                       { return nil }
func (m *h1eofConn) LocalAddr() net.Addr                { return h1eofAddr{} }
func (m *h1eofConn) RemoteAddr() net.Addr               { return h1eofAddr{} }
func (m *h1eofConn) SetDeadline(_ time.Time) error      { return nil }
func (m *h1eofConn) SetReadDeadline(_ time.Time) error  { return nil }
func (m *h1eofConn) SetWriteDeadline(_ time.Time) error { return nil }

type h1eofAddr struct{}

func (h1eofAddr) Network() string { return "fake" }
func (h1eofAddr) String() string  { return "fake:0" }
