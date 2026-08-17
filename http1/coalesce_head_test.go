package http1_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// countingConn wraps a net.Conn and counts Write calls, so a test can assert
// the request head reaches the transport as one Write rather than one per
// header line — the whole point of coalescing, since writev is void through TLS
// and each net.Buffers segment would be its own tls.Conn.Write and record.
type countingConn struct {
	net.Conn
	mu     sync.Mutex
	writes int
	bytes  []byte
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	c.bytes = append(c.bytes, p...)
	c.mu.Unlock()
	return c.Conn.Write(p)
}

// TestExchange_WriteRequest_HeadIsOneWrite pins that the request line + header
// block leaves as a single Write. Before coalescing it was one Write per
// segment — seven for an ordinary head — each a separate TLS record and
// syscall.
func TestExchange_WriteRequest_HeadIsOneWrite(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() { // drain and answer so WriteRequest does not block on the pipe
		buf := make([]byte, 4096)
		_, _ = server.Read(buf)
		_, _ = server.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
	}()
	cc := &countingConn{Conn: client}
	c := http1.NewConn(cc)
	ex := c.NewExchange()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ex.WriteRequest(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/r")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte("accept"), Value: []byte("application/json")},
		{Name: []byte("user-agent"), Value: []byte("poseidon")},
	}, true)

	require.NoError(t, err, "WriteRequest")
	cc.mu.Lock()
	writes, wire := cc.writes, string(cc.bytes)
	cc.mu.Unlock()
	require.Equalf(t, 1, writes,
		"head took %d Writes, want 1 (each extra is a TLS record + syscall)", writes)
	want := "GET /r HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"accept: application/json\r\n" +
		"user-agent: poseidon\r\n" +
		"\r\n"
	require.Equalf(t, want, wire, "head bytes:\n%q\nwant:\n%q", wire, want)
}

// TestExchange_WriteRequest_HeadBufReuse drives two exchanges on one Conn so the
// reused headBuf is exercised: the second request must not carry any bytes of
// the first, and a shorter second head must be truncated, not leave a stale
// suffix.
func TestExchange_WriteRequest_HeadBufReuse(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go func() {
		buf := make([]byte, 4096)
		for i := 0; i < 2; i++ {
			_, _ = server.Read(buf)
			_, _ = server.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
		}
	}()
	cc := &countingConn{Conn: client}
	c := http1.NewConn(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// First: a long path so headBuf grows.
	ex1 := c.NewExchange()
	require.NoError(t, ex1.WriteRequest(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/a-deliberately-long-first-path-to-grow-the-buffer")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true), "first WriteRequest")
	_, _, err := ex1.ReadResponse(ctx)
	require.NoError(t, err, "first ReadResponse")
	cc.mu.Lock()
	cc.bytes = nil
	cc.mu.Unlock()

	// Second: a short path. If the buffer were not truncated, the long first
	// path's tail would leak into this head.
	ex2 := c.NewExchange()
	err = ex2.WriteRequest(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/b")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true)

	require.NoError(t, err, "second WriteRequest")
	cc.mu.Lock()
	wire := string(cc.bytes)
	cc.mu.Unlock()
	want := "GET /b HTTP/1.1\r\nHost: example.com\r\n\r\n"
	require.Equalf(t, want, wire, "second head:\n%q\nwant:\n%q", wire, want)
}
