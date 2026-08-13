package http1_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// replayConn serves one canned response over and over, so a benchmark can drive N
// keep-alive exchanges without a goroutine or a net.Pipe per iteration (the
// wireExchange helper the conformance tests use spins both, whose allocations
// would swamp the ones being measured).
//
// The byte stream it produces is exactly the response repeated end to end, so it
// stays well-formed however the reader chooses to buffer across the boundary.
type replayConn struct {
	script []byte
	off    int
}

func (c *replayConn) Read(p []byte) (int, error) {
	if c.off >= len(c.script) {
		c.off = 0 // the next response on this keep-alive connection
	}
	n := copy(p, c.script[c.off:])
	c.off += n
	return n, nil
}

func (c *replayConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *replayConn) Close() error                     { return nil }
func (c *replayConn) LocalAddr() net.Addr              { return sinkAddr{} }
func (c *replayConn) RemoteAddr() net.Addr             { return sinkAddr{} }
func (c *replayConn) SetDeadline(time.Time) error      { return nil }
func (c *replayConn) SetReadDeadline(time.Time) error  { return nil }
func (c *replayConn) SetWriteDeadline(time.Time) error { return nil }

// benchResponse is an ordinary response head: a status line and the headers a
// real server sends. Content-Length is 0 deliberately — ReadResponse reads the
// HEAD only, so a response with a body would leave those octets unread and the
// next exchange would parse them as its status line (it does: the first draft of
// this fixture died on `invalid status line: "{}HTTP/1.1 200 OK"`). A bodyless
// response keeps every iteration measuring the head and nothing else.
const benchResponse = "HTTP/1.1 200 OK\r\n" +
	"Content-Type: application/json\r\n" +
	"Content-Length: 0\r\n" +
	"Date: Mon, 13 Aug 2026 12:00:00 GMT\r\n" +
	"Server: nginx\r\n" +
	"Cache-Control: no-cache\r\n" +
	"\r\n"

func benchRequestFields() []header.Field {
	return []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/api/v1/resource")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte("Accept"), Value: []byte("application/json")},
	}
}

// BenchmarkReadResponse_Head measures one complete keep-alive exchange: write the
// request head, read the response head. http1 is outside the bench-gate's package
// list, so nothing else in the repo reports this number.
//
// It covers both directions on purpose — the read path cannot run without the
// write path having run — so the total is an exchange, not ReadResponse alone.
// Attribute per line with:
//
//	go test -run=^$ -bench=ReadResponse_Head -benchmem -memprofile=mem.out ./http1
//	go tool pprof -sample_index=alloc_objects -list='ReadResponse' mem.out
func BenchmarkReadResponse_Head(b *testing.B) {
	c := http1.NewConn(&replayConn{script: []byte(benchResponse)})
	fields := benchRequestFields()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ex := c.NewExchange()
		if err := ex.WriteRequest(ctx, fields, true); err != nil {
			b.Fatalf("WriteRequest: %v", err)
		}
		if _, _, err := ex.ReadResponse(ctx); err != nil {
			b.Fatalf("ReadResponse: %v", err)
		}
	}
}
