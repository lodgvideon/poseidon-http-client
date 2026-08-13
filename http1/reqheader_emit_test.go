package http1_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// sinkConn is a net.Conn that swallows writes and never yields a read, so a
// benchmark measures request assembly rather than a peer.
//
// Every method is implemented rather than embedding a nil net.Conn: the write
// path touches the deadline setters, and an embedded nil panics on the first one
// that is not overridden. It lives in this untagged file because the benchmarks
// that use it are !race-only and the tests below are not.
type sinkConn struct{}

type sinkAddr struct{}

func (sinkAddr) Network() string { return "sink" }
func (sinkAddr) String() string  { return "sink" }

func (sinkConn) Write(p []byte) (int, error)      { return len(p), nil }
func (sinkConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (sinkConn) Close() error                     { return nil }
func (sinkConn) LocalAddr() net.Addr              { return sinkAddr{} }
func (sinkConn) RemoteAddr() net.Addr             { return sinkAddr{} }
func (sinkConn) SetDeadline(time.Time) error      { return nil }
func (sinkConn) SetReadDeadline(time.Time) error  { return nil }
func (sinkConn) SetWriteDeadline(time.Time) error { return nil }

// captureConn records what the client writes and never yields a read.
type captureConn struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *captureConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}
func (c *captureConn) wire() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}
func (*captureConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (*captureConn) Close() error                     { return nil }
func (*captureConn) LocalAddr() net.Addr              { return sinkAddr{} }
func (*captureConn) RemoteAddr() net.Addr             { return sinkAddr{} }
func (*captureConn) SetDeadline(time.Time) error      { return nil }
func (*captureConn) SetReadDeadline(time.Time) error  { return nil }
func (*captureConn) SetWriteDeadline(time.Time) error { return nil }

func writeOneRequest(t *testing.T, fields []header.Field) string {
	t.Helper()
	cc := &captureConn{}
	ex := http1.NewConn(cc).NewExchange()
	if err := ex.WriteRequest(context.Background(), fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	return cc.wire()
}

func baseFields() []header.Field {
	return []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
}

// TestWriteRequest_DropsConnectionManagedNamesInAnyCase pins the hop-by-hop and
// Host skip, which WriteRequest performs case-insensitively because RFC 9110
// §5.1 makes field names case-insensitive — a caller spelling "Connection" the
// canonical way must not get it onto the wire, where it would contradict the
// framing this client manages itself.
//
// Each name is sent in its RFC spelling rather than lower-case: that is the
// spelling a real caller uses, and the case-folding is the part under test.
func TestWriteRequest_DropsConnectionManagedNamesInAnyCase(t *testing.T) {
	managed := []string{
		"TE", "Host", "Upgrade", "Connection", "Keep-Alive",
		"Proxy-Connection", "Transfer-Encoding",
	}
	for _, name := range managed {
		t.Run(name, func(t *testing.T) {
			fields := append(baseFields(), header.Field{
				Name:  []byte(name),
				Value: []byte("sentinel-value"),
			})
			wire := writeOneRequest(t, fields)
			if strings.Contains(wire, "sentinel-value") {
				t.Errorf("%q reached the wire; this client manages that field itself, "+
					"and a caller-supplied copy can contradict the framing it wrote.\n%s",
					name, wire)
			}
		})
	}
}

// TestWriteRequest_KeepsOrdinaryHeaders is the control, and the lengths are
// chosen deliberately: User-Agent is 10 bytes like "connection" and
// "keep-alive", Referer is 7 like "upgrade", Date is 4 like "host". A skip that
// compared lengths without comparing bytes would drop all three and still pass
// the test above.
func TestWriteRequest_KeepsOrdinaryHeaders(t *testing.T) {
	fields := append(baseFields(),
		header.Field{Name: []byte("User-Agent"), Value: []byte("poseidon")},
		header.Field{Name: []byte("Referer"), Value: []byte("https://example.com/x")},
		header.Field{Name: []byte("Date"), Value: []byte("Tue, 01 Jan 2030 00:00:00 GMT")},
	)
	wire := writeOneRequest(t, fields)
	for _, want := range []string{"user-agent: poseidon", "referer: https://example.com/x", "date: Tue"} {
		if !strings.Contains(wire, want) {
			t.Errorf("missing %q from the request head:\n%s", want, wire)
		}
	}
}

// TestWriteRequest_EmitsLowerCaseNames pins the wire spelling. Field names are
// case-insensitive (RFC 9110 §5.1) so this is a choice rather than a
// requirement, but it is the choice this client has always made and the one the
// byte-wise fold has to preserve.
func TestWriteRequest_EmitsLowerCaseNames(t *testing.T) {
	fields := append(baseFields(),
		header.Field{Name: []byte("X-MiXeD-CaSe"), Value: []byte("v")},
	)
	wire := writeOneRequest(t, fields)
	if !strings.Contains(wire, "x-mixed-case: v") {
		t.Errorf("name was not lower-cased on the wire:\n%s", wire)
	}
	if strings.Contains(wire, "X-MiXeD-CaSe") {
		t.Errorf("original casing reached the wire:\n%s", wire)
	}
}
