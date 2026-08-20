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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, ex.WriteRequest(context.Background(), fields, true), "WriteRequest")
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

			assert.NotContainsf(t, wire, "sentinel-value",
				"%q reached the wire; this client manages that field itself, "+
					"and a caller-supplied copy can contradict the framing it wrote.\n%s",
				name, wire)
		})
	}
}

// TestWriteRequest_KeepsOrdinaryHeaders is the control for the test above, and
// the names are chosen for their LENGTHS because that is what
// isConnectionManagedName switches on first: it compares len(name) and only
// then folds the bytes, so an arm that dropped its byte comparison would
// silently delete every caller header of that length while the managed-names
// test above stayed green.
//
// One ordinary name per arm of that switch — which is why the table is keyed on
// the managed name it guards. The switch has six arms and this covered three of
// them (10, 7, 4). Lengths 2, 16 and 17 had no counterpart, so `case 2: return
// true` in place of the fold on "te" was invisible to the whole package, and
// every two-character request header a caller sent would have been dropped from
// the wire with the suite green (#830). A new arm added without its control here
// should now look wrong.
func TestWriteRequest_KeepsOrdinaryHeaders(t *testing.T) {
	for _, tc := range []struct {
		managed string // the name this arm of the switch exists to skip
		name    string // an ordinary caller header of exactly that length
		value   string
	}{
		{"te", "dt", "2030-01-01"},
		{"host", "Date", "Tue, 01 Jan 2030 00:00:00 GMT"},
		{"upgrade", "Referer", "https://example.com/x"},
		{"connection", "User-Agent", "poseidon"}, // and "keep-alive", same arm
		{"proxy-connection", "X-Correlation-Id", "abc123"},
		{"transfer-encoding", "X-Forwarded-Proto", "https"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Lenf(t, tc.name, len(tc.managed),
				"the control name %q must be exactly as long as the managed name %q it "+
					"guards, or this row proves nothing about that arm of the switch",
				tc.name, tc.managed)
			fields := append(baseFields(),
				header.Field{Name: []byte(tc.name), Value: []byte(tc.value)})

			wire := writeOneRequest(t, fields)

			assert.Containsf(t, wire, strings.ToLower(tc.name)+": "+tc.value,
				"%q (%d bytes, the length of %q) never reached the wire. This client "+
					"skips the names it manages itself by length-then-bytes; an arm that "+
					"stopped comparing the bytes drops every caller header of that length "+
					"silently, and the request goes out missing a field the caller set.\n%s",
				tc.name, len(tc.name), tc.managed, wire)
		})
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

	assert.Containsf(t, wire, "x-mixed-case: v", "name was not lower-cased on the wire:\n%s", wire)
	assert.NotContainsf(t, wire, "X-MiXeD-CaSe", "original casing reached the wire:\n%s", wire)
}
