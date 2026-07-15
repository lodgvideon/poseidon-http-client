package http1_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// errBudget is returned by hostileConn once a test's read budget is spent. It
// is the deterministic stand-in for "the client would have read forever": a
// real hostile server never stops, but a test that let the client read forever
// would hang or OOM the runner instead of failing with a diagnosis.
var errBudget = errors.New("hostileConn: read budget exhausted")

// hostileConn is a net.Conn standing in for a malicious HTTP/1.1 server. It
// writes prefix verbatim, then repeats fill forever — so a fill that contains
// no '\n' models the server that simply never terminates a protocol line, and
// a fill that is a whole well-formed line models the server that sends them
// endlessly.
//
// It counts every byte it hands out and, once budget is spent, fails the Read
// with errBudget. That budget is what makes the unbounded-read tests fail
// deterministically: a bounded client stops on its own long before the budget,
// while an unbounded one drinks the whole thing and reports errBudget — which
// the assertions read as the bug, not as a test infrastructure failure.
type hostileConn struct {
	prefix []byte // written first, verbatim
	fill   []byte // repeated cyclically forever after prefix
	off    int    // read offset into fill
	budget int64  // stop and fail the Read after this many bytes
	served atomic.Int64
}

func (h *hostileConn) Read(p []byte) (int, error) {
	if h.served.Load() >= h.budget {
		return 0, errBudget
	}
	if len(h.prefix) > 0 {
		n := copy(p, h.prefix)
		h.prefix = h.prefix[n:]
		h.served.Add(int64(n))
		return n, nil
	}
	if len(h.fill) == 0 {
		return 0, errBudget
	}
	n := 0
	for n < len(p) {
		c := copy(p[n:], h.fill[h.off:])
		n += c
		h.off = (h.off + c) % len(h.fill)
	}
	h.served.Add(int64(n))
	return n, nil
}

func (h *hostileConn) Write(p []byte) (int, error)      { return len(p), nil }
func (h *hostileConn) Close() error                     { return nil }
func (h *hostileConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (h *hostileConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (h *hostileConn) SetDeadline(time.Time) error      { return nil }
func (h *hostileConn) SetReadDeadline(time.Time) error  { return nil }
func (h *hostileConn) SetWriteDeadline(time.Time) error { return nil }

// hostileExchange wires ex to hc and sends a request, so the response-reading
// paths under test are reached exactly as they are in production.
func hostileExchange(t *testing.T, hc *hostileConn) *http1.Exchange {
	t.Helper()
	c := http1.NewConn(hc)
	t.Cleanup(func() { _ = c.Close() })
	ex := c.NewExchange()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	if err := ex.WriteRequest(context.Background(), fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	return ex
}

// assertBounded is the shared verdict for every vector: the client must have
// stopped itself, with the typed error, having consumed no more than maxServed
// bytes, and must have marked the connection un-poolable. Reaching errBudget
// means it never stopped — the bug.
func assertBounded(t *testing.T, ex *http1.Exchange, hc *hostileConn, err error, maxServed int64) {
	t.Helper()
	served := hc.served.Load()
	if errors.Is(err, errBudget) {
		t.Fatalf("UNBOUNDED: client consumed the entire %d B budget and was still reading; "+
			"a real server would not have stopped at all (served=%d B)", hc.budget, served)
	}
	if err == nil {
		t.Fatalf("hostile response accepted with no error (served=%d B)", served)
	}
	if !errors.Is(err, http1.ErrResponseTooLarge) {
		t.Fatalf("err = %v; want it to wrap ErrResponseTooLarge so a caller can classify it", err)
	}
	if served > maxServed {
		t.Fatalf("client consumed %d B before erroring; want <= %d B (err=%v)", served, maxServed, err)
	}
	// The stream is left mid-line and unresynchronisable; reusing it would
	// feed a later exchange the tail of this one.
	if ex.KeepAlive() {
		t.Fatalf("connection still reported as poolable after %v", err)
	}
	t.Logf("bounded: served=%d B, err=%v", served, err)
}

// maxLineServed is the byte budget a bounded client may consume while failing
// on a single over-long line: bufio's 16 KiB buffer plus generous slack for the
// prefix. A client that reads even 1 MiB for one line is unbounded in practice.
const maxLineServed = 1 << 20 // 1 MiB

// TestReadResponse_StatusLineNeverDelimited_Bounded covers vector 1: a server
// that opens a status line and never sends the '\n' that ends it. ReadString
// grows a fragment list until the delimiter arrives, so the client's memory
// tracks whatever the server chooses to send.
func TestReadResponse_StatusLineNeverDelimited_Bounded(t *testing.T) {
	hc := &hostileConn{
		prefix: []byte("HTTP/1.1 200 "),
		fill:   []byte("A"), // reason phrase that never ends
		budget: 32 << 20,    // 32 MiB: far past any legitimate status line
	}
	ex := hostileExchange(t, hc)

	_, _, err := ex.ReadResponse(context.Background())
	assertBounded(t, ex, hc, err, maxLineServed)
}

// TestReadResponse_HeaderLineNeverDelimited_Bounded covers vector 2: a valid
// status line, then a header line whose value never terminates.
func TestReadResponse_HeaderLineNeverDelimited_Bounded(t *testing.T) {
	hc := &hostileConn{
		prefix: []byte("HTTP/1.1 200 OK\r\nX-Pad: "),
		fill:   []byte("A"),
		budget: 32 << 20,
	}
	ex := hostileExchange(t, hc)

	_, _, err := ex.ReadResponse(context.Background())
	assertBounded(t, ex, hc, err, maxLineServed)
}

// TestReadResponse_EndlessHeaders_Bounded covers vector 3, which is distinct
// from the others: every line is short and well-formed, so no per-line cap can
// catch it. What grows without limit is the accumulated header slice, so this
// needs a cap on the header block as a whole.
//
// The budget is smaller here because the failing path retains a HeaderField per
// line: 4 MiB of wire becomes ~700k fields, which is enough to prove the point
// without putting the runner at risk.
func TestReadResponse_EndlessHeaders_Bounded(t *testing.T) {
	hc := &hostileConn{
		prefix: []byte("HTTP/1.1 200 OK\r\n"),
		fill:   []byte("a: b\r\n"), // each line perfectly legal; the count is the attack
		budget: 4 << 20,
	}
	ex := hostileExchange(t, hc)

	_, _, err := ex.ReadResponse(context.Background())
	// The cap is on accounted header bytes, so the wire bytes needed to reach
	// it are larger than the block itself; allow the whole budget minus a
	// margin, and let errBudget be the thing that fails the test.
	assertBounded(t, ex, hc, err, 4<<20)
}

// TestReadChunkedChunk_SizeLineNeverDelimited_Bounded covers vector 4: headers
// complete normally and announce a chunked body, then the chunk-size line never
// terminates. 'A' is a hex digit, so the line stays syntactically plausible as
// it grows.
func TestReadChunkedChunk_SizeLineNeverDelimited_Bounded(t *testing.T) {
	hc := &hostileConn{
		prefix: []byte("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"),
		fill:   []byte("A"),
		budget: 32 << 20,
	}
	ex := hostileExchange(t, hc)

	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	buf := make([]byte, 4096)
	_, _, err := ex.ReadBodyChunk(buf)
	assertBounded(t, ex, hc, err, maxLineServed)
}

// TestReadResponse_InterimFlood_Bounded covers the 1xx drain loop. Unlike the
// others this one does not grow memory — each interim response is parsed and
// discarded — so it is a livelock rather than a leak: ReadResponse simply never
// returns while the server keeps sending. HTTP/3 already caps this
// (maxInterimResponses); HTTP/1.1 does not.
func TestReadResponse_InterimFlood_Bounded(t *testing.T) {
	hc := &hostileConn{
		fill:   []byte("HTTP/1.1 100 Continue\r\n\r\n"),
		budget: 32 << 20,
	}
	ex := hostileExchange(t, hc)

	done := make(chan error, 1)
	go func() {
		_, _, err := ex.ReadResponse(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		assertBounded(t, ex, hc, err, maxLineServed)
	case <-time.After(30 * time.Second):
		t.Fatalf("ReadResponse did not return within 30s: the 1xx drain loop is unbounded")
	}
}
