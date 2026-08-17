package conn

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// writeCountingConn counts Write calls on the underlying socket. Over a real
// TLS/TCP transport each Write is (at least) one write(2) syscall, so the count
// is a direct, cross-platform proxy for write-syscall volume — no strace
// needed. Mirror of readbuffer_test.go's countingConn, but on the send path.
type writeCountingConn struct {
	net.Conn
	writes atomic.Int64
}

// Write records the call, then delegates to the wrapped connection.
func (c *writeCountingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

// countingWriteDialer wraps another Dialer and keeps a reference to the
// established transport (a writeCountingConn) so the test can read the
// write-syscall count after the connection is up.
type countingWriteDialer struct {
	inner Dialer
	conn  *writeCountingConn
}

// Dial delegates to the inner Dialer and wraps the result in a
// writeCountingConn, retaining it for later inspection.
func (d *countingWriteDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	c, err := d.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	d.conn = &writeCountingConn{Conn: c}
	return d.conn, nil
}

// TestWriteBuffer_MultiChunkUpload_NoDeadlock is the safety net for the
// flush-before-acquireSendCredits invariant introduced with the conn-level
// buffered writer. It uploads a body far larger than any plausible peer
// initial send window, so writeData MUST block in acquireSendCredits mid-body
// waiting on the peer's WINDOW_UPDATE. If a DATA/HEADERS frame were left
// sitting in the bufio buffer at that point (a missed flush), the peer would
// never see it, never consume it, never grant more credit — and the upload
// would hang forever. The test asserts the full body is received and the call
// returns within a bounded 15s watchdog; a regression that drops the flush
// deadlocks here instead of passing.
func TestWriteBuffer_MultiChunkUpload_NoDeadlock(t *testing.T) {
	const bodySize = 4 * 1024 * 1024 // 4 MiB — many WINDOW_UPDATE round-trips.
	body := make([]byte, bodySize)
	_, randErr := rand.Read(body)
	require.NoError(t, randErr, "rand")

	var got int64
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		atomic.StoreInt64(&got, n)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := dialServer(t, srv, cfg)
	defer c.Close()

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		s, err := c.NewStream(ctx)
		if err != nil {
			done <- err
			return
		}
		if err := s.SendHeaders(ctx, []header.Field{
			{Name: []byte(":method"), Value: []byte("POST")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":authority"), Value: []byte("example.com")},
			{Name: []byte(":path"), Value: []byte("/upload")},
		}, false); err != nil {
			done <- err
			return
		}
		if err := s.SendData(ctx, body, true); err != nil {
			done <- err
			return
		}
		for {
			ev, rerr := s.Recv(ctx)
			if rerr != nil {
				done <- rerr
				return
			}
			if ev.EndStream {
				break
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		require.NoError(t, err, "multi-chunk upload failed")
	case <-time.After(15 * time.Second):
		require.FailNow(t, "multi-chunk upload deadlocked: did not complete within 15s "+
			"(missed flush before acquireSendCredits?)")
	}
	assert.EqualValuesf(t, bodySize, atomic.LoadInt64(&got),
		"server received %d bytes, want %d", atomic.LoadInt64(&got), bodySize)
}

// TestWriteBuffer_FewerWriteSyscalls proves the send-path optimization: the
// conn-level bufio.Writer coalesces each frame's 9-byte header and its payload
// into a single flush (one write syscall) instead of the Framer's two separate
// transport writes. It wraps the transport in a writeCountingConn and does a
// POST whose body is sent as several sub-max-frame DATA chunks, then compares
// the write-syscall count against the frame count.
//
// Intuition: for a request of HEADERS + N DATA frames the unbuffered Framer
// issues ~1 + 2N writes (header + payload per frame); the buffered writer
// issues ~1 + N (one flush per frame). This test asserts the measured count is
// strictly below the 2-per-frame unbuffered baseline and is ~1 per frame.
//
// Chunks are 8 KiB (< the 16 KiB write buffer) so header+payload always fit in
// one flush, and the total (56 KiB) stays under the 65535-byte initial send
// window so no chunk blocks or splits — the frame count is deterministic.
func TestWriteBuffer_FewerWriteSyscalls(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	dialer := &countingWriteDialer{inner: &TLSDialer{Config: cfg}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{Dialer: dialer})
	require.NoError(t, err, "Dial")
	defer c.Close()

	const (
		chunk  = 8 * 1024
		nchunk = 7 // 7 * 8 KiB = 56 KiB < 65535 send window
	)
	payload := make([]byte, chunk)

	s, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")

	// Snapshot after handshake + NewStream (which writes nothing — the id is
	// deferred to SendHeaders), just before the request write path.
	writesBefore := dialer.conn.writes.Load()
	framesBefore := c.Stats().FramesSent

	require.NoError(t, s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/upload")},
	}, false), "SendHeaders")
	for i := 0; i < nchunk; i++ {
		last := i == nchunk-1
		require.NoErrorf(t, s.SendData(ctx, payload, last), "SendData %d", i)
	}
	for {
		ev, rerr := s.Recv(ctx)
		require.NoError(t, rerr, "Recv")
		if ev.EndStream {
			break
		}
	}

	writesDelta := dialer.conn.writes.Load() - writesBefore
	framesDelta := c.Stats().FramesSent - framesBefore
	t.Logf("request write syscalls=%d, frames sent=%d (HEADERS 1 + %d DATA + control)",
		writesDelta, framesDelta, nchunk)
	t.Logf("unbuffered baseline ~2 writes/frame = %d; buffered ~1/frame = %d",
		2*framesDelta, framesDelta)

	require.GreaterOrEqualf(t, int64(framesDelta), int64(nchunk),
		"frames sent = %d, want >= %d", framesDelta, nchunk)
	// Below the 2-per-frame unbuffered baseline: coalescing is working.
	assert.Lessf(t, writesDelta, int64(2*framesDelta),
		"write coalescing ineffective: %d writes for %d frames (want < 2/frame)",
		writesDelta, framesDelta)
	// And it is ~1 write/frame (small slack for any concurrent control frame
	// the reader goroutine flushes between the two snapshots).
	assert.LessOrEqualf(t, writesDelta, int64(framesDelta+4),
		"expected ~1 write/frame, got %d writes for %d frames", writesDelta, framesDelta)
}
