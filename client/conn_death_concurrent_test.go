package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// chunkLen is the number of body bytes each handler writes and flushes before
// parking. Every caller reads exactly this many bytes, which is what makes the
// body provably *truncated* when the connection dies: the handler never sent
// END_STREAM, so a reader that reports io.EOF here is reporting a completion
// that never happened.
const chunkLen = 512

// isTerminalConnDeath reports whether err is one of the terminals conn may
// deliver to a caller when the connection dies underneath an open stream.
//
// The set has three members, not one, and that is not laziness. Stream.Recv
// (conn/stream.go:349-361) selects between a buffered event and a closed
// resetSignal, and shutdownStreams (conn/conn.go:1465-1479) may deliver the
// terminal through either channel depending on whether the stream's event
// buffer had room. Which one a given caller observes is a race between two
// ready select arms, so pinning a single code here would buy a nightly flake
// and nothing else. What is deterministic — and what these tests assert — is
// the *class*: terminal, non-nil, and never io.EOF.
func isTerminalConnDeath(err error) bool {
	if errors.Is(err, conn.ErrStreamClosed) || errors.Is(err, conn.ErrConnClosed) {
		return true
	}
	var sre *client.StreamResetError
	if errors.As(err, &sre) {
		switch sre.Code {
		case conn.ErrCodeRefusedStream, frame.ErrCodeInternalError:
			return true
		}
	}
	return false
}

// TestIntegration_ConnDeathMidBody_NoConcurrentCallerSeesCleanShortBody crosses
// the two axes this suite otherwise only tests apart: connection death, and
// concurrency. Every existing death test drives one stream; every existing
// concurrency test drives a healthy connection. The code that only runs at the
// crossing is conn.shutdownStreams, which must fan a terminal out to *every*
// registered stream — a loop nothing here had ever forced past its first
// iteration.
//
// Eight callers are parked mid-body on one connection when it dies. The failure
// that matters is not an ugly error; it is a *clean* one. Each caller holds a
// populated Response (Status 200, headers parsed) and 512 bytes of a body that
// was never finished. If the reader answers io.EOF, every one of those callers
// walks away believing it received a complete 512-byte response, and a caller
// that checks Status before err — the common shape — cannot tell the difference.
// Silent truncation is the bug; an error is the correct outcome.
//
// No Retryer wraps this client, and that is deliberate. Retry in this library
// is opt-in through an explicit client.NewRetryer wrapper rather than a
// ClientOptions field, so a plain NewClient never retries — but the point still
// needs stating, because builtinShouldRetry (client/retry.go:46) treats
// INTERNAL_ERROR as retryable. A Retryer here would quietly re-drive each
// request against a fresh connection and the test would pass without ever
// observing the terminal it exists to check.
func TestIntegration_ConnDeathMidBody_NoConcurrentCallerSeesCleanShortBody(t *testing.T) {
	const n = 8

	quit := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = w.Write(make([]byte, chunkLen))
		w.(http.Flusher).Flush()
		// Park. The response must still be open when the connection dies —
		// END_STREAM must never reach the client, or a clean read is honest
		// and this test is measuring nothing.
		select {
		case <-quit:
		case <-r.Context().Done():
		}
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	// LIFO: close(quit) releases the parked handlers first, so srv.Close —
	// which blocks on outstanding requests — has none left to wait for.
	defer srv.Close()
	defer close(quit)

	c, err := client.NewClient(client.ClientOptions{
		Addr: srv.Listener.Addr().String(),
		// TransportSingleConn is the zero value and the point: all n streams
		// share ONE *conn.Conn, so one death is every caller's death. A pool
		// would let each caller find its own connection and the fan-out would
		// never be exercised.
		Transport: client.TransportSingleConn,
		ConnOpts: conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // a test server's self-signed cert
			NextProtos:         []string{"h2"},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// Generous, and never meant to fire: the bound this test enforces is the
	// waitgroup deadline below, which is far tighter. If a caller were rescued
	// by this deadline instead of by shutdownStreams, ctx.DeadlineExceeded
	// would show up in the outcome and be rejected — a hang must not pass as
	// "terminated with an error".
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	type outcome struct {
		status   int
		read     int
		err      error
		setupErr error
	}
	outcomes := make([]outcome, n)

	midBody := make(chan struct{}, n) // client-side proof: this stream is mid-body
	kill := make(chan struct{})       // released once the connection is dead

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()

			req := client.GET("/")
			req.BodyMode = client.BodyStream // incremental reads; Do returns on HEADERS
			var resp client.Response
			defer resp.Reset()

			if err := c.Do(ctx, req, &resp); err != nil {
				outcomes[i].setupErr = err
				midBody <- struct{}{}
				return
			}
			outcomes[i].status = resp.Status

			// Read the flushed chunk. Returning from ReadFull is the
			// client-side proof that this stream really is mid-body: headers
			// parsed, 512 body bytes delivered, no END_STREAM. Without it the
			// kill below could land before the data ever arrived, and the test
			// would be asserting on streams that had nothing to truncate.
			buf := make([]byte, chunkLen)
			nr, rerr := io.ReadFull(resp.BodyReader, buf)
			outcomes[i].read = nr
			if rerr != nil {
				outcomes[i].setupErr = rerr
				midBody <- struct{}{}
				return
			}

			midBody <- struct{}{}
			<-kill

			_, outcomes[i].err = resp.BodyReader.Read(buf)
		}()
	}

	// Every one of the n streams is now provably mid-body, client-side.
	for range n {
		select {
		case <-midBody:
		case <-time.After(20 * time.Second):
			t.Fatal("not every caller reached mid-body; the crossing was never set up")
		}
	}

	// The connection dies. CloseClientConnections closes the server's sockets
	// outright rather than draining, which is what a crash or a killed pod
	// looks like from here — no GOAWAY, no RST, just a dead reader.
	srv.CloseClientConnections()
	close(kill)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("callers still blocked 15s after the connection died: shutdownStreams did not reach every stream")
	}

	for i, o := range outcomes {
		if o.setupErr != nil {
			t.Fatalf("caller %d never reached the crossing: %v (read %d bytes)", i, o.setupErr, o.read)
		}
		// The control for "the server actually spoke": a caller that never got
		// a real response has no truncated body to be fooled about.
		if o.status != http.StatusOK || o.read != chunkLen {
			t.Fatalf("caller %d: status=%d read=%d, want 200 and %d bytes before the kill", i, o.status, o.read, chunkLen)
		}
		if o.err == nil {
			t.Errorf("caller %d: Read returned nil error after the connection died; it holds a 200 and a truncated body and cannot tell", i)
			continue
		}
		if errors.Is(o.err, io.EOF) || errors.Is(o.err, io.ErrUnexpectedEOF) {
			t.Errorf("caller %d: Read returned %v after the connection died — a %d-byte body the server never finished now reads as complete", i, o.err, chunkLen)
			continue
		}
		if errors.Is(o.err, context.DeadlineExceeded) || errors.Is(o.err, context.Canceled) {
			t.Errorf("caller %d: terminated on its own context (%v), not on the connection's death; conn never woke it", i, o.err)
			continue
		}
		if !isTerminalConnDeath(o.err) {
			t.Errorf("caller %d: Read returned %v, outside the terminal set conn delivers on connection death", i, o.err)
		}
	}
}
