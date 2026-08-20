//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/require"
)

// TestIT_ResetSignal_OverflowNoHang verifies that a stream with a tiny
// event buffer (size 1) does NOT silently hang when the server sends a
// large body without inter-chunk delays. Before the resetSignal fix,
// this test would block until context deadline (15s+).
//
// The stream will get RST(CANCEL) via signalReset, and Recv()
// returns EventReset immediately instead of hanging.
//
// DefaultScheme is "http", not "h2c", and that is the whole difference between
// this test running its scenario and not running it at all. ":scheme" travels to
// the peer verbatim (RFC 9113 §8.3.1), "h2c" is not a scheme any origin server
// recognises, and net/http2 answers it with RST_STREAM(PROTOCOL_ERROR) off the
// HEADERS frame — before a single DATA frame exists to overflow anything. The
// request then failed in ~0.5ms and every assertion below held for a reason that
// had nothing to do with the event buffer: deleting signalReset, deleting the
// whole overflow fallback in Stream.pushLocked, deleting both left this test
// green. With "http" the same deletions hang it to the context deadline, which is
// the failure the paragraph above describes.
func TestIT_ResetSignal_OverflowNoHang(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	// A client with StreamEventBuffer=1, to force overflow.
	c, err := client.NewClient(client.ClientOptions{
		Addr:          srv.H2CAddr,
		DefaultScheme: "http",
		ConnOpts: conn.ConnOptions{
			Dialer:            &conn.PlaintextDialer{},
			StreamEventBuffer: 1, // deliberately tiny
		},
	})
	require.NoErrorf(t, err, "NewClient: %v", err)
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	start := time.Now()
	err = c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/large?bytes=262144", // 256 KB — overflows buffer=1
		BodyMode: client.BodyBuffer,
	}, &resp)
	elapsed := time.Since(start)

	// The KEY assertion, and it is asserted on BOTH outcomes: Do may legitimately
	// succeed when the consumer keeps up, but a success that spent the whole
	// budget is the same hang under another name.
	require.Lessf(t, elapsed, 8*time.Second,
		"Do took %v — likely hung (resetSignal not working)", elapsed)
	if err == nil {
		// Could succeed if the body was small enough or the consumer kept up.
		// That's fine — the important thing is no hang.
		t.Logf("Do succeeded (status=%d, body=%d bytes) in %v", resp.Status, len(resp.Body), elapsed)
		return
	}
	t.Logf("Do returned error in %v (expected): %v", elapsed, err)
}
