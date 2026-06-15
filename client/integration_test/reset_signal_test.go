//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestIT_ResetSignal_OverflowNoHang verifies that a stream with a tiny
// event buffer (size 1) does NOT silently hang when the server sends a
// large body without inter-chunk delays. Before the resetSignal fix,
// this test would block until context deadline (15s+).
//
// The stream will get RST(REFUSED_STREAM) via signalReset, and Recv()
// returns EventReset immediately instead of hanging.
func TestIT_ResetSignal_OverflowNoHang(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)

	// Create a client with StreamEventBuffer=1 to force overflow.
	c, err := client.NewClient(client.ClientOptions{
		Addr:          srv.H2CAddr,
		DefaultScheme: "h2c",
		ConnOpts: conn.ConnOptions{
			Dialer:            &conn.PlaintextDialer{},
			StreamEventBuffer: 1, // deliberately tiny
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	start := time.Now()
	err = c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/large?bytes=262144", // 256 KB — overflows buffer=1
		WantBody: true,
	}, &resp)
	elapsed := time.Since(start)

	if err == nil {
		// Could succeed if the body was small enough or consumer kept up.
		// That's fine — the important thing is no hang.
		t.Logf("Do succeeded (status=%d, body=%d bytes) in %v", resp.Status, len(resp.Body), elapsed)
		return
	}
	// On overflow, we expect either a StreamResetError or context error.
	// The KEY assertion: it must not hang (elapsed must be well under ctx timeout).
	if elapsed > 8*time.Second {
		t.Fatalf("Do took %v — likely hung (resetSignal not working)", elapsed)
	}
	t.Logf("Do returned error in %v (expected): %v", elapsed, err)
}
