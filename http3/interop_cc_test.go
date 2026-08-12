//go:build interop

package http3

import (
	"context"

	"os"
	"strconv"
	"testing"
	"time"
)

// Congestion-control A/B measurement (#362).
//
// BBR shipped opt-in with no number attached to it — the only performance
// mechanism in the repository without one. This is the arm that produces the
// number; H3_INTEROP_CC selects the controller, and the harness runs the same
// transfer twice.
//
// IT MEASURES AN UPLOAD, DELIBERATELY. Congestion control governs the SENDER.
// A GET would measure the server's controller (Caddy's, i.e. quic-go's) and tell
// us nothing about quic/bbr.go, no matter how carefully the network faults were
// set up. Only when poseidon is the one sending does its own cwnd and pacing
// rate decide the completion time.
//
// The interesting cell is loss combined with RTT: NewReno halves its window on
// every loss and reopens one MSS per RTT, so its recovery is paced by the round
// trip, while BBR is meant to hold a rate derived from its bandwidth and
// min-RTT estimates. On a 1 ms loopback the two are indistinguishable — which is
// why lossproxy grew DELAY_MS.

// ccTransferBytes is the upload size, overridable so a slow cell (5% loss,
// 200 ms RTT) can be shortened without editing the test.
func ccTransferBytes() int {
	if v := os.Getenv("H3_CC_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 4 << 20
}

// ccRepeats is how many times the transfer runs; the report gives every sample,
// not just the mean, because one sample cannot show variance and the loss arms
// are genuinely noisy.
func ccRepeats() int {
	if v := os.Getenv("H3_CC_REPEATS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 3
}

// TestInterop_CCGoodput uploads a fixed body and reports completion time and
// goodput. Run it once per controller and compare:
//
//	H3_INTEROP_CC=newreno go test ./http3/ -tags interop -run TestInterop_CCGoodput -v
//	H3_INTEROP_CC=bbr     go test ./http3/ -tags interop -run TestInterop_CCGoodput -v
//
// The result is printed in a single grep-able line per sample so a matrix script
// can collect it without parsing Go test output structure.
func TestInterop_CCGoodput(t *testing.T) {
	cc := os.Getenv("H3_INTEROP_CC")
	if cc == "" {
		cc = "newreno(default)"
	}
	if uploadPath() == "" {
		t.Skip("H3_CC_PATH is unset: this benchmark needs a peer that consumes the " +
			"request body before responding, and the interop peers do not. Run it " +
			"through test/integration/http3/docker-compose.cc.yml, which provides the " +
			"sink and sets the path (#564).")
	}
	size := ccTransferBytes()
	body := make([]byte, size)
	for i := range body { // not all-zero: a compressing middlebox would flatter us
		body[i] = byte(i*31 + 7)
	}

	forEachInteropServer(t, func(t *testing.T, client *Client, host string) {
		// One warm-up transfer, not measured: the first upload on a fresh
		// connection pays the handshake and starts from the initial window, so
		// including it would measure slow-start ramp rather than steady state.
		if _, _, err := doUpload(t, client, host, body); err != nil {
			t.Fatalf("warm-up upload: %v", err)
		}

		var best, total time.Duration
		n := ccRepeats()
		for i := 0; i < n; i++ {
			start := time.Now()
			resp, _, err := doUpload(t, client, host, body)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("upload %d: %v", i, err)
			}
			if resp.Status != 200 && resp.Status != 204 {
				t.Fatalf("upload %d: status = %d", i, resp.Status)
			}
			total += elapsed
			if best == 0 || elapsed < best {
				best = elapsed
			}
			t.Logf("CCRESULT cc=%s server=%s sample=%d bytes=%d ms=%.1f mibps=%.2f",
				cc, host, i, size, float64(elapsed.Microseconds())/1000, mibps(size, elapsed))
		}
		mean := total / time.Duration(n)
		t.Logf("CCSUMMARY cc=%s server=%s bytes=%d samples=%d mean_ms=%.1f best_ms=%.1f mean_mibps=%.2f best_mibps=%.2f",
			cc, host, size, n,
			float64(mean.Microseconds())/1000, float64(best.Microseconds())/1000,
			mibps(size, mean), mibps(size, best))
	})
}

func mibps(n int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return (float64(n) / (1 << 20)) / d.Seconds()
}

// doUpload POSTs body with a timeout wide enough for the slowest matrix cell.
func doUpload(t *testing.T, client *Client, host string, body []byte) (*Response, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return client.Do(ctx, &Request{
		Method: "POST", Scheme: "https", Authority: host, Path: uploadPath(), Body: body,
	})
}

// uploadPath is the target the measured upload is sent to. It MUST be an
// endpoint that consumes the request body before responding, or the timer
// closes on a response that arrived while the payload was still buffered and
// what gets measured is a header round trip (#564).
//
// There is no default. This benchmark is only meaningful against a draining
// peer, and the interop matrix's peers do not drain — Caddy's "/" is a canned
// `respond`, and nginx and aioquic have no such route at all. Running it there
// is how it passed vacuously against three servers at once for as long as it
// existed. The CC harness sets H3_CC_PATH; everywhere else the test skips.
func uploadPath() string { return os.Getenv("H3_CC_PATH") }
