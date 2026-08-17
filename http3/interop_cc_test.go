//go:build interop

package http3

import (
	"context"

	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		t.Skip("H3_CC_PATH is unset. Set it to a path the peer DRAINS — the harnesses " +
			"under test/integration/http3/ point it at /sink, which every peer there " +
			"now serves (#564).")
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
		_, _, warmErr := doUpload(t, client, host, body)
		require.NoError(t, warmErr, "warm-up upload")

		var best, total time.Duration
		n := ccRepeats()
		for i := 0; i < n; i++ {
			start := time.Now()
			resp, _, err := doUpload(t, client, host, body)
			elapsed := time.Since(start)

			require.NoErrorf(t, err, "upload %d", i)
			require.Containsf(t, []int{200, 204}, resp.Status, "upload %d: status = %d", i, resp.Status)
			// Proof, per request, that this peer actually consumed the payload.
			// Without it a peer that answers on the request headers reports a
			// spectacular goodput for a transfer that never happened — which is
			// what Caddy and nginx did for as long as this benchmark existed
			// (#564). Asserting the echoed count makes "did it drain?" a
			// measured fact rather than a property assumed of the config.
			got := sinkReceived(resp)
			require.Equalf(t, size, got,
				"upload %d: peer echoed X-Sink-Received=%d for a %d-byte body; "+
					"a peer that did not consume the payload cannot produce a meaningful "+
					"time, so this measurement is void", i, got, size)
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

// sinkReceived reads the byte count the peer says it consumed, or -1 when the
// header is absent — which is itself the signal that the peer answered without
// draining.
func sinkReceived(resp *Response) int {
	for _, f := range resp.Headers {
		if strings.EqualFold(string(f.Name), "x-sink-received") {
			n, err := strconv.Atoi(string(f.Value))
			if err != nil {
				return -1
			}
			return n
		}
	}
	return -1
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
// There is no default, because whether a peer drains is a property of the peer
// and cannot be guessed. Measured on the old configuration (path "/"), at 1 MiB
// and 8 MiB:
//
//	h3caddy    0.5 ms -> 0.6 ms   flat: answers on the headers
//	h3nginx    0.4 ms -> 0.3 ms   flat: answers on the headers
//	h3aioquic  112.6 ms -> 411.5 ms   scales: it waits for stream_ended
//
// So two of the three were reporting a header round trip as goodput, and the
// third was producing real numbers all along. All three now serve /sink, and
// the assertion on X-Sink-Received above proves per request which case applies
// rather than trusting this comment to stay true.
func uploadPath() string { return os.Getenv("H3_CC_PATH") }
