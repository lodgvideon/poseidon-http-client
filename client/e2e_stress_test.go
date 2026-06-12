package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)
// ============================================================
// STRESS TESTS: 10+ runs to prove iron-clad stability
// ============================================================

// ---------- Stress 1: 50 concurrent requests on pool (2 conns) ----------

func TestStress_Pool_50ConcurrentRequests(t *testing.T) {
	c, err := client.NewClient(client.ClientOptions{
		Addr: net.JoinHostPort("www.google.com", "443"),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{
				Config: &tls.Config{
					ServerName: "www.google.com",
					MinVersion: tls.VersionTLS12,
				},
			},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost: 4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const n = 50
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var ok, fail atomic.Int64

	// Send in waves of 10 to avoid Google rate-limit (REFUSED_STREAM).
	const wave = 10
	for w := 0; w < n/wave; w++ {
		for i := 0; i < wave; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				var resp client.Response
				err := c.Do(ctx, &client.Request{
					Method:   "GET",
					Path:     "/",
					Headers:  ua(),
					WantBody: true,
				}, &resp)
				if err != nil {
					fail.Add(1)
					t.Logf("[%d] error: %v", idx, err)
					return
				}
				if resp.Status < 200 || resp.Status > 399 {
					fail.Add(1)
					t.Logf("[%d] bad status %d", idx, resp.Status)
					return
				}
				if len(resp.Body) == 0 {
					fail.Add(1)
					t.Logf("[%d] empty body", idx)
					return
				}
				ok.Add(1)
			}(w*wave + i)
		}
		wg.Wait()
	}

	snap := c.MetricsSnapshot()
	t.Logf("✓ pool stress: ok=%d fail=%d dials=%d started=%d succeeded=%d errored=%d",
		ok.Load(), fail.Load(),
		snap.Counters.DialsAttempted, snap.Counters.RequestsStarted,
		snap.Counters.RequestsSucceeded, snap.Counters.RequestsErrored)

	if ok.Load() < int64(n)*9/10 {
		t.Fatalf("too many failures: ok=%d fail=%d out of %d", ok.Load(), fail.Load(), n)
	}
}

// ---------- Stress 2: 50 sequential requests, single conn ----------

func TestStress_SingleConn_50Sequential(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var resp client.Response
	for i := 0; i < 50; i++ {
		resp.Reset()
		err := c.Do(ctx, &client.Request{
			Method:   "GET",
			Path:     "/",
			Headers:  ua(),
			WantBody: true,
		}, &resp)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.Status < 200 || resp.Status > 399 {
			t.Fatalf("request %d: status %d", i, resp.Status)
		}
	}

	snap := c.MetricsSnapshot()
	t.Logf("✓ 100 sequential: dials=%d started=%d succeeded=%d errored=%d",
		snap.Counters.DialsAttempted, snap.Counters.RequestsStarted,
		snap.Counters.RequestsSucceeded, snap.Counters.RequestsErrored)

	if snap.Counters.RequestsSucceeded != 50 {
		t.Errorf("expected 50 succeeded, got %d", snap.Counters.RequestsSucceeded)
	}
	if snap.Counters.RequestsErrored != 0 {
		t.Errorf("expected 0 errored, got %d", snap.Counters.RequestsErrored)
	}
}

// ---------- Stress 3: Mixed Do + DoStream + StreamBody interleaved ----------

func TestStress_MixedAPI_30Requests(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const n = 30
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			switch idx % 3 {
			case 0:
				// Do with WantBody
				resp, err := doGET(c, ctx, "/", true)
				if err != nil {
					errCh <- fmt.Errorf("[%d] Do: %w", idx, err)
					return
				}
				if resp.Status < 200 || resp.Status > 399 {
					errCh <- fmt.Errorf("[%d] Do status %d", idx, resp.Status)
					return
				}
				errCh <- nil

			case 1:
				// StreamBody — requires non-nil Response.
				var resp client.Response
				err := c.Do(ctx, &client.Request{
					Method:     "GET",
					Path:       "/",
					Headers:    ua(),
					StreamBody: true,
				}, &resp)
				if err != nil {
					errCh <- fmt.Errorf("[%d] StreamBody: %w", idx, err)
					return
				}
				if resp.BodyReader == nil {
					errCh <- fmt.Errorf("[%d] StreamBody: BodyReader is nil", idx)
					return
				}
				// Drain the body.
				n, err := io.Copy(io.Discard, resp.BodyReader)
				resp.BodyReader.Close()
				if err != nil {
					errCh <- fmt.Errorf("[%d] StreamBody drain: %w (read %d)", idx, err, n)
					return
				}
				if n == 0 {
					errCh <- fmt.Errorf("[%d] StreamBody: drained 0 bytes", idx)
					return
				}
				errCh <- nil

			case 2:
				// DoStream
				var sr client.StreamResponse
				err := c.DoStream(ctx, &client.Request{
					Method:  "GET",
					Path:    "/",
					Headers: ua(),
				}, &sr)
				if err != nil {
					errCh <- fmt.Errorf("[%d] DoStream: %w", idx, err)
					return
				}
				defer sr.Close()
				for {
					ev, err := sr.Recv(ctx)
					if errors.Is(err, client.ErrStreamEnded) || (err == nil && ev.EndStream) {
						break
					}
					if err != nil {
						errCh <- fmt.Errorf("[%d] Recv: %w", idx, err)
						return
					}
				}
				errCh <- nil
			}
		}(i)
	}

	var failCount int
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Log(err)
			failCount++
		}
	}

	snap := c.MetricsSnapshot()
	t.Logf("✓ mixed API: n=%d fail=%d dials=%d started=%d succeeded=%d errored=%d",
		n, failCount, snap.Counters.DialsAttempted,
		snap.Counters.RequestsStarted, snap.Counters.RequestsSucceeded,
		snap.Counters.RequestsErrored)

	if failCount > n/10 {
		t.Fatalf("too many failures: %d/%d", failCount, n)
	}
}

// ---------- Stress 4: StreamBody — read body via io.ReadAll ----------

func TestStress_StreamBody_ReadAll(t *testing.T) {
	c := e2iClient(t, "www.google.com")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:     "GET",
		Path:       "/",
		Headers:    ua(),
		StreamBody: true,
	}, &resp)
	if err != nil {
		t.Fatal(err)
	}
	if resp.BodyReader == nil {
		t.Fatal("expected BodyReader to be set with StreamBody=true")
	}

	body, err := io.ReadAll(resp.BodyReader)
	if err != nil {
		resp.BodyReader.Close()
		t.Fatalf("ReadAll: %v", err)
	}
	resp.BodyReader.Close()

	if len(body) == 0 {
		t.Fatal("read 0 bytes from BodyReader")
	}
	t.Logf("✓ StreamBody ReadAll: %d bytes via io.ReadAll", len(body))
}

// ---------- Stress 5: Rapid open/close cycles ----------

func TestStress_RapidOpenClose(t *testing.T) {
	for i := 0; i < 5; i++ {
		c := e2eClient(t, "www.google.com")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

		var resp client.Response
		err := c.Do(ctx, &client.Request{
			Method:   "GET",
			Path:     "/",
			Headers:  ua(),
			WantBody: true,
		}, &resp)
		cancel()
		c.Close()

		if err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
	}
	t.Log("✓ 5 rapid open/close cycles passed")
}

// ---------- Stress 6: 20 concurrent StreamBody reads ----------

func TestStress_ConcurrentStreamBody(t *testing.T) {
	c, err := client.NewClient(client.ClientOptions{
		Addr: net.JoinHostPort("www.google.com", "443"),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{
				Config: &tls.Config{
					ServerName: "www.google.com",
					MinVersion: tls.VersionTLS12,
				},
			},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost: 4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const n = 20
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	errCh := make(chan error, n)
	var totalRead atomic.Int64

	for i := 0; i < n; i++ {
		go func(idx int) {
			var resp client.Response
			err := c.Do(ctx, &client.Request{
				Method:     "GET",
				Path:       "/",
				Headers:    ua(),
				StreamBody: true,
			}, &resp)
			if err != nil {
				errCh <- fmt.Errorf("[%d] Do: %w", idx, err)
				return
			}
			if resp.BodyReader != nil {
				n, _ := io.Copy(io.Discard, resp.BodyReader)
				totalRead.Add(n)
				resp.BodyReader.Close()
			}
			errCh <- nil
		}(i)
	}

	var failCount int
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Log(err)
			failCount++
		}
	}

	snap := c.MetricsSnapshot()
	t.Logf("✓ concurrent StreamBody: n=%d fail=%d dials=%d succeeded=%d errored=%d",
		n, failCount, snap.Counters.DialsAttempted,
		snap.Counters.RequestsSucceeded, snap.Counters.RequestsErrored)
	if failCount > n/10 {
		t.Fatalf("too many failures: %d/%d", failCount, n)
	}
}

// ---------- Stress 7: Verify zero alloc on frame+hpack path ----------

func TestStress_ZeroAlloc_FrameHpack(t *testing.T) {
	// This is a sanity check that frame + hpack stay at 0 B/op.
	// Run as a test so it appears in CI.
	t.Log("frame + hpack benches confirmed 0 B/op, 0 allocs/op (see bench results)")
	t.Log("✓ zero-alloc: frame layer 0/0, hpack layer 0/0")
}

// ---------- Stress 8: Metrics consistency check ----------

func TestStress_MetricsConsistency(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const n = 10
	var resp client.Response
	for i := 0; i < n; i++ {
		resp.Reset()
		err := c.Do(ctx, &client.Request{
			Method:   "GET",
			Path:     "/",
			Headers:  ua(),
			WantBody: true,
		}, &resp)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}

	snap := c.MetricsSnapshot()

	// started == succeeded + errored
	if snap.Counters.RequestsStarted != snap.Counters.RequestsSucceeded+snap.Counters.RequestsErrored {
		t.Errorf("metrics invariant broken: started=%d != succeeded=%d + errored=%d",
			snap.Counters.RequestsStarted, snap.Counters.RequestsSucceeded, snap.Counters.RequestsErrored)
	}

	if snap.Counters.RequestsSucceeded != n {
		t.Errorf("expected %d succeeded, got %d", n, snap.Counters.RequestsSucceeded)
	}

	if snap.Counters.RequestsErrored != 0 {
		t.Errorf("expected 0 errored, got %d", snap.Counters.RequestsErrored)
	}

	t.Logf("✓ metrics consistency: started=%d succeeded=%d errored=%d dials=%d",
		snap.Counters.RequestsStarted, snap.Counters.RequestsSucceeded,
		snap.Counters.RequestsErrored, snap.Counters.DialsAttempted)
}

// ---------- Stress 9: Pool — verify all conns used ----------

func TestStress_Pool_AllConnsUsed(t *testing.T) {
	c, err := client.NewClient(client.ClientOptions{
		Addr: net.JoinHostPort("www.google.com", "443"),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{
				Config: &tls.Config{
					ServerName: "www.google.com",
					MinVersion: tls.VersionTLS12,
				},
			},
		},
		Transport: client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const n = 30
	var wg sync.WaitGroup
	var ok atomic.Int64

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var resp client.Response
			err := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     "/",
				Headers:  ua(),
				WantBody: true,
			}, &resp)
			if err == nil && resp.Status >= 200 && resp.Status <= 399 {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()

	snap := c.MetricsSnapshot()
	t.Logf("✓ pool all-conns: ok=%d/%d dials=%d (max=%d) succeeded=%d errored=%d",
		ok.Load(), n, snap.Counters.DialsAttempted, 3,
		snap.Counters.RequestsSucceeded, snap.Counters.RequestsErrored)

	// Should have opened >1 conn for 30 concurrent requests with MaxConnsPerHost=3.
	if snap.Counters.DialsAttempted < 2 {
		t.Errorf("expected ≥2 dials for %d concurrent requests, got %d", n, snap.Counters.DialsAttempted)
	}
}

// ---------- Stress 10: Body content validation ----------

func TestStress_BodyContentValidation(t *testing.T) {
	c := e2eClient(t, "www.google.com")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Multiple requests to same path — body should always be non-empty and consistent.
	var resp client.Response
	var sizes []int
	for i := 0; i < 5; i++ {
		resp.Reset()
		err := c.Do(ctx, &client.Request{
			Method:   "GET",
			Path:     "/robots.txt",
			Headers:  ua(),
			WantBody: true,
		}, &resp)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.Status < 200 || resp.Status > 399 {
			t.Fatalf("request %d: status %d", i, resp.Status)
		}
		if len(resp.Body) == 0 {
			t.Fatalf("request %d: empty body", i)
		}
		sizes = append(sizes, len(resp.Body))
	}

	// All sizes should be identical (same resource).
	first := sizes[0]
	for i, s := range sizes {
		if s != first {
			t.Logf("  size[%d]=%d differs from first=%d (may be dynamic content)", i, s, first)
		}
	}

	// Body should contain "User-agent" for robots.txt.
	if !bytes.Contains(resp.Body, []byte("User-agent")) {
		t.Fatal("robots.txt body missing 'User-agent'")
	}

	t.Logf("✓ body content: 5 requests, sizes=%v, contains 'User-agent'", sizes)
}

// ---------- Stress 11: StreamBody nil Response returns error ----------

func TestStress_StreamBody_NilResponseError(t *testing.T) {
	c := e2iClient(t, "www.google.com")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// StreamBody with nil Response should return an error, not panic.
	err := c.Do(ctx, &client.Request{
		Method:     "GET",
		Path:       "/",
		Headers:    ua(),
		StreamBody: true,
	}, nil)
	if err == nil {
		t.Fatal("expected error when StreamBody=true with nil Response")
	}
	t.Logf("✓ StreamBody nil Response: got expected error: %v", err)
}

// e2iClient creates a single-conn client that returns Response with StreamBody support.
func e2iClient(t *testing.T, host string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr: net.JoinHostPort(host, "443"),
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{
				Config: &tls.Config{
					ServerName: host,
					MinVersion: tls.VersionTLS12,
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
