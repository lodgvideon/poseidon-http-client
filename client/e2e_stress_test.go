//go:build e2e_remote

// e2e_stress_test.go hits a real public HTTP/2 endpoint
// (www.google.com:443) to exercise the client end-to-end. These tests
// are inherently flaky because the remote server can RST_STREAM,
// GOAWAY, rate-limit, or re-route at any time — none of which
// indicate a client bug. The reproduction of one such flake is in
// repro_rst7_test.go (always-on).
//
// Run with:  go test -tags=e2e_remote ./client/...
package client_test

import (
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "NewClient(pool)")
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
				derr := c.Do(ctx, &client.Request{
					Method:   "GET",
					Path:     "/",
					Headers:  ua(),
					BodyMode: client.BodyBuffer,
				}, &resp)
				if derr != nil {
					fail.Add(1)
					t.Logf("[%d] error: %v", idx, derr)
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

	require.GreaterOrEqualf(t, ok.Load(), int64(n)*9/10,
		"too many failures: ok=%d fail=%d out of %d", ok.Load(), fail.Load(), n)
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
			BodyMode: client.BodyBuffer,
		}, &resp)

		require.NoErrorf(t, err, "request %d", i)
		require.GreaterOrEqualf(t, resp.Status, 200, "request %d: status %d", i, resp.Status)
		require.LessOrEqualf(t, resp.Status, 399, "request %d: status %d", i, resp.Status)
	}

	snap := c.MetricsSnapshot()
	assert.EqualValues(t, 50, snap.Counters.RequestsSucceeded, "expected 50 succeeded")
	assert.EqualValues(t, 0, snap.Counters.RequestsErrored, "expected 0 errored")
}

// ---------- Stress 3: Mixed Do + DoStream + BodyStream interleaved ----------

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
				// Do with BodyBuffer
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
				// BodyStream — requires non-nil Response.
				var resp client.Response
				err := c.Do(ctx, &client.Request{
					Method:   "GET",
					Path:     "/",
					Headers:  ua(),
					BodyMode: client.BodyStream,
				}, &resp)
				if err != nil {
					errCh <- fmt.Errorf("[%d] BodyStream: %w", idx, err)
					return
				}
				if resp.BodyReader == nil {
					errCh <- fmt.Errorf("[%d] BodyStream: BodyReader is nil", idx)
					return
				}
				// Drain the body.
				read, err := io.Copy(io.Discard, resp.BodyReader)
				resp.BodyReader.Close()
				if err != nil {
					errCh <- fmt.Errorf("[%d] BodyStream drain: %w (read %d)", idx, err, read)
					return
				}
				if read == 0 {
					errCh <- fmt.Errorf("[%d] BodyStream: drained 0 bytes", idx)
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
					ev, rerr := sr.Recv(ctx)
					if errors.Is(rerr, client.ErrStreamEnded) || (rerr == nil && ev.EndStream) {
						break
					}
					if rerr != nil {
						errCh <- fmt.Errorf("[%d] Recv: %w", idx, rerr)
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

	require.LessOrEqualf(t, failCount, n/10, "too many failures: %d/%d", failCount, n)
}

// ---------- Stress 4: BodyStream — read body via io.ReadAll ----------

func TestStress_BodyStream_ReadAll(t *testing.T) {
	c := e2iClient(t, "www.google.com")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resp client.Response
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		Headers:  ua(),
		BodyMode: client.BodyStream,
	}, &resp)

	require.NoError(t, err, "Do(BodyStream)")
	require.NotNil(t, resp.BodyReader, "expected BodyReader to be set with BodyStream=true")
	body, err := io.ReadAll(resp.BodyReader)
	resp.BodyReader.Close()
	require.NoError(t, err, "ReadAll")
	assert.NotEmpty(t, body, "read 0 bytes from BodyReader")
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
			BodyMode: client.BodyBuffer,
		}, &resp)
		cancel()
		c.Close()

		require.NoErrorf(t, err, "cycle %d", i)
	}
}

// ---------- Stress 6: 20 concurrent BodyStream reads ----------

func TestStress_ConcurrentBodyStream(t *testing.T) {
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
	require.NoError(t, err, "NewClient(pool)")
	defer c.Close()
	const n = 20
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	errCh := make(chan error, n)
	var totalRead atomic.Int64

	for i := 0; i < n; i++ {
		go func(idx int) {
			var resp client.Response
			derr := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     "/",
				Headers:  ua(),
				BodyMode: client.BodyStream,
			}, &resp)
			if derr != nil {
				errCh <- fmt.Errorf("[%d] Do: %w", idx, derr)
				return
			}
			if resp.BodyReader != nil {
				read, _ := io.Copy(io.Discard, resp.BodyReader)
				totalRead.Add(read)
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

	require.LessOrEqualf(t, failCount, n/10, "too many failures: %d/%d", failCount, n)
}

// Stress 7 used to be TestStress_ZeroAlloc_FrameHpack, whose entire body was two
// t.Log lines asserting that "frame + hpack benches confirmed 0 B/op". It could
// not fail: it measured nothing and referenced results from elsewhere. The claim
// it made is genuinely enforced, by .github/workflows/bench-gate.yml running
// scripts/bench-gate.sh over ./frame ./hpack (and five more packages) and failing
// on any non-zero B/op or allocs/op line. Removed rather than rewritten, because
// a second, weaker copy of a real gate is worse than no copy.

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
			BodyMode: client.BodyBuffer,
		}, &resp)
		require.NoErrorf(t, err, "request %d", i)
	}
	snap := c.MetricsSnapshot()

	assert.Equal(t, snap.Counters.RequestsSucceeded+snap.Counters.RequestsErrored,
		snap.Counters.RequestsStarted,
		"metrics invariant broken: started must equal succeeded + errored")
	assert.EqualValues(t, n, snap.Counters.RequestsSucceeded, "expected %d succeeded", n)
	assert.EqualValues(t, 0, snap.Counters.RequestsErrored, "expected 0 errored")
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
	require.NoError(t, err, "NewClient(pool)")
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
			derr := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     "/",
				Headers:  ua(),
				BodyMode: client.BodyBuffer,
			}, &resp)
			if derr == nil && resp.Status >= 200 && resp.Status <= 399 {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()

	snap := c.MetricsSnapshot()
	t.Logf("pool all-conns: ok=%d/%d dials=%d (max=3) succeeded=%d errored=%d",
		ok.Load(), n, snap.Counters.DialsAttempted,
		snap.Counters.RequestsSucceeded, snap.Counters.RequestsErrored)
	// Should have opened >1 conn for 30 concurrent requests with MaxConnsPerHost=3.
	assert.GreaterOrEqualf(t, snap.Counters.DialsAttempted, uint64(2),
		"expected >=2 dials for %d concurrent requests, got %d", n, snap.Counters.DialsAttempted)
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
			BodyMode: client.BodyBuffer,
		}, &resp)

		require.NoErrorf(t, err, "request %d", i)
		require.GreaterOrEqualf(t, resp.Status, 200, "request %d: status %d", i, resp.Status)
		require.LessOrEqualf(t, resp.Status, 399, "request %d: status %d", i, resp.Status)
		require.NotEmptyf(t, resp.Body, "request %d: empty body", i)
		sizes = append(sizes, len(resp.Body))
	}

	// All sizes should be identical (same resource) — logged, not asserted:
	// google serves robots.txt dynamically.
	for i, s := range sizes {
		if s != sizes[0] {
			t.Logf("  size[%d]=%d differs from first=%d (may be dynamic content)", i, s, sizes[0])
		}
	}
	assert.Contains(t, string(resp.Body), "User-agent", "robots.txt body missing 'User-agent'")
}

// ---------- Stress 11: BodyStream nil Response returns error ----------

func TestStress_BodyStream_NilResponseError(t *testing.T) {
	c := e2iClient(t, "www.google.com")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// BodyStream with nil Response should return an error, not panic.
	err := c.Do(ctx, &client.Request{
		Method:   "GET",
		Path:     "/",
		Headers:  ua(),
		BodyMode: client.BodyStream,
	}, nil)

	require.Error(t, err, "expected error when BodyStream=true with nil Response")
}

// e2iClient creates a single-conn client that returns Response with BodyStream support.
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
	require.NoErrorf(t, err, "NewClient(%s)", host)
	return c
}
