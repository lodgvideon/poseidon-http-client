package client_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// poolCountConn counts transport Write calls (one per bufio flush == one
// tls.Conn.Write) across every connection the pool dials, sharing one counter.
type poolCountConn struct {
	net.Conn
	writes *atomic.Int64
}

func (c *poolCountConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

type poolCountDialer struct {
	inner  conn.Dialer
	writes atomic.Int64
}

func (d *poolCountDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	raw, err := d.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return &poolCountConn{Conn: raw, writes: &d.writes}, nil
}

// benchPool drives `workers` open-loop GETs through a pool of `conns`
// connections with group-commit on/off and reports frames/flush (k ~= requests
// / transport-writes; each GET is one HEADERS frame) and req/s. It is the
// pool x per-conn-concurrency sweep: a pool spreads offered load across conns,
// lowering per-conn concurrency and therefore k, so this shows where the
// batching win survives on a realistic pooled config.
func benchPool(b *testing.B, groupCommit bool, conns, maxStreams, workers int) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, der := range c.Certificate {
			if cert, err := x509.ParseCertificate(der); err == nil {
				pool.AddCert(cert)
			}
		}
	}
	cfg := &tls.Config{RootCAs: pool, ServerName: "example.com"}
	d := &poolCountDialer{inner: &conn.TLSDialer{Config: cfg}}

	c, err := client.NewPoolClient(srv.Listener.Addr().String(), d,
		client.PoolOptions{MaxConnsPerHost: conns, MaxStreamsPerConn: maxStreams},
		client.WithConnOptions(func(co *conn.ConnOptions) { co.GroupCommit = groupCommit }))
	if err != nil {
		b.Fatalf("NewPoolClient: %v", err)
	}
	defer c.Close()
	c.Warmup(conns)
	time.Sleep(50 * time.Millisecond) // let warmup dials + handshakes finish

	writesBefore := d.writes.Load()
	var counter atomic.Int64
	ctx := context.Background()
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var resp client.Response
			for counter.Add(1) <= int64(b.N) {
				resp.Reset()
				if err := c.Do(ctx, client.GET("/"), &resp); err != nil {
					b.Errorf("Do: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()

	writes := d.writes.Load() - writesBefore
	if writes > 0 {
		b.ReportMetric(float64(b.N)/float64(writes), "frames/flush")
	}
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "req/s")
	}
}

// Realistic pool (100 streams/conn): lazy-grow packs streams onto few conns,
// so per-conn concurrency — and the batching win — is preserved as the conn cap
// rises.
func BenchmarkGCPool_Off_C1_W64(b *testing.B) { benchPool(b, false, 1, 100, 64) }
func BenchmarkGCPool_On_C1_W64(b *testing.B)  { benchPool(b, true, 1, 100, 64) }
func BenchmarkGCPool_Off_C4_W64(b *testing.B) { benchPool(b, false, 4, 100, 64) }
func BenchmarkGCPool_On_C4_W64(b *testing.B)  { benchPool(b, true, 4, 100, 64) }
func BenchmarkGCPool_Off_C8_W64(b *testing.B) { benchPool(b, false, 8, 100, 64) }
func BenchmarkGCPool_On_C8_W64(b *testing.B)  { benchPool(b, true, 8, 100, 64) }

// Forced thin spread (2 streams/conn, up to 64 conns): the dilution boundary —
// load is spread across many conns so per-conn concurrency drops toward 1 and
// batching has little to coalesce.
func BenchmarkGCPool_Off_C64_S2_W64(b *testing.B) { benchPool(b, false, 64, 2, 64) }
func BenchmarkGCPool_On_C64_S2_W64(b *testing.B)  { benchPool(b, true, 64, 2, 64) }
