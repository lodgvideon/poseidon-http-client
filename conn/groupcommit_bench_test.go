package conn

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

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// flushCountConn counts Write calls to the transport. Each Write is exactly one
// bufio flush == one crypto/tls.Conn.Write (>=1 record encrypt + >=1 socket
// write). The ratio framesSent/writes is the mean frames-per-flush (k) that the
// group-commit experiment aims to raise above 1. This is the honest syscall
// proxy for a userspace-TLS client: fewer transport Writes == fewer record
// encrypts + socket writes.
type flushCountConn struct {
	net.Conn
	writes atomic.Int64
	bytes  atomic.Int64
}

func (c *flushCountConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	c.bytes.Add(int64(len(p)))
	return c.Conn.Write(p)
}

// countingDialer wraps an inner Dialer and records the flushCountConn it produced
// so the benchmark can read the transport write count after the run.
type countingDialer struct {
	inner Dialer
	cc    *flushCountConn
}

func (d *countingDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	raw, err := d.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	d.cc = &flushCountConn{Conn: raw}
	return d.cc, nil
}

func benchSetupCounting(b *testing.B, groupCommit bool) (*Conn, *flushCountConn, func()) {
	b.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()

	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			if cert, err := x509.ParseCertificate(certDER); err == nil {
				pool.AddCert(cert)
			}
		}
	}
	cfg := &tls.Config{RootCAs: pool, ServerName: "example.com"}
	dialer := &countingDialer{inner: &TLSDialer{Config: cfg}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{Dialer: dialer, GroupCommit: groupCommit})
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	return c, dialer.cc, func() { _ = c.Close(); srv.Close() }
}

// benchConcurrentWrite drives `workers` open-loop goroutines issuing empty GETs
// over ONE connection and reports frames/flush (k), flush/req, and req/s. k>1
// means group-commit is batching frames from co-queued writers into one
// tls.Conn.Write. req/s here is confounded (the in-process httptest server
// shares cores over loopback); k is the clean, server-independent signal.
func benchConcurrentWrite(b *testing.B, groupCommit bool, workers int) {
	c, cc, teardown := benchSetupCounting(b, groupCommit)
	defer teardown()
	hdrs := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	framesBefore := c.atomicFramesSent.Load()
	writesBefore := cc.writes.Load()

	var counter atomic.Int64
	ctx := context.Background()
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for counter.Add(1) <= int64(b.N) {
				s, err := c.NewStream(ctx)
				if err != nil {
					b.Errorf("NewStream: %v", err)
					return
				}
				if err := s.SendHeaders(ctx, hdrs, true); err != nil {
					b.Errorf("SendHeaders: %v", err)
					return
				}
				for {
					ev, err := s.Recv(ctx)
					if err != nil {
						b.Errorf("Recv: %v", err)
						return
					}
					if ev.EndStream {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()

	frames := c.atomicFramesSent.Load() - framesBefore
	writes := cc.writes.Load() - writesBefore
	k := 1.0
	if writes > 0 {
		k = float64(frames) / float64(writes)
	}
	b.ReportMetric(k, "frames/flush")
	b.ReportMetric(float64(writes)/float64(b.N), "flush/req")
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "req/s")
	}
}

func BenchmarkGroupCommit_Off_W1(b *testing.B)  { benchConcurrentWrite(b, false, 1) }
func BenchmarkGroupCommit_On_W1(b *testing.B)   { benchConcurrentWrite(b, true, 1) }
func BenchmarkGroupCommit_Off_W8(b *testing.B)  { benchConcurrentWrite(b, false, 8) }
func BenchmarkGroupCommit_On_W8(b *testing.B)   { benchConcurrentWrite(b, true, 8) }
func BenchmarkGroupCommit_Off_W64(b *testing.B) { benchConcurrentWrite(b, false, 64) }
func BenchmarkGroupCommit_On_W64(b *testing.B)  { benchConcurrentWrite(b, true, 64) }
