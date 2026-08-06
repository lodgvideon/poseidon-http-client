package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func gcTestServer(t *testing.T) (*httptest.Server, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, der := range c.Certificate {
			if cert, err := x509.ParseCertificate(der); err == nil {
				pool.AddCert(cert)
			}
		}
	}
	return srv, &tls.Config{RootCAs: pool, ServerName: "example.com"}
}

func gcHeaders() []hpack.HeaderField {
	return []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
}

// TestGroupCommit_ConcurrentGETs_NoDeadlock drives many concurrent open-loop
// GETs over one group-commit connection and asserts they all complete with the
// expected 204 (liveness + correctness under the deferral protocol). It logs
// the measured frames/flush; batching is asserted quantitatively by the
// benchmark, not here (contention-dependent).
func TestGroupCommit_ConcurrentGETs_NoDeadlock(t *testing.T) {
	t.Parallel()
	srv, cfg := gcTestServer(t)
	defer srv.Close()

	dialer := &countingDialer{inner: &TLSDialer{Config: cfg}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{Dialer: dialer})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	const (
		workers = 32
		perWork = 200
	)
	hdrs := gcHeaders()
	framesBefore := c.atomicFramesSent.Load()
	writesBefore := dialer.cc.writes.Load()

	done := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func() {
			for i := 0; i < perWork; i++ {
				s, err := c.NewStream(ctx)
				if err != nil {
					done <- err
					return
				}
				if err := s.SendHeaders(ctx, hdrs, true); err != nil {
					done <- err
					return
				}
				for {
					ev, err := s.Recv(ctx)
					if err != nil {
						done <- err
						return
					}
					if ev.EndStream {
						break
					}
				}
			}
			done <- nil
		}()
	}

	deadline := time.After(25 * time.Second)
	for w := 0; w < workers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("worker failed: %v", err)
			}
		case <-deadline:
			t.Fatal("timeout: group-commit deadlocked under concurrent GETs")
		}
	}

	frames := c.atomicFramesSent.Load() - framesBefore
	writes := dialer.cc.writes.Load() - writesBefore
	if writes > 0 {
		t.Logf("frames=%d writes=%d k=%.2f frames/flush", frames, writes, float64(frames)/float64(writes))
	}
}

// faultConn fails every Write once fail is set, simulating a mid-stream
// transport write error.
type faultConn struct {
	net.Conn
	fail atomic.Bool
}

func (c *faultConn) Write(p []byte) (int, error) {
	if c.fail.Load() {
		return 0, errors.New("injected write failure")
	}
	return c.Conn.Write(p)
}

type faultDialer struct {
	inner Dialer
	fc    *faultConn
}

func (d *faultDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	raw, err := d.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	d.fc = &faultConn{Conn: raw}
	return d.fc, nil
}

// TestGroupCommit_WriteError_SurfacesNoHang trips a transport write failure
// while many group-commit writers are in flight and asserts that (a) the run
// terminates (no writer is stranded waiting on a convoy flush that failed), and
// (b) the failure surfaces as an error rather than a false nil-success. This is
// the correctness guard for the synchronous group-commit error path.
func TestGroupCommit_WriteError_SurfacesNoHang(t *testing.T) {
	t.Parallel()
	srv, cfg := gcTestServer(t)
	defer srv.Close()

	dialer := &faultDialer{inner: &TLSDialer{Config: cfg}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{Dialer: dialer})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	hdrs := gcHeaders()
	const workers = 16
	var errSeen atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s, err := c.NewStream(ctx)
				if err != nil {
					errSeen.Add(1)
					return
				}
				if err := s.SendHeaders(ctx, hdrs, true); err != nil {
					errSeen.Add(1)
					return
				}
				for {
					ev, err := s.Recv(ctx)
					if err != nil {
						errSeen.Add(1)
						return
					}
					if ev.EndStream {
						break
					}
				}
			}
		}()
	}

	// Let some traffic flow, then break the transport mid-flight.
	time.Sleep(20 * time.Millisecond)
	dialer.fc.fail.Store(true)

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(20 * time.Second):
		close(stop)
		t.Fatal("timeout: a group-commit writer hung after a transport write error")
	}
	if errSeen.Load() == 0 {
		t.Fatal("expected the injected write failure to surface as an error")
	}
}

// TestGroupCommit_OnByDefault pins the default. The zero ConnOptions must build
// a connection whose batcher is enabled, and DisableGroupCommit must switch it
// off — the inverted field name exists precisely so the zero value is the
// enabled one.
func TestGroupCommit_OnByDefault(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	defer srv.Close()

	on := dialServer(t, srv, cfg)
	defer on.Close()
	if on.wbatch == nil || !on.wbatch.enabled {
		t.Fatal("zero ConnOptions left group-commit disabled; the default is meant to be on")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	off, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer:             &TLSDialer{Config: cfg},
		DisableGroupCommit: true,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer off.Close()
	if off.wbatch == nil || off.wbatch.enabled {
		t.Fatal("DisableGroupCommit did not disable the batcher")
	}
}

// TestGroupCommit_LoneWriterNeverDefers is the property the default rests on: a
// writer that finds no other writer queued must take the plain-flush path, so a
// request with no concurrency is not delayed by batching. It is checked at the
// batcher rather than end-to-end because the claim is structural — the deferral
// is gated on waiters > 0, and nothing else can reach it.
func TestGroupCommit_LoneWriterNeverDefers(t *testing.T) {
	var mu sync.Mutex
	b := newWriteBatcher(true, &mu, nil)

	mu.Lock()
	// No enter() has run, so waiters is 0. commit must return without waiting;
	// if it deferred it would block here forever, since nobody will broadcast.
	done := make(chan error, 1)
	go func() { done <- b.commit() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("lone commit = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lone writer deferred with no other writer queued — a request with no concurrency was delayed")
	}
	mu.Unlock()

	if b.deferring != 0 {
		t.Fatalf("deferring = %d after a lone commit, want 0", b.deferring)
	}
}
