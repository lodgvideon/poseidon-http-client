package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/internal/poolcore"
)

// The HTTP/2 pool's actor internals, named here so the sibling-parity tests
// below can drive it the way they drive the HTTP/1.1 and HTTP/3 actors. They
// are exported from poolcore for exactly this: that package is internal, so the
// names are module-private and the public API is unchanged.
type (
	AcquireReq  = poolcore.AcquireReq
	AcquireResp = poolcore.AcquireResp
	ReleaseMsg  = poolcore.ReleaseMsg
	DialResult  = poolcore.DialResult
	RunState    = poolcore.RunState
)

var CountLive = poolcore.CountLive

// Fixtures the pool tests share with the sibling-parity tests in this package.
// They lived beside the HTTP/2 pool until it moved to internal/poolcore; a test
// helper cannot cross a package boundary, so both packages carry a copy.

func newConnOpts() conn.ConnOptions {
	return conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}}
}

// scriptedResolver: a Resolver whose Watch channel is driven by the test.
type scriptedResolver struct {
	initial []Address
	updates chan []Address
}

func (s *scriptedResolver) Resolve(_ context.Context) ([]Address, error) {
	return s.initial, nil
}

func (s *scriptedResolver) Watch(ctx context.Context) (<-chan []Address, error) {
	out := make(chan []Address, 1)
	out <- s.initial
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case set, ok := <-s.updates:
				if !ok {
					return
				}
				select {
				case out <- set:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (s *scriptedResolver) push(set []Address) { s.updates <- set }

func newScriptedResolver(initial []Address) *scriptedResolver {
	return &scriptedResolver{
		initial: initial,
		updates: make(chan []Address, 8),
	}
}

// startH2Servers boots n httptest+h2 TLS servers each backed by an
// independent counter; returns parsed Addresses, counts, and cleanup.
// counts[i] is incremented each time a new TCP connection reaches the
// StateNew state on server i (i.e. each new dial), which is observable
// without sending an HTTP request.
func startH2Servers(t *testing.T, n int) ([]Address, []*atomic.Int32, func()) {
	t.Helper()
	addrs := make([]Address, n)
	counts := make([]*atomic.Int32, n)
	servers := make([]*httptest.Server, n)
	for i := 0; i < n; i++ {
		c := &atomic.Int32{}
		counts[i] = c
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(200)
		}))
		srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew {
				c.Add(1)
			}
		}
		require.NoError(t, http2.ConfigureServer(srv.Config, &http2.Server{}), "ConfigureServer")
		srv.EnableHTTP2 = true
		srv.StartTLS()
		servers[i] = srv
		host, port := splitHostPortInt(t, srv.Listener.Addr().String())
		addrs[i] = Address{Host: host, Port: port}
	}
	cleanup := func() {
		for _, s := range servers {
			s.Close()
		}
	}
	return addrs, counts, cleanup
}

// hangingDialer blocks Dial until either (a) ctx is cancelled or
// (b) release is closed. Used to verify DialTimeout fires.
// dialStarted is closed on the first Dial call so tests can wait
// until the dial is actually in progress before triggering Close.
type hangingDialer struct {
	release     chan struct{}
	dialStarted chan struct{}
	startOnce   sync.Once
}

func (d *hangingDialer) Dial(ctx context.Context, _ string) (net.Conn, error) {
	if d.dialStarted != nil {
		d.startOnce.Do(func() { close(d.dialStarted) })
	}
	select {
	case <-d.release:
		return nil, errors.New("hangingDialer: released without conn")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// refusingDialer fails every dial, which is what a downed host looks like. dials
// counts what it was actually asked to do, so a test can prove the injection
// fired rather than inferring it from an error that some other path produced.
type refusingDialer struct {
	err   error
	dials atomic.Int32
}

func (d *refusingDialer) Dial(context.Context, string) (net.Conn, error) {
	d.dials.Add(1)
	return nil, d.err
}

func splitHostPortInt(t *testing.T, hp string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(hp)
	require.NoErrorf(t, err, "SplitHostPort(%q)", hp)
	port, err := strconv.Atoi(portStr)
	require.NoErrorf(t, err, "Atoi(%q)", portStr)
	return host, port
}

// waitActive polls SnapshotActive until it holds want addresses or the deadline
// passes, returning the last snapshot length seen.
func waitActive(mp *managedPool, want int, d time.Duration) int {
	deadline := time.Now().Add(d)
	got := -1
	for time.Now().Before(deadline) {
		got = len(mp.SnapshotActive())
		if got == want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	return got
}
