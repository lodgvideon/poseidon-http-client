package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

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
		if err := http2.ConfigureServer(srv.Config, &http2.Server{}); err != nil {
			t.Fatalf("ConfigureServer: %v", err)
		}
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

func splitHostPortInt(t *testing.T, hp string) (string, int) {
	t.Helper()
	for i := len(hp) - 1; i >= 0; i-- {
		if hp[i] == ':' {
			port := 0
			for _, c := range hp[i+1:] {
				port = port*10 + int(c-'0')
			}
			return hp[:i], port
		}
	}
	t.Fatalf("malformed host:port %q", hp)
	return "", 0
}

func newConnOpts() conn.ConnOptions {
	return conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}}
}

func TestManagedPool_StaticResolver_RoundRobin_DistributesDials(t *testing.T) {
	t.Parallel()
	addrs, counts, cleanup := startH2Servers(t, 3)
	defer cleanup()

	mp, err := newManagedPool(
		StaticResolver(addrs...),
		RoundRobin(),
		DrainGraceful,
		newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Second},
	)
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	// 9 sequential acquires — RoundRobin distributes 3-3-3.
	for i := 0; i < 9; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c, release, err := mp.acquire(ctx)
		cancel()
		if err != nil {
			t.Fatalf("acquire[%d] = %v", i, err)
		}
		if !c.IsAlive() {
			t.Fatal("conn not alive after acquire")
		}
		release()
	}
	for i, cnt := range counts {
		if got := cnt.Load(); got < 1 {
			t.Errorf("server[%d] hits = %d, want > 0", i, got)
		}
	}
}

func TestManagedPool_NoAddresses_ReturnsErrNoAddresses(t *testing.T) {
	t.Parallel()
	mp, err := newManagedPool(
		StaticResolver(), // empty
		RoundRobin(),
		DrainGraceful,
		newConnOpts(),
		PoolOptions{MaxConnsPerHost: 1},
	)
	if err != nil {
		t.Fatalf("newManagedPool: %v", err)
	}
	defer mp.close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err = mp.acquire(ctx)
	if err != ErrNoAddresses {
		t.Errorf("acquire err = %v, want ErrNoAddresses", err)
	}
}
