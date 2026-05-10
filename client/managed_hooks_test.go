package client_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// pushResolver is a test-only Resolver whose Watch channel is driven by push().
type pushResolver struct {
	initial []client.Address
	updates chan []client.Address
}

func newPushResolver(initial []client.Address) *pushResolver {
	return &pushResolver{initial: initial, updates: make(chan []client.Address, 4)}
}

func (r *pushResolver) Resolve(_ context.Context) ([]client.Address, error) { return r.initial, nil }

func (r *pushResolver) Watch(ctx context.Context) (<-chan []client.Address, error) {
	out := make(chan []client.Address, 1)
	out <- r.initial
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case set, ok := <-r.updates:
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

func (r *pushResolver) push(set []client.Address) { r.updates <- set }

func startTLSAddr(t *testing.T) (*httptest.Server, client.Address) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return srv, client.Address{Host: host, Port: port}
}

func TestHooks_OnResolverUpdate(t *testing.T) {
	t.Parallel()
	_, a1 := startTLSAddr(t)
	_, a2 := startTLSAddr(t)

	res := newPushResolver([]client.Address{a1})
	var updates atomic.Int32
	var sawAdd atomic.Bool
	hooks := &client.Hooks{
		OnResolverUpdate: func(e client.ResolverUpdateEvent) {
			if updates.Add(1) >= 1 && len(e.Added) == 1 && e.Added[0].String() == a2.String() {
				sawAdd.Store(true)
			}
		},
	}

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportManaged,
		Resolver:  res,
		Selector:  client.RoundRobin(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}},
		Hooks:     hooks,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	res.push([]client.Address{a1, a2})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sawAdd.Load() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("OnResolverUpdate never observed addr2 added; updates fired = %d", updates.Load())
}
