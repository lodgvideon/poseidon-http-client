package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func insecureDialer() conn.Dialer {
	return &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
}

func status204Server(t *testing.T) string {
	return h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
}

func TestNewSingleConnClient_E2E(t *testing.T) {
	c, err := NewSingleConnClient(status204Server(t), insecureDialer())
	if err != nil {
		t.Fatalf("NewSingleConnClient: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp Response
	if err := c.Do(ctx, GET("/"), &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 204 {
		t.Fatalf("status = %d, want 204", resp.Status)
	}
}

func TestNewPoolClient_E2E(t *testing.T) {
	c, err := NewPoolClient(status204Server(t), insecureDialer(),
		PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 10})
	if err != nil {
		t.Fatalf("NewPoolClient: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp Response
	if err := c.Do(ctx, GET("/"), &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 204 {
		t.Fatalf("status = %d, want 204", resp.Status)
	}
}

func TestNewManagedClient_Construction(t *testing.T) {
	addr := status204Server(t)
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	r := StaticResolver(Address{Host: host, Port: port})

	c, err := NewManagedClient(r, insecureDialer(), WithSelector(RoundRobin()))
	if err != nil {
		t.Fatalf("NewManagedClient: %v", err)
	}
	defer c.Close()

	// Resolver is required for the managed transport.
	if _, err := NewManagedClient(nil, insecureDialer()); err == nil {
		t.Fatal("NewManagedClient(nil resolver) should error")
	}
}

func TestConstructors_NilDialerErrors(t *testing.T) {
	if _, err := NewSingleConnClient("h:1", nil); err == nil {
		t.Error("NewSingleConnClient(nil dialer) should error")
	}
	if _, err := NewPoolClient("h:1", nil, PoolOptions{MaxConnsPerHost: 1}); err == nil {
		t.Error("NewPoolClient(nil dialer) should error")
	}
	if _, err := NewManagedClient(StaticResolver(Address{Host: "h", Port: 1}), nil); err == nil {
		t.Error("NewManagedClient(nil dialer) should error")
	}
}

func TestOptions_Apply(t *testing.T) {
	var o ClientOptions
	hooks := &Hooks{}
	for _, opt := range []Option{
		WithHooks(hooks),
		WithDefaultScheme("http"),
		WithRateLimit(10, 5),
		WithMaxResponseBodySize(123),
		WithMaxDecompressedSize(456),
		WithDialBackoff(2 * time.Second),
		WithSelector(RoundRobin()),
		WithConnOptions(func(co *conn.ConnOptions) { co.EnablePush = true }),
	} {
		opt(&o)
	}
	switch {
	case o.Hooks != hooks:
		t.Error("WithHooks not applied")
	case o.DefaultScheme != "http":
		t.Error("WithDefaultScheme not applied")
	case o.RateLimitPerSecond != 10 || o.RateLimitBurst != 5:
		t.Error("WithRateLimit not applied")
	case o.MaxResponseBodySize != 123:
		t.Error("WithMaxResponseBodySize not applied")
	case o.MaxDecompressedSize != 456:
		t.Error("WithMaxDecompressedSize not applied")
	case o.DialBackoff != 2*time.Second:
		t.Error("WithDialBackoff not applied")
	case o.Selector == nil:
		t.Error("WithSelector not applied")
	case !o.ConnOpts.EnablePush:
		t.Error("WithConnOptions not applied")
	}
}
