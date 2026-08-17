package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func insecureDialer() conn.Dialer {
	return &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}}
}

func status204Server(t *testing.T) string {
	return h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
}

func TestNewSingleConnClient_E2E(t *testing.T) {
	c, err := NewSingleConnClient(status204Server(t), insecureDialer())
	require.NoError(t, err, "NewSingleConnClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp Response

	err = c.Do(ctx, GET("/"), &resp)

	require.NoError(t, err, "Do")
	require.Equalf(t, 204, resp.Status, "status = %d, want 204", resp.Status)
}

func TestNewPoolClient_E2E(t *testing.T) {
	c, err := NewPoolClient(status204Server(t), insecureDialer(),
		PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 10})
	require.NoError(t, err, "NewPoolClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var resp Response

	err = c.Do(ctx, GET("/"), &resp)

	require.NoError(t, err, "Do")
	require.Equalf(t, 204, resp.Status, "status = %d, want 204", resp.Status)
}

func TestNewManagedClient_Construction(t *testing.T) {
	addr := status204Server(t)
	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	r := StaticResolver(Address{Host: host, Port: port})

	// A resolver plus an explicit selector is the supported construction.
	c, err := NewManagedClient(r, insecureDialer(), WithSelector(RoundRobin()))

	require.NoError(t, err, "NewManagedClient")
	defer c.Close()

	// Resolver is required for the managed transport.
	_, err = NewManagedClient(nil, insecureDialer())

	require.Error(t, err, "NewManagedClient(nil resolver) should error")
}

func TestConstructors_NilDialerErrors(t *testing.T) {
	_, errSingleConn := NewSingleConnClient("h:1", nil)
	_, errPool := NewPoolClient("h:1", nil, PoolOptions{MaxConnsPerHost: 1})
	_, errManaged := NewManagedClient(StaticResolver(Address{Host: "h", Port: 1}), nil)

	assert.Error(t, errSingleConn, "NewSingleConnClient(nil dialer) should error")
	assert.Error(t, errPool, "NewPoolClient(nil dialer) should error")
	assert.Error(t, errManaged, "NewManagedClient(nil dialer) should error")
}

func TestOptions_Apply(t *testing.T) {
	var o ClientOptions
	hooks := &Hooks{}
	opts := []Option{
		WithHooks(hooks),
		WithDefaultScheme("http"),
		WithRateLimit(10, 5),
		WithMaxResponseBodySize(123),
		WithMaxDecompressedSize(456),
		WithDialBackoff(2 * time.Second),
		WithSelector(RoundRobin()),
		WithConnOptions(func(co *conn.ConnOptions) { co.EnablePush = true }),
	}

	for _, opt := range opts {
		opt(&o)
	}

	assert.True(t, o.Hooks == hooks, "WithHooks not applied")
	assert.Equal(t, "http", o.DefaultScheme, "WithDefaultScheme not applied")
	assert.True(t, o.RateLimitPerSecond == 10 && o.RateLimitBurst == 5, "WithRateLimit not applied")
	assert.Equal(t, int64(123), o.MaxResponseBodySize, "WithMaxResponseBodySize not applied")
	assert.Equal(t, int64(456), o.MaxDecompressedSize, "WithMaxDecompressedSize not applied")
	assert.Equal(t, 2*time.Second, o.DialBackoff, "WithDialBackoff not applied")
	assert.True(t, o.Selector != nil, "WithSelector not applied")
	assert.True(t, o.ConnOpts.EnablePush, "WithConnOptions not applied")
}
