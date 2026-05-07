package client

import (
	"fmt"
	"strings"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ClientOptions tunes a Client. Addr and ConnOpts.Dialer are required.
type ClientOptions struct {
	// Addr is the "host:port" target used both as the dial target and
	// as the default :authority for requests that don't set one.
	Addr string

	// ConnOpts is forwarded verbatim to conn.Dial. ConnOpts.Dialer
	// must be non-nil.
	ConnOpts conn.ConnOptions

	// DialBackoff suppresses repeated dial attempts within this window
	// after a failed dial. Zero disables suppression (immediate retry).
	DialBackoff time.Duration
}

// Client is a high-level HTTP/2 client wrapping a single connection.
// It is safe for concurrent use by multiple goroutines.
type Client struct {
	tr        transport
	authority string
}

// NewClient validates opts and constructs a Client. It does NOT dial;
// the first Do or DoStream call triggers a lazy connection establish.
func NewClient(opts ClientOptions) (*Client, error) {
	if strings.TrimSpace(opts.Addr) == "" {
		return nil, fmt.Errorf("client: ClientOptions.Addr is required")
	}
	if opts.ConnOpts.Dialer == nil {
		return nil, fmt.Errorf("client: ClientOptions.ConnOpts.Dialer is required")
	}
	tr := &singleConn{
		addr:     opts.Addr,
		connOpts: opts.ConnOpts,
		backoff:  opts.DialBackoff,
	}
	return &Client{tr: tr, authority: deriveAuthority(opts.Addr)}, nil
}

// Close releases the underlying transport. Subsequent Do/DoStream
// calls return ErrClosed. Idempotent.
func (c *Client) Close() error {
	return c.tr.close()
}

// deriveAuthority strips the port if it equals 80 (http) or 443 (https).
func deriveAuthority(addr string) string {
	host, port, ok := strings.Cut(addr, ":")
	if !ok {
		return addr
	}
	if port == "80" || port == "443" {
		return host
	}
	return addr
}
