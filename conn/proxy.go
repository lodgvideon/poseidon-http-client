package conn

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
)

// ProxyDialer dials through an HTTP/1.1 CONNECT proxy.
//
// Flow:
//  1. TCP (or TLS) connect to the proxy.
//  2. Send "CONNECT target:port HTTP/1.1".
//  3. Read "HTTP/1.1 200" response.
//  4. Return the raw net.Conn — a tunnel to the target.
//
// The caller (typically TLSDialer or a wrapper) then does TLS + ALPN h2
// over this tunnel.
//
// Proxy URL formats:
//
//	http://host:port         — plaintext to proxy
//	https://host:port        — TLS to proxy, then CONNECT tunnel
//	http://user:pass@host:port — Basic auth
type ProxyDialer struct {
	// ProxyURL is the proxy endpoint. Scheme "http" (default) means plaintext
	// to proxy; "https" means TLS to proxy before sending CONNECT.
	ProxyURL *url.URL

	// ProxyTLS is the TLS config used when ProxyURL scheme is "https".
	// Ignored for "http" proxies. nil → tls.Config with ServerName from URL.
	ProxyTLS *tls.Config
}

// Dial connects to addr (host:port) through the HTTP proxy.
func (d *ProxyDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	proxyURL := d.ProxyURL
	if proxyURL == nil {
		return nil, fmt.Errorf("conn: ProxyDialer: ProxyURL is nil")
	}

	// Resolve proxy address.
	proxyAddr := proxyURL.Host
	if !strings.Contains(proxyAddr, ":") {
		proxyAddr = proxyAddr + ":80"
	}

	// Step 1: connect to the proxy.
	var proxyConn net.Conn
	if proxyURL.Scheme == "https" {
		cfg := d.ProxyTLS
		if cfg == nil {
			cfg = &tls.Config{ServerName: proxyURL.Hostname()}
		} else {
			cfg = cfg.Clone()
		}
		td := &tls.Dialer{Config: cfg}
		c, err := td.DialContext(ctx, "tcp", proxyAddr)
		if err != nil {
			return nil, fmt.Errorf("conn: proxy tls dial %s: %w", proxyAddr, err)
		}
		proxyConn = c
	} else {
		var nd net.Dialer
		c, err := nd.DialContext(ctx, "tcp", proxyAddr)
		if err != nil {
			return nil, fmt.Errorf("conn: proxy dial %s: %w", proxyAddr, err)
		}
		proxyConn = c
	}

	// Step 2: send CONNECT.
	connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n", addr, addr)

	// Proxy-Authorization: Basic if userinfo present.
	if u := proxyURL.User; u != nil {
		creds := u.Username()
		if p, ok := u.Password(); ok {
			creds += ":" + p
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(creds))
		connectReq += "Proxy-Authorization: Basic " + encoded + "\r\n"
	}
	connectReq += "\r\n"

	if _, err := io.WriteString(proxyConn, connectReq); err != nil {
		_ = proxyConn.Close()
		return nil, fmt.Errorf("conn: proxy write CONNECT: %w", err)
	}

	// Step 3: read response.
	br := bufio.NewReader(proxyConn)
	line, err := br.ReadString('\n')
	if err != nil {
		_ = proxyConn.Close()
		return nil, fmt.Errorf("conn: proxy read CONNECT response: %w", err)
	}

	// Parse "HTTP/1.x 200".
	parts := strings.SplitN(strings.TrimSpace(line), " ", 3)
	if len(parts) < 2 || parts[1] != "200" {
		_ = proxyConn.Close()
		return nil, fmt.Errorf("conn: proxy CONNECT %s: %s", addr, strings.TrimSpace(line))
	}

	// Drain remaining headers until empty line.
	for {
		hdr, err := br.ReadString('\n')
		if err != nil {
			_ = proxyConn.Close()
			return nil, fmt.Errorf("conn: proxy read CONNECT headers: %w", err)
		}
		if strings.TrimSpace(hdr) == "" {
			break
		}
	}

	// Step 4: return the raw tunnel connection.
	// If bufio.Reader buffered data beyond the headers, wrap the conn
	// so reads see the buffered data first.
	if br.Buffered() > 0 {
		return &bufferedConn{Conn: proxyConn, reader: br}, nil
	}
	return proxyConn, nil
}

// bufferedConn wraps a net.Conn so that a bufio.Reader's buffered bytes
// are consumed first, then reads fall through to the underlying Conn.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

// ProxyTLSDialer combines ProxyDialer + TLS into a single Dialer.
// It dials through an HTTP proxy, then performs a TLS handshake with ALPN
// h2 over the tunnel. Use this when you want conn.Dial to transparently
// route through a proxy:
//
//	client := conn.Dial(ctx, "target:443", conn.ConnOptions{
//	    Dialer: &conn.ProxyTLSDialer{
//	        ProxyURL: proxyURL,
//	        TLSConfig: &tls.Config{ServerName: "target"},
//	    },
//	})
type ProxyTLSDialer struct {
	// ProxyURL is the HTTP proxy endpoint (see ProxyDialer for formats).
	ProxyURL *url.URL

	// ProxyTLS is the TLS config for the proxy connection itself.
	// Used only when ProxyURL scheme is "https". nil → default.
	ProxyTLS *tls.Config

	// TLSConfig is the TLS config for the target connection (after tunnel).
	// ServerName should match the target host. nil → tls.Config with
	// ServerName from the dial address.
	TLSConfig *tls.Config
}

// Dial connects through proxy → TLS handshake → returns the TLS connection.
func (d *ProxyTLSDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	// Step 1: CONNECT tunnel.
	tunnel, err := (&ProxyDialer{ProxyURL: d.ProxyURL, ProxyTLS: d.ProxyTLS}).Dial(ctx, addr)
	if err != nil {
		return nil, err
	}

	// Step 2: TLS over tunnel.
	cfg := d.TLSConfig
	if cfg == nil {
		cfg = &tls.Config{}
	} else {
		cfg = cfg.Clone()
	}
	hasH2 := false
	for _, p := range cfg.NextProtos {
		if p == "h2" {
			hasH2 = true
			break
		}
	}
	if !hasH2 {
		cfg.NextProtos = append([]string{"h2"}, cfg.NextProtos...)
	}
	// Extract host for ServerName if not set.
	if cfg.ServerName == "" {
		host, _, _ := net.SplitHostPort(addr)
		cfg.ServerName = host
	}

	tc := tls.Client(tunnel, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = tunnel.Close()
		return nil, fmt.Errorf("conn: proxy tls handshake: %w", err)
	}
	if tc.ConnectionState().NegotiatedProtocol != "h2" {
		_ = tc.Close()
		return nil, ErrALPNFailed
	}
	return tc, nil
}

func (bc *bufferedConn) Read(b []byte) (int, error) {
	return bc.reader.Read(b)
}
