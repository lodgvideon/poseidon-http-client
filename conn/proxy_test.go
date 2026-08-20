package conn

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startProxyTest starts a fake HTTP proxy on a random port.
// It accepts CONNECT requests and responds 200, creating a tunnel
// to the target. Returns the proxy address.
func startFakeProxy(t *testing.T, targetHandler func(net.Conn)) *url.URL {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "proxy listen")

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go handleProxyConn(t, c, targetHandler)
		}
	}()
	t.Cleanup(func() { ln.Close() })

	return &url.URL{
		Scheme: "http",
		Host:   ln.Addr().String(),
	}
}

func handleProxyConn(t *testing.T, c net.Conn, targetHandler func(net.Conn)) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))

	br := bufio.NewReader(c)
	// Read the CONNECT request line.
	reqLine, err := br.ReadString('\n')
	if err != nil {
		t.Logf("proxy: read request: %v", err)
		return
	}

	// Parse target from "CONNECT host:port HTTP/1.1".
	target := reqLine[len("CONNECT "):]
	if idx := strings.Index(target, " "); idx >= 0 {
		target = target[:idx]
	}
	target = strings.TrimSpace(target)

	// Drain remaining headers.
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Logf("proxy: read header: %v", err)
			return
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}

	// Connect to target.
	tc, err := net.Dial("tcp", target)
	if err != nil {
		fmt.Fprintf(c, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return
	}
	defer tc.Close()

	// Respond 200.
	fmt.Fprintf(c, "HTTP/1.1 200 Connection Established\r\n\r\n")

	// Flush any buffered data from the request.
	if br.Buffered() > 0 {
		buf := make([]byte, br.Buffered())
		br.Read(buf)
		tc.Write(buf)
	}

	// Bidirectional copy.
	go func() {
		io.Copy(tc, c)
		tc.Close()
	}()
	io.Copy(c, tc)
}

func TestProxyDialer_Plaintext(t *testing.T) {
	// Start a target echo server.
	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "target listen")
	defer targetLn.Close()
	go func() {
		for {
			c, err := targetLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c) // echo
			}(c)
		}
	}()

	proxyURL := startFakeProxy(t, nil)
	d := &ProxyDialer{ProxyURL: proxyURL}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := []byte("hello through proxy")
	buf := make([]byte, len(msg))

	conn, err := d.Dial(ctx, targetLn.Addr().String())
	require.NoError(t, err, "Dial through the CONNECT proxy")
	defer conn.Close()
	_, werr := conn.Write(msg)
	require.NoError(t, werr, "write through the tunnel")
	_, rerr := io.ReadFull(conn, buf)

	require.NoError(t, rerr, "read back through the tunnel")
	assert.Equalf(t, string(msg), string(buf),
		"echo = %q, want %q — the CONNECT tunnel did not carry the bytes end to end", buf, msg)
}

// TestProxyDialer_BasicAuth pins the credential itself, not the word "Basic".
// The old assertion was strings.Contains(auth, "Basic"), which a dialer emitting
// "Basic " and nothing else satisfies — truncated credentials, the username
// without the password, a stale encoding — while the test built the URL with
// url.UserPassword and never asserted those bytes arrived (#833). The
// username-only form takes a separate branch that nothing exercised.
func TestProxyDialer_BasicAuth(t *testing.T) {
	for _, tc := range []struct {
		name string
		user *url.Userinfo
		want string
	}{
		{"username and password", url.UserPassword("testuser", "testpass"), "testuser:testpass"},
		{"username only", url.User("testuser"), "testuser"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxyURL, auth := rawProxy(t, "HTTP/1.1 200 OK\r\n\r\n")
			proxyURL.User = tc.user
			d := &ProxyDialer{ProxyURL: proxyURL}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			tunnel, err := d.Dial(ctx, "target:443")

			require.NoError(t, err, "Dial through the authenticating proxy")
			t.Cleanup(func() { _ = tunnel.Close() })
			var header string
			select {
			case header = <-auth:
			case <-time.After(time.Second):
				require.FailNow(t, "the proxy never saw a Proxy-Authorization header")
			}
			scheme, encoded, ok := strings.Cut(header, " ")
			require.Truef(t, ok, "Proxy-Authorization = %q, want \"Basic <base64>\"", header)
			assert.Equalf(t, "Basic", scheme, "auth scheme = %q, want Basic", scheme)
			decoded, derr := base64.StdEncoding.DecodeString(encoded)
			require.NoErrorf(t, derr, "credential %q is not valid base64", encoded)
			assert.Equalf(t, tc.want, string(decoded),
				"credential decoded to %q, want %q — a proxy that receives the username "+
					"without the password answers 407, and the caller sees an unreachable "+
					"target rather than a credential it got wrong", decoded, tc.want)
		})
	}
}

func TestProxyDialer_NilURL(t *testing.T) {
	d := &ProxyDialer{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := d.Dial(ctx, "target:443")

	assert.Error(t, err, "a ProxyDialer with no ProxyURL must refuse rather than dial the target directly")
}

func TestProxyDialer_BadResponse(t *testing.T) {
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "proxy listen")
	defer proxyLn.Close()

	go func() {
		c, err := proxyLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		// Read and discard the CONNECT request.
		br := bufio.NewReader(c)
		for {
			line, err := br.ReadString('\n')
			if err != nil || strings.TrimSpace(line) == "" {
				break
			}
		}
		fmt.Fprintf(c, "HTTP/1.1 407 Proxy Auth Required\r\n\r\n")
	}()

	proxyURL := &url.URL{Scheme: "http", Host: proxyLn.Addr().String()}
	d := &ProxyDialer{ProxyURL: proxyURL}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = d.Dial(ctx, "target:443")

	require.Error(t, err, "a non-200 CONNECT response must not be handed back as a working tunnel")
	assert.Containsf(t, err.Error(), "407",
		"error = %q, want the proxy's status carried through so the caller can tell auth failure "+
			"from an unreachable target", err)
}
