package conn

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"
)

// startProxyTest starts a fake HTTP proxy on a random port.
// It accepts CONNECT requests and responds 200, creating a tunnel
// to the target. Returns the proxy address.
func startFakeProxy(t *testing.T, targetHandler func(net.Conn)) *url.URL {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}

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
	if err != nil {
		t.Fatal(err)
	}
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

	conn, err := d.Dial(ctx, targetLn.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Write and read back.
	msg := []byte("hello through proxy")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("echo = %q, want %q", buf, msg)
	}
}

func TestProxyDialer_BasicAuth(t *testing.T) {
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxyLn.Close()

	receivedAuth := make(chan string, 1)
	go func() {
		c, err := proxyLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(5 * time.Second))

		br := bufio.NewReader(c)
		var authHeader string
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "Proxy-Authorization:") {
				authHeader = trimmed
			}
			if trimmed == "" {
				break
			}
		}
		receivedAuth <- authHeader
		fmt.Fprintf(c, "HTTP/1.1 200 OK\r\n\r\n")
	}()

	proxyURL := &url.URL{
		Scheme: "http",
		Host:   proxyLn.Addr().String(),
		User:   url.UserPassword("testuser", "testpass"),
	}
	d := &ProxyDialer{ProxyURL: proxyURL}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = d.Dial(ctx, "target:443")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	select {
	case auth := <-receivedAuth:
		if !strings.Contains(auth, "Basic") {
			t.Errorf("auth = %q, want Basic", auth)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for auth header")
	}
}

func TestProxyDialer_NilURL(t *testing.T) {
	d := &ProxyDialer{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := d.Dial(ctx, "target:443")
	if err == nil {
		t.Fatal("expected error for nil ProxyURL")
	}
}

func TestProxyDialer_BadResponse(t *testing.T) {
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	if err == nil {
		t.Fatal("expected error for 407 response")
	}
	if !strings.Contains(err.Error(), "407") {
		t.Fatalf("error = %q, want 407", err)
	}
}
