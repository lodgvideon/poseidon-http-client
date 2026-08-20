package conn

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The four tests in proxy_test.go cover a working plaintext tunnel, a nil
// ProxyURL, a 407, and "an auth header containing the word Basic". Judged by
// equivalence classes over ProxyDialer.Dial that left three whole branches with
// no test at all (#833): the malformed status line the SplitN guard exists for,
// the "https" proxy scheme with its separate TLS connect, and the nil-ProxyTLS
// default that synthesises a config.

// rawProxy starts a listener that reads one CONNECT request, publishes the
// Proxy-Authorization header it carried, and answers with reply verbatim. It
// holds the socket open afterwards so a dial that legitimately succeeds has a
// tunnel to return.
func rawProxy(t *testing.T, reply string) (*url.URL, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "proxy listen")
	t.Cleanup(func() { _ = ln.Close() })

	auth := make(chan string, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		br := bufio.NewReader(c)
		seen := ""
		for {
			line, rerr := br.ReadString('\n')
			if rerr != nil {
				return
			}
			trimmed := strings.TrimSpace(line)
			if v, ok := strings.CutPrefix(trimmed, "Proxy-Authorization: "); ok {
				seen = v
			}
			if trimmed == "" {
				break
			}
		}
		auth <- seen
		_, _ = io.WriteString(c, reply)
		time.Sleep(500 * time.Millisecond)
	}()
	return &url.URL{Scheme: "http", Host: ln.Addr().String()}, auth
}

// TestProxyDialer_MalformedStatusLine is the hostile-peer case the
// `len(parts) < 2` arm exists for. The 407 test supplies a well-formed line, so
// that arm was unreached: a proxy answering with one token made the dialer index
// past the end of its own split.
func TestProxyDialer_MalformedStatusLine(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply string
	}{
		{"a version with no status code", "HTTP/1.1\r\n\r\n"},
		{"an empty status line", "\r\n\r\n"},
		{"a bare status code with no version", "200\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxyURL, _ := rawProxy(t, tc.reply)
			d := &ProxyDialer{ProxyURL: proxyURL}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			c, err := d.Dial(ctx, "target.example:443")

			require.Errorf(t, err,
				"a CONNECT answer of %q was accepted as a working tunnel; a proxy is an "+
					"untrusted peer and its status line has to be parsed defensively, not "+
					"indexed into", tc.reply)
			assert.Truef(t, c == nil,
				"Dial returned a connection alongside its error; a caller that ignores err "+
					"would write its request into a tunnel that was never established")
		})
	}
}

// connectProxyHandler answers a CONNECT with 200 and holds the hijacked tunnel
// open until done closes, so an https-scheme dial has something to return.
func connectProxyHandler(seen chan<- string, done <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "expected CONNECT", http.StatusMethodNotAllowed)
			return
		}
		select {
		case seen <- r.Host:
		default:
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijacker", http.StatusInternalServerError)
			return
		}
		raw, _, err := hj.Hijack()
		if err != nil {
			return
		}
		defer raw.Close()
		_, _ = io.WriteString(raw, "HTTP/1.1 200 Connection Established\r\n\r\n")
		<-done
	}
}

// TestProxyDialer_HTTPSProxyScheme drives the branch a "https" ProxyURL takes:
// a tls.Dialer with a cloned ProxyTLS instead of a plain net.Dialer. Nothing
// reached it, so the CONNECT could have been sent in cleartext to a TLS proxy
// with the whole package green — which is the one thing an operator chooses an
// https proxy scheme to prevent, since the CONNECT line names the target host
// and carries the Proxy-Authorization credential.
func TestProxyDialer_HTTPSProxyScheme(t *testing.T) {
	seen := make(chan string, 1)
	done := make(chan struct{})
	srv := httptest.NewUnstartedServer(connectProxyHandler(seen, done))
	srv.StartTLS()
	t.Cleanup(func() {
		close(done)
		srv.Close()
	})
	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, der := range c.Certificate {
			if cert, perr := x509.ParseCertificate(der); perr == nil {
				pool.AddCert(cert)
			}
		}
	}
	proxyURL, err := url.Parse(srv.URL)
	require.NoError(t, err, "parse the httptest URL")
	require.Equalf(t, "https", proxyURL.Scheme, "the fixture must be an https proxy, got %q", proxyURL.Scheme)

	for _, tc := range []struct {
		name    string
		tlsCfg  *tls.Config
		wantErr bool
	}{
		{"a caller-supplied ProxyTLS that trusts the proxy",
			&tls.Config{RootCAs: pool, ServerName: "example.com", MinVersion: tls.VersionTLS12}, false},
		{"the nil-ProxyTLS default still verifies the proxy", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &ProxyDialer{ProxyURL: proxyURL, ProxyTLS: tc.tlsCfg}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			tunnel, derr := d.Dial(ctx, "target.example:443")

			if tc.wantErr {
				require.Errorf(t, derr,
					"a nil ProxyTLS dialled an untrusted proxy without complaint; the "+
						"synthesised config sets only ServerName, so dropping verification "+
						"here would silently downgrade every https-proxy deployment")
				assert.Truef(t, tunnel == nil, "Dial returned a tunnel alongside its error")
				return
			}
			require.NoErrorf(t, derr,
				"the https proxy scheme must TLS-connect to the proxy before sending "+
					"CONNECT; a plaintext dial reaches a TLS listener and dies: %v", derr)
			t.Cleanup(func() { _ = tunnel.Close() })
			select {
			case host := <-seen:
				assert.Equalf(t, "target.example:443", host,
					"the proxy was asked to tunnel to %q, want target.example:443", host)
			case <-time.After(2 * time.Second):
				require.FailNow(t, "the proxy never received a CONNECT, so the tunnel above "+
					"was not established by this dialer")
			}
		})
	}
}
