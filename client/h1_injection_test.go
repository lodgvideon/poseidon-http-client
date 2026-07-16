package client_test

// RFC 9110 §5.5 request-injection conformance at the client/ layer.
//
// http1/injection_test.go proves the wire writer refuses to encode a poisoned
// request. These prove the refusal survives the layer callers actually use:
// client.Do, against a real net/http origin, with the header value and the
// authority reached the way an application reaches them.
//
// Why this layer is worth a second test rather than being redundant with the
// http1 one: client/validateRequest is the only thing between an application
// and the wire on this path, and it does NOT validate header values or
// Authority at all (it checks Method and Path for whitespace, the pseudo-header
// prefix, forbidden headers, and the TE value). So the assertion here is that
// the http1-layer check is genuinely reached through the full Do path and is
// not bypassed by anything client/ does on the way down.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// startH1SpyServer records every header field name every request carried, so a
// test can assert the origin never saw an injected field. A real net/http
// server is used on purpose: it is the thing that would parse a split request
// into two, so it is the honest oracle for "did the split work".
func startH1SpyServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()

	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		for name := range r.Header {
			seen = append(seen, name)
		}
		mu.Unlock()
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

// TestConformance_RFC9110_Sec5_5_ClientDo_HeaderValueCRLF_Refused is the
// end-to-end statement of the bug: an application that puts attacker-influenced
// text into a header value must not thereby be able to add header fields to its
// own request (RFC 9112 §11.2).
func TestConformance_RFC9110_Sec5_5_ClientDo_HeaderValueCRLF_Refused(t *testing.T) {
	t.Parallel()
	srv, headersSeen := startH1SpyServer(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	err = c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
		Headers: []conn.HeaderField{
			{Name: []byte("x-user"), Value: []byte("bob\r\nX-Injected: pwned")},
		},
	}, &resp)

	if err == nil {
		t.Errorf("Do accepted a request with CRLF in a header value (RFC 9110 §5.5)")
	}
	for _, name := range headersSeen() {
		if strings.EqualFold(name, "X-Injected") {
			t.Errorf("REQUEST SPLIT: origin received the injected field %q; headers seen: %v",
				name, headersSeen())
		}
	}
}

// TestConformance_RFC9110_Sec5_5_ClientDo_AuthorityCRLF_Refused covers the
// authority vector end to end. Request.Authority is never validated by
// client/validateRequest, and it becomes the Host field value, so this is the
// §5.5 rule reached by the path least likely to be guarded by an application's
// own input handling: an authority usually comes from configuration or service
// discovery rather than from a request parameter, so it is trusted by habit.
func TestConformance_RFC9110_Sec5_5_ClientDo_AuthorityCRLF_Refused(t *testing.T) {
	t.Parallel()
	srv, headersSeen := startH1SpyServer(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	err = c.Do(context.Background(), &client.Request{
		Method:    "GET",
		Path:      "/",
		Authority: "example.com\r\nX-Injected: pwned",
		BodyMode:  client.BodyBuffer,
	}, &resp)

	if err == nil {
		t.Errorf("Do accepted a request with CRLF in :authority (RFC 9110 §5.5)")
	}
	for _, name := range headersSeen() {
		if strings.EqualFold(name, "X-Injected") {
			t.Errorf("REQUEST SPLIT via Authority: origin received %q", name)
		}
	}
}

// TestConformance_RFC9110_Sec5_5_ClientDo_LegalRequestUnaffected is the
// over-rejection guard for the client path: the validator must leave a normal
// request alone, header value spaces and all.
func TestConformance_RFC9110_Sec5_5_ClientDo_LegalRequestUnaffected(t *testing.T) {
	t.Parallel()
	srv := startH1Server(t)

	c, err := client.NewClient(client.ClientOptions{
		Transport: client.TransportH1SingleConn,
		Addr:      srv.Listener.Addr().String(),
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var resp client.Response
	resp.Reset()
	if err := c.Do(context.Background(), &client.Request{
		Method:   "GET",
		Path:     "/",
		BodyMode: client.BodyBuffer,
		Headers: []conn.HeaderField{
			{Name: []byte("user-agent"), Value: []byte("poseidon/1.0 (test)")},
		},
	}, &resp); err != nil {
		t.Fatalf("legal request refused: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("status = %d, want 200", resp.Status)
	}
}
