package http1_test

// RFC 9112 / RFC 9110 request-injection conformance tests.
//
// These cover the send path: what this client puts on an HTTP/1.1 wire when a
// caller hands it a hostile header value, header name, method, or target.
// RFC 9110 §5.5 forbids CR/LF/NUL in a field value; RFC 9112 §11.2 is why that
// matters (a CRLF in a value is a request-splitting primitive, because HTTP/1.1
// framing is the delimiter bytes themselves).
//
// This class of bug is HTTP/1.1-only by construction: on HTTP/2 the same value
// is length-prefixed by HPACK into a frame payload, where a CR is an ordinary
// byte that cannot invent a frame boundary. So these assertions live here and
// have no conn/ counterpart.
//
// Harness note: net/http cannot be used to observe this — the assertion is
// about literal bytes on a socket, so each test uses a raw net.Listener and
// compares what arrived. net.Pipe is avoided deliberately: it is unbuffered and
// synchronous, so a client write with no concurrent reader deadlocks.

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// rawCapture starts a listener that accepts one connection, reads whatever the
// client sends until it goes idle, and reports those exact bytes. It returns an
// Exchange wired to that listener plus a func to collect the captured bytes.
//
// The read loop is bounded by a short per-read deadline rather than by an
// expected byte count: the point of most of these tests is that *nothing* is
// written, and a capture that blocks for a request that will never come cannot
// observe that. Each read gets a fresh deadline, so this bounds a stall and not
// the total, which is what keeps it stable under -race on slower CI.
func rawCapture(t *testing.T) (*http1.Exchange, func() string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	got := make(chan string, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			got <- ""
			return
		}
		defer c.Close()
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			// Fresh deadline per read: bounds a stall, never the whole loop.
			_ = c.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
			n, rerr := c.Read(buf)
			sb.Write(buf[:n])
			if rerr != nil {
				break
			}
		}
		got <- sb.String()
	}()

	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn := http1.NewConn(nc)
	t.Cleanup(func() { _ = conn.Close() })

	return conn.NewExchange(), func() string { return <-got }
}

// fields builds a minimal valid H2-style field list and applies overrides.
func fields(extra ...hpack.HeaderField) []hpack.HeaderField {
	base := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	return append(base, extra...)
}

// TestConformance_RFC9110_Sec5_5_HeaderValueCRLF_NotWritten pins the core bug.
//
// RFC 9110 §5.5: field values containing CR, LF or NUL are "invalid and
// dangerous"; the section binds a RECIPIENT to reject-or-replace and states no
// sender-side prohibition, so refusing to emit them is this client's policy.
// The reason it is the right policy is the one below.
//
// A caller-supplied value carrying CRLF used to be concatenated straight into
// the header block, so the bytes after the CRLF arrived at the server as
// additional header fields of the client's own request — request splitting
// (RFC 9112 §11.2).
//
// The assertion is on the wire, not on the error: an error return with the
// bytes still sent would be worthless, and so would an error raised after a
// partial write. Both halves are checked.
func TestConformance_RFC9110_Sec5_5_HeaderValueCRLF_NotWritten(t *testing.T) {
	ex, capture := rawCapture(t)

	err := ex.WriteRequest(context.Background(), fields(hpack.HeaderField{
		Name:  []byte("x-user"),
		Value: []byte("bob\r\nX-Injected: pwned\r\nX-Evil: yes"),
	}), true)

	// The wire is asserted before the error, and with Errorf rather than Fatalf,
	// so that a regression reports the actual security failure (bytes on the
	// socket) rather than only its proxy (a nil error). A Fatalf on the error
	// here would short-circuit the assertion that matters.
	wire := capture()
	if strings.Contains(wire, "X-Injected") {
		t.Errorf("REQUEST SPLIT: caller's header VALUE became a header FIELD.\nwire:\n%s", wire)
	}
	if wire != "" {
		t.Errorf("nothing must reach the wire; got %d bytes:\n%s", len(wire), wire)
	}
	if !errors.Is(err, http1.ErrInvalidRequest) {
		t.Errorf("WriteRequest err = %v, want ErrInvalidRequest", err)
	}
}

// TestConformance_RFC9110_Sec5_5_HeaderValueNUL_NotWritten covers the third
// byte §5.5 names. NUL is not a delimiter to this client, so it splits nothing
// here; it is forbidden because it terminates a C string, letting one value
// mean different things to this client and to a C proxy in front of the origin.
func TestConformance_RFC9110_Sec5_5_HeaderValueNUL_NotWritten(t *testing.T) {
	ex, capture := rawCapture(t)

	err := ex.WriteRequest(context.Background(), fields(hpack.HeaderField{
		Name:  []byte("x-user"),
		Value: []byte("bob\x00admin"),
	}), true)

	if wire := capture(); wire != "" {
		t.Errorf("nothing must reach the wire; got:\n%q", wire)
	}
	if !errors.Is(err, http1.ErrInvalidRequest) {
		t.Errorf("WriteRequest err = %v, want ErrInvalidRequest", err)
	}
}

// TestConformance_RFC9110_Sec5_5_AuthorityCRLF_NotWritten covers the same rule
// reached through :authority, which WriteRequest interpolates into the Host
// field value. This is the vector that survives the client/ layer: client's
// validateRequest checks Method and Path for whitespace but never looks at
// Request.Authority at all, so an authority from configuration or service
// discovery reaches this concatenation unchecked.
func TestConformance_RFC9110_Sec5_5_AuthorityCRLF_NotWritten(t *testing.T) {
	ex, capture := rawCapture(t)

	err := ex.WriteRequest(context.Background(), []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com\r\nX-Injected: pwned")},
	}, true)

	wire := capture()
	if strings.Contains(wire, "X-Injected") {
		t.Errorf("REQUEST SPLIT via :authority:\nwire:\n%s", wire)
	}
	if wire != "" {
		t.Errorf("nothing must reach the wire; got:\n%s", wire)
	}
	if !errors.Is(err, http1.ErrInvalidRequest) {
		t.Errorf("WriteRequest err = %v, want ErrInvalidRequest", err)
	}
}

// TestConformance_RFC9110_Sec5_6_2_HeaderNameToken_NotWritten pins the name
// side. §5.1 makes field names case-insensitive; §5.6.2 is what says a name is
// a token. A name carrying ':' or CR forges a field boundary exactly as a
// value's CR forges a line boundary.
func TestConformance_RFC9110_Sec5_6_2_HeaderNameToken_NotWritten(t *testing.T) {
	for _, tc := range []struct {
		desc string
		name string
	}{
		{"embedded CRLF", "x-user\r\nX-Injected"},
		{"embedded colon forges a field boundary", "x-user: pwned\r\nX-Injected"},
		{"embedded space", "x user"},
		{"NUL", "x-user\x00"},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ex, capture := rawCapture(t)

			err := ex.WriteRequest(context.Background(), fields(hpack.HeaderField{
				Name:  []byte(tc.name),
				Value: []byte("v"),
			}), true)

			if wire := capture(); wire != "" {
				t.Errorf("nothing must reach the wire; got:\n%q", wire)
			}
			if !errors.Is(err, http1.ErrInvalidRequest) {
				t.Errorf("WriteRequest err = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

// TestConformance_RFC9112_Sec3_RequestLine_NotWritten pins the request line.
//
// RFC 9112 §3: request-line = method SP request-target SP HTTP-version. A
// method or target carrying SP or CTL re-cuts that line.
//
// client/ already rejects whitespace in Method and Path (containsAnyWhitespace
// uses unicode.IsSpace, which is true for both CR and LF — verified). That does
// not make this redundant: http1 is a public package, so a caller who uses it
// directly never passes through client/ at all, and the wire writer is the last
// line of defence. Note client's check does NOT catch NUL — IsSpace('\x00') is
// false — which this one does.
func TestConformance_RFC9112_Sec3_RequestLine_NotWritten(t *testing.T) {
	for _, tc := range []struct {
		desc         string
		method, path string
	}{
		{"method with CRLF", "GET /evil HTTP/1.1\r\nX-Injected: pwned\r\n\r\nGET", "/"},
		{"method with space", "GET /admin", "/"},
		{"method empty", "", "/"},
		{"target with CRLF", "GET", "/\r\nX-Injected: pwned"},
		{"target with space", "GET", "/a b"},
		{"target with NUL", "GET", "/a\x00b"},
		{"target empty", "GET", ""},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ex, capture := rawCapture(t)

			err := ex.WriteRequest(context.Background(), []hpack.HeaderField{
				{Name: []byte(":method"), Value: []byte(tc.method)},
				{Name: []byte(":path"), Value: []byte(tc.path)},
				{Name: []byte(":authority"), Value: []byte("example.com")},
			}, true)

			if wire := capture(); wire != "" {
				t.Errorf("nothing must reach the wire; got:\n%q", wire)
			}
			if !errors.Is(err, http1.ErrInvalidRequest) {
				t.Errorf("WriteRequest err = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

// TestConformance_RFC9112_Sec3_2_EmptyAuthorityEmptyHost is the boundary that
// keeps the fix from overshooting into a different bug.
//
// The rule is RFC 9112 §3.2, not RFC 9110 §7.2 — §7.2 defines what Host means
// and when a user agent must generate one, while the HTTP/1.1-specific
// obligation to send it EMPTY lives in 9112: "If the authority component is
// missing or undefined for the target URI, then a client MUST send a Host
// header field with an empty field value."
//
// So an empty :authority is not an error and must not be one: the conformant
// output is the literal line "Host: ", and a validator that rejected it would
// violate a MUST while trying to satisfy §5.5.
func TestConformance_RFC9112_Sec3_2_EmptyAuthorityEmptyHost(t *testing.T) {
	ex, capture := rawCapture(t)

	err := ex.WriteRequest(context.Background(), []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("")},
	}, true)
	if err != nil {
		t.Fatalf("empty authority must be sent, not refused (RFC 9112 §3.2); err = %v", err)
	}

	wire := capture()
	if !strings.Contains(wire, "Host: \r\n") {
		t.Errorf("want an empty Host field value %q in:\n%q", "Host: \r\n", wire)
	}
	if !strings.HasPrefix(wire, "GET / HTTP/1.1\r\n") {
		t.Errorf("request line malformed:\n%q", wire)
	}
}

// TestConformance_RFC9110_Sec5_5_LegalValuesUnaffected guards the other
// direction: the validator must not reject what §5.5 permits. field-value
// allows SP and HTAB internally, and a token name is case-insensitive per §5.1,
// so a normal request with an upper-case name and a spaced value still goes out
// intact and still gets lower-cased.
func TestConformance_RFC9110_Sec5_5_LegalValuesUnaffected(t *testing.T) {
	ex, capture := rawCapture(t)

	err := ex.WriteRequest(context.Background(), fields(
		hpack.HeaderField{Name: []byte("User-Agent"), Value: []byte("poseidon/1.0 (test)")},
		hpack.HeaderField{Name: []byte("accept"), Value: []byte("text/html, application/json;q=0.9")},
		hpack.HeaderField{Name: []byte("x-tab"), Value: []byte("a\tb")},
	), true)
	if err != nil {
		t.Fatalf("legal request refused: %v", err)
	}

	wire := capture()
	for _, want := range []string{
		"GET / HTTP/1.1\r\n",
		"Host: example.com\r\n",
		"user-agent: poseidon/1.0 (test)\r\n",
		"accept: text/html, application/json;q=0.9\r\n",
		"x-tab: a\tb\r\n",
	} {
		if !strings.Contains(wire, want) {
			t.Errorf("missing %q in:\n%q", want, wire)
		}
	}
}
