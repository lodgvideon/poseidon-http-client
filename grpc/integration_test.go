package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// startGRPCServer starts an HTTP/2 test server. The peer is net/http2 rather
// than a real gRPC server on purpose: this repository carries no third-party
// gRPC dependency, and every wire-level obligation a gRPC server has — the
// header block, DATA framing, the trailer block — is expressible with
// net/http's own trailer support.
func startGRPCServer(t *testing.T, h http.Handler) (*httptest.Server, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, der := range c.Certificate {
			if cert, err := x509.ParseCertificate(der); err == nil {
				pool.AddCert(cert)
			}
		}
	}
	return srv, &tls.Config{RootCAs: pool, ServerName: "example.com"}
}

func dialGRPC(t *testing.T, srv *httptest.Server, cfg *tls.Config) *ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc, err := Dial(ctx, srv.Listener.Addr().String(), Options{
		Conn:      conn.ConnOptions{Dialer: &conn.TLSDialer{Config: cfg}},
		Authority: "example.com",
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

// srvReadMessage reads one length-prefixed message from a request body.
func srvReadMessage(r io.Reader) ([]byte, error) {
	var hdr [prefixLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	if hdr[0] != 0 {
		return nil, errors.New("server: client set the compressed flag")
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	msg := make([]byte, n)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// srvWriteMessage writes one length-prefixed message and flushes it, so the
// client sees it as a distinct DATA frame rather than waiting for the handler
// to return.
func srvWriteMessage(w http.ResponseWriter, msg []byte) error {
	out, err := AppendMessage(nil, msg)
	if err != nil {
		return err
	}
	if _, err := w.Write(out); err != nil {
		return err
	}
	w.(http.Flusher).Flush()
	return nil
}

// srvBeginResponse writes the gRPC response header block.
func srvBeginResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/grpc")
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()
}

// srvFinish sets the grpc-status trailer. http.TrailerPrefix defers the field
// to the trailer block without pre-declaring it in the header block.
func srvFinish(w http.ResponseWriter, code Code, msg string) {
	w.Header().Set(http.TrailerPrefix+"grpc-status", fmt.Sprintf("%d", uint32(code)))
	if msg != "" {
		w.Header().Set(http.TrailerPrefix+"grpc-message", msg)
	}
}

func TestIntegration_Unary_Echo(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg, err := srvReadMessage(r.Body)
		if err != nil {
			t.Errorf("server read: %v", err)
			return
		}
		srvBeginResponse(w)
		if err := srvWriteMessage(w, append([]byte("echo:"), msg...)); err != nil {
			t.Errorf("server write: %v", err)
		}
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := cc.Invoke(ctx, "/test.Svc/Echo", []byte("ping"), nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(got) != "echo:ping" {
		t.Fatalf("response = %q", got)
	}
}

// TestIntegration_Unary_RequestShape pins the request the client puts on the
// wire: a gRPC server rejects the call outright without any one of these.
func TestIntegration_Unary_RequestShape(t *testing.T) {
	var (
		mu   sync.Mutex
		seen http.Header
		verb string
		path string
	)
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = r.Header.Clone()
		verb, path = r.Method, r.URL.Path
		mu.Unlock()
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, nil)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cc.Invoke(ctx, "/pkg.Service/Method", []byte("x"), nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if verb != "POST" {
		t.Errorf("method = %q, want POST", verb)
	}
	if path != "/pkg.Service/Method" {
		t.Errorf("path = %q", path)
	}
	if ct := seen.Get("Content-Type"); ct != "application/grpc" {
		t.Errorf("content-type = %q", ct)
	}
	if te := seen.Get("Te"); te != "trailers" {
		t.Errorf("te = %q, want trailers", te)
	}
	if ua := seen.Get("User-Agent"); ua != DefaultUserAgent {
		t.Errorf("user-agent = %q", ua)
	}
	if ae := seen.Get("Grpc-Accept-Encoding"); ae != "identity" {
		t.Errorf("grpc-accept-encoding = %q", ae)
	}
	// The context above carries a deadline, so grpc-timeout must be present
	// and must parse back to something no longer than what remains.
	to := seen.Get("Grpc-Timeout")
	if to == "" {
		t.Fatal("grpc-timeout absent although the call context had a deadline")
	}
	d, err := decodeTimeout(to)
	if err != nil {
		t.Fatalf("grpc-timeout %q: %v", to, err)
	}
	if d <= 0 || d > 6*time.Second {
		t.Fatalf("grpc-timeout = %v, outside the 5s deadline that was set", d)
	}
}

func TestIntegration_Unary_NoDeadlineOmitsTimeout(t *testing.T) {
	var (
		mu sync.Mutex
		to string
	)
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		to = r.Header.Get("Grpc-Timeout")
		mu.Unlock()
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, nil)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	if _, err := cc.Invoke(context.Background(), "/t.S/M", []byte("x"), nil); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if to != "" {
		t.Fatalf("grpc-timeout = %q, want absent for a deadline-less context", to)
	}
}

func TestIntegration_ServerStreaming(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := srvReadMessage(r.Body); err != nil {
			t.Errorf("server read: %v", err)
			return
		}
		srvBeginResponse(w)
		for i := 0; i < 3; i++ {
			if err := srvWriteMessage(w, []byte(fmt.Sprintf("chunk-%d", i))); err != nil {
				t.Errorf("server write: %v", err)
				return
			}
		}
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/test.Svc/Server", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Send(ctx, []byte("go")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := s.CloseSend(ctx); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	var got []string
	for {
		msg, err := s.Recv(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		got = append(got, string(msg))
	}
	if strings.Join(got, ",") != "chunk-0,chunk-1,chunk-2" {
		t.Fatalf("messages = %v", got)
	}
	if s.Status().Code != OK {
		t.Fatalf("status = %v", s.Status())
	}
}

// TestIntegration_EmptyServerStream covers a successful call that carries no
// messages at all: HEADERS, then trailers, and no DATA in between. The first
// Recv must report io.EOF with an OK status rather than blocking or inventing
// a failure.
func TestIntegration_EmptyServerStream(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/test.Svc/Empty", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Send(ctx, []byte("x")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := s.CloseSend(ctx); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := s.Recv(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv = %v, want io.EOF", err)
	}
	if s.Status().Code != OK {
		t.Fatalf("status = %v, want OK", s.Status())
	}
}

func TestIntegration_ClientStreaming(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var total int
		for {
			msg, err := srvReadMessage(r.Body)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Errorf("server read: %v", err)
				return
			}
			total += len(msg)
		}
		srvBeginResponse(w)
		if err := srvWriteMessage(w, []byte(fmt.Sprintf("%d", total))); err != nil {
			t.Errorf("server write: %v", err)
		}
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/test.Svc/Client", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	for i := 0; i < 4; i++ {
		if err := s.Send(ctx, []byte("abcde")); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if err := s.CloseSend(ctx); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	msg, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(msg) != "20" {
		t.Fatalf("total = %q, want 20", msg)
	}
	if _, err := s.Recv(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("second Recv = %v, want io.EOF", err)
	}
}

func TestIntegration_SendAfterCloseSend(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, nil)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/t.S/M", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.CloseSend(ctx); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if err := s.CloseSend(ctx); err != nil {
		t.Fatalf("second CloseSend = %v, want nil (idempotent)", err)
	}
	if err := s.Send(ctx, []byte("late")); !errors.Is(err, ErrSendClosed) {
		t.Fatalf("Send after CloseSend = %v, want ErrSendClosed", err)
	}
}

// TestIntegration_Bidi_Concurrent is the acid test for the claim that
// conn.Stream is genuinely full-duplex: the client writes from one goroutine
// while reading from another, and the server interleaves its replies with the
// requests rather than waiting for the request stream to end.
func TestIntegration_Bidi_Concurrent(t *testing.T) {
	const n = 20
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srvBeginResponse(w)
		for {
			msg, err := srvReadMessage(r.Body)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Errorf("server read: %v", err)
				return
			}
			if err := srvWriteMessage(w, append([]byte("re:"), msg...)); err != nil {
				t.Errorf("server write: %v", err)
				return
			}
		}
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/test.Svc/Bidi", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()

	sendErr := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if err := s.Send(ctx, []byte(fmt.Sprintf("m%d", i))); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- s.CloseSend(ctx)
	}()

	var got []string
	for {
		msg, err := s.Recv(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv after %d messages: %v", len(got), err)
		}
		got = append(got, string(msg))
	}
	if err := <-sendErr; err != nil {
		t.Fatalf("send side: %v", err)
	}
	if len(got) != n {
		t.Fatalf("received %d messages, want %d", len(got), n)
	}
	for i, m := range got {
		if want := fmt.Sprintf("re:m%d", i); m != want {
			t.Fatalf("message %d = %q, want %q", i, m, want)
		}
	}
	if s.Status().Code != OK {
		t.Fatalf("status = %v", s.Status())
	}
}

// TestIntegration_TrailersOnly is the shape most gRPC errors take: a single
// HEADERS frame carrying both :status and grpc-status, with END_STREAM set and
// no body at all. It arrives as EventHeaders, not EventTrailers, so a client
// that only looks for grpc-status in the trailer block reports the RPC as a
// success with no message.
func TestIntegration_TrailersOnly(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("grpc-status", fmt.Sprintf("%d", uint32(NotFound)))
		w.Header().Set("grpc-message", "user%20142%20not%20found")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/test.Svc/Missing", []byte("x"), nil)
	var st *Status
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %v (%T), want *Status", err, err)
	}
	if st.Code != NotFound {
		t.Fatalf("code = %v, want NOT_FOUND", st.Code)
	}
	if st.Message != "user 142 not found" {
		t.Fatalf("message = %q, want it percent-decoded", st.Message)
	}
}

func TestIntegration_NonOKStatusInTrailers(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, []byte("partial"))
		srvFinish(w, PermissionDenied, "not%20your%20row")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/test.Svc/Denied", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Send(ctx, []byte("x")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := s.CloseSend(ctx); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if msg, err := s.Recv(ctx); err != nil || string(msg) != "partial" {
		t.Fatalf("first Recv = %q, %v", msg, err)
	}
	var st *Status
	if _, err := s.Recv(ctx); !errors.As(err, &st) {
		t.Fatalf("terminal Recv = %v, want *Status", err)
	}
	if st.Code != PermissionDenied || st.Message != "not your row" {
		t.Fatalf("status = %+v", st)
	}
	if _, ok := findField(s.Trailer(), "grpc-status"); !ok {
		t.Fatal("Trailer() does not expose grpc-status")
	}
}

// TestIntegration_HTTPStatusMapping covers the case a proxy produces: an HTTP
// error with no gRPC framing anywhere in the response.
func TestIntegration_HTTPStatusMapping(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no such route", http.StatusNotFound)
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/test.Svc/Nope", []byte("x"), nil)
	var st *Status
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %v (%T), want *Status", err, err)
	}
	if st.Code != Unimplemented {
		t.Fatalf("code = %v, want UNIMPLEMENTED for HTTP 404", st.Code)
	}
}

// TestIntegration_MissingGRPCStatus pins that a 200 response that ends without
// a grpc-status is an INTERNAL failure rather than a silent success — the
// difference between a load generator reporting a broken backend and reporting
// a clean run.
func TestIntegration_MissingGRPCStatus(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, []byte("body"))
		// Handler returns without setting any trailer.
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/test.Svc/Sloppy", []byte("x"), nil)
	var st *Status
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %v (%T), want *Status", err, err)
	}
	// The mapping table's 200 row is UNKNOWN, not INTERNAL: a truly successful
	// response would have carried a grpc-status, so a 200 without one leaves
	// the client with no idea what happened.
	if st.Code != Unknown {
		t.Fatalf("code = %v, want UNKNOWN", st.Code)
	}
}

func TestIntegration_BadContentType(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("<html>"))
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/test.Svc/Html", []byte("x"), nil)
	var st *Status
	if !errors.As(err, &st) || st.Code != Internal {
		t.Fatalf("Invoke error = %v, want INTERNAL for a non-gRPC content-type", err)
	}
}

func TestIntegration_ContentTypeSubtypeAccepted(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		w.Header().Set("Content-Type", "application/grpc+proto")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_ = srvWriteMessage(w, []byte("ok"))
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := cc.Invoke(ctx, "/test.Svc/Proto", []byte("x"), nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("response = %q", got)
	}
}

func TestIntegration_MetadataRoundTrip(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("x-echo-id", r.Header.Get("X-Request-Id"))
		w.Header().Set("x-echo-bin", r.Header.Get("Trace-Bin"))
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_ = srvWriteMessage(w, nil)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	md, err := AppendMetadata(nil, "x-request-id", []byte("req-7"))
	if err != nil {
		t.Fatalf("AppendMetadata: %v", err)
	}
	md, err = AppendMetadata(md, "trace-bin", []byte{0xde, 0xad})
	if err != nil {
		t.Fatalf("AppendMetadata: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/test.Svc/Meta", md)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Send(ctx, []byte("x")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := s.CloseSend(ctx); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	hdr, err := s.Header(ctx)
	if err != nil {
		t.Fatalf("Header: %v", err)
	}
	if v, ok, err := MetadataValue(hdr, "x-echo-id"); !ok || err != nil || string(v) != "req-7" {
		t.Fatalf("echoed id = %q ok=%v err=%v", v, ok, err)
	}
	// The server echoed the base64 the client put on the wire back under a key
	// that also ends in -bin, so reading it decodes and completes the round
	// trip: the original bytes come back byte-for-byte.
	if v, ok, err := MetadataValue(hdr, "x-echo-bin"); !ok || err != nil || string(v) != "\xde\xad" {
		t.Fatalf("echoed binary = % x ok=%v err=%v, want de ad", v, ok, err)
	}
}

func TestIntegration_NewStream_RejectsBadMethod(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	if _, err := cc.NewStream(context.Background(), "test.Svc/M", nil); !errors.Is(err, ErrBadMethod) {
		t.Fatalf("NewStream(no leading slash) = %v, want ErrBadMethod", err)
	}
	if _, err := cc.NewStream(context.Background(), "/t.S/M", []conn.HeaderField{
		{Name: []byte("content-type"), Value: []byte("text/plain")},
	}); !errors.Is(err, ErrReservedMetadata) {
		t.Fatalf("NewStream(reserved metadata) = %v, want ErrReservedMetadata", err)
	}
}

// TestIntegration_ContextCancelStopsRecv pins that cancelling the call context
// unblocks a Recv that is waiting on a server which never answers.
func TestIntegration_ContextCancelStopsRecv(t *testing.T) {
	release := make(chan struct{})
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		<-release
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	defer close(release)
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	s, err := cc.NewStream(ctx, "/test.Svc/Hang", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Send(ctx, []byte("x")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := s.CloseSend(ctx); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	if _, err := s.Recv(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recv = %v, want context.Canceled", err)
	}
	// The failure is sticky: a second Recv must not resume a broken stream.
	if _, err := s.Recv(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("second Recv = %v, want the recorded context.Canceled", err)
	}
}

// TestIntegration_ConcurrentStreams runs many calls over one ClientConn, which
// is the whole point of the type: gRPC multiplexes rather than pools.
func TestIntegration_ConcurrentStreams(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg, err := srvReadMessage(r.Body)
		if err != nil {
			t.Errorf("server read: %v", err)
			return
		}
		srvBeginResponse(w)
		_ = srvWriteMessage(w, msg)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	const n = 32
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			want := fmt.Sprintf("payload-%d", i)
			got, err := cc.Invoke(ctx, "/test.Svc/Echo", []byte(want), nil)
			if err != nil {
				errs <- fmt.Errorf("call %d: %w", i, err)
				return
			}
			if string(got) != want {
				errs <- fmt.Errorf("call %d: got %q want %q", i, got, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestIntegration_LargeMessageSpansFrames drives a message past both the
// 16 KiB default MAX_FRAME_SIZE and the 64 KiB initial flow-control window, so
// the reassembly path and the send-credit path are both exercised.
func TestIntegration_LargeMessageSpansFrames(t *testing.T) {
	payload := make([]byte, 512<<10)
	for i := range payload {
		payload[i] = byte(i)
	}
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg, err := srvReadMessage(r.Body)
		if err != nil {
			t.Errorf("server read: %v", err)
			return
		}
		srvBeginResponse(w)
		_ = srvWriteMessage(w, msg)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got, err := cc.Invoke(ctx, "/test.Svc/Big", payload, nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("len = %d, want %d", len(got), len(payload))
	}
	for i := range got {
		if got[i] != payload[i] {
			t.Fatalf("byte %d = %d, want %d", i, got[i], payload[i])
		}
	}
}

// TestIntegration_MaxRecvMessageSize pins that the limit is enforced against
// the peer's declared length, not against what has already been buffered.
func TestIntegration_MaxRecvMessageSize(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, make([]byte, 4096))
		srvFinish(w, OK, "")
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc, err := Dial(ctx, srv.Listener.Addr().String(), Options{
		Conn:               conn.ConnOptions{Dialer: &conn.TLSDialer{Config: cfg}},
		Authority:          "example.com",
		MaxRecvMessageSize: 1024,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = cc.Close() }()

	if _, err := cc.Invoke(ctx, "/test.Svc/Big", []byte("x"), nil); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Invoke = %v, want ErrMessageTooLarge", err)
	}
}

// TestIntegration_UnaryRejectsSecondMessage pins that Invoke does not silently
// drop a message a misbehaving server appends after the first.
func TestIntegration_UnaryRejectsSecondMessage(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, []byte("first"))
		_ = srvWriteMessage(w, []byte("second"))
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/test.Svc/Chatty", []byte("x"), nil)
	var st *Status
	if !errors.As(err, &st) || st.Code != Internal {
		t.Fatalf("Invoke = %v, want an INTERNAL status", err)
	}
}

// srvAnswerWithoutDraining writes a complete response and returns without ever
// reading the request body. net/http2's server then follows RFC 9113 §8.1 and
// resets the still-open request stream with NO_ERROR — "stop sending, I have
// already answered". grpc-go's server does the same. It is the shape of every
// unary handler that ignores its input, and #337 is what the client used to
// make of it.
func srvAnswerWithoutDraining(code Code, msg string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		srvBeginResponse(w)
		if code == OK {
			_ = srvWriteMessage(w, []byte("answer"))
		}
		srvFinish(w, code, msg)
	})
}

// benignResetUploadBuf pins the peer's upload window rather than inheriting
// net/http2's 1 MiB default, so the mechanism below is the one actually at
// work. Measured on Go 1.26 against the default window, a 64 KiB body — a
// sixteenth of it — still reproduced the bug 20/20, and a 2 MiB body still did
// so with the window forced to 32 MiB: the default-sized version was winning a
// timing race, not exhausting credit, and would have degraded silently.
const benignResetUploadBuf = 64 << 10

// benignResetRequest is twice the pinned window, so the upload parks in conn's
// acquireSendCredits on credit a handler that never reads the body cannot
// refund, and only the server's RST_STREAM(NO_ERROR) wakes it. That turns the
// ~9%-under-load window of #337 into a one-iteration reproduction.
const benignResetRequest = 2 * benignResetUploadBuf

// startNoDrainServer is startGRPCServer with the pinned upload window.
func startNoDrainServer(t *testing.T, h http.Handler) (*httptest.Server, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.Config.HTTP2 = &http.HTTP2Config{
		MaxReceiveBufferPerStream:     benignResetUploadBuf,
		MaxReceiveBufferPerConnection: benignResetUploadBuf,
	}
	srv.StartTLS()
	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, der := range c.Certificate {
			if cert, err := x509.ParseCertificate(der); err == nil {
				pool.AddCert(cert)
			}
		}
	}
	return srv, &tls.Config{RootCAs: pool, ServerName: "example.com"}
}

// TestConformance_RFC9113_Sec8_1_BenignResetKeepsResponse pins RFC 9113 §8.1: a
// RST_STREAM(NO_ERROR) that follows a complete response is a request to stop
// sending, not a failed call. conn closes the stream on it per §5.1, so the
// send side fails — and Invoke must still return the answer already buffered
// on the receive side rather than the send-side error.
func TestConformance_RFC9113_Sec8_1_BenignResetKeepsResponse(t *testing.T) {
	srv, cfg := startNoDrainServer(t, srvAnswerWithoutDraining(OK, ""))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := cc.Invoke(ctx, "/test.Svc/NoDrain", make([]byte, benignResetRequest), nil)
	if err != nil {
		t.Fatalf("Invoke = %q, %v; want the response the server already sent", got, err)
	}
	if string(got) != "answer" {
		t.Fatalf("Invoke = %q, want %q", got, "answer")
	}
}

// TestConformance_RFC9113_Sec8_1_BenignResetKeepsNonOKStatus is the other half: tolerating the
// benign reset must not launder a failed call into a success, nor replace the
// server's own diagnosis with the reset's INTERNAL mapping.
func TestConformance_RFC9113_Sec8_1_BenignResetKeepsNonOKStatus(t *testing.T) {
	srv, cfg := startNoDrainServer(t, srvAnswerWithoutDraining(NotFound, "no such row"))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/test.Svc/NoDrain", make([]byte, benignResetRequest), nil)
	var st *Status
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %v (%T), want *Status", err, err)
	}
	if st.Code != NotFound || st.Message != "no such row" {
		t.Fatalf("status = %v %q, want NOT_FOUND %q", st.Code, st.Message, "no such row")
	}
}

// TestConformance_RFC9113_Sec8_1_CloseSendAfterBenignReset covers the same
// defect through the streaming API, where the caller drives the half-close
// itself.
//
// The barrier is draining the response, not a sleep. Draining to io.EOF
// consumes the trailers, and the RST_STREAM follows them on the wire, so by
// the time Recv reports EOF the reader goroutine has already latched the
// stream closed — CloseSend is guaranteed to meet it. An earlier version slept
// 200 ms instead, which could only fail *silently*: measured with the guard
// reverted, a 0 ms sleep exercised it 0/50 times while still passing.
func TestConformance_RFC9113_Sec8_1_CloseSendAfterBenignReset(t *testing.T) {
	srv, cfg := startNoDrainServer(t, srvAnswerWithoutDraining(OK, ""))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/test.Svc/NoDrain", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()

	if err := s.Send(ctx, []byte("x")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	msg, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(msg) != "answer" {
		t.Fatalf("Recv = %q, want %q", msg, "answer")
	}
	if _, err := s.Recv(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("second Recv = %v, want io.EOF", err)
	}
	if st := s.Status(); st.Code != OK {
		t.Fatalf("status = %v, want OK", st.Code)
	}
	if err := s.CloseSend(ctx); err != nil {
		t.Fatalf("CloseSend after a benign reset = %v, want nil", err)
	}
	// The scope guard. Half-closing a stream the peer has torn down is a no-op
	// and reporting it would make callers discard the response; delivering a
	// MESSAGE to that stream is not, and must still fail. Widening
	// benignHalfClose to cover Send passed the whole grpc suite before this.
	if err := s.Send(ctx, []byte("late")); err == nil {
		t.Fatal("Send after a benign reset = nil; a message that never reached the server must be an error")
	}
}

// TestConformance_RFC9113_Sec8_1_SendAfterBenignResetStillFails is the scope
// guard that _CloseSendAfterBenignReset cannot provide. There the late Send
// meets an already-latched sendErr and fails before reaching the wire, so
// widening benignHalfClose to cover Send goes unnoticed. Here nothing has been
// sent yet — the handler answers off the HEADERS frame alone — so the first
// Send is the one whose own SendData meets the closed stream.
//
// Half-closing a stream the peer has torn down is a no-op and reporting it
// would make callers discard the response §8.1 says they must keep. Handing a
// MESSAGE to that stream is not a no-op: it never reaches the server, and
// telling the caller otherwise would silently drop it.
func TestConformance_RFC9113_Sec8_1_SendAfterBenignResetStillFails(t *testing.T) {
	srv, cfg := startNoDrainServer(t, srvAnswerWithoutDraining(OK, ""))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/test.Svc/NoDrain", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()

	// Drain to io.EOF without ever sending: the reset follows the trailers on
	// the wire, so the stream is latched closed by the time EOF is reported.
	if _, err := s.Recv(ctx); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if _, err := s.Recv(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("second Recv = %v, want io.EOF", err)
	}
	if err := s.Send(ctx, []byte("late")); err == nil {
		t.Fatal("Send after a benign reset = nil; the message never reached the server")
	}
}
