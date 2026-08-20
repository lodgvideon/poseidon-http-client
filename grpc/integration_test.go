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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err, "Dial")
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
		if !assert.NoError(t, err, "server read") {
			return
		}
		srvBeginResponse(w)
		assert.NoError(t, srvWriteMessage(w, append([]byte("echo:"), msg...)), "server write")
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := cc.Invoke(ctx, "/test.Svc/Echo", []byte("ping"), nil)

	require.NoError(t, err, "Invoke")
	require.Equalf(t, "echo:ping", string(got), "response = %q", got)
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

	_, err := cc.Invoke(ctx, "/pkg.Service/Method", []byte("x"), nil)

	require.NoError(t, err, "Invoke")
	mu.Lock()
	defer mu.Unlock()
	assert.Equalf(t, "POST", verb, "method = %q, want POST", verb)
	assert.Equalf(t, "/pkg.Service/Method", path, "path = %q", path)
	assert.Equalf(t, "application/grpc", seen.Get("Content-Type"),
		"content-type = %q", seen.Get("Content-Type"))
	assert.Equalf(t, "trailers", seen.Get("Te"), "te = %q, want trailers", seen.Get("Te"))
	assert.Equalf(t, DefaultUserAgent, seen.Get("User-Agent"),
		"user-agent = %q", seen.Get("User-Agent"))
	assert.Equalf(t, "identity", seen.Get("Grpc-Accept-Encoding"),
		"grpc-accept-encoding = %q", seen.Get("Grpc-Accept-Encoding"))
	// The context above carries a deadline, so grpc-timeout must be present
	// and must parse back to something no longer than what remains.
	to := seen.Get("Grpc-Timeout")
	require.NotEmpty(t, to, "grpc-timeout absent although the call context had a deadline")
	d, err := decodeTimeout(to)
	require.NoErrorf(t, err, "grpc-timeout %q", to)
	require.Truef(t, d > 0 && d <= 6*time.Second,
		"grpc-timeout = %v, outside the 5s deadline that was set", d)
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

	_, err := cc.Invoke(context.Background(), "/t.S/M", []byte("x"), nil)

	require.NoError(t, err, "Invoke")
	mu.Lock()
	defer mu.Unlock()
	require.Emptyf(t, to, "grpc-timeout = %q, want absent for a deadline-less context", to)
}

func TestIntegration_ServerStreaming(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := srvReadMessage(r.Body); !assert.NoError(t, err, "server read") {
			return
		}
		srvBeginResponse(w)
		for i := 0; i < 3; i++ {
			if err := srvWriteMessage(w, []byte(fmt.Sprintf("chunk-%d", i))); !assert.NoError(t, err, "server write") {
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
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	require.NoError(t, s.Send(ctx, []byte("go")), "Send")
	require.NoError(t, s.CloseSend(ctx), "CloseSend")
	var got []string
	for {
		msg, err := s.Recv(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "Recv")
		got = append(got, string(msg))
	}

	require.Equalf(t, "chunk-0,chunk-1,chunk-2", strings.Join(got, ","), "messages = %v", got)
	require.Equalf(t, OK, s.Status().Code, "status = %v", s.Status())
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
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	require.NoError(t, s.Send(ctx, []byte("x")), "Send")
	require.NoError(t, s.CloseSend(ctx), "CloseSend")
	_, recvErr := s.Recv(ctx)

	require.ErrorIsf(t, recvErr, io.EOF, "Recv = %v, want io.EOF", recvErr)
	require.Equalf(t, OK, s.Status().Code, "status = %v, want OK", s.Status())
}

func TestIntegration_ClientStreaming(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var total int
		for {
			msg, err := srvReadMessage(r.Body)
			if errors.Is(err, io.EOF) {
				break
			}
			if !assert.NoError(t, err, "server read") {
				return
			}
			total += len(msg)
		}
		srvBeginResponse(w)
		assert.NoError(t, srvWriteMessage(w, []byte(fmt.Sprintf("%d", total))), "server write")
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/test.Svc/Client", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	for i := 0; i < 4; i++ {
		require.NoErrorf(t, s.Send(ctx, []byte("abcde")), "Send %d", i)
	}
	require.NoError(t, s.CloseSend(ctx), "CloseSend")
	msg, err := s.Recv(ctx)

	require.NoError(t, err, "Recv")
	require.Equalf(t, "20", string(msg), "total = %q, want 20", msg)
	_, err = s.Recv(ctx)
	require.ErrorIsf(t, err, io.EOF, "second Recv = %v, want io.EOF", err)
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
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	first := s.CloseSend(ctx)
	second := s.CloseSend(ctx)
	lateSend := s.Send(ctx, []byte("late"))

	require.NoError(t, first, "CloseSend")
	require.NoErrorf(t, second, "second CloseSend = %v, want nil (idempotent)", second)
	require.ErrorIsf(t, lateSend, ErrSendClosed,
		"Send after CloseSend = %v, want ErrSendClosed", lateSend)
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
			if !assert.NoError(t, err, "server read") {
				return
			}
			if err := srvWriteMessage(w, append([]byte("re:"), msg...)); !assert.NoError(t, err, "server write") {
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
	require.NoError(t, err, "NewStream")
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
		require.NoErrorf(t, err, "Recv after %d messages", len(got))
		got = append(got, string(msg))
	}

	require.NoError(t, <-sendErr, "send side")
	require.Lenf(t, got, n, "received %d messages, want %d", len(got), n)
	for i, m := range got {
		require.Equalf(t, fmt.Sprintf("re:m%d", i), m, "message %d = %q", i, m)
	}
	require.Equalf(t, OK, s.Status().Code, "status = %v", s.Status())
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
	require.Truef(t, errors.As(err, &st), "Invoke error = %v (%T), want *Status", err, err)
	require.Equalf(t, NotFound, st.Code, "code = %v, want NOT_FOUND", st.Code)
	require.Equalf(t, "user 142 not found", st.Message,
		"message = %q, want it percent-decoded", st.Message)
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
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	require.NoError(t, s.Send(ctx, []byte("x")), "Send")
	require.NoError(t, s.CloseSend(ctx), "CloseSend")
	msg, err := s.Recv(ctx)
	require.NoErrorf(t, err, "first Recv = %q, %v", msg, err)
	require.Equalf(t, "partial", string(msg), "first Recv = %q", msg)
	_, terminalErr := s.Recv(ctx)

	var st *Status
	require.Truef(t, errors.As(terminalErr, &st), "terminal Recv = %v, want *Status", terminalErr)
	require.Truef(t, st.Code == PermissionDenied && st.Message == "not your row", "status = %+v", st)
	_, ok := findField(s.Trailer(), "grpc-status")
	require.True(t, ok, "Trailer() does not expose grpc-status")
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
	require.Truef(t, errors.As(err, &st), "Invoke error = %v (%T), want *Status", err, err)
	require.Equalf(t, Unimplemented, st.Code, "code = %v, want UNIMPLEMENTED for HTTP 404", st.Code)
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
	require.Truef(t, errors.As(err, &st), "Invoke error = %v (%T), want *Status", err, err)
	// The mapping table's 200 row is UNKNOWN, not INTERNAL: a truly successful
	// response would have carried a grpc-status, so a 200 without one leaves
	// the client with no idea what happened.
	require.Equalf(t, Unknown, st.Code, "code = %v, want UNKNOWN", st.Code)
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
	require.Truef(t, errors.As(err, &st) && st.Code == Internal,
		"Invoke error = %v, want INTERNAL for a non-gRPC content-type", err)
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

	require.NoError(t, err, "Invoke")
	require.Equalf(t, "ok", string(got), "response = %q", got)
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
	require.NoError(t, err, "AppendMetadata")
	md, err = AppendMetadata(md, "trace-bin", []byte{0xde, 0xad})
	require.NoError(t, err, "AppendMetadata")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := cc.NewStream(ctx, "/test.Svc/Meta", md)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	require.NoError(t, s.Send(ctx, []byte("x")), "Send")
	require.NoError(t, s.CloseSend(ctx), "CloseSend")
	hdr, err := s.Header(ctx)

	require.NoError(t, err, "Header")
	v, ok, verr := MetadataValue(hdr, "x-echo-id")
	require.Truef(t, ok && verr == nil && string(v) == "req-7",
		"echoed id = %q ok=%v err=%v", v, ok, verr)
	// The server echoed the base64 the client put on the wire back under a key
	// that also ends in -bin, so reading it decodes and completes the round
	// trip: the original bytes come back byte-for-byte.
	v, ok, verr = MetadataValue(hdr, "x-echo-bin")
	require.Truef(t, ok && verr == nil && string(v) == "\xde\xad",
		"echoed binary = % x ok=%v err=%v, want de ad", v, ok, verr)
}

func TestIntegration_NewStream_RejectsBadMethod(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	_, badMethodErr := cc.NewStream(context.Background(), "test.Svc/M", nil)
	_, reservedErr := cc.NewStream(context.Background(), "/t.S/M", []conn.HeaderField{
		{Name: []byte("content-type"), Value: []byte("text/plain")},
	})

	require.ErrorIsf(t, badMethodErr, ErrBadMethod,
		"NewStream(no leading slash) = %v, want ErrBadMethod", badMethodErr)
	require.ErrorIsf(t, reservedErr, ErrReservedMetadata,
		"NewStream(reserved metadata) = %v, want ErrReservedMetadata", reservedErr)
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
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	require.NoError(t, s.Send(ctx, []byte("x")), "Send")
	require.NoError(t, s.CloseSend(ctx), "CloseSend")
	// The second Recv gets a context of its own, and a bounded one. Its purpose
	// is to observe the grpc-level latch, and the latch is read before pump is
	// entered — so on healthy code this deadline is never approached. Without a
	// deadline the same call, on a stream whose latch had been lost, would go
	// back into pump and block on a server that is still holding the request
	// open: the regression would surface as `panic: test timed out` minutes
	// later instead of as this test's own assertion. That is how it surfaced the
	// first time.
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer secondCancel()

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	_, recvErr := s.Recv(ctx)
	_, secondErr := s.Recv(secondCtx)

	require.ErrorIsf(t, recvErr, context.Canceled, "Recv = %v, want context.Canceled", recvErr)
	// The failure is sticky: a second Recv must not resume a broken stream.
	require.ErrorIsf(t, secondErr, context.Canceled,
		"second Recv = %v, want the recorded context.Canceled", secondErr)
	// Name the mechanism. errors.Is alone does not distinguish the grpc latch
	// from conn re-reporting its own latched failure a second time, and conn
	// does latch — so that assertion holds with Stream.fail removed entirely.
	// Identity does distinguish them: fail stores one *Status and hands the same
	// value back to every later Recv, whereas a fresh trip through pump builds a
	// new one out of statusFromTransport.
	require.Truef(t, secondErr == recvErr,
		"second Recv returned a different error value (%v) from the first (%v) — it "+
			"re-entered pump and re-derived the failure instead of reporting the "+
			"one Stream.fail recorded", secondErr, recvErr)
}

// TestIntegration_ConcurrentStreams runs many calls over one ClientConn, which
// is the whole point of the type: gRPC multiplexes rather than pools.
func TestIntegration_ConcurrentStreams(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg, err := srvReadMessage(r.Body)
		if !assert.NoError(t, err, "server read") {
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
	errs := make(chan error, n)

	var wg sync.WaitGroup
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
		assert.NoError(t, err, "a concurrent call on the shared connection failed")
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
		if !assert.NoError(t, err, "server read") {
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

	require.NoError(t, err, "Invoke")
	require.Lenf(t, got, len(payload), "len = %d, want %d", len(got), len(payload))
	for i := range got {
		require.Equalf(t, payload[i], got[i], "byte %d = %d, want %d", i, got[i], payload[i])
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
	require.NoError(t, err, "Dial")
	defer func() { _ = cc.Close() }()

	_, err = cc.Invoke(ctx, "/test.Svc/Big", []byte("x"), nil)

	require.ErrorIsf(t, err, ErrMessageTooLarge, "Invoke = %v, want ErrMessageTooLarge", err)
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
	require.Truef(t, errors.As(err, &st) && st.Code == Internal,
		"Invoke = %v, want an INTERNAL status", err)
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

	require.NoErrorf(t, err, "Invoke = %q, %v; want the response the server already sent", got, err)
	require.Equalf(t, "answer", string(got), "Invoke = %q, want %q", got, "answer")
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
	require.Truef(t, errors.As(err, &st), "Invoke error = %v (%T), want *Status", err, err)
	require.Truef(t, st.Code == NotFound && st.Message == "no such row",
		"status = %v %q, want NOT_FOUND %q", st.Code, st.Message, "no such row")
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
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	// The benign reset can beat this first Send to the stream: the no-drain
	// server answers off the HEADERS alone and RSTs, and if that RST lands
	// first the Send legitimately returns conn.ErrStreamClosed — the very §8.1
	// case this test is about. Demanding the Send succeed made the test race
	// the server (it flaked on loaded Linux CI). Either outcome is fine; the
	// response is buffered before the reset and recoverable through Recv below.
	sendErr := s.Send(ctx, []byte("x"))
	msg, recvErr := s.Recv(ctx)
	_, eofErr := s.Recv(ctx)
	closeSendErr := s.CloseSend(ctx)
	// The scope guard. Half-closing a stream the peer has torn down is a no-op
	// and reporting it would make callers discard the response; delivering a
	// MESSAGE to that stream is not, and must still fail. Widening
	// benignHalfClose to cover Send passed the whole grpc suite before this.
	lateSendErr := s.Send(ctx, []byte("late"))

	require.Truef(t, sendErr == nil || errors.Is(sendErr, conn.ErrStreamClosed), "Send: %v", sendErr)
	require.NoError(t, recvErr, "Recv")
	require.Equalf(t, "answer", string(msg), "Recv = %q, want %q", msg, "answer")
	require.ErrorIsf(t, eofErr, io.EOF, "second Recv = %v, want io.EOF", eofErr)
	require.Equalf(t, OK, s.Status().Code, "status = %v, want OK", s.Status().Code)
	require.NoErrorf(t, closeSendErr, "CloseSend after a benign reset = %v, want nil", closeSendErr)
	require.Error(t, lateSendErr,
		"Send after a benign reset = nil; a message that never reached the server must be an error")
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
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	// Drain to io.EOF without ever sending. EOF reports END_STREAM being
	// CONSUMED; the peer's RST_STREAM is a separate frame that follows it, so EOF
	// says nothing about the stream being latched closed yet. Inferring that was
	// the race in #709 — on a loaded runner the Send below reached a stream the
	// reader had not torn down, and succeeded.
	_, firstErr := s.Recv(ctx)
	require.NoError(t, firstErr, "Recv")
	_, eofErr := s.Recv(ctx)
	require.ErrorIsf(t, eofErr, io.EOF, "second Recv = %v, want io.EOF", eofErr)
	// Wait for the reset itself, on the transport stream underneath — grpc's own
	// Recv stopped at the trailers, so the reset event is still queued or still
	// in flight, and this blocks until the reader delivers it.
	//
	// That is enough to order the Send, and the reason is a single mutex section:
	// conn's endWithReset (conn/stream.go) pushes this event AND sets the
	// stream's closed flag while holding s.mu, releasing it only after both,
	// while sendData takes the same s.mu before testing closed. So a Send issued
	// after this event is observed cannot slip in front of the flag — at worst it
	// blocks on the mutex the reader still holds and sees closed the instant it
	// is released. Waiting on EOF had no such ordering.
	ev, err := s.s.Recv(ctx)
	require.NoError(t, err, "waiting for the peer's RST_STREAM")
	require.Equalf(t, conn.EventReset, ev.Type,
		"stream event = %v, want EventReset (the benign reset that follows the trailers)", ev.Type)

	lateSendErr := s.Send(ctx, []byte("late"))

	require.Error(t, lateSendErr,
		"Send after a benign reset = nil; the message never reached the server")
}

// srvNonOKWithGRPCStatus writes a non-200 response whose header block carries
// grpc-status: v. Real gRPC servers never do this — the protocol fixes
// ":status 200" for every response — so it models a broken intermediary, or a
// peer choosing its own values.
func srvNonOKWithGRPCStatus(status int, v string, trailersOnly bool, body []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("grpc-status", v)
		w.WriteHeader(status)
		if trailersOnly {
			return
		}
		w.(http.Flusher).Flush()
		if len(body) > 0 {
			_, _ = w.Write(body)
		}
	})
}

// TestConformance_GRPC_NonOKStatusCannotReportOK pins that a response the
// client has already classified as failed cannot declare itself successful.
//
// The preference for a grpc-status found in a non-200 block is deliberate and
// stays: the status-mapping table is defined for use "only for clients that
// received a response that did not include grpc-status. If grpc-status was
// provided, it must be used", and grpc-java's HTTP-error path puts one there.
// But that rule settles whose diagnosis wins, not whether a diagnosis may
// contradict the transport it arrived on. OK is the one value where honouring
// it manufactures a success: the body has already been dropped on this path,
// so the caller received io.EOF with zero messages — an empty success it
// cannot tell from a real one, on a value the peer chose.
//
// Both spellings of the response are covered. The Trailers-Only shape reaches
// finish from a different branch of onHeaders, before the non-200 check runs
// at all, and "00" parses to OK as surely as "0".
func TestConformance_GRPC_NonOKStatusCannotReportOK(t *testing.T) {
	for _, tc := range []struct {
		name         string
		httpStatus   int
		grpcStatus   string
		trailersOnly bool
		body         []byte
		wantCode     Code
	}{
		{"header block, 500", 500, "0", false, []byte("ignored"), Unknown},
		{"trailers-only, 500", 500, "0", true, nil, Unknown},
		{"padded zero", 500, "00", false, nil, Unknown},
		{"404 maps to UNIMPLEMENTED", 404, "0", false, nil, Unimplemented},
		// The over-rejection guards: a non-OK grpc-status on a non-200 must
		// still win over the table, and a 200 carrying grpc-status OK is the
		// ordinary successful call.
		{"non-OK diagnosis still wins", 400, "8", false, nil, ResourceExhausted},
		{"200 with OK is untouched", 200, "0", true, nil, OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, cfg := startGRPCServer(t, srvNonOKWithGRPCStatus(tc.httpStatus, tc.grpcStatus, tc.trailersOnly, tc.body))
			defer srv.Close()
			cc := dialGRPC(t, srv, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s, err := cc.NewStream(ctx, "/test.Svc/M", nil)
			require.NoError(t, err, "NewStream")
			defer func() { _ = s.Close() }()

			require.NoError(t, s.CloseSend(ctx), "CloseSend")
			for {
				if _, rerr := s.Recv(ctx); rerr != nil {
					break
				}
			}

			require.Equalf(t, tc.wantCode, s.Status().Code,
				"status = %v, want %v (HTTP %d, grpc-status %q)",
				s.Status().Code, tc.wantCode, tc.httpStatus, tc.grpcStatus)
		})
	}
}
