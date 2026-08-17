package conn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startH2TestServer(t *testing.T, h http.Handler) (*httptest.Server, *tls.Config) {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	pool := x509.NewCertPool()
	for _, c := range srv.TLS.Certificates {
		for _, certDER := range c.Certificate {
			cert, err := x509.ParseCertificate(certDER)
			if err == nil {
				pool.AddCert(cert)
			}
		}
	}
	return srv, &tls.Config{RootCAs: pool, ServerName: "example.com"}
}

func dialServer(t *testing.T, srv *httptest.Server, cfg *tls.Config) *Conn {
	t.Helper()
	return dialServerOpts(t, srv, cfg, 0)
}

// dialServerOpts is dialServer with an explicit per-stream event-buffer size.
// A test that reads a body larger than a few frames needs one: with the default
// buffer, a reader that gets ahead of the test's drain loop overflows the
// channel, and the connection then cancels its own stream by design. Under
// coverage instrumentation the drain loop is slow enough for that to happen.
func dialServerOpts(t *testing.T, srv *httptest.Server, cfg *tls.Config, eventBuf int) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer:            &TLSDialer{Config: cfg},
		StreamEventBuffer: eventBuf,
	})
	require.NoErrorf(t, err, "Dial")
	return c
}

// drainBody reads a stream to its end and returns the concatenated DATA.
//
// It distinguishes a reset from a clean end, which the bare `if ev.EndStream {
// break }` loop most tests use cannot: the connection sets EndStream on the
// EventReset it delivers when the event channel overflows, so that loop exits
// as if the body were complete and the test then reports a byte-count mismatch.
// That reads as data corruption when it is really "this test fell behind the
// reader" — the failure it produced on CI cost a full investigation to tell
// apart from a flow-control bug.
func drainBody(ctx context.Context, t *testing.T, s StreamRef) []byte {
	t.Helper()
	var got []byte
	for {
		ev, err := s.Recv(ctx)
		require.NoErrorf(t, err, "Recv after %d bytes", len(got))
		require.NotEqualf(t, EventReset, ev.Type,
			"stream reset with code %d after %d bytes — not a short body; "+
				"if this is RSTCode 8 (CANCEL) the event buffer overflowed and this "+
				"test needs a larger StreamEventBuffer", ev.RSTCode, len(got))
		if ev.Type == EventData {
			got = append(got, ev.Data...)
		}
		if ev.EndStream {
			return got
		}
	}
}

func TestIntegration_EmptyGET(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	require.NoErrorf(t, err, "NewStream")

	err = s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true)

	require.NoErrorf(t, err, "SendHeaders")
	ev, err := s.Recv(ctx)
	require.NoErrorf(t, err, "Recv")
	require.Equalf(t, EventHeaders, ev.Type, "type = %v", ev.Type)
	var status string
	for _, f := range ev.Headers {
		if string(f.Name) == ":status" {
			status = string(f.Value)
		}
	}
	assert.Equalf(t, "204", status, "status = %q, want 204", status)
	if !ev.EndStream {
		// Empty 204 may close immediately or via separate end frame.
		ev2, err2 := s.Recv(ctx)
		require.NoErrorf(t, err2, "Recv 2")
		require.True(t, ev2.EndStream, "never observed END_STREAM")
	}
}

func TestIntegration_POST_1KB_Echo(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	require.NoErrorf(t, err, "NewStream")
	body := make([]byte, 1024)
	for i := range body {
		body[i] = byte(i)
	}
	require.NoErrorf(t, s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/echo")},
		{Name: []byte("content-length"), Value: []byte("1024")},
	}, false), "SendHeaders")

	err = s.SendData(ctx, body, true)

	require.NoErrorf(t, err, "SendData")
	// Drain events until we see EndStream.
	var got []byte
	for {
		ev, rerr := s.Recv(ctx)
		require.NoErrorf(t, rerr, "Recv")
		if ev.Type == EventData {
			got = append(got, ev.Data...)
		}
		if ev.EndStream {
			break
		}
	}
	require.Lenf(t, got, len(body), "echo len = %d, want %d", len(got), len(body))
}

func TestIntegration_ContextCancel_TearsDownStream(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// Block until client cancels — the request context is cancelled
		// when the underlying stream is reset.
		<-r.Context().Done()
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()
	outerCtx, outerCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer outerCancel()
	streamCtx, streamCancel := context.WithCancel(outerCtx)
	defer streamCancel()
	s, err := c.NewStream(outerCtx)
	require.NoErrorf(t, err, "NewStream")
	require.NoErrorf(t, s.SendHeaders(outerCtx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/never")},
	}, true), "SendHeaders")
	go func() {
		time.Sleep(100 * time.Millisecond)
		streamCancel()
	}()

	_, recvErr := s.Recv(streamCtx)

	require.ErrorIsf(t, recvErr, context.Canceled, "Recv err = %v, want context.Canceled", recvErr)
	_ = s.Close() // sends RST_STREAM(CANCEL); subsequent NewStream should work
	s2, err := c.NewStream(outerCtx)
	require.NoErrorf(t, err, "NewStream after cancel")
	_ = s2.Close()
}

// TestConformance_RFC7540_Sec6_5_2_MaxFrameSize_FramerReadLimit verifies that
// when the client advertises SETTINGS_MAX_FRAME_SIZE > 16384, the Framer's
// read limit is raised to match, so the peer can send DATA frames up to the
// advertised size without triggering ErrFrameTooLarge (RFC §6.5.2).
func TestConformance_RFC7540_Sec6_5_2_MaxFrameSize_FramerReadLimit(t *testing.T) {
	const bodySize = 20480 // 20 KiB — between default 16384 and advertised 32768

	srv, tlsCfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		payload := make([]byte, bodySize)
		for i := range payload {
			payload[i] = byte(i)
		}
		_, _ = w.Write(payload)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer:   &TLSDialer{Config: tlsCfg},
		Settings: AdvertisedSettings{MaxFrameSize: 32768},
	})
	require.NoErrorf(t, err, "Dial")
	defer c.Close()
	s, err := c.NewStream(ctx)
	require.NoErrorf(t, err, "NewStream")

	err = s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/large")},
	}, true)

	require.NoErrorf(t, err, "SendHeaders")
	var received int
	for {
		ev, rerr := s.Recv(ctx)
		require.NoErrorf(t, rerr, "Recv")
		if ev.Type == EventData {
			received += len(ev.Data)
		}
		if ev.EndStream {
			break
		}
	}
	require.Equalf(t, bodySize, received, "received %d bytes, want %d", received, bodySize)
}
