package client_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestH1_BodyStream_Incremental pins that HTTP/1.1 serves BodyMode=BodyStream.
//
// It used to return ErrStreamingUnsupported, on the claim that h1Exchange
// "buffers whole responses and has no incremental path". That was never true of
// the code — h1Exchange.Recv reads one chunk per call through
// http1.Exchange.ReadBodyChunk and marks the last one EndStream. Only the
// dispatch in beginRespStream rejected it.
//
// The handler blocks between chunks until the reader has consumed the first one,
// so a buffered implementation cannot pass: it would deadlock rather than
// return early bytes.
func TestH1_BodyStream_Incremental(t *testing.T) {
	firstRead := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("first-chunk"))
		w.(http.Flusher).Flush()
		select {
		case <-firstRead:
		case <-time.After(5 * time.Second):
		}
		_, _ = w.Write([]byte("second-chunk"))
	}))
	defer srv.Close()

	c, err := client.NewClient(client.ClientOptions{
		Transport:     client.TransportH1SingleConn,
		Addr:          srv.Listener.Addr().String(),
		DefaultScheme: "http",
		ConnOpts:      conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var resp client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp); err != nil {
		t.Fatalf("Do(BodyStream) over HTTP/1.1: %v", err)
	}
	if resp.BodyReader == nil {
		t.Fatal("BodyReader is nil on the streaming path")
	}
	defer func() { _ = resp.BodyReader.Close() }()

	buf := make([]byte, len("first-chunk"))
	n, err := io.ReadFull(resp.BodyReader, buf)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if string(buf[:n]) != "first-chunk" {
		t.Fatalf("first read = %q, want %q", buf[:n], "first-chunk")
	}
	// Only now let the server produce the rest. A buffered transport would still
	// be waiting for the whole body here and this would time out.
	close(firstRead)

	rest, err := io.ReadAll(resp.BodyReader)
	if err != nil {
		t.Fatalf("read rest: %v", err)
	}
	if string(rest) != "second-chunk" {
		t.Fatalf("rest = %q, want %q", rest, "second-chunk")
	}
}

// TestH1_BodyStream_CloseMidBodyReleasesConn pins that abandoning a streamed H1
// body mid-read still returns the connection to its owner, and that a later
// request on the same client works. h1Exchange owns the release (sync.Once
// guarded); the release the streaming caller holds is the no-op the h1
// transports return, so there must be exactly one owner.
func TestH1_BodyStream_CloseMidBodyReleasesConn(t *testing.T) {
	body := make([]byte, 256*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	c, err := client.NewClient(client.ClientOptions{
		Transport:     client.TransportH1SingleConn,
		Addr:          srv.Listener.Addr().String(),
		DefaultScheme: "http",
		ConnOpts:      conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &resp); err != nil {
		t.Fatalf("first Do: %v", err)
	}
	small := make([]byte, 128)
	if _, err := resp.BodyReader.Read(small); err != nil {
		t.Fatalf("partial read: %v", err)
	}
	if err := resp.BodyReader.Close(); err != nil {
		t.Fatalf("Close mid-body: %v", err)
	}

	// The client must still work. If the release were double-counted or lost,
	// this either blocks or fails.
	var second client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/"}, &second); err != nil {
		t.Fatalf("second Do after a mid-body Close: %v", err)
	}
	if second.Status != 200 {
		t.Fatalf("second status = %d, want 200", second.Status)
	}
}
