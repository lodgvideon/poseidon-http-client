package http1

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// A pooled keep-alive connection whose server reaps it between the checkout
// probe and the write is the canonical retryable HTTP/1.1 failure: the request
// produced no response, so replaying it cannot duplicate anything. It used to
// surface as an opaque `http1: read status line: EOF`, which the client's retry
// classifier had no way to match — so the H2 and H3 equivalents were retried and
// this one never was.
//
// ErrServerClosedIdle is that signal, and it is deliberately narrow: an EOF
// after any response byte says the server was answering and stopped, which is no
// evidence at all about whether it processed the request.

// exchangeOverPipe wires an Exchange to one end of a net.Pipe and hands back the
// peer end for the test to script.
func exchangeOverPipe(t *testing.T) (*Exchange, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	c := NewConn(client)
	return c.NewExchange(), server
}

// writeSimpleRequest sends a minimal request so the exchange is in the state
// ReadResponse expects.
func writeSimpleRequest(t *testing.T, ex *Exchange, peer net.Conn) {
	t.Helper()
	go func() {
		buf := make([]byte, 4096)
		_, _ = peer.Read(buf) // drain the request; content does not matter here
	}()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.test")},
	}
	if err := ex.WriteRequest(context.Background(), fields, true); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
}

// TestErrServerClosedIdle_ClosedBeforeAnyByte is the gate. The server accepts
// the request and closes without writing a single byte.
func TestErrServerClosedIdle_ClosedBeforeAnyByte(t *testing.T) {
	ex, peer := exchangeOverPipe(t)
	writeSimpleRequest(t, ex, peer)

	go func() {
		time.Sleep(5 * time.Millisecond)
		_ = peer.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := ex.ReadResponse(ctx)

	if err == nil {
		t.Fatal("ReadResponse returned no error after the peer closed without responding")
	}
	if !errors.Is(err, ErrServerClosedIdle) {
		t.Errorf("error is %v, want ErrServerClosedIdle — without the type the client's "+
			"retry classifier cannot tell this from any other read failure, and the one "+
			"safely retryable H1 failure goes unretried", err)
	}
	if !errors.Is(err, io.EOF) {
		t.Errorf("error is %v; it should still wrap the underlying EOF for diagnosis", err)
	}
}

// TestErrServerClosedIdle_NotAfterAPartialResponse is the boundary, and it is
// the half that matters for correctness. A server that sent part of a response
// and then vanished may well have processed the request; classifying that as
// safely retryable would replay it.
func TestErrServerClosedIdle_NotAfterAPartialResponse(t *testing.T) {
	cases := []struct {
		name string
		sent string
	}{
		{"partial status line", "HTTP/1.1 20"},
		{"status line then nothing", "HTTP/1.1 200 OK\r\n"},
		{"status line and a header", "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ex, peer := exchangeOverPipe(t)
			writeSimpleRequest(t, ex, peer)

			go func() {
				_, _ = peer.Write([]byte(tc.sent))
				time.Sleep(5 * time.Millisecond)
				_ = peer.Close()
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _, err := ex.ReadResponse(ctx)

			if err == nil {
				t.Fatal("ReadResponse returned no error on a truncated response")
			}
			if errors.Is(err, ErrServerClosedIdle) {
				t.Errorf("a response truncated after %q was classified as "+
					"ErrServerClosedIdle — the server had started answering, so it may "+
					"have processed the request, and the caller would replay it",
					tc.sent)
			}
		})
	}
}

// TestErrServerClosedIdle_NotAfterAnInterimResponse is the half of that boundary
// the truncation cases above cannot express, and it is the one the guard's
// firstRead conjunct exists for.
//
// Every case above is cut mid-message, so the failing read consumed something
// and readConsumedNothing alone rejects it. A 1xx is different: it is a COMPLETE
// message, ReadResponse drains it and loops back for the final status line, and
// nothing obliges the peer to send one — closing after an interim is legal. That
// second trip round the loop meets an EOF having consumed nothing on that read,
// so readConsumedNothing is true again and errors.Is(rerr, io.EOF) holds again.
// Only firstRead separates it from a server that never answered at all.
//
// Getting that wrong is not cosmetic. ErrServerClosedIdle is the one H1 failure
// client/retry.go replays (see builtinShouldRetry), and its licence is that no
// part of a response ever arrived. A 100 Continue is the server saying it has
// the request head and wants the body — the strongest evidence available on this
// connection that it is acting on the request — so replaying it duplicates work
// the peer has already begun.
func TestErrServerClosedIdle_NotAfterAnInterimResponse(t *testing.T) {
	ex, peer := exchangeOverPipe(t)
	writeSimpleRequest(t, ex, peer)

	go func() {
		// net.Pipe's Write returns only once the reader has consumed every byte,
		// so the close below cannot race the interim: the client has it before
		// the socket goes away, and ReadResponse is already back at the top of
		// the loop reading a status line that will never arrive.
		_, _ = peer.Write([]byte("HTTP/1.1 100 Continue\r\n\r\n"))
		_ = peer.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := ex.ReadResponse(ctx)

	if err == nil {
		t.Fatal("ReadResponse returned no error after the peer closed following a 1xx")
	}
	if errors.Is(err, ErrServerClosedIdle) {
		t.Errorf("error is %v, want anything but ErrServerClosedIdle — the server sent "+
			"100 Continue and only then closed, so it had the request and had begun "+
			"answering it; classifying that as \"never responded\" is what makes the "+
			"retry classifier replay a request the peer already acted on", err)
	}
	if !errors.Is(err, io.EOF) {
		t.Errorf("error is %v; it should still wrap the underlying EOF for diagnosis", err)
	}
}
