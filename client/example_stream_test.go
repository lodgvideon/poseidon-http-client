package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ExampleClient_DoStream consumes a streaming response event-by-event.
// DoStream returns once the response HEADERS arrive; the caller then pumps
// StreamResponse.Recv until ErrStreamEnded and MUST Close the stream.
func ExampleClient_DoStream() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	req := client.GET("/events")

	var sr client.StreamResponse
	if err := c.DoStream(ctx, req, &sr); err != nil {
		log.Fatal(err)
	}
	// Close is mandatory: it returns the pooled stream slot and sends
	// RST_STREAM(CANCEL) if we bail out before EndStream.
	defer func() { _ = sr.Close() }()

	fmt.Println("status:", sr.Status)
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		switch ev.Type {
		case client.EventData:
			// ev.Data aliases a pooled buffer recycled on the next Recv;
			// copy it (DataCopy) to retain past this iteration.
			fmt.Printf("chunk: %d bytes\n", len(ev.Data))
		case client.EventTrailers:
			fmt.Println("got trailers:", len(ev.Trailers))
		case client.EventReset:
			fmt.Println("peer reset:", ev.ResetCode)
		}
	}
}

// ExampleClient_Stream uses the callback form, which always closes the
// underlying StreamResponse for you — eliminating the most common DoStream
// footgun of forgetting Close and leaking a connection slot.
func ExampleClient_Stream() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("api.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()

	// Accumulate streamed chunks. fn must not retain ev.Data past its
	// return, so we copy each chunk into our own buffer.
	var body []byte
	err = c.Stream(ctx, client.GET("/events"), func(ev client.StreamEvent) error {
		if ev.Type == client.EventData {
			body = append(body, ev.DataCopy()...)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("received %d bytes total\n", len(body))
}

// ExampleStreamResponse_WaitTrailers drains the body and then collects the
// response trailers. WaitTrailers discards any remaining EventData events,
// returning the trailer fields once EventTrailers arrives.
func ExampleStreamResponse_WaitTrailers() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("grpc.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	req := client.GET("/v1/stream")

	var sr client.StreamResponse
	if err := c.DoStream(ctx, req, &sr); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = sr.Close() }()

	// Skip ahead past the body straight to the trailers (e.g. a gRPC
	// grpc-status trailer). A nil slice means the server sent none.
	trailers, err := sr.WaitTrailers(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, t := range trailers {
		fmt.Printf("%s: %s\n", t.Name, t.Value)
	}
}

// Example_streamBodyReader sets Request.StreamBody so Do returns as soon as
// the response HEADERS arrive, exposing the body as an io.ReadCloser on
// Response.BodyReader. The caller MUST Close it (Response.Reset also closes
// it) to release the connection slot.
func Example_streamBodyReader() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("files.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	req := client.GET("/large-file.bin")
	req.StreamBody = true // body arrives lazily via Response.BodyReader

	var resp client.Response
	if err := c.Do(ctx, req, &resp); err != nil {
		log.Fatal(err)
	}
	// BodyReader is non-nil only because StreamBody was set. Always Close it.
	defer func() { _ = resp.BodyReader.Close() }()

	n, err := io.Copy(io.Discard, resp.BodyReader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status %d, streamed %d bytes\n", resp.Status, n)
}

// Example_uploadBody streams a request body from an io.Reader. Setting
// Request.BodyReader makes the client chunk the reader into DATA frames
// (respecting flow control); ContentLength, when > 0, emits a
// content-length header in the initial HEADERS frame.
func Example_uploadBody() {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec
	c, err := client.NewSingleConnClient("upload.example.com:443", dialer)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	payload := strings.NewReader(`{"event":"login","user":42}`)

	req := client.NewRequest("POST", "/v1/ingest")
	req.BodyReader = payload
	req.ContentLength = int64(payload.Len()) // emits content-length
	req.Headers = []client.HeaderField{client.H("content-type", "application/json")}

	var resp client.Response
	if err := c.Do(ctx, req, &resp); err != nil {
		log.Fatal(err)
	}
	fmt.Println("status:", resp.Status)
}
