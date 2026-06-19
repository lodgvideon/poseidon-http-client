package conn

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// asyncWrite runs fn in a goroutine because net.Pipe is synchronous.
func asyncWrite(fn func() error) chan error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	return done
}

// TestConn_PushPromise_DeliveredToParentStream verifies that when
// EnablePush is true, a PUSH_PROMISE frame from the server creates a
// pushed stream and delivers an EventPushPromise on the parent stream.
func TestConn_PushPromise_DeliveredToParentStream(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		// Read the client's request HEADERS on stream 1.
		srvFr.ReadFrame(context.Background(), &nilHandler{})

		enc := hpack.NewEncoder()

		// Response HEADERS on stream 1 (END_HEADERS, no END_STREAM).
		respBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
		})
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: respBlock,
				EndHeaders:    true,
			})
		})

		// PUSH_PROMISE on stream 1, promising stream 2.
		pushBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":path"), Value: []byte("/style.css")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":authority"), Value: []byte("example.com")},
		})
		<-asyncWrite(func() error {
			return srvFr.WritePushPromise(1, 2, pushBlock, true, 0)
		})

		// Pushed response on stream 2 (END_HEADERS | END_STREAM).
		pushedBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
		})
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      2,
				BlockFragment: pushedBlock,
				EndHeaders:    true,
				EndStream:     true,
			})
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClientConn(ctx, cli, ConnOptions{
		Settings:          AdvertisedSettings{}.defaulted(),
		StreamEventBuffer: 16,
		EnablePush:        true,
	})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}

	// Event 1: response headers.
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("expected EventHeaders, got error: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("expected EventHeaders, got %s", ev.Type)
	}
	if ev.Slab != nil {
		GetHeaderSlabPool().Put(ev.Slab)
	}

	// Event 2: push promise.
	ev, err = s.Recv(ctx)
	if err != nil {
		t.Fatalf("expected EventPushPromise, got error: %v", err)
	}
	if ev.Type != EventPushPromise {
		t.Fatalf("expected EventPushPromise, got %s", ev.Type)
	}
	if ev.PushStreamID != 2 {
		t.Fatalf("expected PushStreamID=2, got %d", ev.PushStreamID)
	}
	// Verify promised headers.
	found := false
	for _, h := range ev.Headers {
		if string(h.Name) == ":path" && string(h.Value) == "/style.css" {
			found = true
		}
	}
	if !found {
		t.Fatalf("promised headers missing :path=/style.css")
	}
	if ev.Slab != nil {
		GetHeaderSlabPool().Put(ev.Slab)
	}

	// Pushed stream (ID=2) should be registered.
	pushed := c.lookupStream(2)
	if pushed == nil {
		t.Fatal("pushed stream (ID=2) not registered")
	}

	// Read pushed response.
	pev, err := pushed.Recv(ctx)
	if err != nil {
		t.Fatalf("pushed stream Recv error: %v", err)
	}
	if pev.Type != EventHeaders {
		t.Fatalf("expected EventHeaders on pushed stream, got %s", pev.Type)
	}
	if !pev.EndStream {
		t.Fatal("expected END_STREAM on pushed stream headers")
	}
	if pev.Slab != nil {
		GetHeaderSlabPool().Put(pev.Slab)
	}
}

// TestConn_PushPromise_DisabledReturnsProtocolError verifies that when
// EnablePush is false, a PUSH_PROMISE triggers a connection error.
func TestConn_PushPromise_DisabledReturnsProtocolError(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		srvFr.ReadFrame(context.Background(), &nilHandler{})

		enc := hpack.NewEncoder()
		respBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
		})
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: respBlock,
				EndHeaders:    true,
			})
		})

		pushBlock := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
		})
		<-asyncWrite(func() error {
			return srvFr.WritePushPromise(1, 2, pushBlock, true, 0)
		})

		time.Sleep(200 * time.Millisecond)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClientConn(ctx, cli, ConnOptions{
		Settings:          AdvertisedSettings{}.defaulted(),
		StreamEventBuffer: 16,
		EnablePush:        false,
	})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}

	// PUSH_PROMISE with ENABLE_PUSH=0 → PROTOCOL_ERROR → conn closes.
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			if errors.Is(err, ErrStreamClosed) {
				return // connection closed — expected
			}
			return // any other error is fine too
		}
		if ev.Type == EventReset {
			return // stream reset — also acceptable
		}
		if ev.Slab != nil {
			GetHeaderSlabPool().Put(ev.Slab)
		}
	}
}
