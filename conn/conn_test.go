package conn

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// pipeServer is a minimal HTTP/2 peer driver used by conn unit tests.
// IMPORTANT: net.Pipe is synchronous; writes must run in goroutines so
// they don't deadlock the symmetrical peer/client write pair.
func pipeServer(t *testing.T, srv net.Conn, after func(srvFr *frame.Framer)) {
	t.Helper()
	defer srv.Close()
	preface := make([]byte, 24)
	if _, err := readN(srv, preface); err != nil {
		t.Logf("preface read: %v", err)
		return
	}
	srvFr := frame.NewFramer(srv, srv)

	writeDone := make(chan error, 1)
	go func() { writeDone <- srvFr.WriteSettings(frame.SettingsParams{}) }()
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("server read client settings: %v", err)
		return
	}
	if err := <-writeDone; err != nil {
		t.Logf("server write settings: %v", err)
		return
	}
	go func() { writeDone <- srvFr.WriteSettingsAck() }()
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("server read client ack: %v", err)
		return
	}
	if err := <-writeDone; err != nil {
		t.Logf("server write settings ack: %v", err)
		return
	}
	if after != nil {
		after(srvFr)
	}
}

func TestConn_HandshakeAndIdle(t *testing.T) {
	cli, srv := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pipeServer(t, srv, nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	_ = c.Close()
	<-done
}

func TestConn_NewStream_RespectsConcurrencyOne(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		// Idle the server side; client will open one stream then try a
		// second, which must fail.
		_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	s1, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream 1: %v", err)
	}
	if s1.ID() != 1 {
		t.Fatalf("first stream id = %d, want 1", s1.ID())
	}

	if _, err := c.NewStream(ctx); err != ErrTooManyStreams {
		t.Fatalf("NewStream 2 err = %v, want ErrTooManyStreams", err)
	}
}

func TestConn_StreamSendHeaders_AndPeerEcho(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		// Read client HEADERS, then send back HEADERS+END_STREAM.
		var got bytes.Buffer
		hh := captureHandler{block: &got}
		if _, err := srvFr.ReadFrame(context.Background(), &hh); err != nil {
			return
		}
		// Encode response :status 200 with hpack on the server side.
		enc := hpack.NewEncoder()
		block := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":status"), Value: []byte("200")},
		})
		writeDone := make(chan error, 1)
		go func() {
			writeDone <- srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: block,
				EndHeaders:    true,
				EndStream:     true,
			})
		}()
		<-writeDone // ensure write completes before goroutine returns
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
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
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventHeaders || !ev.EndStream {
		t.Fatalf("event = %+v", ev)
	}
}

func TestConn_TwoSequentialStreams(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		enc := hpack.NewEncoder()
		respond := func(streamID uint32) {
			_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
			block := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("204")},
			})
			writeDone := make(chan error, 1)
			go func() {
				writeDone <- srvFr.WriteHeaders(frame.WriteHeadersParams{
					StreamID:      streamID,
					BlockFragment: block,
					EndHeaders:    true,
					EndStream:     true,
				})
			}()
			<-writeDone
		}
		respond(1)
		respond(3)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	for i := 0; i < 2; i++ {
		s, err := c.NewStream(ctx)
		if err != nil {
			t.Fatalf("NewStream %d: %v", i, err)
		}
		if err := s.SendHeaders(ctx, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":scheme"), Value: []byte("http")},
			{Name: []byte(":authority"), Value: []byte("x")},
			{Name: []byte(":path"), Value: []byte("/")},
		}, true); err != nil {
			t.Fatalf("SendHeaders %d: %v", i, err)
		}
		ev, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if !ev.EndStream {
			t.Fatalf("event %d not end-of-stream: %+v", i, ev)
		}
		_ = s.Close()
	}
}

func TestConn_NewStream_AfterClose_ReturnsErrConnClosed(t *testing.T) {
	cli, srv := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pipeServer(t, srv, nil)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	_ = c.Close()
	if _, err := c.NewStream(ctx); err != ErrConnClosed {
		t.Fatalf("err = %v, want ErrConnClosed", err)
	}
	<-done
}

// captureHandler records the fragment of a single HEADERS frame.
type captureHandler struct {
	block *bytes.Buffer
}

func (h captureHandler) OnData(frame.FrameHeader, []byte, uint8) error { return nil }
func (h captureHandler) OnHeaders(_ frame.FrameHeader, hb frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	h.block.Write(hb)
	return nil
}
func (h captureHandler) OnPriority(frame.FrameHeader, frame.Priority) error       { return nil }
func (h captureHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error       { return nil }
func (h captureHandler) OnSettings(frame.FrameHeader, frame.SettingsParams) error { return nil }
func (h captureHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (h captureHandler) OnPing(frame.FrameHeader, [8]byte) error                         { return nil }
func (h captureHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
func (h captureHandler) OnWindowUpdate(frame.FrameHeader, uint32) error                  { return nil }
func (h captureHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error       { return nil }

func TestConn_Close_IsIdempotent(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
}

func TestConn_Close_RacedFromTwoGoroutines(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.Close() }()
	}
	wg.Wait()
}

