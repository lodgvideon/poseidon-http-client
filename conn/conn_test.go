package conn

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// pipeServer is a minimal HTTP/2 peer driver used by conn unit tests.
// IMPORTANT: net.Pipe is synchronous; writes must run in goroutines so
// they don't deadlock the symmetrical peer/client write pair.
func pipeServer(t *testing.T, srv net.Conn, after func(srvFr *frame.Framer)) {
	t.Helper()
	pipeServerWithSettings(t, srv, frame.SettingsParams{}, after)
}

// pipeServerWithSettings is pipeServer with a peer SETTINGS payload of the
// caller's choosing. The handshake already reads the client's SETTINGS ACK,
// so an advertised value is guaranteed applied to Conn.peerSettings by the
// time NewClientConn returns — no extra frame plumbing in the test body.
func pipeServerWithSettings(t *testing.T, srv net.Conn, params frame.SettingsParams, after func(srvFr *frame.Framer)) {
	t.Helper()
	defer srv.Close()
	preface := make([]byte, 24)
	if _, err := readN(srv, preface); err != nil {
		t.Logf("preface read: %v", err)
		return
	}
	srvFr := frame.NewFramer(srv, srv)

	writeDone := make(chan error, 1)
	go func() { writeDone <- srvFr.WriteSettings(params) }()
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

	require.NoError(t, err, "NewClientConn")
	assert.NoError(t, c.Close(), "Close after an idle handshake")
	<-done
}

func TestConn_NewStream_RespectsAdvertisedLimit(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		// Idle: client opens MaxConcurrentStreams streams then a final
		// allocation must fail before any frames go out.
		_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	opts := ConnOptions{Settings: AdvertisedSettings{MaxConcurrentStreams: 2}}.defaulted()
	c, err := NewClientConn(ctx, cli, opts)
	require.NoError(t, err, "NewClientConn")
	defer c.Close()

	_, first := c.NewStream(ctx)
	_, second := c.NewStream(ctx)
	_, third := c.NewStream(ctx)

	require.NoError(t, first, "NewStream 1")
	require.NoError(t, second, "NewStream 2")
	assert.Equalf(t, ErrTooManyStreams, third, "NewStream 3 err = %v, want ErrTooManyStreams", third)
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
	require.NoError(t, err, "NewClientConn")
	defer c.Close()
	s, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")

	require.NoError(t, s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true), "SendHeaders")

	ev, rerr := s.Recv(ctx)
	require.NoError(t, rerr, "Recv")
	assert.Equalf(t, EventHeaders, ev.Type, "event = %+v", ev)
	assert.Truef(t, ev.EndStream, "event = %+v", ev)
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
	require.NoError(t, err, "NewClientConn")
	defer c.Close()

	for i := 0; i < 2; i++ {
		s, nerr := c.NewStream(ctx)
		require.NoErrorf(t, nerr, "NewStream %d", i)

		require.NoErrorf(t, s.SendHeaders(ctx, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":scheme"), Value: []byte("http")},
			{Name: []byte(":authority"), Value: []byte("x")},
			{Name: []byte(":path"), Value: []byte("/")},
		}, true), "SendHeaders %d", i)

		ev, rerr := s.Recv(ctx)
		require.NoErrorf(t, rerr, "Recv %d", i)
		assert.Truef(t, ev.EndStream, "event %d not end-of-stream: %+v", i, ev)
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
	require.NoError(t, err, "NewClientConn")
	_ = c.Close()

	_, nerr := c.NewStream(ctx)

	assert.Equalf(t, ErrConnClosed, nerr, "err = %v, want ErrConnClosed", nerr)
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
func (h captureHandler) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error           { return nil }
func (h captureHandler) OnOrigin(frame.FrameHeader, []string) error                      { return nil }

func TestConn_Close_IsIdempotent(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	require.NoError(t, err, "NewClientConn")

	first := c.Close()
	second := c.Close()

	assert.NoError(t, first, "Close 1")
	assert.NoError(t, second, "Close 2")
}

func TestConn_Close_RacedFromTwoGoroutines(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	require.NoError(t, err, "NewClientConn")
	errs := make([]error, 4)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = c.Close() }()
	}
	wg.Wait()

	for i, cerr := range errs {
		assert.NoErrorf(t, cerr, "concurrent Close %d — every racing Close must be a clean no-op", i)
	}
	assert.False(t, c.IsAlive(), "connection still alive after four concurrent Close calls")
}

func TestConn_IsAlive_FreshConnTrue(t *testing.T) {
	cli, srv := net.Pipe()
	stopSrv := make(chan struct{})
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		pipeServer(t, srv, func(_ *frame.Framer) {
			<-stopSrv
		})
	}()
	t.Cleanup(func() {
		close(stopSrv)
		<-srvDone
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{})

	require.NoError(t, err, "NewClientConn")
	defer c.Close()
	assert.True(t, c.IsAlive(), "fresh conn must be alive")
}

func TestConn_IsAlive_AfterCloseFalse(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{})
	require.NoError(t, err, "NewClientConn")

	_ = c.Close()

	assert.False(t, c.IsAlive(), "closed conn must not be alive")
}

func TestConn_IsAlive_AfterPeerGoAwayFalse(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		_ = srvFr.WriteGoAway(0, frame.ErrCodeNoError, nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{})
	require.NoError(t, err, "NewClientConn")
	defer c.Close()

	// Wait for the reader to observe GOAWAY.
	alive := aliveWithin(c, false, time.Second)

	assert.False(t, alive, "conn still alive after peer GOAWAY")
}

func TestConn_PeerMaxConcurrentStreams_Default(t *testing.T) {
	t.Parallel()
	c := &Conn{}

	got := c.PeerMaxConcurrentStreams()

	assert.Zerof(t, got, "PeerMaxConcurrentStreams empty peerSettings = %d, want 0", got)
}

func TestConn_PeerMaxConcurrentStreams_AfterSettings(t *testing.T) {
	t.Parallel()
	c := &Conn{}
	c.psMu.Lock()
	setPeerSetting(&c.peerSettings, frame.SettingMaxConcurrentStreams, 250)
	c.psMu.Unlock()

	got := c.PeerMaxConcurrentStreams()

	assert.EqualValuesf(t, 250, got, "PeerMaxConcurrentStreams after SETTINGS = %d, want 250", got)
}
