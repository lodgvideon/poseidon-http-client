package conn

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestStreamClose_IdempotentAfterRecycle is a regression test for a nil-pointer
// panic: on the both-ended path Close() recycles the stream (resetting closed,
// w, localEnded, remoteEnded), so a second Close() used to skip the idempotency
// guard and dereference the nil-ed w in writeRSTStream. The released guard now
// makes a repeat Close a safe no-op.
func TestStreamClose_IdempotentAfterRecycle(t *testing.T) {
	c := &Conn{}
	s := newStream(1, 8, c, 65535)
	s.mu.Lock()
	s.localEnded = true
	s.remoteEnded = true
	s.mu.Unlock()

	require.NoError(t, s.ref().Close(), "first Close")

	assert.NotPanics(t, func() {
		assert.NoError(t, s.ref().Close(), "second Close returned an error, want nil")
	}, "second Close() panicked; Close must be idempotent after recycle")
}

// TestApplyPeerSettings_OversizedInitialWindow_ConnError is a regression test
// for a flow-control hang: a peer SETTINGS_INITIAL_WINDOW_SIZE above 2^31-1
// delivered with no open streams used to be stored verbatim (the delta guard
// only iterates open streams) and later seed a negative int32 send window,
// wedging SendData. RFC 7540 §6.5.2 requires FLOW_CONTROL_ERROR.
func TestApplyPeerSettings_OversizedInitialWindow_ConnError(t *testing.T) {
	c := newDynSettingsConn()

	var p frame.SettingsParams
	setPeerSetting(&p, frame.SettingInitialWindowSize, 0x80000000) // 2^31, one past max

	err := c.applyPeerSettings(p)

	var ce *ConnError
	require.Truef(t, errors.As(err, &ce),
		"applyPeerSettings(0x80000000) = %v, want *ConnError FLOW_CONTROL_ERROR", err)
	assert.Equalf(t, frame.ErrCodeFlowControlError, ce.Code,
		"applyPeerSettings(0x80000000) = %v, want FLOW_CONTROL_ERROR per RFC 7540 §6.5.2", err)
}

// TestOnContinuation_FloodCapped is a regression test for the CONTINUATION-flood
// memory-exhaustion DoS (CVE-2024-27316 class): connHandler used to append every
// CONTINUATION fragment into pendingBuf with no cap. It now returns a ConnError
// once the accumulated block exceeds maxHeaderBytes.
func TestOnContinuation_FloodCapped(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	h.maxHeaderBytes = 4096 // small cap for a fast, deterministic test
	m.addStream(1)

	// Initial HEADERS without END_HEADERS opens the pending buffer.
	startFH := frame.FrameHeader{Type: frame.FrameHeaders, Flags: 0, StreamID: 1}
	require.NoError(t, h.OnHeaders(startFH, frame.HeaderBlock(nil), nil, 0), "OnHeaders")

	frag := make([]byte, 1024)
	contFH := frame.FrameHeader{Type: frame.FrameContinuation, Length: 1024, Flags: 0, StreamID: 1}

	var got error
	for i := 0; i < 64; i++ { // 64 KiB worth — must trip the 4 KiB cap well before
		if err := h.OnContinuation(contFH, frame.HeaderBlock(frag)); err != nil {
			got = err
			break
		}
	}

	var ce *ConnError
	require.Truef(t, errors.As(got, &ce),
		"OnContinuation flood = %v (bufLen=%d), want *ConnError ENHANCE_YOUR_CALM", got, len(h.pendingBuf))
	assert.Equalf(t, frame.ErrCodeEnhanceYourCalm, ce.Code,
		"OnContinuation flood = %v (bufLen=%d), want ENHANCE_YOUR_CALM", got, len(h.pendingBuf))
}

// TestReaderLoop_StreamErrorResetsOnlyThatStream is a regression test: a
// per-stream flow-control overrun surfaces as a *StreamError from the frame
// dispatch. The reader loop used to treat every error as connection-fatal,
// killing all streams. It must now reset only the offending stream and keep
// the connection — and other in-flight streams — alive (RFC 7540 §5.4.2).
func TestReaderLoop_StreamErrorResetsOnlyThatStream(t *testing.T) {
	cli, srv := net.Pipe()

	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		pipeServer(t, srv, func(srvFr *frame.Framer) {
			ctx := context.Background()
			// Drain the two client HEADERS frames (streams 1 and 3).
			if _, err := srvFr.ReadFrame(ctx, &nilHandler{}); err != nil {
				return
			}
			if _, err := srvFr.ReadFrame(ctx, &nilHandler{}); err != nil {
				return
			}
			// 50-byte DATA on stream 1 overruns its 10-byte recv window -> *StreamError.
			_ = srvFr.WriteData(1, false, make([]byte, 50))
			// Stay alive until the client closes the pipe, so this goroutine
			// exits deterministically instead of on a fixed timer.
			for {
				if _, err := srvFr.ReadFrame(ctx, &nilHandler{}); err != nil {
					return
				}
			}
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := ConnOptions{Settings: AdvertisedSettings{InitialWindowSize: 10}}.defaulted()
	c, err := NewClientConn(ctx, cli, opts)
	require.NoError(t, err, "NewClientConn")
	defer c.Close()
	// defer c.Close() (above) closes the pipe so the server goroutine's read
	// loop errors and returns; wait for it here so it cannot outlive the test.
	t.Cleanup(func() { <-srvDone })

	hdrs := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	a, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream A")
	b, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream B")
	require.NoError(t, a.SendHeaders(ctx, hdrs, false), "A.SendHeaders")
	require.NoError(t, b.SendHeaders(ctx, hdrs, false), "B.SendHeaders")

	// Stream A receives the per-stream reset.
	aCtx, aCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer aCancel()
	ev, err := a.Recv(aCtx)

	require.NoErrorf(t, err, "A.Recv = (%+v, %v), want EventReset", ev, err)
	assert.Equalf(t, EventReset, ev.Type, "A.Recv = %+v, want EventReset", ev)
	// The connection must stay alive — the reader loop must not have exited.
	select {
	case <-c.readerDone:
		assert.Fail(t, "reader loop exited; a per-stream error must not kill the connection")
	default:
	}
	assert.True(t, c.IsAlive(), "IsAlive() = false; connection killed by a per-stream error")

	// Stream B is untouched: Recv times out (no reset), proving B survived.
	bCtx, bCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer bCancel()
	bEv, bErr := b.Recv(bCtx)
	assert.Equalf(t, context.DeadlineExceeded, bErr,
		"B.Recv = (%+v, %v), want DeadlineExceeded (B must be unaffected)", bEv, bErr)
}

// TestApplyPeerSettings_InitWindowDelta_NoDoubleApply is a regression test for
// a flow-control over-credit race: applyPeerSettings (merge + retroactive
// delta) and writeHeadersWithPriority (insert + seed) coordinated the per-stream
// send window across separate critical sections, so a freshly opened stream
// could be BOTH seeded at the new INITIAL_WINDOW_SIZE AND credited the delta,
// yielding newInitial+delta instead of newInitial. Now both run mutually
// exclusively under psMu, so the window is always exactly newInitial.
func TestApplyPeerSettings_InitWindowDelta_NoDoubleApply(t *testing.T) {
	// The double-apply interleaving needs real parallelism to be a meaningful
	// discriminator; force >=2 Ps. Run under -race in CI to also catch the
	// data race directly.
	if runtime.GOMAXPROCS(0) < 2 {
		prev := runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)
	}
	const (
		oldInitial = 65535
		newInitial = 131070
		doubled    = newInitial + (newInitial - oldInitial)
		iters      = 2000
	)
	hdr := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte("x")},
		{Name: []byte(":path"), Value: []byte("/")},
	}

	for it := 0; it < iters; it++ {
		rw := dblApplyRW{done: make(chan struct{})}
		opts := ConnOptions{}.defaulted()
		c := &Conn{
			transport:          dblApplyConn{rw: rw},
			fr:                 frame.NewFramer(rw, rw),
			enc:                hpack.NewEncoder(),
			dec:                hpack.NewDecoder(),
			opts:               opts,
			nextID:             1,
			streams:            map[uint32]*Stream{},
			readerDone:         make(chan struct{}),
			drainDone:          make(chan struct{}),
			pingWaiters:        make(map[[8]byte]chan struct{}),
			connRecvWindow:     int32(connInitialRecvWindow),
			peerConnSendWindow: int32(connInitialRecvWindow),
		}
		c.fcOutCond = sync.NewCond(&c.fcOutMu)
		setPeerSetting(&c.peerSettings, frame.SettingInitialWindowSize, oldInitial)

		s := newStream(0, c.opts.StreamEventBuffer, c, int32(connInitialRecvWindow))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			var sp frame.SettingsParams
			setPeerSetting(&sp, frame.SettingInitialWindowSize, newInitial)
			_ = c.applyPeerSettings(sp)
		}()
		go func() {
			defer wg.Done()
			_ = c.writeHeadersWithPriority(context.Background(), s, hdr, true, nil)
		}()
		wg.Wait()

		s.mu.Lock()
		got := s.sendWindow
		s.mu.Unlock()
		close(rw.done)

		// doubled is called out by name because it is the specific wrong answer
		// the double-apply produces; any other value is a different bug.
		require.NotEqualf(t, int32(doubled), got,
			"iter %d: double-apply — sendWindow=%d, want %d", it, got, newInitial)
		require.Equalf(t, int32(newInitial), got, "iter %d: sendWindow=%d, want %d", it, got, newInitial)
	}
}

// TestOnDataReceived_StreamOverrun_CreditsConnWindow is a regression test: when
// a per-stream recv-window overrun resets the stream (a non-fatal *StreamError),
// the DATA payload must still be debited against the CONNECTION window so the
// peer's connection send credit is not leaked (RFC 7540 §6.9.1). Before the fix,
// onDataReceived returned the *StreamError before touching the connection window.
func TestOnDataReceived_StreamOverrun_CreditsConnWindow(t *testing.T) {
	const connStart = 1 << 20
	c := &Conn{connRecvWindow: connStart}
	s := newStream(1, 8, c, 10) // tiny per-stream recv window
	s.id = 1

	const length = 50 // > stream window (10), < refund threshold (no WINDOW_UPDATE)
	err := c.onDataReceived(s, length)

	var se *StreamError
	require.Truef(t, errors.As(err, &se),
		"onDataReceived = %v, want *StreamError FLOW_CONTROL_ERROR", err)
	assert.Equalf(t, frame.ErrCodeFlowControlError, se.Code,
		"onDataReceived = %v, want FLOW_CONTROL_ERROR", err)
	assert.Equalf(t, int32(connStart-length), c.connRecvWindow,
		"connRecvWindow = %d, want %d (overrun bytes must still be debited at conn scope)",
		c.connRecvWindow, connStart-length)
}

// dblApplyRW discards writes and blocks reads until done is closed.
type dblApplyRW struct{ done chan struct{} }

func (b dblApplyRW) Write(p []byte) (int, error) { return len(p), nil }
func (b dblApplyRW) Read(p []byte) (int, error) {
	<-b.done
	return 0, io.EOF
}

// dblApplyConn adapts an io.ReadWriter to net.Conn for Conn construction.
type dblApplyConn struct{ rw io.ReadWriter }

func (n dblApplyConn) Read(p []byte) (int, error)       { return n.rw.Read(p) }
func (n dblApplyConn) Write(p []byte) (int, error)      { return n.rw.Write(p) }
func (n dblApplyConn) Close() error                     { return nil }
func (n dblApplyConn) LocalAddr() net.Addr              { return nil }
func (n dblApplyConn) RemoteAddr() net.Addr             { return nil }
func (n dblApplyConn) SetDeadline(time.Time) error      { return nil }
func (n dblApplyConn) SetReadDeadline(time.Time) error  { return nil }
func (n dblApplyConn) SetWriteDeadline(time.Time) error { return nil }
