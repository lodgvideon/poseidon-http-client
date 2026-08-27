package poolcore

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Test fixtures copied from the client package's test files when the pool
// machinery moved here. They are duplicated rather than shared because a test
// helper cannot cross a package boundary, and client still needs its copies for
// the sibling-parity tests that compare this pool against the HTTP/1.1 and
// HTTP/3 ones.

// runFakeH2Server does the HTTP/2 handshake on srv (server side of a
// net.Pipe), then invokes after with the server's *frame.Framer for
// per-test frame interactions. If after blocks, it must return when
// signaled by the test (typically by closing the pipe via c.Close()).
func runFakeH2Server(srv net.Conn, after func(srvFr *frame.Framer)) {
	defer srv.Close()
	preface := make([]byte, 24)
	if _, err := readFull(srv, preface); err != nil {
		return
	}
	srvFr := frame.NewFramer(srv, srv)
	writeDone := make(chan error, 1)
	go func() { writeDone <- srvFr.WriteSettings(frame.SettingsParams{}) }()
	if _, err := srvFr.ReadFrame(context.Background(), nopHandler{}); err != nil {
		return
	}
	if err := <-writeDone; err != nil {
		return
	}
	go func() { writeDone <- srvFr.WriteSettingsAck() }()
	if _, err := srvFr.ReadFrame(context.Background(), nopHandler{}); err != nil {
		return
	}
	if err := <-writeDone; err != nil {
		return
	}
	if after != nil {
		after(srvFr)
	}
}

// fakeDialer returns the client end of a net.Pipe. Each Dial spins up
// a fresh in-memory pipe pair and a goroutine running runFakeH2Server.
type fakeDialer struct {
	dialCount atomic.Int32
	srvAfter  func(srvFr *frame.Framer)
}

// Dial implements conn.Dialer.
func (d *fakeDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	d.dialCount.Add(1)
	cli, srv := net.Pipe()
	go runFakeH2Server(srv, d.srvAfter)
	return cli, nil
}

// failingDialer always errors.
type failingDialer struct {
	err       error
	dialCount atomic.Int32
}

func (d *failingDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	d.dialCount.Add(1)
	return nil, d.err
}

// would be flaky in the passing direction.
func waitStats(p *Pool, want func(Stats) bool, d time.Duration) Stats {
	deadline := time.Now().Add(d)
	var s Stats
	for {
		s = p.Stats()
		if want(s) || time.Now().After(deadline) {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readFull reads len(buf) bytes from r, retrying on short reads.
func readFull(r io.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		x, err := r.Read(buf[n:])
		if x > 0 {
			n += x
		}
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// nopHandler is a frame.Handler with no-op methods, used to skip frames
// during fake-server handshake while ReadFrame's contract is satisfied.
type nopHandler struct{}

func (nopHandler) OnData(frame.FrameHeader, []byte, uint8) error { return nil }
func (nopHandler) OnHeaders(frame.FrameHeader, frame.HeaderBlock, *frame.Priority, uint8) error {
	return nil
}
func (nopHandler) OnPriority(frame.FrameHeader, frame.Priority) error { return nil }
func (nopHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error { return nil }
func (nopHandler) OnSettings(frame.FrameHeader, frame.SettingsParams) error {
	return nil
}
func (nopHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (nopHandler) OnPing(frame.FrameHeader, [8]byte) error                         { return nil }
func (nopHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
func (nopHandler) OnWindowUpdate(frame.FrameHeader, uint32) error                  { return nil }
func (nopHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error       { return nil }
func (nopHandler) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error           { return nil }
func (nopHandler) OnOrigin(frame.FrameHeader, []string) error                      { return nil }

// readFull reads len(buf) bytes from r, retrying on short reads.

// countingRecorder is the pool.Recorder the pool tests assert on. It stands in
// for the client package's Metrics, which cannot come along: this package is
// below it, and the pool only ever raises these six events anyway.
type countingRecorder struct {
	DialsAttempted   atomic.Int64
	DialsFailed      atomic.Int64
	ConnsClosedN     atomic.Int64
	GoAwaysReceived  atomic.Int64
	DialsObserved    atomic.Int64
	AcquiresObserved atomic.Int64
}

func (r *countingRecorder) DialAttempted()  { r.DialsAttempted.Add(1) }
func (r *countingRecorder) DialFailed()     { r.DialsFailed.Add(1) }
func (r *countingRecorder) ConnClosed()     { r.ConnsClosedN.Add(1) }
func (r *countingRecorder) GoAwayReceived() { r.GoAwaysReceived.Add(1) }

func (r *countingRecorder) ObserveDial(time.Duration) { r.DialsObserved.Add(1) }

func (r *countingRecorder) ObserveAcquire(time.Duration) { r.AcquiresObserved.Add(1) }
