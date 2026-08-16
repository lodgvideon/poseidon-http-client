package conn

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// Two receive-path parameters could not be set from outside the package, and both
// are parameters a cross-library comparison has to pin before its numbers mean
// anything: the connection-level receive window was fixed at 65535 unless
// AutoTuneRecvWindow was on, and the read buffer was a compile-time constant with no
// counterpart to ConnOptions.WriteBufferSize (#696).
//
// These pin the clamping and, for the window, that the value actually reaches the
// thing that governs the refund — a field that is stored but never read would satisfy
// any test written against the options struct alone.

// TestDefaulted_ReadBufferSize covers the clamp, which mirrors WriteBufferSize's.
func TestDefaulted_ReadBufferSize(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int
		want int
	}{
		{"zero takes the default", 0, readBufferSize},
		{"negative takes the default", -1, readBufferSize},
		{"below the floor is raised", minReadBufferSize - 1, minReadBufferSize},
		{"at the floor is kept", minReadBufferSize, minReadBufferSize},
		{"in range is kept", 64 * 1024, 64 * 1024},
		{"at the ceiling is kept", maxReadBufferSize, maxReadBufferSize},
		{"above the ceiling is lowered", maxReadBufferSize + 1, maxReadBufferSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (ConnOptions{ReadBufferSize: tc.in}).defaulted().ReadBufferSize; got != tc.want {
				t.Errorf("ReadBufferSize %d defaulted to %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// TestDefaulted_ReadBufferSizeDefaultIsNotRaised is the boundary the write side gets
// wrong and this one must not: the default (16 KiB) sits above the floor, so passing
// it explicitly and leaving it zero have to agree.
//
// WriteBufferSize's documented floor is 16393 while its default is 16384, so an
// explicit default is silently raised nine bytes above an implicit one. That was
// measured and found to cost nothing, so it is not being fixed here — but the read
// side is new, and starting it consistent is free.
func TestDefaulted_ReadBufferSizeDefaultIsNotRaised(t *testing.T) {
	implicit := (ConnOptions{}).defaulted().ReadBufferSize
	explicit := (ConnOptions{ReadBufferSize: readBufferSize}).defaulted().ReadBufferSize
	if implicit != explicit {
		t.Errorf("leaving ReadBufferSize zero gives %d but passing the default gives %d — "+
			"the floor is above the default, so the two disagree", implicit, explicit)
	}
}

// TestDefaulted_StaticConnWindowSize covers its clamp. Zero stays zero because it
// means "unset", not "as small as possible".
func TestDefaulted_StaticConnWindowSize(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   uint32
		want uint32
	}{
		{"zero means unset", 0, 0},
		{"below the handshake window is raised", 1024, connInitialRecvWindow},
		{"at the handshake window is kept", connInitialRecvWindow, connInitialRecvWindow},
		{"in range is kept", 1 << 20, 1 << 20},
		{"above the RFC maximum is lowered", 1 << 31, uint32(maxFlowWindow)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (ConnOptions{StaticConnWindowSize: tc.in}).defaulted().StaticConnWindowSize; got != tc.want {
				t.Errorf("StaticConnWindowSize %d defaulted to %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// newConnForTunables builds a Conn the way NewClientConn does, without dialing or
// handshaking: the constructor's transport work is not what these assert.
//
// It calls initialConnRecvTarget rather than re-deriving the target. That is the
// whole reason the function exists — an earlier version of this helper carried its
// own copy of the precedence rule, so deleting the tuner check from the constructor
// left every test below green. A helper that reimplements what it is testing is
// testing itself.
func newConnForTunables(t *testing.T, opts ConnOptions) *Conn {
	t.Helper()
	opts = opts.defaulted()
	c := &Conn{opts: opts, connRecvWindow: int32(connInitialRecvWindow)}
	c.connRecvTarget.Store(initialConnRecvTarget(opts))
	c.tuner = newRecvWindowTuner(opts, opts.Settings.InitialWindowSize)
	return c
}

// TestStaticConnWindow_ReachesTheRefundIncrement is the assertion that matters. The
// option is not a stored number: it has to reach refundIncrement, which is what
// decides the WINDOW_UPDATE the connection actually sends. Asserting on
// c.connRecvTarget alone would pass for a field that is stored and never read.
//
// refundIncrement returns target-minus-window when the target is higher, so a static
// window of N gives back everything spent PLUS the N-65535 the connection is growing
// by — one WINDOW_UPDATE that carries it to the requested size, without any separate
// handshake frame.
func TestStaticConnWindow_ReachesTheRefundIncrement(t *testing.T) {
	const want = 1 << 20
	c := newConnForTunables(t, ConnOptions{StaticConnWindowSize: want})

	if got := c.connRecvTarget.Load(); got != want {
		t.Fatalf("connRecvTarget = %d, want %d", got, want)
	}
	// Spend some window, then ask what the refund would be.
	const spent = 32 * 1024
	inc := refundIncrement(c.connRecvTarget.Load(), c.connRecvWindow-spent, spent)
	if wantInc := uint32(want) - (connInitialRecvWindow - spent); inc != wantInc {
		t.Errorf("refund increment = %d, want %d — the static window must reach the "+
			"increment, since that is the only thing that widens the connection", inc, wantInc)
	}

	// The default connection is unchanged: target equals the window, so the refund is
	// exactly what was spent.
	def := newConnForTunables(t, ConnOptions{})
	if got := refundIncrement(def.connRecvTarget.Load(), def.connRecvWindow-spent, spent); got != spent {
		t.Errorf("a default connection refunds %d for %d spent, want %d — the new option "+
			"changed behaviour for callers who did not set it", got, spent, spent)
	}
}

// TestStaticConnWindow_IgnoredWhenTuning pins the documented precedence. Both writing
// connRecvTarget with different policies is the state this avoids, so the tuner has
// to win outright rather than the two racing to whichever ran last.
func TestStaticConnWindow_IgnoredWhenTuning(t *testing.T) {
	c := newConnForTunables(t, ConnOptions{
		StaticConnWindowSize: 1 << 20,
		AutoTuneRecvWindow:   true,
	})
	if got := c.connRecvTarget.Load(); got != connInitialRecvWindow {
		t.Errorf("connRecvTarget = %d with the tuner on, want the handshake window %d — "+
			"the static size is documented as ignored there, and letting it seed the "+
			"target hands one value to two writers with different growth policies",
			got, connInitialRecvWindow)
	}
	if c.tuner == nil {
		t.Fatal("no tuner was built, so this test is not exercising the case it names")
	}
}

// TestReadBufferSize_ReachesTheReader pins the other half: the option has to size the
// bufio.Reader the Framer reads through, not merely survive defaulted().
//
// It observes the real transport rather than building a reader of its own. An earlier
// version did the latter — `bufio.NewReaderSize(fake, opts.ReadBufferSize)` — and so
// asserted that bufio honours its own size argument, which it does regardless of what
// the constructor passes: replacing opts.ReadBufferSize with the old constant inside
// NewClientConn left it green.
//
// A bufio.Reader hands its whole buffer to the transport's Read, so the length of the
// slice the transport sees IS the buffer size. NewClientConn fails right after (the
// fake never returns a frame), which is fine: the read has already happened, and the
// constructor's error is not what is under test.
func TestReadBufferSize_ReachesTheReader(t *testing.T) {
	for _, size := range []int{minReadBufferSize, 64 * 1024} {
		tr := &readSizeConn{}
		_, _ = NewClientConn(context.Background(), tr, ConnOptions{
			ReadBufferSize: size,
			Dialer:         &PlaintextDialer{},
		})
		if tr.maxRead == 0 {
			t.Fatalf("the transport was never read from, so nothing was observed for size %d", size)
		}
		if tr.maxRead != size {
			t.Errorf("the transport was handed a %d-byte read buffer, want %d — "+
				"ConnOptions.ReadBufferSize is not sizing the reader the Framer reads through",
				tr.maxRead, size)
		}
	}
}

// readSizeConn records the largest slice its Read is handed, which is the size of the
// bufio.Reader wrapping it, and then fails so the handshake does not block.
type readSizeConn struct {
	net.Conn
	maxRead int
}

func (c *readSizeConn) Read(p []byte) (int, error) {
	if len(p) > c.maxRead {
		c.maxRead = len(p)
	}
	return 0, io.EOF
}

func (c *readSizeConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *readSizeConn) Close() error                     { return nil }
func (c *readSizeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *readSizeConn) SetWriteDeadline(time.Time) error { return nil }
func (c *readSizeConn) SetDeadline(time.Time) error      { return nil }
