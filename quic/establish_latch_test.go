package quic

import (
	"context"
	"crypto/tls"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// Establish's error paths and the single-close latch (#683).
//
// terminateLocked's own doc says it "runs once on every terminal path, which
// makes it the one place that must not miss". Establish was a path it missed:
// every error return went straight back to the caller, so c.hs.Close() never
// ran. crypto/tls parks a QUICConn's handshake on a goroutine that only Close
// releases, so a handshake given up on leaked that goroutine plus the
// tls.QUICConn and its buffers — one per abandoned handshake, for the process
// lifetime, which is what a client dialling an unreachable or brownout peer
// produces in a loop.
//
// WHY THE ASSERTION IS NOT A GOROUTINE COUNT. The goroutine is the consequence
// that matters, and on the toolchain this module pins it is not observable at
// all: a test asserting it would pass with the latch deleted, which is worse
// than no test. crypto/tls in go1.25.13 wires the handshake goroutine's escape
// hatch to the context handed to QUICConn.Start —
//
//	c.quic.cancelc = handshakeCtx.Done()   // crypto/tls/conn.go:1538, go1.25.13
//
// — and quicWaitForSignal selects on it, so Establish's own `defer cancel()` on
// the whole-handshake bound releases the goroutine on the way out whether or not
// anything latched. Measured on the unfixed code: gone before the first poll
// after Establish returns. Go 1.26 replaced that with a plain `c.quic.ctx` field
// and an unconditional `c.quic.blockedc <- struct{}{}`, so nothing but
// QUICConn.Close drains it and the leak is permanent — that is the toolchain the
// issue reproduced on, and the ci job `quic-next-toolchain` is what keeps
// watching it.
//
// So the assertion below is the half of the latch that IS toolchain-independent,
// and it pins the same single call rather than a downstream effect of it:
// terminateLocked closes c.hs and publishes c.closeErr/c.done in one body, so a
// caller that can read the handshake's error back off the connection is a caller
// whose handshake goroutine was released. It is asserted through the public
// OpenStreamContext, which parks on that latch (waitStreamCredit) — with the
// latch missing it parks forever, which is the shape of the bug a caller sees.
//
// The rows are the error returns of Establish, one each. Establish latches by
// wrapping handshakeLocked rather than by a call at each return, so the coverage
// here is a check on that wrapper rather than four independent fixes — but a row
// per return is what proves the wrapper actually sits outside all of them.

// latchBudget is how long a caller parked on the connection is given to wake.
// Every row runs inside a synctest bubble, so this is exact fake time: a passing
// run spends none of it, and a failing one spends none in real time either.
const latchBudget = 5 * time.Second

// assertLatched fails unless c terminated carrying want: a caller parked on the
// connection's latch must wake, and must read back the error that ended the
// handshake rather than its own ctx expiring.
func assertLatched(t *testing.T, c *Conn, want error, explain string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), latchBudget)
	defer cancel()
	start := time.Now()

	_, got := c.OpenStreamContext(ctx)

	// Elapsed time is the discriminator, not the error value: the latch may
	// legitimately publish context.DeadlineExceeded — the bound expiring is one of
	// the rows — which is exactly what a caller whose own ctx ran out also reads.
	// Inside the bubble this is exact: a woken caller spends none of the budget, a
	// parked one spends all of it, and neither costs real time.
	elapsed := time.Since(start)
	require.Lessf(t, elapsed, latchBudget,
		"a caller is still parked on the connection %v after Establish "+
			"returned %v — the abandoned handshake never latched terminateLocked, "+
			"so c.done was never closed and c.hs.Close never ran (%s)",
		elapsed, want, explain)
	require.ErrorIsf(t, got, want,
		"the terminated connection reports %v, want %v — the latch "+
			"publishes closeErr before closing done, and first-error-wins means it "+
			"must be the error that ended the handshake (%s)", got, want, explain)
}

// craftedPeerConn returns a client Conn plus the channel its transport reads
// from, so a test can hand the handshake one crafted server Initial. The packet
// cannot be built before the connection exists: NewConn draws a random
// connection ID and the Initial keys derive from it (RFC 9001 §5.2), so the
// sealing keys are InitialKeys(c.origDCID) rather than a fixed set.
func craftedPeerConn(t *testing.T) (*Conn, chan []byte) {
	t.Helper()
	_, pool := genServerCert(t)
	rx := make(chan []byte, 1)
	tx := make(chan []byte, 8) // the ClientHello, and a CONNECTION_CLOSE if one is sent
	c, err := NewConn(&chanPC{rx: rx, tx: tx}, &tls.Config{ServerName: "example.com", RootCAs: pool},
		AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1 << 20}))
	require.NoErrorf(t, err, "NewConn: %v", err)
	return c, rx
}

// serverInitialWith seals frames into a server Initial the client c will accept
// and decrypt.
func serverInitialWith(t *testing.T, c *Conn, frames []byte) []byte {
	t.Helper()
	_, serverKeys := InitialKeys(c.origDCID)
	return craftServerInitial(t, serverKeys, nil, []byte{0xaa}, 0, frames)
}

func TestConn_Establish_LatchesOnEveryErrorPath(t *testing.T) {
	tests := []struct {
		name string
		// build returns a connection poised to fail Establish. It runs inside the
		// bubble, because the TLS handshake it starts belongs to the bubble too.
		build func(t *testing.T) *Conn
		// wantEstablish, when non-nil, is the sentinel Establish itself must
		// return — the guard against a row that has drifted into staging some other
		// failure and would then pass for the wrong reason.
		wantEstablish error
		// wantTimeout replaces wantEstablish where the error's identity is not
		// stable: at the bound's expiry the ctx timer and the transport read
		// deadline come due at the same instant, so the caller sees either
		// context.DeadlineExceeded or the transport's i/o timeout
		// (handshaketimeout_test.go measured both). isTimeout covers the pair.
		wantTimeout bool
		// wantLatched is the error the latch must publish. Nil means "exactly the
		// error Establish returned", which is the invariant on every return that
		// does not close the connection on its own way out.
		wantLatched error
		explain     string
	}{
		{
			name: "the whole-handshake bound expires",
			build: func(t *testing.T) *Conn {
				return blackholeConn(t, WithHandshakeTimeout(2*time.Second))
			},
			wantTimeout: true,
			explain: "readWithPTO's error return — the path the issue's own repro takes, " +
				"and the one a client dialling a brownout peer takes in a loop",
		},
		{
			name: "the Initial flight cannot be written",
			build: func(t *testing.T) *Conn {
				_, pool := genServerCert(t)
				c, err := NewConn(&failWritePC{failOn: 1},
					&tls.Config{ServerName: "example.com", RootCAs: pool},
					AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1 << 20}))
				if err != nil {
					t.Fatalf("NewConn: %v", err)
				}
				return c
			},
			wantEstablish: errSendFailed,
			explain: "sendInitialFlight's error return — the ClientHello never left the " +
				"host, so no peer will ever answer and the connection is finished",
		},
		{
			name: "the peer closes during the handshake",
			build: func(t *testing.T) *Conn {
				c, rx := craftedPeerConn(t)
				rx <- serverInitialWith(t, c, AppendConnectionClose(nil, false, ErrCodeConnectionRefused, 0, nil))
				return c
			},
			wantEstablish: ErrHandshakeClosed,
			explain: "the ErrHandshakeClosed return — the loop exits on c.closed, which " +
				"OnConnectionClose sets without latching anything itself",
		},
		{
			name: "the peer commits a protocol violation",
			build: func(t *testing.T) *Conn {
				c, rx := craftedPeerConn(t)
				// A STREAM frame is not permitted in an Initial packet (RFC 9000 §12.4
				// Table 3); permitInSpace rejects it as a PROTOCOL_VIOLATION.
				rx <- serverInitialWith(t, c, []byte{0x08, 0x00})
				return c
			},
			wantEstablish: ErrProtocolViolation,
			wantLatched:   ErrConnClosed,
			explain: "the c.fail return — the one that already latched, through " +
				"closeWithErrorLocked, and the wrapper must not clobber it: the caller " +
				"reads back the close, not the violation the catch-all saw",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := tc.build(t)

				err := c.Establish(context.Background())

				require.Errorf(t, err, "Establish succeeded; this row stages a failure, so a green "+
					"handshake means the fixture stopped staging it (%s)", tc.explain)
				if tc.wantEstablish != nil {
					require.ErrorIsf(t, err, tc.wantEstablish,
						"Establish returned %v, want %v — the row is staging a "+
							"different failure than it means to (%s)", err, tc.wantEstablish, tc.explain)
				}
				if tc.wantTimeout {
					require.Truef(t, isTimeout(err),
						"Establish returned %v, which isTimeout rejects — the row "+
							"stages the bound expiring (%s)", err, tc.explain)
				}
				want := tc.wantLatched
				if want == nil {
					want = err
				}
				assertLatched(t, c, want, tc.explain)
			})
		})
	}
}

// staggeredCtx reports no error until Err has been called n times, then reports
// cancelled. It is what makes Poll's arm→recheck return testable: that return
// exists for a cancel landing between the entry guard and the read deadline
// being armed, which is a genuine race and not one a test can hit by timing.
type staggeredCtx struct {
	context.Context
	mu sync.Mutex
	n  int
}

func (c *staggeredCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n > 0 {
		c.n--
		return c.Context.Err()
	}
	return context.Canceled
}

// TestConn_Poll_LatchesOnCtxCancel covers the other two returns the same
// invariant reaches. Poll's doc has always said "Every error return latches
// terminateLocked so a blocked Do wakes", and its two ctx.Err() returns did not.
// The ctx there is the connection-lifetime one — both callers pass connCtx — so
// its cancel IS the connection ending, and a Do parked on the connection had
// nothing to wake it. In practice the cancel usually follows a Close that
// latched first, which is why nothing noticed; "usually" is not the invariant
// the doc states.
func TestConn_Poll_LatchesOnCtxCancel(t *testing.T) {
	tests := []struct {
		name string
		// skip is how many ctx.Err() calls report nil before the cancel is seen:
		// none lands on the entry guard, one on the arm→recheck guard.
		skip    int
		explain string
	}{
		{
			name:    "the entry guard",
			skip:    0,
			explain: "the cancel is already visible when Poll starts",
		},
		{
			name: "the arm→recheck guard",
			skip: 1,
			explain: "the cancel lands after the entry guard, so the freshly-armed " +
				"future read deadline would otherwise mask it",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel() // releases Poll's read watchdog before the bubble ends
				c := blackholeConn(t)

				err := c.Poll(&staggeredCtx{Context: parent, n: tc.skip})

				require.ErrorIsf(t, err, context.Canceled,
					"Poll returned %v, want context.Canceled — the row is not "+
						"landing on %s (%s)", err, tc.name, tc.explain)
				assertLatched(t, c, context.Canceled, tc.explain)
			})
		})
	}
}

// TestConn_Establish_LatchKeepsTheHandshakeError pins the ordering the wrapper
// depends on: a caller that closes the connection after a failed Establish gets
// the handshake's error back, not ErrConnClosed. terminateLocked is
// first-error-wins, so latching in Establish is what makes Close idempotent here
// rather than a second, later teardown that renames the failure.
func TestConn_Establish_LatchKeepsTheHandshakeError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := blackholeConn(t, WithHandshakeTimeout(2*time.Second))

		err := c.Establish(context.Background())

		require.Error(t, err,
			"Establish succeeded against a blackhole; it can only end by running its bound out")
		_ = c.Close()
		assertLatched(t, c, err,
			"Close after a failed Establish must not overwrite the handshake's error with ErrConnClosed")
	})
}
