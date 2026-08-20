package conn

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestStream_RecvAfterClose_RefusesOnEveryNonRecyclingExit names the mechanism.
//
// Stream.recv has two refusal gates in one critical section: the generation
// check, and the released gate that stops a reader BETWEEN two Recv calls from
// registering on a struct its own request has already Closed. Both tests in
// recvafterclose_test.go set connDone before Close — the one Stream.close path
// that recycles immediately — and the recycle bumps gen, so the generation check
// answers first and the released gate is never the deciding mechanism. One of
// them even accepts either error, so it cannot tell them apart. Deleting the
// released gate outright left the whole package green (#832).
//
// Stream.close has three other exits that set released and do NOT recycle, so
// gen is unchanged and only the released gate can refuse. Each is driven here,
// and the fixture asserts the non-recycle before the Act block: without that,
// this test would silently degrade into another copy of the gen-check tests.
//
// With the gate gone a Recv on any of the three passes the gen check, increments
// recvActive, and parks on the still-live channel until its own context expires
// — a full context's worth of waiting on a finished stream, plus a recvActive
// the next owner's recycle has to wait behind.
func TestStream_RecvAfterClose_RefusesOnEveryNonRecyclingExit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state func(s *Stream)
	}{
		{"already torn down by a peer reset or a push overflow",
			func(s *Stream) { s.closed = true }},
		{"both sides ended, so markStreamDone owns the recycle",
			func(s *Stream) { s.localEnded, s.remoteEnded = true, true }},
		{"the ordinary abandon path that ends in RST_STREAM",
			func(s *Stream) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := newGoAwayConn()
			c.fr = frame.NewFramer(&buf, bytes.NewReader(nil))
			s := newStream(1, 8, c, 65535)
			s.id = 1
			c.streams[1] = s
			s.mu.Lock()
			tc.state(s)
			s.mu.Unlock()
			ref := s.ref()
			gen := s.gen.Load()
			require.NoError(t, ref.Close(), "Close")
			require.Equalf(t, gen, s.gen.Load(),
				"the struct was recycled, so the generation check would refuse this handle and "+
					"the released gate would never be the mechanism under test — this fixture "+
					"has to reach a close path that leaves gen alone")

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			_, err := ref.Recv(ctx)
			elapsed := time.Since(start)

			assert.ErrorIsf(t, err, ErrStreamClosed,
				"Recv after Close = %v, want exactly ErrStreamClosed — this handle's lifetime "+
					"is still current, so the only thing that can refuse it is the released gate", err)
			assert.Lessf(t, elapsed, time.Second,
				"Recv after Close took %v; it registered and parked on the orphaned channel "+
					"instead of refusing, and would have waited out the caller's whole context", elapsed)
			s.mu.Lock()
			ra := s.recvActive
			s.mu.Unlock()
			assert.Zerof(t, ra,
				"recvActive = %d on a released struct; the next request to claim it would have "+
					"its own recycle deferred behind a reader that has nothing to do with it", ra)
		})
	}
}
