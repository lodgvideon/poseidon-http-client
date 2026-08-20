package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctrlCollector captures MAX_DATA / MAX_STREAM_DATA frames decoded from the
// queued control bytes.
type ctrlCollector struct {
	nopFrameHandler
	streamData map[uint64]uint64
	data       uint64
	dataSet    bool
}

func (h *ctrlCollector) OnMaxStreamData(id, maximum uint64) error {
	if h.streamData == nil {
		h.streamData = map[uint64]uint64{}
	}
	h.streamData[id] = maximum
	return nil
}
func (h *ctrlCollector) OnMaxData(maximum uint64) error {
	h.data, h.dataSet = maximum, true
	return nil
}

// TestConn_ReceiveFlowControl_GrantsCredit checks that consuming enough received
// data raises the advertised per-stream and connection limits and queues the
// corresponding MAX_STREAM_DATA / MAX_DATA frames (RFC 9000 §4.1). Data must stay
// within the advertised limits, so it is delivered a window at a time, consuming
// between deliveries to earn more credit.
func TestConn_ReceiveFlowControl_GrantsCredit(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	require.NoError(t, err, "open the stream the credit is granted on")
	h := &connFrameHandler{c: c}
	win := int(DefaultStreamRecvWindow)

	// Fill the stream window exactly and consume it: raises the stream limit.
	errFirst := h.OnStream(s.ID(), 0, false, make([]byte, win))
	consumedFirst := len(s.Recv())
	streamMaxAfterFirst := s.recvMax
	// The raised limit admits another window; consuming it crosses the connection
	// half-window and grants at the connection level too.
	errSecond := h.OnStream(s.ID(), uint64(win), false, make([]byte, win))
	consumedSecond := len(s.Recv())
	var col ctrlCollector
	parseErr := ParseFrames(c.pendingCtrl, &col)

	require.NoError(t, errFirst, "a full window of data must be within the advertised limit")
	require.Equalf(t, win, consumedFirst, "consumed %d bytes, want %d", consumedFirst, win)
	require.Greaterf(t, streamMaxAfterFirst, DefaultStreamRecvWindow,
		"stream recvMax = %d, want > %d", streamMaxAfterFirst, DefaultStreamRecvWindow)
	require.NoError(t, errSecond, "the raised limit must admit a second window")
	require.Equalf(t, win, consumedSecond, "consumed %d bytes, want %d", consumedSecond, win)
	require.Greaterf(t, c.connRecvMax, DefaultConnRecvWindow,
		"conn recvMax = %d, want > %d", c.connRecvMax, DefaultConnRecvWindow)
	require.NoError(t, parseErr, "the queued control frames must decode")
	assert.Equalf(t, s.recvMax, col.streamData[s.ID()],
		"MAX_STREAM_DATA = %d, want %d", col.streamData[s.ID()], s.recvMax)
	assert.Truef(t, col.dataSet && col.data == c.connRecvMax,
		"MAX_DATA = %d (set=%v), want %d", col.data, col.dataSet, c.connRecvMax)
}

// TestConformance_RFC9000_Sec41_StreamFlowControlEnforced checks that stream data
// ending past the advertised per-stream limit is a FLOW_CONTROL_ERROR.
func TestConformance_RFC9000_Sec41_StreamFlowControlEnforced(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	require.NoError(t, err, "open the stream the limit is enforced on")
	h := &connFrameHandler{c: c}

	overErr := h.OnStream(s.ID(), 0, false, make([]byte, int(DefaultStreamRecvWindow)+1))
	code, ok := closeCodeFor(ErrFlowControl)

	require.ErrorIsf(t, overErr, ErrFlowControl,
		"data past the stream limit = %v, want ErrFlowControl", overErr)
	assert.Truef(t, ok && code == ErrCodeFlowControlError,
		"closeCodeFor(ErrFlowControl) = %#x,%v, want FLOW_CONTROL_ERROR", code, ok)
}

// TestConformance_RFC9000_Sec41_ConnFlowControlEnforced checks that data across
// streams exceeding the advertised connection limit is a FLOW_CONTROL_ERROR even
// when each stream stays within its own limit.
func TestConformance_RFC9000_Sec41_ConnFlowControlEnforced(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 8}, connRecvMax: 300 << 10}
	s1, _ := c.OpenStream()
	s2, _ := c.OpenStream()
	h := &connFrameHandler{c: c}

	withinErr := h.OnStream(s1.ID(), 0, false, make([]byte, 200<<10))
	overErr := h.OnStream(s2.ID(), 0, false, make([]byte, 200<<10))

	require.NoError(t, withinErr, "the first stream stays inside the connection limit")
	require.ErrorIsf(t, overErr, ErrFlowControl,
		"combined data past the connection limit = %v, want ErrFlowControl", overErr)
}

// TestConn_FlowControl_RetransmitNoDoubleCount checks that re-delivered bytes (a
// retransmission) count once against the connection limit, keyed on the stream's
// highest received offset.
func TestConn_FlowControl_RetransmitNoDoubleCount(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: 300 << 10}
	s, _ := c.OpenStream()
	h := &connFrameHandler{c: c}

	firstErr := h.OnStream(s.ID(), 0, false, make([]byte, 200<<10))
	retransErr := h.OnStream(s.ID(), 0, false, make([]byte, 200<<10)) // retransmission

	require.NoError(t, firstErr, "the first delivery is within the connection limit")
	require.NoErrorf(t, retransErr, "retransmitted data must not re-count: %v", retransErr)
	assert.Equalf(t, uint64(200<<10), c.connRecvTotal,
		"connRecvTotal = %d, want %d (retransmit counted once)", c.connRecvTotal, 200<<10)
}

func TestConn_ReceiveFlowControl_NoGrantBelowThreshold(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	require.NoError(t, err, "open the stream the grant threshold is measured on")
	h := &connFrameHandler{c: c}
	small := int(DefaultStreamRecvWindow/2) - 1 // just under the half-window batch threshold

	deliverErr := h.OnStream(s.ID(), 0, false, make([]byte, small))
	s.Recv()

	require.NoError(t, deliverErr, "a sub-threshold delivery is within the advertised limit")
	assert.Empty(t, c.pendingCtrl, "no credit grant expected below the half-window threshold")
	assert.Equalf(t, DefaultStreamRecvWindow, s.recvMax,
		"stream recvMax should be unchanged, got %d", s.recvMax)
}

// TestConformance_RFC9000_Sec133_NoCreditAfterFinalSize pins the §13.3 SHOULD that
// an endpoint stop sending MAX_STREAM_DATA once the receiving part is in Size Known
// or Reset Recvd: the final size is settled, so extra credit buys the peer nothing
// and only costs a control frame. Connection-level MAX_DATA is unaffected.
func TestConformance_RFC9000_Sec133_NoCreditAfterFinalSize(t *testing.T) {
	win := int(DefaultStreamRecvWindow)

	for _, tc := range []struct {
		name  string
		close func(h *connFrameHandler, id uint64) error
	}{
		{"size_known", func(h *connFrameHandler, id uint64) error {
			return h.OnStream(id, uint64(win), true, nil) // FIN at the current offset
		}},
		{"reset_recvd", func(h *connFrameHandler, id uint64) error {
			return h.OnResetStream(id, 0, uint64(win))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
			s, err := c.OpenStream()
			require.NoError(t, err, "open the stream whose final size becomes known")
			h := &connFrameHandler{c: c}
			require.NoError(t, h.OnStream(s.ID(), 0, false, make([]byte, win)),
				"fill the window before the final size is settled")
			require.NoError(t, tc.close(h, s.ID()), "settle the final size")
			before := s.recvMax

			s.Recv() // consuming a full window would normally advance the limit

			assert.Equalf(t, before, s.recvMax,
				"recvMax advanced %d -> %d after the final size was known", before, s.recvMax)
		})
	}

	// Control: without a FIN or reset the same consumption does advance the limit.
	t.Run("control_open_stream", func(t *testing.T) {
		c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
		s, err := c.OpenStream()
		require.NoError(t, err, "open the control stream, which never learns a final size")
		h := &connFrameHandler{c: c}
		require.NoError(t, h.OnStream(s.ID(), 0, false, make([]byte, win)), "fill the window")
		before := s.recvMax

		s.Recv()

		assert.NotEqual(t, before, s.recvMax,
			"control: an open stream did not advance recvMax, so the test proves nothing")
	})
}

// TestConn_ReceiveFlowControl_GrantsAtExactlyTheThreshold is the accept side of
// the half-window grant batch.
//
// TestConn_ReceiveFlowControl_NoGrantBelowThreshold consumes
// DefaultStreamRecvWindow/2 - 1 and asserts nothing is queued. There was no case
// at exactly DefaultStreamRecvWindow/2, which is the FIRST value that must grant,
// so turning the threshold comparison from >= into > — moving the first grant a
// byte later — left the whole suite green. One byte is harmless; the comparison
// being unpinned is not, because this is the only place credit is ever extended
// and a peer that has stopped sending is waiting on it. #843.
func TestConn_ReceiveFlowControl_GrantsAtExactlyTheThreshold(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	require.NoError(t, err, "open the stream the grant threshold is measured on")
	h := &connFrameHandler{c: c}
	exact := int(DefaultStreamRecvWindow / 2) // the first amount that must grant

	deliverErr := h.OnStream(s.ID(), 0, false, make([]byte, exact))
	consumed := len(s.Recv())

	require.NoError(t, deliverErr, "half a window is well within the advertised limit")
	require.Equalf(t, exact, consumed, "consumed %d bytes, want %d", consumed, exact)
	var col ctrlCollector
	require.NoError(t, ParseFrames(c.pendingCtrl, &col), "the queued control frames must decode")
	want := uint64(exact) + DefaultStreamRecvWindow
	assert.Equalf(t, want, s.recvMax,
		"stream recvMax = %d, want %d — consuming exactly half a window is the threshold "+
			"at which credit is extended, and the first grant arriving a byte late is a "+
			"comparison nothing else in the suite holds", s.recvMax, want)
	assert.Equalf(t, want, col.streamData[s.ID()],
		"MAX_STREAM_DATA = %d, want %d — the limit must be advertised, not only recorded "+
			"locally", col.streamData[s.ID()], want)
}

// TestConformance_RFC9000_Sec41_ConnLimitAcceptsItsLastByte pins the connection
// flow-control comparison AT the limit, in both directions.
//
// TestConformance_RFC9000_Sec41_ConnFlowControlEnforced charges 400 KiB against a
// 300 KiB limit — 100 KiB past it — so the comparison could turn from > into >=
// and nothing noticed. RFC 9000 §4.1 makes the advertised maximum the last byte
// the peer MAY send, not the first it may not: an endpoint that refuses data
// ending exactly at its own limit closes conformant connections with
// FLOW_CONTROL_ERROR. The stream-level twin of this comparison is caught by twelve
// tests, because plenty of fixtures fill a stream window exactly; nothing ever
// landed on the connection limit. #843.
func TestConformance_RFC9000_Sec41_ConnLimitAcceptsItsLastByte(t *testing.T) {
	const limit = 300 << 10
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 8}, connRecvMax: limit}
	s1, err1 := c.OpenStream()
	require.NoError(t, err1, "open the first stream")
	s2, err2 := c.OpenStream()
	require.NoError(t, err2, "open the second stream")
	h := &connFrameHandler{c: c}

	firstErr := h.OnStream(s1.ID(), 0, false, make([]byte, 200<<10))
	atLimitErr := h.OnStream(s2.ID(), 0, false, make([]byte, 100<<10)) // total == limit exactly
	totalAtLimit := c.connRecvTotal
	onePastErr := h.OnStream(s2.ID(), 100<<10, false, make([]byte, 1)) // one byte past

	require.NoError(t, firstErr, "200 KiB is inside the 300 KiB connection limit")
	assert.NoErrorf(t, atLimitErr,
		"data ending exactly at the advertised connection limit = %v, want nil — §4.1 "+
			"makes connRecvMax the last byte the peer may send, and refusing it drops a "+
			"conformant peer's final window", atLimitErr)
	assert.EqualValuesf(t, limit, totalAtLimit,
		"connRecvTotal = %d, want exactly the limit %d — if the fixture did not land ON "+
			"the limit it is testing the same thing as the over-limit case",
		totalAtLimit, limit)
	assert.ErrorIsf(t, onePastErr, ErrFlowControl,
		"one byte past the connection limit = %v, want ErrFlowControl", onePastErr)
}

// fillPendingCtrl fills c.pendingCtrl to the maxPendingCtrl bound the way a peer
// does it — a PATH_CHALLENGE flood, answered with a 9-byte PATH_RESPONSE each —
// so the bound under test is reached by peer input rather than by assignment.
func fillPendingCtrl(t *testing.T, c *Conn) {
	t.Helper()
	h := &connFrameHandler{c: c}
	for i := 0; len(c.pendingCtrl) < maxPendingCtrl; i++ {
		require.NoErrorf(t, h.OnPathChallenge(&[8]byte{byte(i), byte(i >> 8)}),
			"OnPathChallenge %d while filling the control queue", i)
		require.Lessf(t, i, 1000, "the control queue never filled; the fixture is wrong")
	}
	require.GreaterOrEqualf(t, len(c.pendingCtrl), maxPendingCtrl,
		"the control queue must start AT the bound, got %d bytes", len(c.pendingCtrl))
}

// TestConn_RegrantConnLimit_BoundedByPendingCtrl and its two siblings pin the
// maxPendingCtrl guard at the three regrant sites, none of which any test
// reached: each could be deleted outright and the whole suite stayed green.
//
// This is a peer-driven memory bound, the shape CONTRIBUTING's peer-input policy
// exists for. A peer that spams DATA_BLOCKED / STREAM_DATA_BLOCKED at a stale
// limit, or drives repeated loss episodes on a connection with many granted
// streams, is answered with a fresh credit frame every time; maxPendingCtrl is
// the only thing between that and unbounded growth of a buffer flush later tries
// to put in a single datagram. #842.
//
// Each test has a control arm one byte below the bound, where the queue MUST
// grow — without it, an arm whose regrant never fires at all would pass.
func TestConn_RegrantConnLimit_BoundedByPendingCtrl(t *testing.T) {
	newConn := func() *Conn { return &Conn{connRecvMax: DefaultConnRecvWindow} }

	t.Run("at_the_bound_nothing_is_queued", func(t *testing.T) {
		c := newConn()
		fillPendingCtrl(t, c)
		before := len(c.pendingCtrl)

		err := (&connFrameHandler{c: c}).OnDataBlocked(0) // stale limit: a regrant is due

		require.NoError(t, err, "DATA_BLOCKED at a stale limit")
		assert.Equalf(t, before, len(c.pendingCtrl),
			"the control queue grew from %d to %d bytes while already at the %d-byte "+
				"bound: a peer repeating DATA_BLOCKED can then grow it without limit",
			before, len(c.pendingCtrl), maxPendingCtrl)
	})
	t.Run("control_below_the_bound_it_grows", func(t *testing.T) {
		c := newConn()
		fillPendingCtrl(t, c)
		c.pendingCtrl = c.pendingCtrl[:maxPendingCtrl-1]
		before := len(c.pendingCtrl)

		err := (&connFrameHandler{c: c}).OnDataBlocked(0)

		require.NoError(t, err, "DATA_BLOCKED at a stale limit")
		assert.Greaterf(t, len(c.pendingCtrl), before,
			"control: one byte below the bound the regrant must be queued (%d -> %d); "+
				"without this the case above is satisfied by a regrant that never fires",
			before, len(c.pendingCtrl))
	})
}

// TestConn_RegrantStreamLimit_BoundedByPendingCtrl is the per-stream twin. #842.
func TestConn_RegrantStreamLimit_BoundedByPendingCtrl(t *testing.T) {
	newConn := func(t *testing.T) (*Conn, *Stream) {
		t.Helper()
		c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
		s, err := c.OpenStream()
		require.NoError(t, err, "open the stream the regrant is for")
		return c, s
	}

	t.Run("at_the_bound_nothing_is_queued", func(t *testing.T) {
		c, s := newConn(t)
		fillPendingCtrl(t, c)
		before := len(c.pendingCtrl)

		err := (&connFrameHandler{c: c}).OnStreamDataBlocked(s.ID(), 0)

		require.NoError(t, err, "STREAM_DATA_BLOCKED on a created bidi stream")
		assert.Equalf(t, before, len(c.pendingCtrl),
			"the control queue grew from %d to %d bytes at the %d-byte bound: a peer "+
				"repeating STREAM_DATA_BLOCKED can then grow it without limit",
			before, len(c.pendingCtrl), maxPendingCtrl)
	})
	t.Run("control_below_the_bound_it_grows", func(t *testing.T) {
		c, s := newConn(t)
		fillPendingCtrl(t, c)
		c.pendingCtrl = c.pendingCtrl[:maxPendingCtrl-1]
		before := len(c.pendingCtrl)

		err := (&connFrameHandler{c: c}).OnStreamDataBlocked(s.ID(), 0)

		require.NoError(t, err, "STREAM_DATA_BLOCKED on a created bidi stream")
		assert.Greaterf(t, len(c.pendingCtrl), before,
			"control: one byte below the bound the regrant must be queued (%d -> %d)",
			before, len(c.pendingCtrl))
	})
}

// TestConn_RegrantAfterLoss_BoundedByPendingCtrl is the loss-episode site, where
// the guard sits INSIDE the per-stream loop: a connection with many granted
// streams would otherwise append one frame per stream however full the queue
// already is. #842.
func TestConn_RegrantAfterLoss_BoundedByPendingCtrl(t *testing.T) {
	newConn := func(t *testing.T) *Conn {
		t.Helper()
		c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 8}, connRecvMax: DefaultConnRecvWindow}
		c.grantedStreams = map[uint64]struct{}{}
		for i := 0; i < 8; i++ {
			s, err := c.OpenStream()
			require.NoError(t, err, "open a granted stream")
			c.grantedStreams[s.ID()] = struct{}{}
		}
		require.Len(t, c.grantedStreams, 8,
			"the loop needs several granted streams, or one guard check is all it does")
		return c
	}

	t.Run("at_the_bound_nothing_is_queued", func(t *testing.T) {
		c := newConn(t)
		fillPendingCtrl(t, c)
		before := len(c.pendingCtrl)

		c.regrantAfterLoss()

		assert.Equalf(t, before, len(c.pendingCtrl),
			"the control queue grew from %d to %d bytes at the %d-byte bound: with the "+
				"guard gone, one loss episode appends a frame for every granted stream",
			before, len(c.pendingCtrl), maxPendingCtrl)
	})
	t.Run("control_below_the_bound_one_frame_is_queued", func(t *testing.T) {
		c := newConn(t)
		fillPendingCtrl(t, c)
		c.pendingCtrl = c.pendingCtrl[:maxPendingCtrl-1]
		before := len(c.pendingCtrl)

		c.regrantAfterLoss()

		assert.Greaterf(t, len(c.pendingCtrl), before,
			"control: one byte below the bound the loop must queue a re-grant (%d -> %d)",
			before, len(c.pendingCtrl))
		assert.LessOrEqualf(t, len(c.pendingCtrl), maxPendingCtrl+64,
			"the loop queued %d bytes from one byte below the bound: it must stop once the "+
				"first frame carries it past, not run to the end of the stream set",
			len(c.pendingCtrl))
	})
}
