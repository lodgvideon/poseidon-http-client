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
