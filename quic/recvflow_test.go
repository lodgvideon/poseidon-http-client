package quic

import "testing"

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
	if err != nil {
		t.Fatal(err)
	}
	h := &connFrameHandler{c: c}
	win := int(DefaultStreamRecvWindow)

	// Fill the stream window exactly and consume it: raises the stream limit.
	if err := h.OnStream(s.ID(), 0, false, make([]byte, win)); err != nil {
		t.Fatal(err)
	}
	if got := len(s.Recv()); got != win {
		t.Fatalf("consumed %d bytes, want %d", got, win)
	}
	if s.recvMax <= DefaultStreamRecvWindow {
		t.Fatalf("stream recvMax = %d, want > %d", s.recvMax, DefaultStreamRecvWindow)
	}

	// The raised limit admits another window; consuming it crosses the connection
	// half-window and grants at the connection level too.
	if err := h.OnStream(s.ID(), uint64(win), false, make([]byte, win)); err != nil {
		t.Fatal(err)
	}
	if got := len(s.Recv()); got != win {
		t.Fatalf("consumed %d bytes, want %d", got, win)
	}
	if c.connRecvMax <= DefaultConnRecvWindow {
		t.Fatalf("conn recvMax = %d, want > %d", c.connRecvMax, DefaultConnRecvWindow)
	}

	var col ctrlCollector
	if err := ParseFrames(c.pendingCtrl, &col); err != nil {
		t.Fatal(err)
	}
	if col.streamData[s.ID()] != s.recvMax {
		t.Fatalf("MAX_STREAM_DATA = %d, want %d", col.streamData[s.ID()], s.recvMax)
	}
	if !col.dataSet || col.data != c.connRecvMax {
		t.Fatalf("MAX_DATA = %d (set=%v), want %d", col.data, col.dataSet, c.connRecvMax)
	}
}

// TestConformance_RFC9000_Sec41_StreamFlowControlEnforced checks that stream data
// ending past the advertised per-stream limit is a FLOW_CONTROL_ERROR.
func TestConformance_RFC9000_Sec41_StreamFlowControlEnforced(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	h := &connFrameHandler{c: c}
	if err := h.OnStream(s.ID(), 0, false, make([]byte, int(DefaultStreamRecvWindow)+1)); err != ErrFlowControl {
		t.Fatalf("data past the stream limit = %v, want ErrFlowControl", err)
	}
	if code, ok := closeCodeFor(ErrFlowControl); !ok || code != ErrCodeFlowControlError {
		t.Fatalf("closeCodeFor(ErrFlowControl) = %#x,%v, want FLOW_CONTROL_ERROR", code, ok)
	}
}

// TestConformance_RFC9000_Sec41_ConnFlowControlEnforced checks that data across
// streams exceeding the advertised connection limit is a FLOW_CONTROL_ERROR even
// when each stream stays within its own limit.
func TestConformance_RFC9000_Sec41_ConnFlowControlEnforced(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 8}, connRecvMax: 300 << 10}
	s1, _ := c.OpenStream()
	s2, _ := c.OpenStream()
	h := &connFrameHandler{c: c}
	if err := h.OnStream(s1.ID(), 0, false, make([]byte, 200<<10)); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStream(s2.ID(), 0, false, make([]byte, 200<<10)); err != ErrFlowControl {
		t.Fatalf("combined data past the connection limit = %v, want ErrFlowControl", err)
	}
}

// TestConn_FlowControl_RetransmitNoDoubleCount checks that re-delivered bytes (a
// retransmission) count once against the connection limit, keyed on the stream's
// highest received offset.
func TestConn_FlowControl_RetransmitNoDoubleCount(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: 300 << 10}
	s, _ := c.OpenStream()
	h := &connFrameHandler{c: c}
	if err := h.OnStream(s.ID(), 0, false, make([]byte, 200<<10)); err != nil {
		t.Fatal(err)
	}
	if err := h.OnStream(s.ID(), 0, false, make([]byte, 200<<10)); err != nil { // retransmission
		t.Fatalf("retransmitted data must not re-count: %v", err)
	}
	if c.connRecvTotal != 200<<10 {
		t.Fatalf("connRecvTotal = %d, want %d (retransmit counted once)", c.connRecvTotal, 200<<10)
	}
}

func TestConn_ReceiveFlowControl_NoGrantBelowThreshold(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	h := &connFrameHandler{c: c}

	small := int(DefaultStreamRecvWindow/2) - 1 // just under the half-window batch threshold
	if err := h.OnStream(s.ID(), 0, false, make([]byte, small)); err != nil {
		t.Fatal(err)
	}
	s.Recv()
	if len(c.pendingCtrl) != 0 {
		t.Fatal("no credit grant expected below the half-window threshold")
	}
	if s.recvMax != DefaultStreamRecvWindow {
		t.Fatalf("stream recvMax should be unchanged, got %d", s.recvMax)
	}
}
