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
// corresponding MAX_STREAM_DATA / MAX_DATA frames (RFC 9000 §4.1).
func TestConn_ReceiveFlowControl_GrantsCredit(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}, connRecvMax: DefaultConnRecvWindow}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	h := &connFrameHandler{c: c}

	// Deliver and consume half the connection window (also well past the stream
	// window), which should grant on both levels.
	n := int(DefaultConnRecvWindow / 2)
	if err := h.OnStream(s.ID(), 0, false, make([]byte, n)); err != nil {
		t.Fatal(err)
	}
	if got := len(s.Recv()); got != n {
		t.Fatalf("consumed %d bytes, want %d", got, n)
	}

	if s.recvMax <= DefaultStreamRecvWindow {
		t.Fatalf("stream recvMax = %d, want > %d", s.recvMax, DefaultStreamRecvWindow)
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
