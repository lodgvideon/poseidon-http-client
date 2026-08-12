package conn

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// A DATA frame can trip both refund thresholds at once, and on a single-stream
// download it does so on every refund: the connection counter and the stream
// counter are advanced by the same length on every frame, and both thresholds
// are recvWindowRefundThreshold by default, so they cross together. They
// decouple only when several streams share the connection and the connection
// counter runs ahead.
//
// Emitting the two WINDOW_UPDATEs through separate calls meant two wmu
// acquisitions and two socket writes per refund on exactly the shape a bulk
// download has (#455 item 1).

// wuPairConn builds a Conn whose writes land in a counting sink, so the test can
// assert on socket writes rather than on bytes. Windows are seeded so a single
// DATA frame of refundTripLen trips both thresholds at once.
func wuPairConn(t *testing.T) (*Conn, *Stream, *countingSink) {
	t.Helper()
	sink := &countingSink{}
	wb := bufio.NewWriterSize(sink, writeBufferSize)
	c := &Conn{
		fr:      frame.NewFramer(wb, bytes.NewReader(nil)),
		streams: map[uint32]*Stream{},
		opts: ConnOptions{
			Settings: AdvertisedSettings{InitialWindowSize: 65535, MaxConcurrentStreams: 100},
		}.defaulted(),
	}
	c.wbatch = newWriteBatcher(false, &c.wmu, wb)
	c.connRecvWindow = 1 << 20

	s := &Stream{id: 1, recvWindow: 1 << 20}
	c.streams[1] = s
	return c, s, sink
}

// refundTripLen is at least both thresholds, so one frame trips both.
const refundTripLen = recvWindowRefundThreshold

// TestOnDataReceived_BothRefunds_OneWrite is the gate: when a DATA frame trips
// both thresholds, the two WINDOW_UPDATEs must reach the socket in ONE write.
func TestOnDataReceived_BothRefunds_OneWrite(t *testing.T) {
	c, s, sink := wuPairConn(t)

	if err := c.onDataReceived(s, refundTripLen); err != nil {
		t.Fatalf("onDataReceived: %v", err)
	}

	if sink.writes != 1 {
		t.Errorf("socket writes = %d, want 1 — the stream and connection refunds went "+
			"out in separate flushes, which on a single-stream download is every refund",
			sink.writes)
	}
}

// TestOnDataReceived_BothRefunds_EmitsBoth is the control that keeps the gate
// honest: one write must not mean one WINDOW_UPDATE. It decodes what actually
// reached the wire and requires a refund for the stream AND for the connection.
//
// Without this, dropping either refund entirely would pass the write-count gate
// and look like an improvement.
func TestOnDataReceived_BothRefunds_EmitsBoth(t *testing.T) {
	sink := &captureSink{}
	wb := bufio.NewWriterSize(sink, writeBufferSize)
	c := &Conn{
		fr:      frame.NewFramer(wb, bytes.NewReader(nil)),
		streams: map[uint32]*Stream{},
		opts: ConnOptions{
			Settings: AdvertisedSettings{InitialWindowSize: 65535, MaxConcurrentStreams: 100},
		}.defaulted(),
	}
	c.wbatch = newWriteBatcher(false, &c.wmu, wb)
	c.connRecvWindow = 1 << 20
	s := &Stream{id: 1, recvWindow: 1 << 20}
	c.streams[1] = s

	if err := c.onDataReceived(s, refundTripLen); err != nil {
		t.Fatalf("onDataReceived: %v", err)
	}

	ids := windowUpdateStreamIDs(t, sink.buf.Bytes())
	var sawStream, sawConn bool
	for _, id := range ids {
		switch id {
		case 0:
			sawConn = true
		case 1:
			sawStream = true
		}
	}
	if !sawStream {
		t.Errorf("no WINDOW_UPDATE for stream 1; got ids %v — the stream's window is "+
			"never replenished and the download stalls", ids)
	}
	if !sawConn {
		t.Errorf("no WINDOW_UPDATE for the connection; got ids %v — the connection "+
			"window is never replenished and every stream stalls", ids)
	}
}

// captureSink keeps the bytes so a test can decode the frames that were sent.
type captureSink struct {
	buf    bytes.Buffer
	writes int
}

func (s *captureSink) Write(p []byte) (int, error) {
	s.writes++
	return s.buf.Write(p)
}

// windowUpdateStreamIDs walks a frame stream and returns the stream id of every
// WINDOW_UPDATE in it, in order.
func windowUpdateStreamIDs(t *testing.T, b []byte) []uint32 {
	t.Helper()
	var ids []uint32
	for len(b) >= 9 {
		length := int(b[0])<<16 | int(b[1])<<8 | int(b[2])
		typ := b[3]
		streamID := (uint32(b[5])<<24 | uint32(b[6])<<16 | uint32(b[7])<<8 | uint32(b[8])) &^ (1 << 31)
		if len(b) < 9+length {
			t.Fatalf("truncated frame: want %d payload bytes, have %d", length, len(b)-9)
		}
		if typ == byte(frame.FrameWindowUpdate) {
			ids = append(ids, streamID)
		}
		b = b[9+length:]
	}
	return ids
}
