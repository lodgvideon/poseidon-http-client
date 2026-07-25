package conn

import (
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConformance_RFC9113_Sec5_4_2_NoRSTStreamInResponseToReceivedRST pins RFC
// 9113 §5.4.2: "To avoid looping, an endpoint MUST NOT send a RST_STREAM in
// response to a RST_STREAM frame." A received RST_STREAM whose reset event cannot
// be enqueued (a slow consumer has filled the stream's event buffer) must still
// not provoke an outbound RST_STREAM — the reset is surfaced through the reset
// signal instead. Previously OnRSTStream delivered via s.push, whose overflow
// path emits an outbound RST_STREAM(REFUSED_STREAM), closing exactly the loop the
// rule forbids.
func TestConformance_RFC9113_Sec5_4_2_NoRSTStreamInResponseToReceivedRST(t *testing.T) {
	m := newFakeStreamMap()
	m.bufSize = 1
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	// Fill the stream's 1-slot event buffer so the reset event cannot enqueue.
	s.events <- StreamEvent{Type: EventData}

	if err := h.OnRSTStream(frame.FrameHeader{Type: frame.FrameRSTStream, StreamID: 1}, frame.ErrCodeCancel); err != nil {
		t.Fatalf("OnRSTStream: %v", err)
	}

	// A buggy overflow path writes the outbound RST asynchronously; give it time.
	// After the fix no such goroutine is spawned, so this only adds latency and
	// never flakes (the count stays 0 regardless of how long we wait).
	time.Sleep(100 * time.Millisecond)
	if got, code := m.w.rstSnapshot(); got != 0 {
		t.Errorf("client sent %d outbound RST_STREAM(%v) after a received RST — §5.4.2: an endpoint "+
			"MUST NOT send a RST_STREAM in response to a RST_STREAM frame", got, code)
	}

	// The reset is still surfaced to the slow consumer via the reset signal.
	select {
	case <-s.resetSignal:
	default:
		t.Error("reset not signaled to the consumer after the buffer overflowed")
	}
}
