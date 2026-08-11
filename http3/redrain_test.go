package http3

import (
	"context"
	"testing"
)

// lateDeliveryStream reproduces the window recvStep's second drain exists for.
//
// The QUIC reader is asynchronous, so a stream can go from "nothing readable" to
// "finished, with bytes" between the drain at the top of a receive step and the
// RecvState snapshot below it. Concluding on that snapshot without draining again
// throws away the bytes that arrived in the gap — for a response that fits in one
// burst, the whole response.
//
// The fake makes that window exact instead of hoping for it: the first Recv
// returns nothing and marks the stream finished, and only the SECOND Recv hands
// over the payload.
type lateDeliveryStream struct {
	quicStream
	payload   []byte
	firstDone bool
	delivered bool
}

func (s *lateDeliveryStream) Recv() []byte {
	if !s.firstDone {
		s.firstDone = true
		return nil // nothing yet — and RecvState below will say "finished"
	}
	if s.delivered {
		return nil
	}
	s.delivered = true
	return s.payload
}

func (s *lateDeliveryStream) RecvState() (finished, reset bool, code uint64) {
	// Finished from the first snapshot onwards, which is what makes the second
	// drain the only thing standing between the caller and a lost response.
	return true, false, 0
}

func (s *lateDeliveryStream) WaitReadable(context.Context) error { return nil }

// TestRecvStep_DrainsAgainAfterFinishedFlips is the gate for the async-finished
// re-drain. Without the second drain recvStep reports ended with the payload
// still undelivered, and the caller concludes on an empty stream.
func TestRecvStep_DrainsAgainAfterFinishedFlips(t *testing.T) {
	payload := AppendFrameHeader(nil, FrameData, 3)
	payload = append(payload, 'a', 'b', 'c')

	conn := &fakeConn{req: &fakeStream{}}
	client, _ := NewClientFake(conn, nil)
	stream := &lateDeliveryStream{payload: payload}

	var fr FrameReader
	ended, err := client.recvStep(context.Background(), stream, &fr)
	if err != nil {
		t.Fatalf("recvStep: %v", err)
	}
	if ended {
		t.Fatal("recvStep reported the stream ended while bytes were still undelivered — " +
			"finished flipped between the first drain and the snapshot, and without the " +
			"second drain those bytes are lost")
	}
	if fr.Buffered() == 0 {
		t.Error("no bytes were fed to the frame reader; the second drain did not run")
	}
}

// TestRecvStep_EndsOnceEverythingIsDrained is the other side: after the late
// bytes have been taken, the next step must conclude rather than spin.
func TestRecvStep_EndsOnceEverythingIsDrained(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, _ := NewClientFake(conn, nil)
	stream := &lateDeliveryStream{payload: nil, firstDone: true, delivered: true}

	var fr FrameReader
	ended, err := client.recvStep(context.Background(), stream, &fr)
	if err != nil {
		t.Fatalf("recvStep: %v", err)
	}
	if !ended {
		t.Error("recvStep did not conclude on a finished, fully drained stream")
	}
}
