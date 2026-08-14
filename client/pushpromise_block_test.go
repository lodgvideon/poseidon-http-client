package client

import (
	"context"
	"io"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// scriptedRespStream replays a fixed event sequence. respStream is Recv+Close,
// so nothing else has to be stubbed.
type scriptedRespStream struct {
	events []conn.StreamEvent
	i      int
}

func (s *scriptedRespStream) Recv(context.Context) (conn.StreamEvent, error) {
	if s.i >= len(s.events) {
		return conn.StreamEvent{}, io.EOF
	}
	ev := s.events[s.i]
	s.i++
	return ev, nil
}

func (s *scriptedRespStream) Close() error { return nil }

// promiseEvent builds an EventPushPromise carrying a pooled header block with a
// field in it. Release clears and truncates the field slice before returning the
// block to the pool, so len(blk.Fields()) afterwards is a direct read of whether
// the event was released — no sync.Pool identity guess needed. (Single-threaded
// test: nothing else can draw the block back out and refill it in between.)
func promiseEvent() (conn.StreamEvent, *conn.HeaderBlock) {
	blk := conn.NewHeaderBlock([]conn.HeaderField{
		{Name: []byte("x-promised"), Value: []byte("promised")},
	})
	return conn.StreamEvent{
		Type:         conn.EventPushPromise,
		PushStreamID: 2,
		Headers:      blk.Fields(),
		Block:        blk,
	}, blk
}

// TestBodyReader_PushPromise_ReleasesBlock pins that a PUSH_PROMISE reaching the
// streaming body reader hands its header block back.
//
// A PUSH_PROMISE is delivered on the PARENT stream, which is the stream this
// reader is draining, so it lands here whenever a push handler is installed and
// the caller used Do with a streaming body. Client.Do's buffered path dispatches
// the promise and grpc's receive loop explicitly drops it with a Release; this
// path had no arm at all, so the event fell out of the switch and the block was
// dropped on the floor while every neighbouring skip arm recycled. Divergence
// between sibling paths is the shape, which is why the assertion is on the block
// rather than on the (unchanged) bytes the caller reads.
func TestBodyReader_PushPromise_ReleasesBlock(t *testing.T) {
	promise, blk := promiseEvent()
	r := &responseBodyReader{
		ctx: context.Background(),
		stream: &scriptedRespStream{events: []conn.StreamEvent{
			promise,
			{Type: conn.EventData, Data: []byte("body"), EndStream: true},
		}},
	}

	buf := make([]byte, 16)
	n, err := r.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read: %v", err)
	}
	if got := string(buf[:n]); got != "body" {
		t.Errorf("Read = %q, want %q — the promise must be skipped, not surfaced", got, "body")
	}
	if n := len(blk.Fields()); n != 0 {
		t.Errorf("promise block still holds %d fields: it was never released", n)
	}
}

// TestStreamResponse_PushPromise_ReleasesBlock is the same pin for
// StreamResponse.Recv, the other streaming path that reads the parent stream.
// StreamResponse exposes no push surface, so the promise is skipped — but the
// block still belongs to the pool, exactly as in the EventHeaders and
// EventInterimHeaders arms beside it.
func TestStreamResponse_PushPromise_ReleasesBlock(t *testing.T) {
	promise, blk := promiseEvent()
	sr := &StreamResponse{
		stream: &scriptedRespStream{events: []conn.StreamEvent{
			promise,
			{Type: conn.EventData, Data: []byte("body"), EndStream: true},
		}},
	}

	ev, err := sr.Recv(context.Background())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventData {
		t.Errorf("Recv returned %v, want EventData — the promise must be skipped, not surfaced", ev.Type)
	}
	if n := len(blk.Fields()); n != 0 {
		t.Errorf("promise block still holds %d fields: it was never released", n)
	}
}
