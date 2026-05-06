package conn

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func TestStreamEventType_String(t *testing.T) {
	cases := []struct {
		t    StreamEventType
		want string
	}{
		{EventHeaders, "headers"},
		{EventData, "data"},
		{EventTrailers, "trailers"},
		{EventReset, "reset"},
	}
	for _, c := range cases {
		if got := c.t.String(); got != c.want {
			t.Fatalf("%v: got %q, want %q", c.t, got, c.want)
		}
	}
}

func TestStreamEvent_TypeDispatch(t *testing.T) {
	headers := []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}
	e := StreamEvent{Type: EventHeaders, Headers: headers, EndStream: false}
	if e.Type != EventHeaders || len(e.Headers) != 1 {
		t.Fatalf("event = %+v", e)
	}
	r := StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeCancel}
	if r.Type != EventReset || r.RSTCode != frame.ErrCodeCancel {
		t.Fatalf("reset event = %+v", r)
	}
}

// fakeStreamWriter records what would have gone to the wire.
type fakeStreamWriter struct {
	mu          sync.Mutex
	headerCalls int
	dataCalls   int
	rstCalls    int
	lastRSTCode frame.ErrCode
}

func (w *fakeStreamWriter) writeHeaders(_ context.Context, _ *Stream, _ []hpack.HeaderField, _ bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.headerCalls++
	return nil
}
func (w *fakeStreamWriter) writeData(_ context.Context, _ *Stream, _ []byte, _ bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dataCalls++
	return nil
}
func (w *fakeStreamWriter) writeRSTStream(_ *Stream, code frame.ErrCode) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rstCalls++
	w.lastRSTCode = code
	return nil
}

func newTestStream(buf int) (*Stream, *fakeStreamWriter) {
	w := &fakeStreamWriter{}
	s := newStream(1, buf, w, 65535)
	return s, w
}

func TestStream_ID(t *testing.T) {
	s, _ := newTestStream(8)
	if s.ID() != 1 {
		t.Fatalf("ID = %d, want 1", s.ID())
	}
}

func TestStream_SendHeaders_DelegatesToWriter(t *testing.T) {
	s, w := newTestStream(8)
	err := s.SendHeaders(context.Background(),
		[]hpack.HeaderField{{Name: []byte(":method"), Value: []byte("GET")}},
		true)
	if err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	if w.headerCalls != 1 {
		t.Fatalf("headerCalls = %d, want 1", w.headerCalls)
	}
}

func TestStream_SendData_AfterEndStream_ReturnsErrStreamClosed(t *testing.T) {
	s, _ := newTestStream(8)
	if err := s.SendHeaders(context.Background(), nil, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	err := s.SendData(context.Background(), []byte("x"), false)
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendData err = %v, want ErrStreamClosed", err)
	}
}

func TestStream_Recv_ReturnsBufferedEvent(t *testing.T) {
	s, _ := newTestStream(8)
	s.push(StreamEvent{Type: EventHeaders, EndStream: true})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if e.Type != EventHeaders || !e.EndStream {
		t.Fatalf("event = %+v", e)
	}
}

func TestStream_Recv_BlocksUntilCancel(t *testing.T) {
	s, _ := newTestStream(8)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Recv(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Recv err = %v, want DeadlineExceeded", err)
	}
}

func TestStream_Close_SendsRSTOnce(t *testing.T) {
	s, w := newTestStream(8)
	if err := s.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
	if w.rstCalls != 1 {
		t.Fatalf("rstCalls = %d, want exactly 1 (idempotent)", w.rstCalls)
	}
	if w.lastRSTCode != frame.ErrCodeCancel {
		t.Fatalf("rst code = %v, want CANCEL", w.lastRSTCode)
	}
}

func TestStream_Close_AfterEndStream_DoesNotSendRST(t *testing.T) {
	s, w := newTestStream(8)
	s.markRemoteEnd() // simulate END_STREAM observed
	if err := s.SendHeaders(context.Background(), nil, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	// Both directions ended -> Close is a no-op on the wire.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if w.rstCalls != 0 {
		t.Fatalf("rstCalls = %d, want 0 (already closed cleanly)", w.rstCalls)
	}
}
