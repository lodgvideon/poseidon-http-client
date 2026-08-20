package conn

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
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
		{EventPushPromise, "push_promise"},
		{StreamEventType(99), "unknown"},
	}
	for _, c := range cases {
		got := c.t.String()

		assert.Equalf(t, c.want, got, "%v: got %q, want %q", c.t, got, c.want)
	}
}

func TestStreamEvent_TypeDispatch(t *testing.T) {
	headers := []header.Field{{Name: []byte(":status"), Value: []byte("200")}}
	e := StreamEvent{Type: EventHeaders, Headers: headers, EndStream: false}
	r := StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeCancel}

	assert.Equalf(t, EventHeaders, e.Type, "event = %+v", e)
	assert.Lenf(t, e.Headers, 1, "event = %+v", e)
	assert.Equalf(t, EventReset, r.Type, "reset event = %+v", r)
	assert.Equalf(t, frame.ErrCodeCancel, r.RSTCode, "reset event = %+v", r)
}

// fakeStreamWriter records what would have gone to the wire.
type fakeStreamWriter struct {
	mu          sync.Mutex
	headerCalls int
	dataCalls   int
	rstCalls    int
	lastRSTCode frame.ErrCode
	// doneCalls and bestEffortRSTs count the two calls Stream used to reach by
	// downcasting to *Conn, which made them silent no-ops here. A fake that
	// cannot observe them is a fake that certifies a lifecycle production does
	// not run.
	doneCalls       int
	lastDoneID      uint32
	bestEffortRSTs  int
	lastBestEffortC frame.ErrCode
}

func (w *fakeStreamWriter) markStreamDone(id uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.doneCalls++
	w.lastDoneID = id
}

func (w *fakeStreamWriter) writeRSTStreamBestEffort(_ *Stream, code frame.ErrCode) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.bestEffortRSTs++
	w.lastBestEffortC = code
}

func (w *fakeStreamWriter) writeHeadersWithPriority(_ context.Context, _ *Stream, _ []header.Field, _ bool, _ *frame.Priority) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.headerCalls++
	return nil
}
func (w *fakeStreamWriter) writeData(_ context.Context, _ *Stream, _ uint64, _ []byte, _ bool) error {
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

// rstSnapshot returns the outbound-RST count and last code under the lock, so a
// test can read them race-free while push's overflow goroutine may still write.
func (w *fakeStreamWriter) rstSnapshot() (int, frame.ErrCode) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rstCalls, w.lastRSTCode
}

func newTestStream(buf int) (*Stream, *fakeStreamWriter) {
	w := &fakeStreamWriter{}
	s := newStream(1, buf, w, 65535)
	return s, w
}

func TestStream_ID(t *testing.T) {
	s, _ := newTestStream(8)

	assert.Equalf(t, uint32(1), s.ID(), "ID = %d, want 1", s.ID())
}

func TestStream_SendHeaders_DelegatesToWriter(t *testing.T) {
	s, w := newTestStream(8)

	err := s.ref().SendHeaders(context.Background(),
		[]header.Field{{Name: []byte(":method"), Value: []byte("GET")}},
		true)

	require.NoError(t, err, "SendHeaders")
	assert.Equalf(t, 1, w.headerCalls, "headerCalls = %d, want 1", w.headerCalls)
}

func TestStream_SendData_AfterEndStream_ReturnsErrStreamClosed(t *testing.T) {
	s, _ := newTestStream(8)
	require.NoError(t, s.ref().SendHeaders(context.Background(), nil, true), "SendHeaders")

	err := s.ref().SendData(context.Background(), []byte("x"), false)

	assert.Truef(t, errors.Is(err, ErrStreamClosed), "SendData err = %v, want ErrStreamClosed", err)
}

func TestStream_Recv_ReturnsBufferedEvent(t *testing.T) {
	s, _ := newTestStream(8)
	s.push(StreamEvent{Type: EventHeaders, EndStream: true})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, err := s.ref().Recv(ctx)

	require.NoError(t, err, "Recv")
	assert.Equalf(t, EventHeaders, e.Type, "event = %+v", e)
	assert.Truef(t, e.EndStream, "event = %+v, want EndStream set", e)
}

func TestStream_Recv_BlocksUntilCancel(t *testing.T) {
	s, _ := newTestStream(8)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := s.ref().Recv(ctx)

	assert.Truef(t, errors.Is(err, context.DeadlineExceeded),
		"Recv err = %v, want DeadlineExceeded", err)
}

func TestStream_Close_SendsRSTOnce(t *testing.T) {
	s, w := newTestStream(8)

	first := s.ref().Close()
	second := s.ref().Close()

	require.NoError(t, first, "Close 1")
	require.NoError(t, second, "Close 2")
	rstCalls, code := w.rstSnapshot()
	assert.Equalf(t, 1, rstCalls, "rstCalls = %d, want exactly 1 (idempotent)", rstCalls)
	assert.Equalf(t, frame.ErrCodeCancel, code, "rst code = %v, want CANCEL", code)
}

func TestStream_Close_AfterEndStream_DoesNotSendRST(t *testing.T) {
	s, w := newTestStream(8)
	// Simulate END_STREAM observed. The field is set directly, as every other
	// test that needs this state does: the production path (deliverEnd) also
	// enqueues an event, and this test wants the flag and nothing else. Nothing
	// else touches s yet, so the write needs no lock.
	s.remoteEnded = true
	require.NoError(t, s.ref().SendHeaders(context.Background(), nil, true), "SendHeaders")

	// Both directions ended -> Close is a no-op on the wire.
	err := s.ref().Close()

	require.NoError(t, err, "Close")
	rstCalls, _ := w.rstSnapshot()
	assert.Zerof(t, rstCalls, "rstCalls = %d, want 0 (already closed cleanly)", rstCalls)
}
