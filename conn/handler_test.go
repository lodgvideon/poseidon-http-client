package conn

import (
	"bytes"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStreamMap is the bare interface handler.go needs from *Conn.
type fakeStreamMap struct {
	mu           sync.Mutex
	streams      map[uint32]*Stream
	w            *fakeStreamWriter
	bufSize      int
	origins      []string
	altSvc       []frame.AltSvcEntry
	connRecvOnly uint32 // bytes routed through accountConnRecvOnly (unknown-stream DATA)
	pushEnabled  bool   // controls pushSupport() for push tests
}

func (m *fakeStreamMap) lookupStream(id uint32) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[id]
}

// isIdleStream returns false so these unit tests exercise the closed-stream
// (lenient) path for an unknown stream id; idle-stream connection errors are
// covered against the real *Conn in conformance_streamstate_test.go.
func (*fakeStreamMap) isIdleStream(uint32) bool  { return false }
func (*fakeStreamMap) isKnownOrigin([]byte) bool { return false }

// connOps no-op satisfaction so the wider production interface is met
// in tests that only exercise stream lookup behaviour.
func (*fakeStreamMap) onDataReceived(*Stream, uint32) error         { return nil }
func (*fakeStreamMap) markStreamDone(uint32)                        {}
func (*fakeStreamMap) wakeSendWaiters()                             {}
func (*fakeStreamMap) onWindowUpdate(uint32, uint32) error          { return nil }
func (*fakeStreamMap) applyPeerSettings(frame.SettingsParams) error { return nil }
func (*fakeStreamMap) writeSettingsAck() error                      { return nil }
func (*fakeStreamMap) writePingAck([8]byte) error                   { return nil }
func (*fakeStreamMap) deliverPingAck([8]byte)                       {}
func (*fakeStreamMap) onGoAwayReceived(uint32, frame.ErrCode)       {}
func (m *fakeStreamMap) pushSupport() (bool, int)                   { return m.pushEnabled, 8 }
func (*fakeStreamMap) notePromisedID(uint32) error                  { return nil }
func (*fakeStreamMap) reservePushedStream(uint32) (*Stream, error)  { return nil, nil }
func (*fakeStreamMap) rstStream(uint32, frame.ErrCode) error        { return nil }
func (m *fakeStreamMap) storeOrigins(origins []string)              { m.origins = origins }
func (m *fakeStreamMap) storeAltSvc(entries []frame.AltSvcEntry)    { m.altSvc = entries }
func (*fakeStreamMap) bumpFramesReceived()                          {}

func (m *fakeStreamMap) accountConnRecvOnly(length uint32) error {
	m.connRecvOnly += length
	return nil
}

func newFakeStreamMap() *fakeStreamMap {
	w := &fakeStreamWriter{}
	return &fakeStreamMap{
		streams: map[uint32]*Stream{},
		w:       w,
		bufSize: 8,
	}
}

func (m *fakeStreamMap) addStream(id uint32) *Stream {
	s := newStream(id, m.bufSize, m.w, 65535)
	m.mu.Lock()
	m.streams[id] = s
	m.mu.Unlock()
	return s
}

// encodeBlock builds an HPACK header block for pinned, well-known fields.
func encodeBlock(t *testing.T, fields []hpack.HeaderField) []byte {
	t.Helper()
	enc := hpack.NewEncoder()
	return enc.EncodeBlock(nil, fields)
}

func TestHandler_OnHeaders_EndStream_PushesEventAndMarksRemoteEnd(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)
	block := encodeBlock(t, []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
	})
	fh := frame.FrameHeader{
		Type:     frame.FrameHeaders,
		Length:   uint32(len(block)),
		Flags:    frame.FlagHeadersEndHeaders | frame.FlagHeadersEndStream,
		StreamID: 1,
	}

	err := h.OnHeaders(fh, frame.HeaderBlock(block), nil, 0)

	require.NoErrorf(t, err, "OnHeaders")
	select {
	case e := <-s.events:
		assert.Equalf(t, EventHeaders, e.Type, "event = %+v", e)
		assert.Truef(t, e.EndStream, "event = %+v", e)
		require.Lenf(t, e.Headers, 1, "headers = %+v", e.Headers)
		assert.Equalf(t, ":status", string(e.Headers[0].Name), "headers = %+v", e.Headers)
	default:
		require.FailNow(t, "no event pushed")
	}
	s.mu.Lock()
	remoteEnded := s.remoteEnded
	s.mu.Unlock()
	require.True(t, remoteEnded, "remoteEnded not set")
}

func TestHandler_OnData_PushesDataEvent(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)
	s.headersReceived = true // the response HEADERS have arrived; DATA is now valid body
	fh := frame.FrameHeader{Type: frame.FrameData, Length: 5, StreamID: 1}

	err := h.OnData(fh, []byte("hello"), 0)

	require.NoErrorf(t, err, "OnData")
	select {
	case e := <-s.events:
		assert.Equalf(t, EventData, e.Type, "type = %v", e.Type)
		assert.Truef(t, bytes.Equal(e.Data, []byte("hello")), "data = %q", e.Data)
	default:
		require.FailNow(t, "no event")
	}
}

func TestHandler_OnRSTStream_PushesEventReset(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)
	fh := frame.FrameHeader{Type: frame.FrameRSTStream, StreamID: 1}

	err := h.OnRSTStream(fh, frame.ErrCodeCancel)

	require.NoErrorf(t, err, "OnRSTStream")
	e := <-s.events
	assert.Equalf(t, EventReset, e.Type, "event = %+v", e)
	assert.Equalf(t, frame.ErrCodeCancel, e.RSTCode, "event = %+v", e)
}

func TestHandler_OnPushPromise_ReturnsConnError(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	m.addStream(1)
	fh := frame.FrameHeader{Type: frame.FramePushPromise, StreamID: 1}

	err := h.OnPushPromise(fh, 4, nil, 0)

	require.Error(t, err, "expected error")
	ce, ok := err.(*ConnError)
	require.Truef(t, ok, "err type = %T, want *ConnError", err)
	require.Equalf(t, frame.ErrCodeProtocolError, ce.Code, "code = %v", ce.Code)
}

func TestHandler_OnOrigin_StoresOrigins(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	origins := []string{"https://example.com", "https://cdn.example.com"}

	err := h.OnOrigin(frame.FrameHeader{}, origins)

	require.NoErrorf(t, err, "OnOrigin")
	require.Lenf(t, m.origins, 2, "origins = %v", m.origins)
	assert.Equalf(t, "https://example.com", m.origins[0], "origins = %v", m.origins)
}

func TestHandler_OnAltSvc_StoresEntries(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	entries := []frame.AltSvcEntry{
		{Origin: "https://example.com", AltValue: `h2=":8080"`},
	}

	err := h.OnAltSvc(frame.FrameHeader{}, entries)

	require.NoErrorf(t, err, "OnAltSvc")
	require.Lenf(t, m.altSvc, 1, "altSvc = %v", m.altSvc)
	assert.Equalf(t, "https://example.com", m.altSvc[0].Origin, "altSvc = %v", m.altSvc)
}
