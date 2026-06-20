package conn

import (
	"bytes"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// fakeStreamMap is the bare interface handler.go needs from *Conn.
type fakeStreamMap struct {
	mu      sync.Mutex
	streams map[uint32]*Stream
	w       *fakeStreamWriter
	bufSize int
	origins []string
	altSvc  []frame.AltSvcEntry
}

func (m *fakeStreamMap) lookupStream(id uint32) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[id]
}

// connOps no-op satisfaction so the wider production interface is met
// in tests that only exercise stream lookup behaviour.
func (*fakeStreamMap) onDataReceived(*Stream, uint32) error                 { return nil }
func (*fakeStreamMap) markStreamDone(uint32)                                {}
func (*fakeStreamMap) onWindowUpdate(uint32, uint32) error                  { return nil }
func (*fakeStreamMap) applyPeerSettings(frame.SettingsParams) error         { return nil }
func (*fakeStreamMap) writeSettingsAck() error                              { return nil }
func (*fakeStreamMap) writePingAck([8]byte) error                           { return nil }
func (*fakeStreamMap) deliverPingAck([8]byte)                               {}
func (*fakeStreamMap) onGoAwayReceived(uint32, frame.ErrCode)               {}
func (*fakeStreamMap) pushSupport() (bool, int)                             { return false, 8 }
func (*fakeStreamMap) registerPushedStream(uint32) *Stream                  { return nil }
func (*fakeStreamMap) rstStream(uint32, frame.ErrCode) error                { return nil }
func (m *fakeStreamMap) storeOrigins(origins []string)            { m.origins = origins }
func (m *fakeStreamMap) storeAltSvc(entries []frame.AltSvcEntry)  { m.altSvc = entries }
func (*fakeStreamMap) bumpFramesReceived()                        {}

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
	dec := hpack.NewDecoder()
	h := newConnHandler(m, dec)
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
	if err := h.OnHeaders(fh, frame.HeaderBlock(block), nil, 0); err != nil {
		t.Fatalf("OnHeaders: %v", err)
	}
	select {
	case e := <-s.events:
		if e.Type != EventHeaders || !e.EndStream {
			t.Fatalf("event = %+v", e)
		}
		if len(e.Headers) != 1 || string(e.Headers[0].Name) != ":status" {
			t.Fatalf("headers = %+v", e.Headers)
		}
	default:
		t.Fatalf("no event pushed")
	}
	s.mu.Lock()
	if !s.remoteEnded {
		t.Fatalf("remoteEnded not set")
	}
	s.mu.Unlock()
}

func TestHandler_OnData_PushesDataEvent(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	fh := frame.FrameHeader{Type: frame.FrameData, Length: 5, StreamID: 1}
	if err := h.OnData(fh, []byte("hello"), 0); err != nil {
		t.Fatalf("OnData: %v", err)
	}
	select {
	case e := <-s.events:
		if e.Type != EventData {
			t.Fatalf("type = %v", e.Type)
		}
		if !bytes.Equal(e.Data, []byte("hello")) {
			t.Fatalf("data = %q", e.Data)
		}
	default:
		t.Fatalf("no event")
	}
}

func TestHandler_OnRSTStream_PushesEventReset(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)
	fh := frame.FrameHeader{Type: frame.FrameRSTStream, StreamID: 1}
	if err := h.OnRSTStream(fh, frame.ErrCodeCancel); err != nil {
		t.Fatalf("OnRSTStream: %v", err)
	}
	e := <-s.events
	if e.Type != EventReset || e.RSTCode != frame.ErrCodeCancel {
		t.Fatalf("event = %+v", e)
	}
}

func TestHandler_OnPushPromise_ReturnsConnError(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	m.addStream(1)
	fh := frame.FrameHeader{Type: frame.FramePushPromise, StreamID: 1}
	err := h.OnPushPromise(fh, 4, nil, 0)
	if err == nil {
		t.Fatalf("expected error")
	}
	ce, ok := err.(*ConnError)
	if !ok {
		t.Fatalf("err type = %T, want *ConnError", err)
	}
	if ce.Code != frame.ErrCodeProtocolError {
		t.Fatalf("code = %v", ce.Code)
	}
}

func TestHandler_OnOrigin_StoresOrigins(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	origins := []string{"https://example.com", "https://cdn.example.com"}
	if err := h.OnOrigin(frame.FrameHeader{}, origins); err != nil {
		t.Fatalf("OnOrigin: %v", err)
	}
	if len(m.origins) != 2 || m.origins[0] != "https://example.com" {
		t.Fatalf("origins = %v", m.origins)
	}
}

func TestHandler_OnAltSvc_StoresEntries(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	entries := []frame.AltSvcEntry{
		{Origin: "https://example.com", AltValue: `h2=":8080"`},
	}
	if err := h.OnAltSvc(frame.FrameHeader{}, entries); err != nil {
		t.Fatalf("OnAltSvc: %v", err)
	}
	if len(m.altSvc) != 1 || m.altSvc[0].Origin != "https://example.com" {
		t.Fatalf("altSvc = %v", m.altSvc)
	}
}
