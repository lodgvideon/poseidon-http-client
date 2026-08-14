package frame

import (
	"bytes"
	"context"
	"sync"
	"testing"
)

// capturingTracer records every event. It COPIES what it is handed, because the
// Framer reuses one FrameInfo per direction and Payload aliases the read
// buffer; a tracer that stored the pointer would see the last frame N times.
// That it has to do this is the retention rule frame.Tracer documents.
type capturingTracer struct {
	mu     sync.Mutex
	events []FrameInfo
}

func (c *capturingTracer) TraceFrame(fi *FrameInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *fi
	cp.Payload = append([]byte(nil), fi.Payload...)
	c.events = append(c.events, cp)
}

func (c *capturingTracer) snapshot() []FrameInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]FrameInfo(nil), c.events...)
}

func (c *capturingTracer) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = nil
}

// TestFramer_NoTracer_NoPanic pins the default: a Framer with no tracer keeps
// working, and the emit sites are dead code.
func TestFramer_NoTracer_NoPanic(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	if err := fr.WriteRSTStream(1, ErrCodeCancel); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := fr.ReadFrame(context.Background(), &recordingHandler{}); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// writeOneOfEach writes one frame of every type the Framer can emit, through a
// traced Framer, and returns the events in order. Shared by the two send-side
// tests so they assert on the same run without either of them growing into a
// single unreadable function.
func writeOneOfEach(t *testing.T) []FrameInfo {
	t.Helper()
	tr := &capturingTracer{}
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetTracer(tr)

	if err := fr.WriteSettings(SettingsParams{
		Pairs: [maxSettingsPairs]SettingPair{
			{ID: SettingEnablePush, Value: 0},
			{ID: SettingInitialWindowSize, Value: 65535},
		},
		N: 2,
	}); err != nil {
		t.Fatalf("WriteSettings: %v", err)
	}
	if err := fr.WriteSettingsAck(); err != nil {
		t.Fatalf("WriteSettingsAck: %v", err)
	}
	if err := fr.WriteHeaders(WriteHeadersParams{
		StreamID: 1, BlockFragment: []byte{0x82, 0x84}, EndHeaders: true, EndStream: true,
	}); err != nil {
		t.Fatalf("WriteHeaders: %v", err)
	}
	if err := fr.WriteData(1, false, make([]byte, 16)); err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	if err := fr.WriteRSTStream(1, ErrCodeCancel); err != nil {
		t.Fatalf("WriteRSTStream: %v", err)
	}
	if err := fr.WriteWindowUpdate(0, 32768); err != nil {
		t.Fatalf("WriteWindowUpdate: %v", err)
	}
	if err := fr.WritePing(false, [8]byte{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
		t.Fatalf("WritePing: %v", err)
	}
	if err := fr.WriteGoAway(7, ErrCodeEnhanceYourCalm, []byte("slow down")); err != nil {
		t.Fatalf("WriteGoAway: %v", err)
	}
	if err := fr.WritePushPromise(1, 2, []byte{0x82}, true, 0); err != nil {
		t.Fatalf("WritePushPromise: %v", err)
	}

	ev := tr.snapshot()
	if len(ev) != 9 {
		t.Fatalf("got %d events, want 9: %+v", len(ev), ev)
	}
	return ev
}

// TestFramer_TraceSend_EveryFrameIsSeen: every write path emits exactly one
// event, in order, in the send direction.
func TestFramer_TraceSend_EveryFrameIsSeen(t *testing.T) {
	ev := writeOneOfEach(t)
	wantTypes := []FrameType{
		FrameSettings, FrameSettings, FrameHeaders, FrameData, FrameRSTStream,
		FrameWindowUpdate, FramePing, FrameGoAway, FramePushPromise,
	}
	for i, want := range wantTypes {
		if ev[i].Header.Type != want {
			t.Errorf("event %d: type = %v, want %v", i, ev[i].Header.Type, want)
		}
		if ev[i].Dir != DirSend {
			t.Errorf("event %d: Dir = %v, want send", i, ev[i].Dir)
		}
		// Payload is documented as receive-side only; the send path assembles
		// frames from the caller's buffers.
		if ev[i].Payload != nil {
			t.Errorf("event %d: Payload = %x, want nil on the send side", i, ev[i].Payload)
		}
	}
	// WriteHeaders' coalescing fast path bypasses writeHeader entirely; without
	// its own emit the client's most common frame is missing from every trace.
	if ev[2].Header.StreamID != 1 || ev[2].Header.Flags != FlagHeadersEndStream|FlagHeadersEndHeaders {
		t.Errorf("HEADERS event = %+v, want stream 1 END_STREAM|END_HEADERS", ev[2].Header)
	}
	if ev[3].Header.Length != 16 {
		t.Errorf("DATA len = %d, want 16", ev[3].Header.Length)
	}
	if ev[1].Header.Flags&FlagSettingsAck == 0 {
		t.Errorf("SETTINGS ACK event flags = %v, want ACK set", ev[1].Header.Flags)
	}
}

// TestFramer_TraceSend_Details covers the decoded scalars each Write method
// stages, and — the half that a single emit site makes possible to get wrong —
// that no frame inherits the previous one's staging.
func TestFramer_TraceSend_Details(t *testing.T) {
	ev := writeOneOfEach(t)

	if ev[0].Settings.N != 2 {
		t.Fatalf("SETTINGS event = %+v, want 2 pairs", ev[0].Settings)
	}
	if got := ev[0].Settings.Pairs[1]; got.ID != SettingInitialWindowSize || got.Value != 65535 {
		t.Errorf("SETTINGS pair[1] = %+v, want INITIAL_WINDOW_SIZE=65535", got)
	}
	if ev[4].ErrCode != ErrCodeCancel {
		t.Errorf("RST_STREAM code = %v, want CANCEL", ev[4].ErrCode)
	}
	if ev[5].WindowIncrement != 32768 {
		t.Errorf("WINDOW_UPDATE inc = %d, want 32768", ev[5].WindowIncrement)
	}
	if ev[6].Ping != [8]byte{1, 2, 3, 4, 5, 6, 7, 8} {
		t.Errorf("PING data = %x", ev[6].Ping)
	}
	if ev[7].ErrCode != ErrCodeEnhanceYourCalm || ev[7].LastStreamID != 7 {
		t.Errorf("GOAWAY event = %+v, want last=7 ENHANCE_YOUR_CALM", ev[7])
	}
	if ev[8].PromisedID != 2 {
		t.Errorf("PUSH_PROMISE promised = %d, want 2", ev[8].PromisedID)
	}

	// Staleness. Detail is staged on the Framer by the Write method and consumed
	// by one emit site, so a missing clear shows up as a field on a frame type
	// that has no such field: the ACK inheriting the SETTINGS before it, or the
	// PING inheriting the RST_STREAM two frames back.
	if ev[1].Settings.N != 0 {
		t.Errorf("SETTINGS ACK carried %d stale pairs", ev[1].Settings.N)
	}
	for i, e := range ev {
		if e.Header.Type != FrameRSTStream && e.Header.Type != FrameGoAway && e.ErrCode != 0 {
			t.Errorf("event %d (%v) carried a stale error code %v", i, e.Header.Type, e.ErrCode)
		}
		if e.Header.Type != FrameWindowUpdate && e.WindowIncrement != 0 {
			t.Errorf("event %d (%v) carried a stale increment %d", i, e.Header.Type, e.WindowIncrement)
		}
		if e.Header.Type != FramePushPromise && e.PromisedID != 0 {
			t.Errorf("event %d (%v) carried a stale promised id %d", i, e.Header.Type, e.PromisedID)
		}
		if e.Header.Type != FramePing && e.Ping != [8]byte{} {
			t.Errorf("event %d (%v) carried stale ping data %x", i, e.Header.Type, e.Ping)
		}
	}
}

// TestFramer_TraceSend_HeadersSlowPath covers the padded/priority branch, which
// goes through writeHeader rather than the coalescing fast path.
func TestFramer_TraceSend_HeadersSlowPath(t *testing.T) {
	tr := &capturingTracer{}
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetTracer(tr)

	prio := Priority{StreamDep: 1, Weight: 15}
	if err := fr.WriteHeaders(WriteHeadersParams{
		StreamID: 3, BlockFragment: []byte{0x82}, EndHeaders: true, PadLength: 4, Priority: &prio,
	}); err != nil {
		t.Fatalf("WriteHeaders: %v", err)
	}
	ev := tr.snapshot()
	if len(ev) != 1 {
		t.Fatalf("got %d events, want 1", len(ev))
	}
	if ev[0].Header.Flags&FlagHeadersPadded == 0 || ev[0].Header.Flags&FlagHeadersPriority == 0 {
		t.Errorf("flags = %v, want PADDED and PRIORITY set", ev[0].Header.Flags)
	}
	// 1 pad-length byte + 5 priority + 1 block + 4 padding.
	if ev[0].Header.Length != 11 {
		t.Errorf("Length = %d, want 11 (the wire length, padding included)", ev[0].Header.Length)
	}
}

// TestFramer_TraceSend_NotEmittedWhenRejected: a frame the Framer refuses to
// write never reached the wire, so it must not appear in a wire trace.
func TestFramer_TraceSend_NotEmittedWhenRejected(t *testing.T) {
	tr := &capturingTracer{}
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetTracer(tr)

	if err := fr.WriteData(0, false, nil); err != ErrInvalidStreamID {
		t.Fatalf("WriteData(0) = %v, want ErrInvalidStreamID", err)
	}
	if err := fr.WriteWindowUpdate(1, 0); err != ErrZeroIncrement {
		t.Fatalf("WriteWindowUpdate(0) = %v, want ErrZeroIncrement", err)
	}
	if err := fr.WriteData(1, false, make([]byte, defaultMaxFrameSize+1)); err != ErrFrameTooLarge {
		t.Fatalf("oversized WriteData = %v, want ErrFrameTooLarge", err)
	}
	if ev := tr.snapshot(); len(ev) != 0 {
		t.Fatalf("rejected writes produced %d trace events: %+v", len(ev), ev)
	}
}

// TestFramer_TraceSend_RejectedFrameLeavesNoStaleDetail closes the one hole the
// staging design has: a Write method stages its decoded detail and only THEN
// discovers the frame is oversized, so the event never fires and the detail is
// left sitting on the Framer for the next frame to claim.
//
// Reachable because SetMaxFrameSize takes any value, including one below the
// RFC's 16384 floor, at which point even a 4-byte RST_STREAM is refused.
func TestFramer_TraceSend_RejectedFrameLeavesNoStaleDetail(t *testing.T) {
	tr := &capturingTracer{}
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetTracer(tr)
	fr.SetMaxFrameSize(2) // smaller than any control frame's payload

	if err := fr.WriteRSTStream(1, ErrCodeEnhanceYourCalm); err != ErrFrameTooLarge {
		t.Fatalf("WriteRSTStream = %v, want ErrFrameTooLarge", err)
	}
	if err := fr.WriteWindowUpdate(1, 4242); err != ErrFrameTooLarge {
		t.Fatalf("WriteWindowUpdate = %v, want ErrFrameTooLarge", err)
	}
	if ev := tr.snapshot(); len(ev) != 0 {
		t.Fatalf("oversized writes were traced: %+v", ev)
	}

	// Now let a frame through and check it did not inherit either.
	fr.SetMaxFrameSize(16384)
	if err := fr.WriteSettingsAck(); err != nil {
		t.Fatalf("WriteSettingsAck: %v", err)
	}
	ev := tr.snapshot()
	if len(ev) != 1 {
		t.Fatalf("got %d events, want 1", len(ev))
	}
	if ev[0].ErrCode != 0 || ev[0].WindowIncrement != 0 {
		t.Fatalf("SETTINGS ACK inherited detail from the rejected frames: %+v", ev[0])
	}
}

func TestFramer_TraceRecv_Details(t *testing.T) {
	// Build a stream of frames with an untraced writer, then read them back
	// through a traced one.
	var wire bytes.Buffer
	w := NewFramer(&wire, nil)
	mustWrite := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	mustWrite(w.WriteSettings(SettingsParams{
		Pairs: [maxSettingsPairs]SettingPair{{ID: SettingMaxConcurrentStreams, Value: 250}},
		N:     1,
	}))
	mustWrite(w.WriteHeaders(WriteHeadersParams{StreamID: 1, BlockFragment: []byte{0x88}, EndHeaders: true}))
	mustWrite(w.WriteData(1, true, []byte("hello")))
	mustWrite(w.WriteRSTStream(1, ErrCodeRefusedStream))
	mustWrite(w.WriteWindowUpdate(0, 1000))
	mustWrite(w.WritePing(true, [8]byte{9, 9}))
	mustWrite(w.WritePushPromise(3, 4, []byte{0x88}, true, 3))
	mustWrite(w.WriteGoAway(11, ErrCodeNoError, nil))

	tr := &capturingTracer{}
	rd := bytes.NewReader(wire.Bytes())
	fr := NewFramer(nil, rd)
	fr.SetTracer(tr)
	h := &recordingHandler{}
	for i := 0; i < 8; i++ {
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
	}

	ev := tr.snapshot()
	if len(ev) != 8 {
		t.Fatalf("got %d events, want 8", len(ev))
	}
	for i, e := range ev {
		if e.Dir != DirRecv {
			t.Errorf("event %d: Dir = %v, want recv", i, e.Dir)
		}
	}
	if ev[0].Settings.N != 1 || ev[0].Settings.Pairs[0].Value != 250 {
		t.Errorf("SETTINGS event = %+v, want MAX_CONCURRENT_STREAMS=250", ev[0].Settings)
	}
	if ev[1].Header.Type != FrameHeaders || string(ev[1].Payload) != "\x88" {
		t.Errorf("HEADERS event = %+v, payload %x", ev[1].Header, ev[1].Payload)
	}
	// Payload is the RAW wire payload — nothing stripped — so DATA shows what
	// arrived, not what dispatch handed the Handler.
	if string(ev[2].Payload) != "hello" || ev[2].Header.Flags&FlagDataEndStream == 0 {
		t.Errorf("DATA event = %+v payload=%q", ev[2].Header, ev[2].Payload)
	}
	if ev[3].ErrCode != ErrCodeRefusedStream {
		t.Errorf("RST_STREAM code = %v, want REFUSED_STREAM", ev[3].ErrCode)
	}
	if ev[4].WindowIncrement != 1000 {
		t.Errorf("WINDOW_UPDATE inc = %d, want 1000", ev[4].WindowIncrement)
	}
	if ev[5].Ping != [8]byte{9, 9} {
		t.Errorf("PING data = %x", ev[5].Ping)
	}
	// PUSH_PROMISE here is PADDED, so the promised id sits after the pad-length
	// byte. Reading it from offset 0 would report a wildly wrong stream id.
	if ev[6].PromisedID != 4 {
		t.Errorf("PUSH_PROMISE promised = %d, want 4", ev[6].PromisedID)
	}
	if ev[7].LastStreamID != 11 || ev[7].ErrCode != ErrCodeNoError {
		t.Errorf("GOAWAY event = %+v", ev[7])
	}
}

// TestFramer_TraceRecv_MalformedAndUnknown is the case the seam exists for: the
// frames that never reach a Handler. An unknown type is dropped by §5.5 and a
// malformed one aborts dispatch, and neither is observable from above — but
// both are exactly what a bug report needs.
func TestFramer_TraceRecv_MalformedAndUnknown(t *testing.T) {
	var wire bytes.Buffer
	// Unknown extension type 0x0b, 2-byte payload.
	wire.Write([]byte{0, 0, 2, 0x0b, 0, 0, 0, 0, 1, 0xde, 0xad})
	// RST_STREAM with a 3-byte payload — a FRAME_SIZE_ERROR that dispatch
	// rejects.
	wire.Write([]byte{0, 0, 3, 0x03, 0, 0, 0, 0, 1, 0, 0, 8})

	tr := &capturingTracer{}
	fr := NewFramer(nil, bytes.NewReader(wire.Bytes()))
	fr.SetTracer(tr)
	h := &recordingHandler{}

	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("unknown frame should be ignored, got %v", err)
	}
	if _, err := fr.ReadFrame(context.Background(), h); err != ErrRSTWrongLength {
		t.Fatalf("malformed RST_STREAM = %v, want ErrRSTWrongLength", err)
	}

	ev := tr.snapshot()
	if len(ev) != 2 {
		t.Fatalf("got %d events, want both frames traced", len(ev))
	}
	if ev[0].Header.Type != FrameType(0x0b) || string(ev[0].Payload) != "\xde\xad" {
		t.Errorf("unknown-type event = %+v payload=%x", ev[0].Header, ev[0].Payload)
	}
	if ev[1].Header.Type != FrameRSTStream || ev[1].Header.Length != 3 {
		t.Errorf("malformed event = %+v", ev[1].Header)
	}
	// The renderer does not invent a code from a short payload; it leaves the
	// field alone and lets the header's Length tell the story.
	if ev[1].ErrCode != 0 {
		t.Errorf("short RST_STREAM reported code %v, want the zero value", ev[1].ErrCode)
	}
}

// TestFramer_TraceRecv_ContinuityViolationTraced: RFC 9113 §6.10 kills the
// connection before dispatch. The offending frame still has to appear.
func TestFramer_TraceRecv_ContinuityViolationTraced(t *testing.T) {
	var wire bytes.Buffer
	w := NewFramer(&wire, nil)
	// HEADERS without END_HEADERS opens a field block...
	_ = w.WriteHeaders(WriteHeadersParams{StreamID: 1, BlockFragment: []byte{0x88}})
	// ...and a DATA frame instead of a CONTINUATION is a connection error.
	_ = w.WriteData(1, false, []byte("x"))

	tr := &capturingTracer{}
	fr := NewFramer(nil, bytes.NewReader(wire.Bytes()))
	fr.SetTracer(tr)
	h := &recordingHandler{}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("HEADERS: %v", err)
	}
	if _, err := fr.ReadFrame(context.Background(), h); err != ErrContinuationExpected {
		t.Fatalf("interleaved DATA = %v, want ErrContinuationExpected", err)
	}
	ev := tr.snapshot()
	if len(ev) != 2 || ev[1].Header.Type != FrameData {
		t.Fatalf("events = %+v, want the interleaving DATA frame traced", ev)
	}
}

// TestFramer_Trace_ConcurrentDirections is the reason there are two scratch
// structs rather than one. conn reads on its reader goroutine and writes under
// wmu, so a single Framer hands both directions to the same Tracer at once;
// under -race a shared scratch fails here.
func TestFramer_Trace_ConcurrentDirections(t *testing.T) {
	var wire bytes.Buffer
	w := NewFramer(&wire, nil)
	for i := 0; i < 200; i++ {
		_ = w.WriteWindowUpdate(1, 7)
	}
	raw := append([]byte(nil), wire.Bytes()...)

	tr := &capturingTracer{}
	fr := NewFramer(&bytes.Buffer{}, bytes.NewReader(raw))
	fr.SetTracer(tr)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		h := &recordingHandler{}
		for i := 0; i < 200; i++ {
			if _, err := fr.ReadFrame(context.Background(), h); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = fr.WriteRSTStream(1, ErrCodeCancel)
		}
	}()
	wg.Wait()

	if got := len(tr.snapshot()); got != 400 {
		t.Fatalf("got %d events, want 400", got)
	}
}

// echoingHandler writes a frame back through the same Framer from inside a
// dispatch callback — which is exactly what conn does: its reader goroutine
// answers SETTINGS with an ACK, PING with a PING ACK, and DATA with a
// WINDOW_UPDATE, all from inside ReadFrame.
type echoingHandler struct {
	recordingHandler
	fr *Framer
}

func (h *echoingHandler) OnSettings(fh FrameHeader, s SettingsParams) error {
	if err := h.recordingHandler.OnSettings(fh, s); err != nil {
		return err
	}
	if fh.Flags&FlagSettingsAck == 0 {
		return h.fr.WriteSettingsAck()
	}
	return nil
}

// TestFramer_Trace_ReentrantWriteFromDispatch is the same-goroutine hazard the
// -race detector cannot see. The read scratch and the write scratch are
// separate fields, so a write issued from inside dispatch cannot corrupt the
// event for the frame being dispatched — but only because emitRecv completes
// BEFORE dispatch rather than holding the event live across it.
func TestFramer_Trace_ReentrantWriteFromDispatch(t *testing.T) {
	var wire bytes.Buffer
	w := NewFramer(&wire, nil)
	in := SettingsParams{N: 1}
	in.Pairs[0] = SettingPair{ID: SettingMaxConcurrentStreams, Value: 250}
	if err := w.WriteSettings(in); err != nil {
		t.Fatalf("WriteSettings: %v", err)
	}

	tr := &capturingTracer{}
	var out bytes.Buffer
	fr := NewFramer(&out, bytes.NewReader(wire.Bytes()))
	fr.SetTracer(tr)
	h := &echoingHandler{fr: fr}
	if _, err := fr.ReadFrame(context.Background(), h); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	ev := tr.snapshot()
	if len(ev) != 2 {
		t.Fatalf("got %d events, want the inbound SETTINGS and the ACK written from its dispatch", len(ev))
	}
	if ev[0].Dir != DirRecv || ev[0].Settings.N != 1 || ev[0].Settings.Pairs[0].Value != 250 {
		t.Errorf("inbound SETTINGS event = %+v; the nested write clobbered the read scratch", ev[0])
	}
	if ev[1].Dir != DirSend || ev[1].Header.Flags&FlagSettingsAck == 0 {
		t.Errorf("second event = %+v, want the outbound SETTINGS ACK", ev[1])
	}
	// The ACK carries no parameters of its own and must not have inherited the
	// inbound frame's.
	if ev[1].Settings.N != 0 {
		t.Errorf("outbound ACK carried %d parameters from the frame it answered", ev[1].Settings.N)
	}
}

// TestFramer_SetTracer_Nil turns tracing back off.
func TestFramer_SetTracer_Nil(t *testing.T) {
	tr := &capturingTracer{}
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetTracer(tr)
	_ = fr.WriteWindowUpdate(1, 1)
	tr.reset()
	fr.SetTracer(nil)
	_ = fr.WriteWindowUpdate(1, 1)
	if ev := tr.snapshot(); len(ev) != 0 {
		t.Fatalf("tracer still called after SetTracer(nil): %+v", ev)
	}
}

// discardTracer is the cheapest possible non-nil Tracer. It exists so the
// bench-gate covers the tracing path itself and not just the nil check: the
// caveat in #610 is that boxing an event into an interface argument allocates,
// and the only proof that this design does not is a benchmark with a tracer
// installed.
type discardTracer struct{}

func (discardTracer) TraceFrame(*FrameInfo) {}

func BenchmarkFramer_WriteWindowUpdate_Traced(b *testing.B) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetTracer(discardTracer{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = fr.WriteWindowUpdate(1, 65535)
	}
}

func BenchmarkFramer_WriteHeaders_minimal_Traced(b *testing.B) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetTracer(discardTracer{})
	block := []byte{0x82, 0x84, 0x86}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = fr.WriteHeaders(WriteHeadersParams{StreamID: 1, BlockFragment: block, EndHeaders: true})
	}
}

func BenchmarkFramer_ReadFrame_DATA_1KB_Traced(b *testing.B) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	data := make([]byte, 1024)
	_ = fr.WriteData(1, false, data)
	raw := append([]byte{}, buf.Bytes()...)
	rdr := bytes.NewReader(raw)
	fr.r = rdr
	fr.SetTracer(discardTracer{})
	h := &recordingHandler{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rdr.Reset(raw)
		_, _ = fr.ReadFrame(context.Background(), h)
	}
}

// BenchmarkFramer_ReadFrame_SETTINGS_Traced is the widest recv detail path:
// SETTINGS is the one frame type whose trace event carries a decoded table
// rather than a scalar, so it is the one most likely to allocate.
func BenchmarkFramer_ReadFrame_SETTINGS_Traced(b *testing.B) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	_ = fr.WriteSettings(SettingsParams{
		Pairs: [maxSettingsPairs]SettingPair{
			{ID: SettingHeaderTableSize, Value: 4096},
			{ID: SettingMaxConcurrentStreams, Value: 250},
			{ID: SettingInitialWindowSize, Value: 65535},
		},
		N: 3,
	})
	raw := append([]byte{}, buf.Bytes()...)
	rdr := bytes.NewReader(raw)
	fr.r = rdr
	fr.SetTracer(discardTracer{})
	h := &recordingHandler{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rdr.Reset(raw)
		_, _ = fr.ReadFrame(context.Background(), h)
	}
}
