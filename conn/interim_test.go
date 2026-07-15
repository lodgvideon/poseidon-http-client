package conn

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// earlyHintsHandler replies 103 Early Hints (with a Link header, as
// Cloudflare/Fastly/Shopify do) and then the real 200 + body.
func earlyHintsHandler(interim int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i < interim; i++ {
			w.Header().Add("Link", "</style.css>; rel=preload; as=style")
			w.WriteHeader(http.StatusEarlyHints) // 103
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	}
}

func statusOf(hdrs []hpack.HeaderField) string {
	for _, f := range hdrs {
		if string(f.Name) == ":status" {
			return string(f.Value)
		}
	}
	return ""
}

func getStream(t *testing.T, ctx context.Context, c *Conn) *Stream {
	t.Helper()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	return s
}

// TestConformance_RFC7540_Sec8_1_InterimHeadersNotTrailers pins that a 1xx
// informational HEADERS block does not latch the stream into "headers
// received", so the following final HEADERS is delivered as EventHeaders and
// not misclassified as a trailer section. RFC 7540 §8.1 defines trailers as
// HEADERS arriving after a *final* (non-informational) status code.
func TestConformance_RFC7540_Sec8_1_InterimHeadersNotTrailers(t *testing.T) {
	srv, cfg := startH2TestServer(t, earlyHintsHandler(1, "the actual body"))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := getStream(t, ctx, c)

	// First block: the 103 informational response, surfaced as its own event
	// so it can never be mistaken for the final response.
	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv 1: %v", err)
	}
	if ev.Type != EventInterimHeaders {
		t.Fatalf("interim event type = %v, want EventInterimHeaders", ev.Type)
	}
	if got := statusOf(ev.Headers); got != "103" {
		t.Fatalf("interim status = %q, want 103", got)
	}
	if ev.EndStream {
		t.Fatal("interim response must not carry END_STREAM")
	}

	// Second block: the final 200. Must be EventHeaders, NOT EventTrailers.
	ev, err = s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv 2: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("final event type = %v, want EventHeaders (final response "+
			"misclassified as trailers)", ev.Type)
	}
	if got := statusOf(ev.Headers); got != "200" {
		t.Fatalf("final status = %q, want 200", got)
	}
}

// deliverBlock feeds one complete HEADERS block (END_HEADERS set) for stream
// id through the handler, as a hostile peer would.
func deliverBlock(t *testing.T, h *connHandler, id uint32, fields []hpack.HeaderField, endStream bool) error {
	t.Helper()
	block := encodeBlock(t, fields)
	flags := frame.FlagHeadersEndHeaders
	if endStream {
		flags |= frame.FlagHeadersEndStream
	}
	return h.OnHeaders(frame.FrameHeader{
		Type:     frame.FrameHeaders,
		Length:   uint32(len(block)),
		Flags:    flags,
		StreamID: id,
	}, block, nil, 0)
}

func status(v string) []hpack.HeaderField {
	return []hpack.HeaderField{{Name: []byte(":status"), Value: []byte(v)}}
}

// TestConformance_RFC7540_Sec8_1_TrailersWithoutEndStream_StreamError pins
// RFC 7540 §8.1: "An endpoint that receives a HEADERS frame without the
// END_STREAM flag set after receiving a final (non-informational) status code
// MUST treat the corresponding request or response as malformed". §8.1.2.6
// routes malformed to a stream error of type PROTOCOL_ERROR (§5.4.2), so the
// connection itself survives. Without this rule a peer can stream trailer
// blocks forever.
func TestConformance_RFC7540_Sec8_1_TrailersWithoutEndStream_StreamError(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	if err := deliverBlock(t, h, 1, status("200"), false); err != nil {
		t.Fatalf("final HEADERS: %v", err)
	}
	// A trailer block that does not terminate the stream is malformed.
	err := deliverBlock(t, h, 1, []hpack.HeaderField{
		{Name: []byte("x-trailer"), Value: []byte("v")},
	}, false)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("err = %v (%T), want *StreamError", err, err)
	}
	if se.Code != frame.ErrCodeProtocolError {
		t.Errorf("code = %v, want PROTOCOL_ERROR", se.Code)
	}
	if se.StreamID != s.id {
		t.Errorf("StreamID = %d, want %d", se.StreamID, s.id)
	}
}

// TestConformance_RFC7540_Sec8_1_TrailersWithEndStream_Accepted is the
// positive half: a well-formed trailer section still arrives as EventTrailers.
func TestConformance_RFC7540_Sec8_1_TrailersWithEndStream_Accepted(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	if err := deliverBlock(t, h, 1, status("200"), false); err != nil {
		t.Fatalf("final HEADERS: %v", err)
	}
	if err := deliverBlock(t, h, 1, []hpack.HeaderField{
		{Name: []byte("x-trailer"), Value: []byte("v")},
	}, true); err != nil {
		t.Fatalf("trailers: %v", err)
	}
	<-s.events // the 200
	ev := <-s.events
	if ev.Type != EventTrailers {
		t.Fatalf("type = %v, want EventTrailers", ev.Type)
	}
	if !ev.EndStream {
		t.Error("trailers must carry END_STREAM")
	}
}

// TestConformance_RFC7540_Sec8_1_InterimFlood_Bounded pins that a peer cannot
// stream 1xx blocks forever: past maxInterimResponses the stream is reset.
func TestConformance_RFC7540_Sec8_1_InterimFlood_Bounded(t *testing.T) {
	m := newFakeStreamMap()
	m.bufSize = maxInterimResponses + 10 // never drop on channel overflow
	h := newConnHandler(m, hpack.NewDecoder())
	m.addStream(1)

	var err error
	sent := 0
	for i := 0; i < maxInterimResponses+5; i++ {
		if err = deliverBlock(t, h, 1, status("103"), false); err != nil {
			break
		}
		sent++
	}
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("accepted %d interim responses with err=%v; want a *StreamError past the %d cap",
			sent, err, maxInterimResponses)
	}
	if se.Code != frame.ErrCodeEnhanceYourCalm {
		t.Errorf("code = %v, want ENHANCE_YOUR_CALM", se.Code)
	}
	if sent != maxInterimResponses {
		t.Errorf("accepted %d interim responses, want exactly %d", sent, maxInterimResponses)
	}
}

// TestConformance_RFC7540_Sec8_1_InterimWithEndStream_StreamError pins that a
// 1xx may not terminate the stream: an informational response is not a
// complete response, and admitting END_STREAM here would end the exchange with
// a 1xx and no final status.
func TestConformance_RFC7540_Sec8_1_InterimWithEndStream_StreamError(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	m.addStream(1)

	err := deliverBlock(t, h, 1, status("103"), true)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("err = %v (%T), want *StreamError", err, err)
	}
	if se.Code != frame.ErrCodeProtocolError {
		t.Errorf("code = %v, want PROTOCOL_ERROR", se.Code)
	}
}
