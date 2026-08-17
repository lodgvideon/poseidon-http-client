package conn

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// earlyHintsHandler replies 103 Early Hints (with a Link header, as
// Cloudflare/Fastly/Shopify do) and then the real 200 + body.
func earlyHintsHandler(interim int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		// Every header is set before the first WriteHeader, and none is touched
		// after. net/http's HTTP/2 server hands the header map to its write
		// goroutine when it emits a 1xx and encodes it there, so a handler that
		// mutates the map after a 1xx races with that encode. The race is in the
		// harness, not in the code under test — but it is real, and flaky enough
		// to pass -race by luck.
		w.Header().Add("Link", "</style.css>; rel=preload; as=style")
		w.Header().Set("Content-Type", "text/plain")
		for i := 0; i < interim; i++ {
			w.WriteHeader(http.StatusEarlyHints) // 103
		}
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

func getStream(t *testing.T, ctx context.Context, c *Conn) StreamRef {
	t.Helper()
	s, err := c.NewStream(ctx)
	require.NoErrorf(t, err, "NewStream")
	require.NoErrorf(t, s.SendHeaders(ctx, []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}, true), "SendHeaders")
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

	first, err1 := s.Recv(ctx)
	second, err2 := s.Recv(ctx)

	// First block: the 103 informational response, surfaced as its own event
	// so it can never be mistaken for the final response.
	require.NoErrorf(t, err1, "Recv 1")
	require.Equalf(t, EventInterimHeaders, first.Type,
		"interim event type = %v, want EventInterimHeaders", first.Type)
	assert.Equalf(t, "103", statusOf(first.Headers),
		"interim status = %q, want 103", statusOf(first.Headers))
	assert.False(t, first.EndStream, "interim response must not carry END_STREAM")

	// Second block: the final 200. Must be EventHeaders, NOT EventTrailers.
	require.NoErrorf(t, err2, "Recv 2")
	require.Equalf(t, EventHeaders, second.Type,
		"final event type = %v, want EventHeaders (final response misclassified as trailers)",
		second.Type)
	assert.Equalf(t, "200", statusOf(second.Headers),
		"final status = %q, want 200", statusOf(second.Headers))
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
	require.NoErrorf(t, deliverBlock(t, h, 1, status("200"), false), "final HEADERS")

	// A trailer block that does not terminate the stream is malformed.
	err := deliverBlock(t, h, 1, []hpack.HeaderField{
		{Name: []byte("x-trailer"), Value: []byte("v")},
	}, false)

	var se *StreamError
	require.ErrorAsf(t, err, &se, "err = %v (%T), want *StreamError", err, err)
	assert.Equalf(t, frame.ErrCodeProtocolError, se.Code, "code = %v, want PROTOCOL_ERROR", se.Code)
	assert.Equalf(t, s.id, se.StreamID, "StreamID = %d, want %d", se.StreamID, s.id)
}

// TestConformance_RFC7540_Sec8_1_TrailersWithEndStream_Accepted is the
// positive half: a well-formed trailer section still arrives as EventTrailers.
func TestConformance_RFC7540_Sec8_1_TrailersWithEndStream_Accepted(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)
	require.NoErrorf(t, deliverBlock(t, h, 1, status("200"), false), "final HEADERS")

	err := deliverBlock(t, h, 1, []hpack.HeaderField{
		{Name: []byte("x-trailer"), Value: []byte("v")},
	}, true)

	require.NoErrorf(t, err, "trailers")
	<-s.events // the 200
	ev := <-s.events
	require.Equalf(t, EventTrailers, ev.Type, "type = %v, want EventTrailers", ev.Type)
	assert.True(t, ev.EndStream, "trailers must carry END_STREAM")
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
	require.ErrorAsf(t, err, &se,
		"accepted %d interim responses with err=%v; want a *StreamError past the %d cap",
		sent, err, maxInterimResponses)
	assert.Equalf(t, frame.ErrCodeEnhanceYourCalm, se.Code, "code = %v, want ENHANCE_YOUR_CALM", se.Code)
	assert.Equalf(t, maxInterimResponses, sent,
		"accepted %d interim responses, want exactly %d", sent, maxInterimResponses)
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
	require.ErrorAsf(t, err, &se, "err = %v (%T), want *StreamError", err, err)
	assert.Equalf(t, frame.ErrCodeProtocolError, se.Code, "code = %v, want PROTOCOL_ERROR", se.Code)
}
