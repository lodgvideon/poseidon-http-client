package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// scriptedStream replays a fixed conn.StreamEvent sequence as a protoStream.
// It bypasses the conn layer deliberately, so drainResponse can be driven with
// frame sequences that conn's RFC 7540 §8.1 enforcement now rejects at the
// source. That keeps drainResponse's own bounds under test independently of
// the layer above it (CONTRIBUTING.md "Peer-input policy": every consumer of
// peer-controlled bytes carries its own bound).
type scriptedStream struct {
	events []conn.StreamEvent
	i      int
}

func (s *scriptedStream) Recv(context.Context) (conn.StreamEvent, error) {
	if s.i >= len(s.events) {
		return conn.StreamEvent{}, conn.ErrStreamClosed
	}
	ev := s.events[s.i]
	s.i++
	return ev, nil
}

func (*scriptedStream) SendHeaders(context.Context, []conn.HeaderField, bool) error { return nil }
func (*scriptedStream) SendHeadersWithPriority(context.Context, []conn.HeaderField, bool, *frame.Priority) error {
	return nil
}
func (*scriptedStream) SendData(context.Context, []byte, bool) error { return nil }
func (*scriptedStream) Close() error                                 { return nil }

func hdr(name, value string) hpack.HeaderField {
	return hpack.HeaderField{Name: []byte(name), Value: []byte(value)}
}

// TestDrainResponse_TrailerFlood_Bounded pins that drainResponse does not
// accumulate trailer fields across repeated EventTrailers blocks. conn now
// rejects a trailer block that does not set END_STREAM (RFC 7540 §8.1), so a
// live peer cannot reach this path — this is the defence-in-depth layer, and
// it is tested directly because nothing else can reach it.
func TestDrainResponse_TrailerFlood_Bounded(t *testing.T) {
	const floodBlocks = 500
	events := []conn.StreamEvent{
		{Type: conn.EventHeaders, Headers: []hpack.HeaderField{hdr(":status", "200")}},
	}
	// A peer that ignores §8.1: trailer blocks that never terminate the stream.
	for i := 0; i < floodBlocks; i++ {
		events = append(events, conn.StreamEvent{
			Type:    conn.EventTrailers,
			Headers: []hpack.HeaderField{hdr("x-flood", "v")},
		})
	}
	events = append(events, conn.StreamEvent{
		Type:      conn.EventTrailers,
		Headers:   []hpack.HeaderField{hdr("x-final", "v")},
		EndStream: true,
	})

	var resp Response
	resp.Reset()
	req := &Request{Method: "GET", Path: "/", WantTrailers: true}

	err := drainResponse(context.Background(), nil, &scriptedStream{events: events},
		req, &resp, nil, 1<<20, 1<<20)

	require.NoError(t, err, "drainResponse over a trailer flood must complete, not fail")
	// One trailer section per response: the last block wins, nothing accumulates.
	require.Lenf(t, resp.Trailers, 1,
		"Trailers grew to %d fields across %d blocks; want 1 (no accumulation)",
		len(resp.Trailers), floodBlocks+1)
	assert.Equal(t, "x-final", string(resp.Trailers[0].Name),
		"Trailers[0].Name must come from the last block (last block must win)")
}

// TestDrainResponse_InterimHeaders_Dropped pins that Client.Do surfaces only
// the final response: conn.EventInterimHeaders is dropped, the final status
// wins, and the interim block never contaminates Headers or Trailers.
func TestDrainResponse_InterimHeaders_Dropped(t *testing.T) {
	events := []conn.StreamEvent{
		{Type: conn.EventInterimHeaders, Headers: []hpack.HeaderField{
			hdr(":status", "103"), hdr("link", "</s.css>; rel=preload"),
		}},
		{Type: conn.EventHeaders, Headers: []hpack.HeaderField{
			hdr(":status", "200"), hdr("content-type", "text/plain"),
		}},
		{Type: conn.EventData, Data: []byte("body"), EndStream: true},
	}
	var resp Response
	resp.Reset()
	req := &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}

	err := drainResponse(context.Background(), nil, &scriptedStream{events: events},
		req, &resp, nil, 1<<20, 1<<20)

	require.NoError(t, err, "drainResponse must pump past an interim block, not fail on it")
	assert.Equal(t, 200, resp.Status, "the final status must win over the 103 that preceded it")
	for _, f := range resp.Headers {
		assert.NotEqual(t, "link", string(f.Name),
			"interim 1xx header leaked into Response.Headers")
	}
	assert.Equal(t, "body", string(resp.Body), "the final response body must survive the interim block")
}
