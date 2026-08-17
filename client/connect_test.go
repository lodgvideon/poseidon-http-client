package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func TestBuildHeaders_ProtocolExtendedConnect(t *testing.T) {
	t.Parallel()

	req := &Request{
		Method:    "CONNECT",
		Scheme:    "https",
		Authority: "example.com",
		Path:      "/chat",
		Protocol:  "websocket",
	}
	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	hdrs := buildHeaders(req, "default.example.com", "https", sp)

	var foundProtocol bool
	for _, h := range hdrs {
		if string(h.Name) == ":protocol" {
			foundProtocol = true
			assert.Equalf(t, "websocket", string(h.Value), ":protocol value = %q, want websocket", h.Value)
		}
	}
	assert.True(t, foundProtocol, "expected :protocol pseudo-header in output")
}

func TestBuildHeaders_NoProtocolWhenEmpty(t *testing.T) {
	t.Parallel()

	req := &Request{
		Method:    "GET",
		Scheme:    "https",
		Authority: "example.com",
		Path:      "/",
	}
	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	hdrs := buildHeaders(req, "default.example.com", "https", sp)

	for _, h := range hdrs {
		require.NotEqual(t, ":protocol", string(h.Name),
			":protocol should not be emitted when Protocol is empty")
	}
}

func TestBuildHeaders_ProtocolOrdering(t *testing.T) {
	t.Parallel()

	req := &Request{
		Method:    "CONNECT",
		Scheme:    "https",
		Authority: "example.com",
		Path:      "/ws",
		Protocol:  "websocket",
		Headers: []conn.HeaderField{
			{Name: []byte("sec-websocket-key"), Value: []byte("dGhlIHNhbXBsZSBub25jZQ==")},
		},
	}
	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	hdrs := buildHeaders(req, "default.example.com", "https", sp)

	// :protocol must appear after :path but before regular headers
	protoIdx := -1
	pathIdx := -1
	regularIdx := -1
	for i, h := range hdrs {
		switch string(h.Name) {
		case ":protocol":
			protoIdx = i
		case ":path":
			pathIdx = i
		case "sec-websocket-key":
			regularIdx = i
		}
	}
	require.GreaterOrEqual(t, protoIdx, 0, ":protocol not found")
	require.GreaterOrEqual(t, pathIdx, 0, ":path not found")
	require.GreaterOrEqual(t, regularIdx, 0, "regular header not found")
	assert.GreaterOrEqualf(t, protoIdx, pathIdx,
		":protocol (idx %d) should come after :path (idx %d)", protoIdx, pathIdx)
	assert.LessOrEqualf(t, protoIdx, regularIdx,
		":protocol (idx %d) should come before regular headers (idx %d)", protoIdx, regularIdx)
}
