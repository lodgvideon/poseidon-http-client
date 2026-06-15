package client

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func TestBuildHeaders_ProtocolExtendedConnect(t *testing.T) {
	t.Parallel()

	req := &Request{
		Method:   "CONNECT",
		Scheme:   "https",
		Authority: "example.com",
		Path:     "/chat",
		Protocol: "websocket",
	}
	sp := hdrSlicePool.Get().(*[]conn.HeaderField)
	hdrs := buildHeaders(req, "default.example.com", "https", sp)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	var foundProtocol bool
	for _, h := range hdrs {
		if string(h.Name) == ":protocol" {
			foundProtocol = true
			if string(h.Value) != "websocket" {
				t.Errorf(":protocol value = %q, want websocket", h.Value)
			}
		}
	}
	if !foundProtocol {
		t.Error("expected :protocol pseudo-header in output")
	}
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
	hdrs := buildHeaders(req, "default.example.com", "https", sp)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

	for _, h := range hdrs {
		if string(h.Name) == ":protocol" {
			t.Fatal(":protocol should not be emitted when Protocol is empty")
		}
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
	hdrs := buildHeaders(req, "default.example.com", "https", sp)
	defer func() { *sp = (*sp)[:0]; hdrSlicePool.Put(sp) }()

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
	if protoIdx < 0 {
		t.Fatal(":protocol not found")
	}
	if pathIdx < 0 {
		t.Fatal(":path not found")
	}
	if regularIdx < 0 {
		t.Fatal("regular header not found")
	}
	if protoIdx < pathIdx {
		t.Errorf(":protocol (idx %d) should come after :path (idx %d)", protoIdx, pathIdx)
	}
	if protoIdx > regularIdx {
		t.Errorf(":protocol (idx %d) should come before regular headers (idx %d)", protoIdx, regularIdx)
	}
}
