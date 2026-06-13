package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestConn_CanCoalesce(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.storeOrigins([]string{
		"https://example.com",
		"https://cdn.example.com",
	})

	if !c.CanCoalesce("https://example.com") {
		t.Error("expected CanCoalesce(example.com) = true")
	}
	if !c.CanCoalesce("https://cdn.example.com") {
		t.Error("expected CanCoalesce(cdn.example.com) = true")
	}
	if c.CanCoalesce("https://evil.com") {
		t.Error("expected CanCoalesce(evil.com) = false")
	}
}

func TestConn_CanCoalesce_NoOrigins(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	if c.CanCoalesce("https://example.com") {
		t.Error("expected false when no ORIGIN frame received")
	}
}

func TestConn_Origins(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.storeOrigins([]string{"https://a.com", "https://b.com"})

	got := c.Origins()
	if len(got) != 2 {
		t.Fatalf("expected 2 origins, got %d", len(got))
	}

	// Verify it's a copy
	got[0] = "modified"
	again := c.Origins()
	if again[0] != "https://a.com" {
		t.Error("Origins() should return a copy")
	}
}

func TestConn_Origins_Empty(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	if got := c.Origins(); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestConnHandler_OnOrigin(t *testing.T) {
	t.Parallel()

	m := newFakeStreamMap()
	h := newConnHandler(m, nil)

	origins := []string{"https://a.com", "https://b.com"}
	if err := h.OnOrigin(frame.FrameHeader{}, origins); err != nil {
		t.Fatalf("OnOrigin error: %v", err)
	}

	if len(m.origins) != 2 {
		t.Fatalf("expected 2 origins stored, got %d", len(m.origins))
	}
	if m.origins[0] != "https://a.com" || m.origins[1] != "https://b.com" {
		t.Fatalf("origins mismatch: %v", m.origins)
	}
}
