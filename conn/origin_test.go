package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConn_CanCoalesce(t *testing.T) {
	t.Parallel()
	c := &Conn{}
	c.storeOrigins([]string{
		"https://example.com",
		"https://cdn.example.com",
	})

	first := c.CanCoalesce("https://example.com")
	second := c.CanCoalesce("https://cdn.example.com")
	stranger := c.CanCoalesce("https://evil.com")

	assert.True(t, first, "expected CanCoalesce(example.com) = true")
	assert.True(t, second, "expected CanCoalesce(cdn.example.com) = true")
	assert.False(t, stranger, "expected CanCoalesce(evil.com) = false")
}

func TestConn_CanCoalesce_NoOrigins(t *testing.T) {
	t.Parallel()
	c := &Conn{}

	got := c.CanCoalesce("https://example.com")

	assert.False(t, got, "expected false when no ORIGIN frame received")
}

func TestConn_Origins(t *testing.T) {
	t.Parallel()
	c := &Conn{}
	c.storeOrigins([]string{"https://a.com", "https://b.com"})

	got := c.Origins()

	require.Lenf(t, got, 2, "expected 2 origins, got %d", len(got))
	// Verify it's a copy
	got[0] = "modified"
	again := c.Origins()
	assert.Equal(t, "https://a.com", again[0], "Origins() should return a copy")
}

func TestConn_Origins_Empty(t *testing.T) {
	t.Parallel()
	c := &Conn{}

	got := c.Origins()

	// == nil, not require.Nil: an empty non-nil slice would pass a reflective
	// nil check, and "no ORIGIN frame" is what nil distinguishes here.
	require.Truef(t, got == nil, "expected nil, got %v", got)
}

func TestConnHandler_OnOrigin(t *testing.T) {
	t.Parallel()
	m := newFakeStreamMap()
	h := newConnHandler(m, nil)
	origins := []string{"https://a.com", "https://b.com"}

	err := h.OnOrigin(frame.FrameHeader{}, origins)

	require.NoErrorf(t, err, "OnOrigin error")
	require.Lenf(t, m.origins, 2, "expected 2 origins stored, got %d", len(m.origins))
	assert.Equalf(t, "https://a.com", m.origins[0], "origins mismatch: %v", m.origins)
	assert.Equalf(t, "https://b.com", m.origins[1], "origins mismatch: %v", m.origins)
}
