package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec46_MaxStreamsRaisesLimit checks that a MAX_STREAMS
// frame raises the cumulative bidirectional stream limit so the client can open
// more streams than the peer's initial grant (RFC 9000 §4.6).
func TestConformance_RFC9000_Sec46_MaxStreamsRaisesLimit(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	h := &connFrameHandler{c: c}
	_, err := c.OpenStream()
	require.NoError(t, err, "the first OpenStream fills the initial limit of 1")
	_, capped := c.OpenStream() // capped at the initial 1

	raise := h.OnMaxStreams(false, 3) // peer raises the limit to 3
	_, second := c.OpenStream()
	_, third := c.OpenStream()
	_, fourth := c.OpenStream() // now capped at 3

	assert.Equalf(t, ErrTooManyStreams, capped, "2nd OpenStream = %v, want ErrTooManyStreams", capped)
	require.NoError(t, raise, "OnMaxStreams(bidi, 3)")
	assert.NoErrorf(t, second, "OpenStream after MAX_STREAMS(3): %v", second)
	assert.NoErrorf(t, third, "3rd OpenStream: %v", third)
	assert.Equalf(t, ErrTooManyStreams, fourth, "4th OpenStream = %v, want ErrTooManyStreams", fourth)
}

// TestConformance_RFC9000_Sec46_MaxStreamsTooLarge checks that a MAX_STREAMS value
// above 2^60 is a FRAME_ENCODING_ERROR (RFC 9000 §19.11).
func TestConformance_RFC9000_Sec46_MaxStreamsTooLarge(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	h := &connFrameHandler{c: c}

	over := h.OnMaxStreams(false, maxStreamsLimit+1)
	atLimit := h.OnMaxStreams(true, maxStreamsLimit) // exactly 2^60 is legal

	assert.Equalf(t, ErrFrameEncoding, over, "MAX_STREAMS > 2^60 = %v, want ErrFrameEncoding", over)
	assert.NoErrorf(t, atLimit, "MAX_STREAMS == 2^60 = %v, want nil", atLimit)
}

// TestConn_MaxStreams_NonIncreasingIgnored checks that a MAX_STREAMS that does not
// increase the limit is ignored (RFC 9000 §19.11).
func TestConn_MaxStreams_NonIncreasingIgnored(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 5}}
	h := &connFrameHandler{c: c}

	err := h.OnMaxStreams(false, 3) // 3 < 5

	require.NoError(t, err, "a non-increasing MAX_STREAMS is legal, merely inert")
	assert.Equalf(t, uint64(5), c.peer.InitialMaxStreamsBidi,
		"bidi limit = %d, want 5 (non-increasing MAX_STREAMS ignored)", c.peer.InitialMaxStreamsBidi)
}

// TestConn_MaxStreams_Uni checks that a MAX_STREAMS with the unidirectional type
// raises the uni limit, not the bidi one.
func TestConn_MaxStreams_Uni(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1, InitialMaxStreamsUni: 1}}
	h := &connFrameHandler{c: c}

	err := h.OnMaxStreams(true, 4)

	require.NoError(t, err, "OnMaxStreams(uni, 4)")
	assert.Equalf(t, uint64(4), c.peer.InitialMaxStreamsUni,
		"uni limit = %d, want 4", c.peer.InitialMaxStreamsUni)
	assert.Equalf(t, uint64(1), c.peer.InitialMaxStreamsBidi,
		"bidi limit = %d, want 1 (unchanged)", c.peer.InitialMaxStreamsBidi)
}
