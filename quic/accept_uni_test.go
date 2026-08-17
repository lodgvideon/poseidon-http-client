package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec21_AcceptServerUniStream checks that a
// server-initiated unidirectional stream (id&3==3) is accepted, its bytes are
// delivered, and it is queued exactly once for AcceptUniStream.
func TestConformance_RFC9000_Sec21_AcceptServerUniStream(t *testing.T) {
	c := &Conn{localMaxStreamsUni: 3}
	h := &connFrameHandler{c: c}

	err := h.OnStream(3, 0, false, []byte{0x00, 0x04})

	require.NoErrorf(t, err, "accept server uni: %v", err)
	s := c.AcceptUniStream()
	require.NotNilf(t, s, "AcceptUniStream = %v, want stream 3", s)
	assert.Equalf(t, uint64(3), s.ID(), "AcceptUniStream = %v, want stream 3", s)
	assert.Equalf(t, []byte{0x00, 0x04}, s.recv.bytes(), "delivered %x, want 0004", s.recv.bytes())
	assert.Nil(t, c.AcceptUniStream(), "only one uni stream should be queued")

	// A later frame on the now-known stream is delivered without re-accepting.
	err = h.OnStream(3, 2, false, []byte{0x07})

	require.NoError(t, err)
	assert.Nil(t, c.AcceptUniStream(), "an already-accepted stream must not be re-queued")
	assert.Equalf(t, []byte{0x00, 0x04, 0x07}, s.recv.bytes(),
		"reassembled %x, want 000407", s.recv.bytes())
}

// TestConformance_RFC9000_Sec46_UniStreamLimit checks that a server uni stream
// beyond our advertised limit is a STREAM_LIMIT_ERROR connection error.
func TestConformance_RFC9000_Sec46_UniStreamLimit(t *testing.T) {
	c := &Conn{localMaxStreamsUni: 3}
	h := &connFrameHandler{c: c}

	// ids 3,7,11 are within the limit (id>>2 = 0,1,2); id 15 (>>2 = 3) exceeds it.
	err := h.OnStream(15, 0, false, []byte{0x00})
	code, ok := closeCodeFor(ErrTooManyUniStreams)

	require.ErrorIsf(t, err, ErrTooManyUniStreams,
		"over-limit uni = %v, want ErrTooManyUniStreams", err)
	require.Truef(t, ok, "closeCodeFor(ErrTooManyUniStreams) = %#x,%v, want STREAM_LIMIT_ERROR", code, ok)
	assert.Equalf(t, ErrCodeStreamLimitError, code,
		"closeCodeFor(ErrTooManyUniStreams) = %#x,%v, want STREAM_LIMIT_ERROR", code, ok)
}

// TestConformance_RFC9000_Sec21_ServerBidiRejected checks that a server-initiated
// bidirectional stream (id&3==1) is a connection error for an HTTP/3 client.
func TestConformance_RFC9000_Sec21_ServerBidiRejected(t *testing.T) {
	c := &Conn{localMaxStreamsUni: 3}
	h := &connFrameHandler{c: c}

	err := h.OnStream(1, 0, false, []byte("x"))
	code, ok := closeCodeFor(ErrServerBidiStream)

	require.ErrorIsf(t, err, ErrServerBidiStream, "server bidi = %v, want ErrServerBidiStream", err)
	require.Truef(t, ok, "closeCodeFor(ErrServerBidiStream) = %#x,%v, want STREAM_LIMIT_ERROR", code, ok)
	assert.Equalf(t, ErrCodeStreamLimitError, code,
		"closeCodeFor(ErrServerBidiStream) = %#x,%v, want STREAM_LIMIT_ERROR", code, ok)
}

// TestConn_AcceptUniStream_Order checks that accepted streams are returned in the
// order they were accepted.
func TestConn_AcceptUniStream_Order(t *testing.T) {
	c := &Conn{localMaxStreamsUni: 3}
	h := &connFrameHandler{c: c}

	for _, id := range []uint64{3, 7, 11} {
		require.NoErrorf(t, h.OnStream(id, 0, false, []byte{byte(id)}), "accept %d", id)
	}

	for _, want := range []uint64{3, 7, 11} {
		s := c.AcceptUniStream()
		require.NotNilf(t, s, "AcceptUniStream = %v, want %d", s, want)
		assert.Equalf(t, want, s.ID(), "AcceptUniStream = %v, want %d", s, want)
	}
	assert.Nil(t, c.AcceptUniStream(), "no more accepted streams expected")
}
