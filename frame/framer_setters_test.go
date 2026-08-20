package frame

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFramer_Close_ReleasesBufferIdempotent(t *testing.T) {
	fr := NewFramer(nil, nil)

	fr.Close()
	fr.Close() // second call must be safe

	require.Nil(t, fr.readBuf, "readBuf should be nil after Close")
}

func TestFramer_SetMaxReadFrameSize_AppliesOnRead(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	require.NoError(t, fr.WriteData(1, false, make([]byte, 200)), "WriteData")
	fr.SetMaxReadFrameSize(64) // smaller than 200-byte frame

	_, err := fr.ReadFrame(t.Context(), &recordingHandler{})

	require.ErrorIsf(t, err, ErrFrameTooLarge, "err = %v, want ErrFrameTooLarge", err)
}

func TestFramer_SetReadBuffer_OverridesInternalBuffer(t *testing.T) {
	fr := NewFramer(nil, nil)
	custom := make([]byte, 1024)

	fr.SetReadBuffer(custom)

	require.NotEmpty(t, fr.readBuf, "SetReadBuffer must replace internal slice")
	assert.Same(t, &custom[0], &fr.readBuf[0], "SetReadBuffer must replace internal slice")
	assert.Equal(t, cap(custom), cap(fr.readBuf), "SetReadBuffer must replace internal slice")
}

func TestFramer_WriteDataPadded_RejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetMaxReadFrameSize(32)

	err := fr.WriteDataPadded(1, false, make([]byte, 64), 0)

	require.ErrorIsf(t, err, ErrFrameTooLarge, "err = %v, want ErrFrameTooLarge", err)
}

func TestFramer_WriteData_RejectsStream0(t *testing.T) {
	fr := NewFramer(&bytes.Buffer{}, nil)

	err := fr.WriteData(0, false, nil)

	require.ErrorIsf(t, err, ErrInvalidStreamID, "err = %v, want ErrInvalidStreamID", err)
}

func TestFramer_WriteWindowUpdate_RejectsZeroIncrement(t *testing.T) {
	fr := NewFramer(&bytes.Buffer{}, nil)

	err := fr.WriteWindowUpdate(1, 0)

	require.ErrorIsf(t, err, ErrZeroIncrement, "err = %v, want ErrZeroIncrement", err)
}

func TestFramer_WriteRSTStream_RejectsStream0(t *testing.T) {
	fr := NewFramer(&bytes.Buffer{}, nil)

	err := fr.WriteRSTStream(0, ErrCodeCancel)

	require.ErrorIsf(t, err, ErrInvalidStreamID, "err = %v, want ErrInvalidStreamID", err)
}

func TestFramer_WriteHeaders_RejectsStream0(t *testing.T) {
	fr := NewFramer(&bytes.Buffer{}, nil)

	err := fr.WriteHeaders(WriteHeadersParams{StreamID: 0})

	require.ErrorIsf(t, err, ErrInvalidStreamID, "err = %v, want ErrInvalidStreamID", err)
}

func TestFramer_WriteHeaders_RejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetMaxReadFrameSize(16)

	err := fr.WriteHeaders(WriteHeadersParams{
		StreamID:      1,
		BlockFragment: make([]byte, 100),
		EndHeaders:    true,
	})

	require.ErrorIsf(t, err, ErrFrameTooLarge, "err = %v, want ErrFrameTooLarge", err)
}

// The three sibling writers that carry the same stream-0 guard WriteData,
// WriteRSTStream and WriteHeaders are pinned for above. RFC 9113 scopes each of
// these frame types to a stream — PRIORITY §6.3, PUSH_PROMISE §6.6,
// CONTINUATION §6.10 all make a Stream Identifier field of 0x00 a connection
// error PROTOCOL_ERROR for the recipient — so emitting one on stream 0 is this
// client handing the peer grounds to tear the connection down.
//
// This is the sibling-divergence shape (#778): one file grew the coverage and
// the siblings silently did not. The guards are EXECUTED by every valid frame of
// those types, so line coverage showed nothing and all three were removable with
// the whole package suite staying green.
//
// Each asserts on the emitted bytes as well as the error, because the two are
// separate properties: a writer that returned the sentinel *after* putting a
// nine-octet header on the wire would leave the peer mid-frame, and only the
// buffer length can tell the two apart.

func TestFramer_WritePriority_RejectsStream0(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)

	err := fr.WritePriority(0, Priority{StreamDep: 3, Weight: 15})

	require.ErrorIsf(t, err, ErrInvalidStreamID,
		"err = %v, want ErrInvalidStreamID — a PRIORITY frame on stream 0 is a "+
			"connection error at the receiver, so this client must never emit one", err)
	assert.Zerof(t, buf.Len(),
		"%d octets written for a refused PRIORITY; the guard must refuse before "+
			"any header reaches the wire, or the peer is left mid-frame", buf.Len())
}

func TestFramer_WritePushPromise_RejectsStream0(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)

	err := fr.WritePushPromise(0, 2, []byte{0x82}, true, 0)

	require.ErrorIsf(t, err, ErrInvalidStreamID,
		"err = %v, want ErrInvalidStreamID — PUSH_PROMISE is sent on an existing "+
			"peer-initiated stream, so stream 0 is a connection error at the receiver", err)
	assert.Zerof(t, buf.Len(),
		"%d octets written for a refused PUSH_PROMISE; the guard must refuse before "+
			"any header reaches the wire", buf.Len())
}

func TestFramer_WriteContinuation_RejectsStream0(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)

	err := fr.WriteContinuation(0, true, []byte{0x82})

	require.ErrorIsf(t, err, ErrInvalidStreamID,
		"err = %v, want ErrInvalidStreamID — a CONTINUATION continues a field "+
			"block on the stream that opened it, and stream 0 can never be that stream", err)
	assert.Zerof(t, buf.Len(),
		"%d octets written for a refused CONTINUATION; the guard must refuse before "+
			"any header reaches the wire", buf.Len())
}
