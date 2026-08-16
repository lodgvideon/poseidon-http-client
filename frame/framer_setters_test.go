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
