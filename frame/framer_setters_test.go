package frame

import (
	"bytes"
	"errors"
	"testing"
)

func TestFramer_Close_ReleasesBufferIdempotent(t *testing.T) {
	fr := NewFramer(nil, nil)
	fr.Close()
	fr.Close() // second call must be safe
	if fr.readBuf != nil {
		t.Fatal("readBuf should be nil after Close")
	}
}

func TestFramer_SetMaxReadFrameSize_AppliesOnRead(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, &buf)
	if err := fr.WriteData(1, false, make([]byte, 200)); err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	fr.SetMaxReadFrameSize(64) // smaller than 200-byte frame
	_, err := fr.ReadFrame(t.Context(), &recordingHandler{})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestFramer_SetMaxHeaderListSize_StoresValue(t *testing.T) {
	fr := NewFramer(nil, nil)
	fr.SetMaxHeaderListSize(2048)
	if fr.maxHeaderListSize != 2048 {
		t.Fatalf("maxHeaderListSize = %d, want 2048", fr.maxHeaderListSize)
	}
}

func TestFramer_SetReadBuffer_OverridesInternalBuffer(t *testing.T) {
	fr := NewFramer(nil, nil)
	custom := make([]byte, 1024)
	fr.SetReadBuffer(custom)
	if &fr.readBuf[0] != &custom[0] || cap(fr.readBuf) != cap(custom) {
		t.Fatal("SetReadBuffer must replace internal slice")
	}
}

func TestFramer_WriteDataPadded_RejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	fr := NewFramer(&buf, nil)
	fr.SetMaxReadFrameSize(32)
	if err := fr.WriteDataPadded(1, false, make([]byte, 64), 0); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestFramer_WriteData_RejectsStream0(t *testing.T) {
	fr := NewFramer(&bytes.Buffer{}, nil)
	if err := fr.WriteData(0, false, nil); !errors.Is(err, ErrInvalidStreamID) {
		t.Fatalf("err = %v, want ErrInvalidStreamID", err)
	}
}

func TestFramer_WriteWindowUpdate_RejectsZeroIncrement(t *testing.T) {
	fr := NewFramer(&bytes.Buffer{}, nil)
	if err := fr.WriteWindowUpdate(1, 0); !errors.Is(err, ErrZeroIncrement) {
		t.Fatalf("err = %v, want ErrZeroIncrement", err)
	}
}

func TestFramer_WriteRSTStream_RejectsStream0(t *testing.T) {
	fr := NewFramer(&bytes.Buffer{}, nil)
	if err := fr.WriteRSTStream(0, ErrCodeCancel); !errors.Is(err, ErrInvalidStreamID) {
		t.Fatalf("err = %v, want ErrInvalidStreamID", err)
	}
}

func TestFramer_WriteHeaders_RejectsStream0(t *testing.T) {
	fr := NewFramer(&bytes.Buffer{}, nil)
	if err := fr.WriteHeaders(WriteHeadersParams{StreamID: 0}); !errors.Is(err, ErrInvalidStreamID) {
		t.Fatalf("err = %v, want ErrInvalidStreamID", err)
	}
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
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}
