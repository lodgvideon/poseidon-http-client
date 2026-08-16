package frame

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WriteDataV exists to remove a copy, so the one thing it must never do is
// change what reaches the wire. Every test here is an equivalence test against
// WriteData on the already-joined payload: same bytes out, for any split of the
// same input.
//
// An equivalence oracle rather than hand-written expected bytes, because the
// failure this guards against is a subtle one — a wrong Length, a piece written
// twice, a piece skipped — and hand-written expectations only catch the splits
// somebody thought of.

// joinBufs concatenates bufs the obvious way, for the oracle side.
func joinBufs(bufs [][]byte) []byte {
	var out []byte
	for _, b := range bufs {
		out = append(out, b...)
	}
	return out
}

// TestWriteDataV_MatchesWriteData is the oracle, over many random splits.
func TestWriteDataV_MatchesWriteData(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i * 7)
	}

	for trial := 0; trial < 500; trial++ {
		n := rng.Intn(len(payload) + 1)
		p := payload[:n]

		// A random split into up to 6 pieces, empty pieces included.
		pieces := rng.Intn(6) + 1
		cuts := make([]int, 0, pieces+1)
		cuts = append(cuts, 0)
		for i := 0; i < pieces-1; i++ {
			cuts = append(cuts, rng.Intn(n+1))
		}
		cuts = append(cuts, n)
		for i := 1; i < len(cuts); i++ {
			for j := i; j > 0 && cuts[j] < cuts[j-1]; j-- {
				cuts[j], cuts[j-1] = cuts[j-1], cuts[j]
			}
		}
		bufs := make([][]byte, 0, pieces)
		for i := 0; i+1 < len(cuts); i++ {
			bufs = append(bufs, p[cuts[i]:cuts[i+1]])
		}
		endStream := rng.Intn(2) == 0
		var vec, ref bytes.Buffer

		vecErr := NewFramer(&vec, nil).WriteDataV(7, endStream, bufs)
		refErr := NewFramer(&ref, nil).WriteData(7, endStream, joinBufs(bufs))

		require.NoErrorf(t, vecErr, "trial %d: WriteDataV: %v", trial, vecErr)
		require.NoErrorf(t, refErr, "trial %d: WriteData: %v", trial, refErr)
		require.Truef(t, bytes.Equal(vec.Bytes(), ref.Bytes()),
			"trial %d: split %v of %d bytes (endStream=%v) produced different wire bytes\n vec %x\n ref %x",
			trial, cuts, n, endStream, vec.Bytes(), ref.Bytes())
	}
}

// TestWriteDataVPadded_MatchesWriteDataPadded is the same oracle for the padded
// form, which has three more places to put a byte in the wrong order.
func TestWriteDataVPadded_MatchesWriteDataPadded(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(255 - i)
	}

	for trial := 0; trial < 200; trial++ {
		n := rng.Intn(len(payload) + 1)
		p := payload[:n]
		cut := rng.Intn(n + 1)
		bufs := [][]byte{p[:cut], p[cut:]}
		padLen := uint8(rng.Intn(8))
		endStream := rng.Intn(2) == 0
		var vec, ref bytes.Buffer

		vecErr := NewFramer(&vec, nil).WriteDataVPadded(3, endStream, bufs, padLen)
		refErr := NewFramer(&ref, nil).WriteDataPadded(3, endStream, joinBufs(bufs), padLen)

		require.NoErrorf(t, vecErr, "trial %d: WriteDataVPadded: %v", trial, vecErr)
		require.NoErrorf(t, refErr, "trial %d: WriteDataPadded: %v", trial, refErr)
		require.Truef(t, bytes.Equal(vec.Bytes(), ref.Bytes()),
			"trial %d: cut %d of %d, padLen %d: different wire bytes\n vec %x\n ref %x",
			trial, cut, n, padLen, vec.Bytes(), ref.Bytes())
	}
}

// TestWriteDataV_EdgeShapes covers the shapes the random oracle reaches rarely
// or not at all: no buffers, every buffer empty, and a zero-length piece
// between two non-empty ones.
func TestWriteDataV_EdgeShapes(t *testing.T) {
	cases := []struct {
		name string
		bufs [][]byte
	}{
		{"nil", nil},
		{"empty slice", [][]byte{}},
		{"one empty", [][]byte{{}}},
		{"all empty", [][]byte{{}, nil, {}}},
		{"hole in the middle", [][]byte{[]byte("ab"), {}, []byte("cd")}},
		{"empty first", [][]byte{{}, []byte("xy")}},
		{"empty last", [][]byte{[]byte("xy"), nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var vec, ref bytes.Buffer

			vecErr := NewFramer(&vec, nil).WriteDataV(1, true, tc.bufs)
			refErr := NewFramer(&ref, nil).WriteData(1, true, joinBufs(tc.bufs))

			require.NoError(t, vecErr, "WriteDataV")
			require.NoError(t, refErr, "WriteData")
			assert.Truef(t, bytes.Equal(vec.Bytes(), ref.Bytes()),
				"wire bytes differ\n vec %x\n ref %x", vec.Bytes(), ref.Bytes())
		})
	}
}

// TestWriteDataV_RejectsZeroStreamID and the size check mirror WriteData: a
// vectored write must not be a way around the validation the scalar one does.
func TestWriteDataV_RejectsZeroStreamID(t *testing.T) {
	var buf bytes.Buffer

	vErr := NewFramer(&buf, nil).WriteDataV(0, false, [][]byte{[]byte("x")})
	vpErr := NewFramer(&buf, nil).WriteDataVPadded(0, false, [][]byte{[]byte("x")}, 1)

	assert.ErrorIsf(t, vErr, ErrInvalidStreamID, "WriteDataV(streamID 0) = %v, want ErrInvalidStreamID", vErr)
	assert.ErrorIsf(t, vpErr, ErrInvalidStreamID, "WriteDataVPadded(streamID 0) = %v, want ErrInvalidStreamID", vpErr)
	assert.Zerof(t, buf.Len(), "a rejected write emitted %d bytes, want none", buf.Len())
}

// TestWriteDataV_RejectsOversizeTotal pins that the limit applies to the SUM.
// Checking each buffer separately would let a caller past the frame-size cap by
// splitting the payload, which is exactly what this API makes easy.
func TestWriteDataV_RejectsOversizeTotal(t *testing.T) {
	half := make([]byte, defaultMaxFrameSize/2+1)
	var buf bytes.Buffer

	err := NewFramer(&buf, nil).WriteDataV(1, false, [][]byte{half, half})

	assert.ErrorIsf(t, err, ErrFrameTooLarge, "two half-max buffers = %v, want ErrFrameTooLarge — the cap is on the "+
		"frame, and splitting the payload must not evade it", err)
	assert.Zerof(t, buf.Len(), "a rejected write emitted %d bytes, want none", buf.Len())
}

// TestWriteDataV_DoesNotAllocate is the gate that keeps the point of the change.
// A vectored write that allocates per frame would have traded a copy for an
// allocation, which on the send path is the worse of the two.
//
// Nothing inside the measured closure asserts: testify reflects and allocates,
// and AllocsPerRun counts the whole process. The write error is captured and
// checked afterwards instead.
func TestWriteDataV_DoesNotAllocate(t *testing.T) {
	fr := NewFramer(discardWriter{}, nil)
	prefix := []byte{0, 0, 0, 4, 0}
	body := make([]byte, 16000)
	bufs := [][]byte{prefix, body}
	var writeErr error

	n := testing.AllocsPerRun(100, func() {
		if err := fr.WriteDataV(1, false, bufs); err != nil {
			writeErr = err
		}
	})

	require.NoError(t, writeErr, "WriteDataV")
	assert.Zerof(t, n, "WriteDataV allocates %.1f per frame, want 0", n)
}

// discardWriter is an io.Writer that keeps nothing, so the allocation gate
// measures the framer rather than a growing buffer.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
