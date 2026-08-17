package conn

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The vectored DATA path must be indistinguishable on the wire from the scalar
// one. Everything here is an equivalence test against writeData on the joined
// payload — same frames, same boundaries, same END_STREAM placement — because
// the risky part is not the API but the cursor: chunks cut ACROSS buffer
// boundaries, so a frame's payload is usually a suffix of one buffer followed by
// a prefix of the next.
//
// Hand-written expectations would only cover the splits somebody thought of. The
// split that breaks a cursor is the one nobody thought of, so the oracle drives
// random ones and compares bytes.
//
// The oracle's reach is bounded, and knowing where matters: since the two loops
// merged into writeDataVec, both arms share everything except dataVec.next's two
// branches, so a fault in the shared loop moves both arms the same way and the
// comparison stays green. That is what the direct cursor tests below are for.

// vecConn builds a Conn whose framer writes into buf, with enough send window
// for the payloads here.
func vecConn(t *testing.T, buf *bytes.Buffer, maxFrame uint32) (*Conn, *Stream) {
	t.Helper()
	c := newGoAwayConn()
	c.fr = frame.NewFramer(buf, bytes.NewReader(nil))
	c.opts.Settings.MaxFrameSize = maxFrame
	s := newStream(1, 8, c, 1<<20)
	s.id = 1
	s.sendWindow = 1 << 20
	c.peerConnSendWindow = 1 << 20
	return c, s
}

// TestWriteDataV_MatchesWriteData is the oracle: for random splits and random
// frame sizes, the vectored path emits byte-identical frames.
func TestWriteDataV_MatchesWriteData(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	payload := make([]byte, 9000)
	for i := range payload {
		payload[i] = byte(i*31 + 5)
	}
	ctx := context.Background()

	for trial := 0; trial < 300; trial++ {
		n := rng.Intn(len(payload) + 1)
		p := payload[:n]
		pieces := rng.Intn(5) + 1
		cuts := []int{0}
		for i := 0; i < pieces-1; i++ {
			cuts = append(cuts, rng.Intn(n+1))
		}
		cuts = append(cuts, n)
		for i := 1; i < len(cuts); i++ {
			for j := i; j > 0 && cuts[j] < cuts[j-1]; j-- {
				cuts[j], cuts[j-1] = cuts[j-1], cuts[j]
			}
		}
		var bufs [][]byte
		for i := 0; i+1 < len(cuts); i++ {
			bufs = append(bufs, p[cuts[i]:cuts[i+1]])
		}
		// A frame size small enough that most trials span several frames, so the
		// cursor is exercised across boundaries rather than only within one.
		maxFrame := uint32(rng.Intn(2000) + 16)
		endStream := rng.Intn(2) == 0
		var vecBuf, refBuf bytes.Buffer
		vc, vs := vecConn(t, &vecBuf, maxFrame)
		rc, rs := vecConn(t, &refBuf, maxFrame)

		verr := vc.writeDataV(ctx, vs, vs.gen.Load(), bufs, endStream)
		rerr := rc.writeData(ctx, rs, rs.gen.Load(), p, endStream)

		require.NoErrorf(t, verr, "trial %d: writeDataV", trial)
		require.NoErrorf(t, rerr, "trial %d: writeData", trial)
		require.Truef(t, bytes.Equal(vecBuf.Bytes(), refBuf.Bytes()),
			"trial %d: %d bytes split at %v, maxFrame %d, endStream %v: wire bytes differ\n vec %x\n ref %x",
			trial, n, cuts, maxFrame, endStream, vecBuf.Bytes(), refBuf.Bytes())
	}
}

// TestWriteDataV_MatchesWriteDataPadded runs the same oracle with DATA padding
// on, where each frame carries a pad byte and pad bytes that the credit
// accounting has to charge for too.
func TestWriteDataV_MatchesWriteDataPadded(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	payload := make([]byte, 4000)
	for i := range payload {
		payload[i] = byte(i)
	}
	ctx := context.Background()

	for trial := 0; trial < 100; trial++ {
		n := rng.Intn(len(payload) + 1)
		p := payload[:n]
		cut := rng.Intn(n + 1)
		bufs := [][]byte{p[:cut], p[cut:]}
		endStream := rng.Intn(2) == 0
		var vecBuf, refBuf bytes.Buffer
		vc, vs := vecConn(t, &vecBuf, 1024)
		vc.opts.Padding = PaddingStrategy{Min: 4, Max: 4, DataOnly: true}
		rc, rs := vecConn(t, &refBuf, 1024)
		rc.opts.Padding = PaddingStrategy{Min: 4, Max: 4, DataOnly: true}

		verr := vc.writeDataV(ctx, vs, vs.gen.Load(), bufs, endStream)
		rerr := rc.writeData(ctx, rs, rs.gen.Load(), p, endStream)

		require.NoErrorf(t, verr, "trial %d: writeDataV", trial)
		require.NoErrorf(t, rerr, "trial %d: writeData", trial)
		require.Truef(t, bytes.Equal(vecBuf.Bytes(), refBuf.Bytes()),
			"trial %d: %d bytes cut at %d, endStream %v: padded wire bytes differ\n vec %x\n ref %x",
			trial, n, cut, endStream, vecBuf.Bytes(), refBuf.Bytes())
	}
}

// TestWriteDataV_EmptyShapes covers the payload-free cases, where the scalar
// path has its own branch: nothing at all when endStream is false, and a bare
// zero-length END_STREAM frame when it is true.
func TestWriteDataV_EmptyShapes(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		bufs [][]byte
	}{
		{"nil", nil},
		{"all empty", [][]byte{{}, nil, {}}},
	} {
		for _, end := range []bool{false, true} {
			t.Run(tc.name, func(t *testing.T) {
				var vecBuf, refBuf bytes.Buffer
				vc, vs := vecConn(t, &vecBuf, 16384)
				rc, rs := vecConn(t, &refBuf, 16384)

				verr := vc.writeDataV(ctx, vs, vs.gen.Load(), tc.bufs, end)
				rerr := rc.writeData(ctx, rs, rs.gen.Load(), nil, end)

				require.NoErrorf(t, verr, "writeDataV")
				require.NoErrorf(t, rerr, "writeData")
				assert.Truef(t, bytes.Equal(vecBuf.Bytes(), refBuf.Bytes()),
					"endStream=%v: wire bytes differ\n vec %x\n ref %x",
					end, vecBuf.Bytes(), refBuf.Bytes())
			})
		}
	}
}

// TestDataVec_NextIsTotal drives the cursor directly at the boundaries the
// writer relies on. It must never index past the buffers, and it must report the
// count it actually produced rather than the count it was asked for — that
// report is the only thing standing between a cursor bug and a DATA frame whose
// header disagrees with its payload.
func TestDataVec_NextIsTotal(t *testing.T) {
	bufs := [][]byte{[]byte("abc"), {}, []byte("de")}
	v, err := newDataVec(bufs)
	require.NoError(t, err)
	require.Equalf(t, 5, v.rem, "rem = %d, want 5", v.rem)
	var dst [][]byte

	dst, got := v.next(4, dst[:0])

	require.Equalf(t, 4, got, "next(4) produced %d bytes, want 4", got)
	assert.Equalf(t, "abcd", string(bytes.Join(dst, nil)),
		"next(4) covered %q, want \"abcd\"", string(bytes.Join(dst, nil)))

	dst, got = v.next(4, dst[:0])

	assert.Equalf(t, 1, got, "next(4) at the tail produced %d, want 1 — a cursor that reported 4 here "+
		"would have the writer declare a 4-byte frame and emit one byte", got)
	assert.Equalf(t, "e", string(bytes.Join(dst, nil)),
		"tail covered %q, want \"e\"", string(bytes.Join(dst, nil)))

	// Past the end: still total, still honest.
	_, got = v.next(1, dst[:0])

	assert.Equalf(t, 0, got, "next past the end produced %d, want 0", got)
}

// TestDataVec_NextSkipsEmpties pins that zero-length members cost nothing and
// cannot stall the walk — an empty buffer between two full ones is exactly the
// shape a caller building [prefix, msg] with an empty message produces.
func TestDataVec_NextSkipsEmpties(t *testing.T) {
	v, err := newDataVec([][]byte{{}, {}, []byte("xy"), {}, []byte("z"), {}})
	require.NoError(t, err)
	var dst [][]byte

	dst, got := v.next(3, dst[:0])

	require.Equalf(t, 3, got, "next(3) produced %d, want 3", got)
	assert.Equalf(t, "xyz", string(bytes.Join(dst, nil)),
		"covered %q, want \"xyz\"", string(bytes.Join(dst, nil)))
	for _, seg := range dst {
		assert.NotEmpty(t, seg,
			"an empty segment reached the frame writer; it should have been skipped")
	}
}

// TestEmitDataV_ClearsScratch is the retention gate. The scratch lives on the
// Conn and is reused, so reslicing it to zero length is not enough: the element
// pointers would keep the last message's backing array alive for as long as the
// connection sits idle in a pool.
func TestEmitDataV_ClearsScratch(t *testing.T) {
	var buf bytes.Buffer
	c, _ := vecConn(t, &buf, 16384)
	body := make([]byte, 512)
	v, err := newDataVec([][]byte{[]byte("hdr"), body})
	require.NoError(t, err)

	segs, err := c.emitDataV(&v, 1, 515, true, 0, c.dvSegs)

	require.NoErrorf(t, err, "emitDataV")
	c.dvSegs = segs
	full := c.dvSegs[:cap(c.dvSegs)]
	for i := range full {
		require.Truef(t, full[i] == nil,
			"scratch slot %d still points at caller memory after the write — an "+
				"idle pooled connection would pin the whole message", i)
	}
}

// TestEmitDataV_UnderrunDoesNotWrite is the safety net for the invariant the
// whole design rests on: the bytes credited, the Length in the header, and the
// bytes written are the same number. If the cursor cannot produce what was
// credited, emitting anything would desynchronise the peer's parser for good, so
// it must write nothing and say so.
func TestEmitDataV_UnderrunDoesNotWrite(t *testing.T) {
	var buf bytes.Buffer
	c, _ := vecConn(t, &buf, 16384)
	v, err := newDataVec([][]byte{[]byte("short")})
	require.NoError(t, err)

	segs, err := c.emitDataV(&v, 1, 100, false, 0, c.dvSegs) // ask for more than exists

	c.dvSegs = segs
	// Identity, not errors.Is: the caller distinguishes this from a transport
	// error by comparing the sentinel directly, so a future wrap would be a
	// behaviour change this test must still see.
	require.Truef(t, err == ErrVecUnderrun,
		"emitDataV with a short cursor = %v, want ErrVecUnderrun", err)
	assert.Zerof(t, buf.Len(),
		"a frame was written anyway (%d bytes); the header would have declared "+
			"100 bytes and 5 would have followed", buf.Len())
}
