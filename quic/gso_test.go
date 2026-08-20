package quic

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gsoCall records one WriteGSO syscall: the segment size argument and the number
// of datagrams and bytes the kernel would slice the super-buffer into.
type gsoCall struct {
	segSize int
	segs    int
	bytes   int
}

// recordGSOPC is a batched-write PacketConn (implements gsoWriter). It records
// every syscall — plain Write and WriteGSO — and splits each WriteGSO super-buffer
// back into its individual datagrams so a test can count syscalls, verify the GSO
// caps per call, and re-parse each datagram off the wire.
type recordGSOPC struct {
	pkts   [][]byte  // individual datagrams, in send order, from Write and split WriteGSO bufs
	writeN int       // plain Write syscalls
	calls  []gsoCall // one entry per WriteGSO syscall
}

func (p *recordGSOPC) Write(b []byte) (int, error) {
	p.writeN++
	p.pkts = append(p.pkts, append([]byte(nil), b...))
	return len(b), nil
}

func (p *recordGSOPC) WriteGSO(buf []byte, segSize int) (int, error) {
	segs := 0
	for off := 0; off < len(buf); off += segSize {
		end := off + segSize
		if end > len(buf) {
			end = len(buf)
		}
		p.pkts = append(p.pkts, append([]byte(nil), buf[off:end]...))
		segs++
	}
	p.calls = append(p.calls, gsoCall{segSize: segSize, segs: segs, bytes: len(buf)})
	return len(buf), nil
}

func (p *recordGSOPC) Read([]byte) (int, error) { return 0, io.EOF }
func (p *recordGSOPC) Close() error             { return nil }

// datagrams counts the UDP datagrams that reached the wire (segments across all
// syscalls), independent of how many syscalls carried them.
func (p *recordGSOPC) datagrams() int { return len(p.pkts) }

// reassembleBody decrypts each captured datagram with opener and concatenates its
// STREAM frame payloads in offset order — the round-trip proof that a batched send
// produced valid, correctly-framed packets.
func reassembleBody(t *testing.T, c *Conn, opener *Opener, pkts [][]byte) []byte {
	t.Helper()
	h := &frameCollector{}
	for i, pkt := range pkts {
		hdr, err := ParseHeader(pkt, len(c.dcid))
		require.NoErrorf(t, err, "pkt %d ParseHeader", i)
		_, _, payload, err := opener.Open(pkt, hdr.PNOffset, 0)
		require.NoErrorf(t, err, "pkt %d Open", i)
		require.NoErrorf(t, ParseFrames(payload, h), "pkt %d ParseFrames", i)
	}
	// STREAM frames arrive in send order (ascending offset); concatenate in place.
	var body []byte
	for _, s := range h.streams {
		require.Equalf(t, len(body), int(s.offset),
			"gap: frame offset %d, reassembled %d bytes so far", s.offset, len(body))
		body = append(body, s.data...)
	}
	return body
}

// gsoSendConn builds a send-ready Conn over pc with a 1-RTT sealer and one open
// bidi stream, plus the paired Opener to decrypt what it writes. Congestion control
// is disabled (cwnd == 0) so a body splits into uniform max-size datagrams.
func gsoSendConn(t *testing.T, pc PacketConn) (*Conn, *Stream, *Opener) {
	t.Helper()
	dcid := []byte("gsotest0")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	require.NoError(t, err, "NewSealer for the send-side 1-RTT keys")
	opener, err := NewOpener(keys)
	require.NoError(t, err, "NewOpener to decrypt what the send path writes")
	c := &Conn{
		pc:           pc,
		dcid:         dcid,
		oneRTTSealer: sealer,
		peer: TransportParams{
			InitialMaxStreamsBidi:          8,
			InitialMaxData:                 1 << 40,
			InitialMaxStreamDataBidiRemote: 1 << 40,
		},
		connMax: 1 << 40,
	}
	s, err := c.OpenStream()
	require.NoError(t, err, "OpenStream for the body the batched send carries")
	return c, s, opener
}

func testBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

// TestGSO_MultiDatagramBodyIsOneWriteGSO is the load-bearing win: a 16 KiB body
// that the send path splits into many ~1200-byte datagrams leaves in a SINGLE
// WriteGSO (writes/op N->1) when the transport supports batching, and every
// datagram re-parses to reassemble the exact body.
func TestGSO_MultiDatagramBodyIsOneWriteGSO(t *testing.T) {
	pc := &recordGSOPC{}
	c, s, op := gsoSendConn(t, pc)
	body := testBody(16 * 1024)

	n, err := s.Send(body, true)

	require.NoError(t, err, "Send of a body larger than one datagram")
	require.Equalf(t, len(body), n, "sent %d, want %d", n, len(body))
	require.GreaterOrEqualf(t, pc.datagrams(), 2, "body did not split: %d datagrams", pc.datagrams())
	require.Truef(t, len(pc.calls) == 1 && pc.writeN == 0,
		"syscalls = %d WriteGSO + %d Write, want exactly 1 WriteGSO (N datagrams -> 1 syscall)",
		len(pc.calls), pc.writeN)
	seg := pc.calls[0].segSize
	// GSO rule: every datagram but the last must be exactly segSize; the last <= segSize.
	for i, pkt := range pc.pkts {
		if i < len(pc.pkts)-1 {
			assert.Lenf(t, pkt, seg,
				"datagram %d is %d bytes, want segSize %d (only the last may be shorter)", i, len(pkt), seg)
		}
		assert.LessOrEqualf(t, len(pkt), seg,
			"datagram %d is %d bytes, exceeds segSize %d", i, len(pkt), seg)
	}
	got := reassembleBody(t, c, op, pc.pkts)
	assert.Truef(t, bytes.Equal(got, body),
		"reassembled %d bytes, want %d — batched datagrams corrupt", len(got), len(body))
}

// TestGSO_FallbackNoGSOWriter proves the pre-GSO behavior is preserved: a
// PacketConn WITHOUT WriteGSO (capturePC) still receives one Write per datagram,
// and the datagrams are byte-for-byte the same sealed packets (they reassemble to
// the body).
func TestGSO_FallbackNoGSOWriter(t *testing.T) {
	pc := &capturePC{}
	c, s, op := gsoSendConn(t, pc)
	body := testBody(16 * 1024)

	_, err := s.Send(body, true)

	require.NoError(t, err, "Send over a transport without WriteGSO")
	require.GreaterOrEqualf(t, len(pc.pkts), 2, "body did not split: %d writes", len(pc.pkts))
	got := reassembleBody(t, c, op, pc.pkts)
	assert.Truef(t, bytes.Equal(got, body),
		"reassembled %d bytes, want %d — the non-GSO path must produce the same packets", len(got), len(body))
}

// TestGSO_SameDatagramsFewerSyscalls sends one body through the GSO and the
// non-GSO transports and asserts they put the SAME datagrams on the wire — GSO
// merely carries them in one syscall instead of N (the writes/op N->1 result).
func TestGSO_SameDatagramsFewerSyscalls(t *testing.T) {
	body := testBody(20 * 1024)
	capPC := &capturePC{}
	_, sc, _ := gsoSendConn(t, capPC)
	gso := &recordGSOPC{}
	_, sg, _ := gsoSendConn(t, gso)

	_, plainErr := sc.Send(body, true)
	_, gsoErr := sg.Send(body, true)

	require.NoError(t, plainErr, "Send over the non-GSO transport")
	require.NoError(t, gsoErr, "Send over the GSO transport")
	assert.Equalf(t, len(capPC.pkts), gso.datagrams(),
		"datagram count differs: GSO %d, non-GSO %d", gso.datagrams(), len(capPC.pkts))
	assert.Lenf(t, gso.calls, 1,
		"GSO syscalls = %d, want 1 (all datagrams in one sendmsg)", len(gso.calls))
	assert.GreaterOrEqualf(t, len(capPC.pkts), 2,
		"non-GSO writes = %d, expected the body to split into many", len(capPC.pkts))
	t.Logf("writes/op: non-GSO=%d GSO=%d for %d datagrams", len(capPC.pkts), len(gso.calls), gso.datagrams())
}

// TestGSO_AddToBatch_SplitsOnSizeChange checks the segment-size invariant: a
// datagram shorter than the batch's segment size is the final segment and forces a
// flush; a datagram of a new size starts a fresh batch. Feeds synthetic packets so
// the accumulation logic is exercised without the seal path.
func TestGSO_AddToBatch_SplitsOnSizeChange(t *testing.T) {
	pc := &recordGSOPC{}
	c := &Conn{pc: pc}
	b := c.newBatch()
	add := func(n int) {
		require.NoErrorf(t, c.addToBatch(&b, make([]byte, n)), "addToBatch(%d)", n)
	}

	add(1200)
	add(1200)
	add(1200)
	add(800) // shorter -> final segment of batch #1, flushes [1200,1200,1200,800]
	add(1200)
	add(1200) // batch #2, still open
	err := c.flushBatch(&b)

	require.NoError(t, err, "flushBatch of the still-open second batch")
	require.Lenf(t, pc.calls, 2, "WriteGSO calls = %d, want 2", len(pc.calls))
	assert.Equalf(t, gsoCall{segSize: 1200, segs: 4, bytes: 4400}, pc.calls[0],
		"call 0 = %+v, want 4 segs of segSize 1200", pc.calls[0])
	assert.Equalf(t, gsoCall{segSize: 1200, segs: 2, bytes: 2400}, pc.calls[1],
		"call 1 = %+v, want 2 segs of segSize 1200", pc.calls[1])
	wantSizes := []int{1200, 1200, 1200, 800, 1200, 1200}
	require.Lenf(t, pc.pkts, len(wantSizes), "datagrams = %d, want %d", len(pc.pkts), len(wantSizes))
	for i, want := range wantSizes {
		assert.Lenf(t, pc.pkts[i], want, "datagram %d = %d bytes, want %d", i, len(pc.pkts[i]), want)
	}
}

// TestGSO_AddToBatch_RespectsCaps feeds far more equal-size datagrams than one GSO
// super-buffer may hold and asserts every WriteGSO stays within both caps (<= 64
// segments AND <= 65535 bytes), so no single sendmsg exceeds the kernel's limits,
// while every datagram is still delivered exactly once.
func TestGSO_AddToBatch_RespectsCaps(t *testing.T) {
	pc := &recordGSOPC{}
	c := &Conn{pc: pc}
	b := c.newBatch()
	const (
		pktSize = 1200
		count   = 200
	)
	for i := 0; i < count; i++ {
		require.NoErrorf(t, c.addToBatch(&b, make([]byte, pktSize)), "addToBatch %d", i)
	}

	err := c.flushBatch(&b)

	require.NoError(t, err, "flushBatch after feeding past both caps")
	assert.Equalf(t, count, pc.datagrams(), "datagrams delivered = %d, want %d", pc.datagrams(), count)
	// The byte cap (65535) binds before the 64-segment cap for 1200-byte datagrams
	// (54*1200=64800 < 65535, 55*1200=66000 > 65535), so >=4 sendmsgs are needed.
	assert.GreaterOrEqualf(t, len(pc.calls), 4,
		"WriteGSO calls = %d, want >=4 (caps force multiple sendmsgs)", len(pc.calls))
	totalSegs := 0
	for i, call := range pc.calls {
		assert.LessOrEqualf(t, call.segs, maxGSOSegments,
			"call %d has %d segments, exceeds cap %d", i, call.segs, maxGSOSegments)
		assert.LessOrEqualf(t, call.bytes, maxGSOBytes,
			"call %d is %d bytes, exceeds cap %d", i, call.bytes, maxGSOBytes)
		totalSegs += call.segs
	}
	assert.Equalf(t, count, totalSegs, "segments across calls = %d, want %d", totalSegs, count)
}

// TestGSO_AddToBatch_SegmentCapBinds is the arm in which the 64-segment cap is
// the one that decides.
//
// TestGSO_AddToBatch_RespectsCaps above feeds 1200-byte datagrams, and its own
// comment does the arithmetic: 54*1200 = 64800 < 65535, so maxGSOBytes fills
// first and b.n never reaches maxGSOSegments. Its `call.segs <= maxGSOSegments`
// assertion is therefore satisfied by the BYTE cap — quadrupling the segment cap
// left the whole suite green. With the segment cap effectively gone, a burst of
// small datagrams builds a super-buffer of more than 64 segments, which sendmsg
// with UDP_SEGMENT rejects with EINVAL on Linux: every datagram in the burst is
// dropped, at the transport, with no retransmit to recover them. #838.
//
// 100-byte datagrams invert which cap binds: 64*100 = 6400, an order of
// magnitude below maxGSOBytes. The reached-the-cap assertion is what keeps this
// arm honest — without it, a change that made the batch flush early would leave
// the test passing while measuring nothing.
func TestGSO_AddToBatch_SegmentCapBinds(t *testing.T) {
	pc := &recordGSOPC{}
	c := &Conn{pc: pc}
	b := c.newBatch()
	const (
		pktSize = 100 // 64*100 = 6400 bytes, far below maxGSOBytes
		count   = 200 // > 3 full segment-capped batches
	)
	for i := 0; i < count; i++ {
		require.NoErrorf(t, c.addToBatch(&b, make([]byte, pktSize)), "addToBatch %d", i)
	}

	err := c.flushBatch(&b)

	require.NoError(t, err, "flushBatch after feeding past the segment cap")
	assert.Equalf(t, count, pc.datagrams(),
		"datagrams delivered = %d, want %d", pc.datagrams(), count)
	maxSegs, totalSegs := 0, 0
	for i, call := range pc.calls {
		assert.LessOrEqualf(t, call.segs, maxGSOSegments,
			"call %d carries %d segments, past the %d the kernel accepts — sendmsg would "+
				"return EINVAL and the whole burst would be lost", i, call.segs, maxGSOSegments)
		assert.Lessf(t, call.bytes, maxGSOBytes,
			"call %d is %d bytes: the byte cap must NOT be what bounds this arm, or it "+
				"measures the same thing as TestGSO_AddToBatch_RespectsCaps", i, call.bytes)
		if call.segs > maxSegs {
			maxSegs = call.segs
		}
		totalSegs += call.segs
	}
	assert.Equalf(t, maxGSOSegments, maxSegs,
		"the fullest sendmsg carried %d segments, want exactly %d — the segment cap is "+
			"not the binding one here, so this arm proves nothing about it", maxSegs, maxGSOSegments)
	assert.GreaterOrEqualf(t, len(pc.calls), 4,
		"WriteGSO calls = %d, want >=4 (200 datagrams at %d per batch)", len(pc.calls), maxGSOSegments)
	assert.Equalf(t, count, totalSegs,
		"segments across calls = %d, want %d — every datagram must still be delivered once",
		totalSegs, count)
}

// TestGSO_SendGSOFallbackLoop exercises the transport primitive's degraded path:
// SendGSO with a nil RawConn (no offload available) must write every datagram
// individually to the fallback writer — the guarantee the non-Linux build and the
// Linux EIO fallback both rely on.
func TestGSO_SendGSOFallbackLoop(t *testing.T) {
	var got [][]byte
	w := writerFunc(func(p []byte) (int, error) {
		got = append(got, append([]byte(nil), p...))
		return len(p), nil
	})
	buf := make([]byte, 3*100+40) // three full 100-byte segments plus a 40-byte tail
	for i := range buf {
		buf[i] = byte(i)
	}
	n, err := SendGSO(nil, nil, w, buf, 100)

	require.NoError(t, err, "SendGSO with a nil RawConn must fall back, not fail")
	assert.Equalf(t, len(buf), n, "wrote %d, want %d", n, len(buf))
	wantSizes := []int{100, 100, 100, 40}
	require.Lenf(t, got, len(wantSizes), "segments = %d, want %d", len(got), len(wantSizes))
	var joined []byte
	for i, seg := range got {
		assert.Lenf(t, seg, wantSizes[i], "segment %d = %d bytes, want %d", i, len(seg), wantSizes[i])
		joined = append(joined, seg...)
	}
	assert.True(t, bytes.Equal(joined, buf), "fallback loop reordered or corrupted the buffer")
}

// writerFunc adapts a function to io.Writer for the fallback-loop test.
type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
