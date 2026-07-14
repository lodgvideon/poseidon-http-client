package quic

import (
	"bytes"
	"io"
	"testing"
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
		if err != nil {
			t.Fatalf("pkt %d ParseHeader: %v", i, err)
		}
		_, _, payload, err := opener.Open(pkt, hdr.PNOffset, 0)
		if err != nil {
			t.Fatalf("pkt %d Open: %v", i, err)
		}
		if err := ParseFrames(payload, h); err != nil {
			t.Fatalf("pkt %d ParseFrames: %v", i, err)
		}
	}
	// STREAM frames arrive in send order (ascending offset); concatenate in place.
	var body []byte
	for _, s := range h.streams {
		if int(s.offset) != len(body) {
			t.Fatalf("gap: frame offset %d, reassembled %d bytes so far", s.offset, len(body))
		}
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
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(keys)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if n != len(body) {
		t.Fatalf("sent %d, want %d", n, len(body))
	}
	if pc.datagrams() < 2 {
		t.Fatalf("body did not split: %d datagrams", pc.datagrams())
	}
	if len(pc.calls) != 1 || pc.writeN != 0 {
		t.Fatalf("syscalls = %d WriteGSO + %d Write, want exactly 1 WriteGSO (N datagrams -> 1 syscall)", len(pc.calls), pc.writeN)
	}
	seg := pc.calls[0].segSize
	// GSO rule: every datagram but the last must be exactly segSize; the last <= segSize.
	for i, pkt := range pc.pkts {
		if i < len(pc.pkts)-1 && len(pkt) != seg {
			t.Fatalf("datagram %d is %d bytes, want segSize %d (only the last may be shorter)", i, len(pkt), seg)
		}
		if len(pkt) > seg {
			t.Fatalf("datagram %d is %d bytes, exceeds segSize %d", i, len(pkt), seg)
		}
	}
	if got := reassembleBody(t, c, op, pc.pkts); !bytes.Equal(got, body) {
		t.Fatalf("reassembled %d bytes, want %d — batched datagrams corrupt", len(got), len(body))
	}
}

// TestGSO_FallbackNoGSOWriter proves the pre-GSO behavior is preserved: a
// PacketConn WITHOUT WriteGSO (capturePC) still receives one Write per datagram,
// and the datagrams are byte-for-byte the same sealed packets (they reassemble to
// the body).
func TestGSO_FallbackNoGSOWriter(t *testing.T) {
	pc := &capturePC{}
	c, s, op := gsoSendConn(t, pc)
	body := testBody(16 * 1024)
	if _, err := s.Send(body, true); err != nil {
		t.Fatal(err)
	}
	if len(pc.pkts) < 2 {
		t.Fatalf("body did not split: %d writes", len(pc.pkts))
	}
	if got := reassembleBody(t, c, op, pc.pkts); !bytes.Equal(got, body) {
		t.Fatalf("reassembled %d bytes, want %d", len(got), len(body))
	}
}

// TestGSO_SameDatagramsFewerSyscalls sends one body through the GSO and the
// non-GSO transports and asserts they put the SAME datagrams on the wire — GSO
// merely carries them in one syscall instead of N (the writes/op N->1 result).
func TestGSO_SameDatagramsFewerSyscalls(t *testing.T) {
	body := testBody(20 * 1024)
	capPC := &capturePC{}
	_, sc, _ := gsoSendConn(t, capPC)
	if _, err := sc.Send(body, true); err != nil {
		t.Fatal(err)
	}
	gso := &recordGSOPC{}
	_, sg, _ := gsoSendConn(t, gso)
	if _, err := sg.Send(body, true); err != nil {
		t.Fatal(err)
	}
	if gso.datagrams() != len(capPC.pkts) {
		t.Fatalf("datagram count differs: GSO %d, non-GSO %d", gso.datagrams(), len(capPC.pkts))
	}
	if len(gso.calls) != 1 {
		t.Fatalf("GSO syscalls = %d, want 1 (all datagrams in one sendmsg)", len(gso.calls))
	}
	if len(capPC.pkts) < 2 {
		t.Fatalf("non-GSO writes = %d, expected the body to split into many", len(capPC.pkts))
	}
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
		if err := c.addToBatch(&b, make([]byte, n)); err != nil {
			t.Fatal(err)
		}
	}
	add(1200)
	add(1200)
	add(1200)
	add(800) // shorter -> final segment of batch #1, flushes [1200,1200,1200,800]
	add(1200)
	add(1200) // batch #2, still open
	if err := c.flushBatch(&b); err != nil {
		t.Fatal(err)
	}
	if len(pc.calls) != 2 {
		t.Fatalf("WriteGSO calls = %d, want 2", len(pc.calls))
	}
	if pc.calls[0].segs != 4 || pc.calls[0].segSize != 1200 {
		t.Fatalf("call 0 = %+v, want 4 segs of segSize 1200", pc.calls[0])
	}
	if pc.calls[1].segs != 2 || pc.calls[1].segSize != 1200 {
		t.Fatalf("call 1 = %+v, want 2 segs of segSize 1200", pc.calls[1])
	}
	wantSizes := []int{1200, 1200, 1200, 800, 1200, 1200}
	if len(pc.pkts) != len(wantSizes) {
		t.Fatalf("datagrams = %d, want %d", len(pc.pkts), len(wantSizes))
	}
	for i, want := range wantSizes {
		if len(pc.pkts[i]) != want {
			t.Fatalf("datagram %d = %d bytes, want %d", i, len(pc.pkts[i]), want)
		}
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
		if err := c.addToBatch(&b, make([]byte, pktSize)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.flushBatch(&b); err != nil {
		t.Fatal(err)
	}
	if pc.datagrams() != count {
		t.Fatalf("datagrams delivered = %d, want %d", pc.datagrams(), count)
	}
	// The byte cap (65535) binds before the 64-segment cap for 1200-byte datagrams
	// (54*1200=64800 < 65535, 55*1200=66000 > 65535), so >=4 sendmsgs are needed.
	if len(pc.calls) < 4 {
		t.Fatalf("WriteGSO calls = %d, want >=4 (caps force multiple sendmsgs)", len(pc.calls))
	}
	totalSegs := 0
	for i, call := range pc.calls {
		if call.segs > maxGSOSegments {
			t.Fatalf("call %d has %d segments, exceeds cap %d", i, call.segs, maxGSOSegments)
		}
		if call.bytes > maxGSOBytes {
			t.Fatalf("call %d is %d bytes, exceeds cap %d", i, call.bytes, maxGSOBytes)
		}
		totalSegs += call.segs
	}
	if totalSegs != count {
		t.Fatalf("segments across calls = %d, want %d", totalSegs, count)
	}
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
	n, err := SendGSO(nil, w, buf, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("wrote %d, want %d", n, len(buf))
	}
	wantSizes := []int{100, 100, 100, 40}
	if len(got) != len(wantSizes) {
		t.Fatalf("segments = %d, want %d", len(got), len(wantSizes))
	}
	var joined []byte
	for i, seg := range got {
		if len(seg) != wantSizes[i] {
			t.Fatalf("segment %d = %d bytes, want %d", i, len(seg), wantSizes[i])
		}
		joined = append(joined, seg...)
	}
	if !bytes.Equal(joined, buf) {
		t.Fatal("fallback loop reordered or corrupted the buffer")
	}
}

// writerFunc adapts a function to io.Writer for the fallback-loop test.
type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
