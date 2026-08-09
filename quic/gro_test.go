package quic

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// groPC is a batched-receive PacketConn: it implements groReader, so the QUIC
// receive path reads through ReadGRO. It hands back one coalesced burst (several
// datagrams concatenated, sliced by segSize) on the first ReadGRO, then reports a
// timeout so a single Poll drains it. dataReads counts the ReadGRO calls that
// returned a datagram/burst — the syscall metric GRO cuts from K to 1.
type groPC struct {
	burst     []byte // the coalesced datagrams, returned once
	segSize   int    // GRO segment size (0 = one datagram)
	delivered bool
	dataReads int
	written   [][]byte
}

func (p *groPC) ReadGRO(buf []byte) (int, int, error) {
	if p.delivered {
		return 0, 0, timeoutError{} // socket drained
	}
	p.delivered = true
	p.dataReads++
	return copy(buf, p.burst), p.segSize, nil
}

// Read satisfies PacketConn; readPacket prefers ReadGRO, so this is unused on the
// GRO path but keeps groPC a valid PacketConn.
func (p *groPC) Read(b []byte) (int, error) {
	if p.delivered {
		return 0, timeoutError{}
	}
	p.delivered = true
	return copy(b, p.burst), nil
}
func (p *groPC) Write(b []byte) (int, error) {
	p.written = append(p.written, append([]byte(nil), b...))
	return len(b), nil
}
func (p *groPC) Close() error                    { return nil }
func (p *groPC) SetReadDeadline(time.Time) error { return nil }

// groServerKit builds the matched server-side sealer and the client's 1-RTT
// receive opener (plus the dcid) for a post-handshake exchange.
func groServerKit(t *testing.T) (sealer *Sealer, opener *Opener, dcid []byte) {
	t.Helper()
	dcid = []byte("grotest0")
	keys, _ := InitialKeys(dcid)
	var err error
	if sealer, err = NewSealer(keys); err != nil {
		t.Fatal(err)
	}
	if opener, err = NewOpener(keys); err != nil {
		t.Fatal(err)
	}
	return sealer, opener, dcid
}

// groRecvConn builds a post-handshake client Conn wired to pc with the 1-RTT
// receive opener installed and one open bidi stream (stream 0) for the server to
// reply on — the conn_poll_test receive rig, parameterized on pc.
func groRecvConn(t *testing.T, pc PacketConn, opener *Opener, dcid []byte, sealer *Sealer) (*Conn, *Stream) {
	t.Helper()
	c := &Conn{
		pc:                pc,
		dcid:              dcid,
		oneRTTSealer:      sealer,
		handshakeComplete: true,
		peer:              TransportParams{InitialMaxStreamsBidi: 1},
	}
	c.keys.OneRTT = opener
	s, err := c.OpenStream() // stream 0 — the one the server replies on
	if err != nil {
		t.Fatal(err)
	}
	return c, s
}

// groStreamBurst seals one server 1-RTT packet per chunk, each carrying the next
// contiguous STREAM segment on stream 0 (the last with FIN) — the datagrams that
// arrive back-to-back and get GRO-coalesced.
func groStreamBurst(t *testing.T, sealer *Sealer, chunks []string) [][]byte {
	t.Helper()
	pkts := make([][]byte, 0, len(chunks))
	var off uint64
	for i, ch := range chunks {
		fin := i == len(chunks)-1
		frames := AppendStream(nil, 0, off, fin, []byte(ch))
		pkts = append(pkts, sealServerPacket(t, sealer, PacketShort, nil, nil, uint64(i), frames))
		off += uint64(len(ch))
	}
	return pkts
}

// concatBurst concatenates the datagrams into a single GRO super-buffer and
// returns it with segSize = len(pkts[0]). It asserts the GRO invariant that every
// datagram but the last equals segSize (the last may be shorter) — the exact shape
// the kernel produces when it coalesces same-size datagrams.
func concatBurst(t *testing.T, pkts [][]byte) (burst []byte, segSize int) {
	t.Helper()
	segSize = len(pkts[0])
	for i, p := range pkts {
		if i < len(pkts)-1 && len(p) != segSize {
			t.Fatalf("packet %d is %d bytes, want segSize %d (GRO coalesces same-size datagrams; only the last may be shorter)", i, len(p), segSize)
		}
		if len(p) > segSize {
			t.Fatalf("packet %d is %d bytes, exceeds segSize %d", i, len(p), segSize)
		}
		burst = append(burst, p...)
	}
	return burst, segSize
}

// TestGRO_CoalescedBurstIsOneReadGRO is the load-bearing win: five equal-size
// server datagrams coalesced into one GRO buffer are split by the receive path and
// each fed through the unchanged recvDatagram, reassembling the exact stream — all
// from a SINGLE data-returning ReadGRO (reads/op 5->1), and acknowledged with one
// batched ACK.
func TestGRO_CoalescedBurstIsOneReadGRO(t *testing.T) {
	sealer, opener, dcid := groServerKit(t)
	chunks := []string{"aaaa", "bbbb", "cccc", "dddd", "eeee"}
	pkts := groStreamBurst(t, sealer, chunks)
	burst, segSize := concatBurst(t, pkts)
	pc := &groPC{burst: burst, segSize: segSize}
	c, s := groRecvConn(t, pc, opener, dcid, sealer)

	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got, want := string(s.Recv()), "aaaabbbbccccddddeeee"; got != want {
		t.Fatalf("Recv = %q, want %q (GRO split must reassemble every coalesced datagram in order)", got, want)
	}
	if !s.Finished() {
		t.Fatal("stream should be finished after the FIN datagram in the burst")
	}
	if pc.dataReads != 1 {
		t.Fatalf("dataReads = %d, want 1 (%d datagrams delivered by ONE ReadGRO)", pc.dataReads, len(pkts))
	}
	if len(pc.written) != 1 {
		t.Fatalf("wrote %d packets, want 1 (single batched ACK for the whole burst)", len(pc.written))
	}
	t.Logf("reads/op: %d datagrams via %d ReadGRO (a non-GRO transport needs %d Reads)", len(pkts), pc.dataReads, len(pkts))
}

// TestGRO_CoalescedBurstShorterLastSegment exercises the tail clamp: the last
// coalesced datagram is shorter than segSize, so recvGRO must slice buf[:n] into
// full segments plus a short final one — and still reassemble the stream exactly.
func TestGRO_CoalescedBurstShorterLastSegment(t *testing.T) {
	sealer, opener, dcid := groServerKit(t)
	// One large datagram then a tiny one: the large frame (>20 bytes) is not padded,
	// so its packet is segSize; the tiny frame is padded to the 20-byte floor, so its
	// packet is shorter than segSize — the last-segment clamp. Only the first packet
	// is non-last, so there is no equal-size-among-many constraint to satisfy.
	big := strings.Repeat("x", 30)
	chunks := []string{big, "y"}
	pkts := groStreamBurst(t, sealer, chunks)
	burst, segSize := concatBurst(t, pkts)
	if len(pkts[len(pkts)-1]) >= segSize {
		t.Fatalf("test setup: last packet %d not shorter than segSize %d", len(pkts[len(pkts)-1]), segSize)
	}
	pc := &groPC{burst: burst, segSize: segSize}
	c, s := groRecvConn(t, pc, opener, dcid, sealer)

	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got, want := string(s.Recv()), big+"y"; got != want {
		t.Fatalf("Recv = %q, want %q (a shorter final segment must reassemble correctly)", got, want)
	}
	if pc.dataReads != 1 {
		t.Fatalf("dataReads = %d, want 1", pc.dataReads)
	}
}

// TestGRO_SingleDatagramSegSizeZero proves the segSize==0 path is the unchanged
// single-datagram receive: ReadGRO reports no coalescing, so recvGRO does exactly
// one recvDatagram, as the pre-GRO path did.
func TestGRO_SingleDatagramSegSizeZero(t *testing.T) {
	sealer, opener, dcid := groServerKit(t)
	frames := AppendStream(nil, 0, 0, true, []byte("response body"))
	pkt := sealServerPacket(t, sealer, PacketShort, nil, nil, 0, frames)
	pc := &groPC{burst: pkt, segSize: 0} // segSize 0 = one datagram
	c, s := groRecvConn(t, pc, opener, dcid, sealer)

	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := string(s.Recv()); got != "response body" {
		t.Fatalf("Recv = %q, want %q", got, "response body")
	}
	if !s.Finished() {
		t.Fatal("stream should be finished")
	}
	if pc.dataReads != 1 {
		t.Fatalf("dataReads = %d, want 1", pc.dataReads)
	}
}

// TestGRO_FallbackNonGROOneReadPerDatagram proves the pre-GRO behavior is
// preserved for a PacketConn WITHOUT ReadGRO (drainingPC): readPacket falls back to
// a plain Read, so the same four datagrams take four reads (one per datagram) and
// still reassemble the stream — GRO is purely additive.
func TestGRO_FallbackNonGROOneReadPerDatagram(t *testing.T) {
	sealer, opener, dcid := groServerKit(t)
	chunks := []string{"aaaa", "bbbb", "cccc", "dddd"}
	pkts := groStreamBurst(t, sealer, chunks)
	pc := &drainingPC{pkts: pkts} // no ReadGRO method → non-GRO fallback path
	c, s := groRecvConn(t, pc, opener, dcid, sealer)

	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got, want := string(s.Recv()), "aaaabbbbccccdddd"; got != want {
		t.Fatalf("Recv = %q, want %q", got, want)
	}
	if pc.i != len(pkts) {
		t.Fatalf("non-GRO reads = %d, want %d (one Read per datagram, unchanged)", pc.i, len(pkts))
	}
}

// TestGRO_RecvGROFallbackSingleDatagram exercises the transport primitive's
// degraded path directly: RecvGRO with a nil RawConn (no offload available) reads
// one datagram via the fallback io.Reader and reports segSize 0 — the guarantee the
// non-Linux build and the Linux nil-fd path both rely on. It runs on every platform
// because both RecvGRO variants fall back to a plain read when rc is nil.
func TestGRO_RecvGROFallbackSingleDatagram(t *testing.T) {
	want := []byte("one whole datagram")
	buf := make([]byte, 64)
	n, seg, err := RecvGRO(nil, nil, bytes.NewReader(want), buf)
	if err != nil {
		t.Fatal(err)
	}
	if seg != 0 {
		t.Fatalf("segSize = %d, want 0 (a plain read is one datagram)", seg)
	}
	if !bytes.Equal(buf[:n], want) {
		t.Fatalf("read %q, want %q", buf[:n], want)
	}
}

// TestGRO_EnableGRONilIsNoop checks the enable primitive is a harmless no-op when
// there is no raw fd — the best-effort contract (a failure to enable GRO must never
// break receiving; the path just stays single-datagram).
func TestGRO_EnableGRONilIsNoop(t *testing.T) {
	if err := EnableGRO(nil); err != nil {
		t.Fatalf("EnableGRO(nil) = %v, want nil", err)
	}
}
