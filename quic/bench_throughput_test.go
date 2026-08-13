package quic

import (
	"io"
	"os"
	"sync/atomic"
	"testing"
)

// countingPC is a PacketConn that counts the datagrams written and drops them.
// Unlike capturePC it does NOT copy the datagram, so the only allocations a
// benchmark using it is charged are the send path's own — the seal-path scratch
// this PR targets plus the retransmit-record copy it intentionally leaves in
// place (RFC 9000 §13.3). Each Write is the OS-independent syscall proxy: one
// Write == one pc.Write == one UDP sendto with no batching.
type countingPC struct{ datagrams atomic.Int64 }

func (p *countingPC) Write(b []byte) (int, error) { p.datagrams.Add(1); return len(b), nil }
func (p *countingPC) Read([]byte) (int, error)    { return 0, io.EOF }
func (p *countingPC) Close() error                { return nil }

// countingGSOPC is countingPC's batched-write twin: it implements gsoWriter, so the
// send path coalesces a multi-datagram burst into one WriteGSO. writes counts
// syscalls (Write + WriteGSO), datagrams counts UDP packets (segments). With GSO a
// 16 KiB body is N datagrams but ONE write — the syscall saving this PR targets.
type countingGSOPC struct {
	writes    atomic.Int64
	datagrams atomic.Int64
}

func (p *countingGSOPC) Write(b []byte) (int, error) {
	p.writes.Add(1)
	p.datagrams.Add(1)
	return len(b), nil
}

func (p *countingGSOPC) WriteGSO(buf []byte, segSize int) (int, error) {
	p.writes.Add(1)
	p.datagrams.Add(int64((len(buf) + segSize - 1) / segSize))
	return len(buf), nil
}
func (p *countingGSOPC) Read([]byte) (int, error) { return 0, io.EOF }
func (p *countingGSOPC) Close() error             { return nil }

// benchSendConn builds a Conn with an installed 1-RTT sealer, huge flow-control
// windows, a counting PacketConn, and one open client bidi stream — the minimal
// rig that drives the real seal→PacketConn.Write send path (writeStreamFrame →
// writeAppFrames → sealPacket → pc.Write) without standing up a live handshake.
// Congestion control is left disabled (cwnd == 0, the struct-literal default),
// so grantable is bounded only by the per-datagram budget and the huge windows.
func benchSendConn(b testing.TB) (*Conn, *Stream, *countingPC) {
	b.Helper()
	pc := &countingPC{}
	c, s := benchSendConnPC(b, pc)
	return c, s, pc
}

// benchSendConnPC is benchSendConn over an arbitrary PacketConn, so a benchmark can
// swap in a batched-write (gsoWriter) transport to measure the syscall saving.
func benchSendConnPC(b testing.TB, pc PacketConn) (*Conn, *Stream) {
	b.Helper()
	dcid := []byte("benchdc0")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	if err != nil {
		b.Fatal(err)
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
		b.Fatal(err)
	}
	return c, s
}

// resetSend rewinds the per-op send state so each iteration re-seals from a fixed
// offset. Keeping the send offset and packet number at zero holds the
// sent-packet map at a reused key (no map growth per op) and keeps the AEAD
// confidentiality counter below its limit (RFC 9001 §6.6), so the steady-state
// allocation figure reflects the seal path alone rather than bookkeeping growth.
//
// The packet-number half of that has a consequence worth stating, because it is
// invisible from the numbers these benchmarks print: a reused key means every
// onSent OVERWRITES one map entry, and overwriting an existing key costs nothing
// however large the element is. Per-packet bookkeeping that scales with DISTINCT
// packet numbers is therefore inexpressible here and will report no change when
// it moves. BenchmarkQUICSend_MonotonicPN is where that cost is visible.
func resetSend(c *Conn, s *Stream) {
	resetSendKeepingPN(c, s)
	c.sendPN[spaceApp] = 0
}

// resetSendKeepingPN is resetSend without the packet-number rewind, for the one
// benchmark that wants packet numbers to advance the way they do on a live
// connection. The AEAD counter is still reset, so a long run cannot trip the
// confidentiality limit (RFC 9001 §6.6) and drag a key update into the
// measurement.
func resetSendKeepingPN(c *Conn, s *Stream) {
	s.sendOffset = 0
	s.finSent = false
	c.connSent = 0
	c.appSendCount = 0
}

// requireSendBench skips the send-path benchmarks unless POSEIDON_BENCH_SEND is
// set. They intentionally allocate — the retransmit-record copy is retained
// until the packet is acknowledged (RFC 9000 §13.3) and is out of scope for the
// seal-scratch reuse — so they must not run under the absolute zero-alloc
// bench-gate, which scans every Benchmark line emitted by ./quic
// (.github/workflows/bench-gate.yml, scripts/bench-gate.sh). A skipped benchmark
// prints no B/op / allocs/op columns, so the gate ignores it. Run locally with:
//
//	POSEIDON_BENCH_SEND=1 go test -run=^$ -bench=BenchmarkQUICSend -benchmem ./quic
func requireSendBench(b testing.TB) {
	if os.Getenv("POSEIDON_BENCH_SEND") == "" {
		b.Skip("send-path bench allocates; set POSEIDON_BENCH_SEND=1 to run (kept out of the zero-alloc bench-gate)")
	}
}

// BenchmarkQUICSend measures the QUIC send hot path for a small request that
// fits a single datagram: writeStreamFrame → writeAppFrames → sealPacket →
// pc.Write. b.ReportAllocs surfaces allocs/op and B/op (the figures PART B drives
// down by reusing per-Conn seal scratch), and the datagrams/op metric is the
// OS-independent syscall proxy — one datagram is one pc.Write is one sendto, so a
// value of ~1.0 confirms the request left in a single unbatched syscall.
func BenchmarkQUICSend(b *testing.B) {
	requireSendBench(b)
	c, s, pc := benchSendConn(b)
	req := []byte("GET / HTTP/3\r\nhost: h3.example\r\naccept: */*\r\n\r\n")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetSend(c, s)
		if _, err := s.Send(req, false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(pc.datagrams.Load())/float64(b.N), "datagrams/op")
}

// benchInFlight is how many unacknowledged packets BenchmarkQUICSend_MonotonicPN
// keeps in the sent-packet map. On a live connection that bound is the
// congestion window; here it is a constant, because the point is a stable window
// rather than a realistic one.
const benchInFlight = 64

// BenchmarkQUICSend_MonotonicPN is BenchmarkQUICSend with the packet number left
// to advance, which is the one thing resetSend holds still.
//
// It exists because the difference is not cosmetic. With the packet number
// pinned, every onSent overwrites a single map entry, and overwriting an
// existing key allocates nothing regardless of the element's size. Shrinking
// sentPacket from 152 to 128 bytes removed exactly one allocation per sent
// packet on the real path (#475, #599) and BenchmarkQUICSend reported the same
// 1 allocs/op before and after — not because the fix did nothing, but because
// the benchmark sends one packet number over and over, and that is the single
// shape where the cost does not exist.
//
// So this measures send plus one map delete, and the delete is deliberate: it
// stands in for the acknowledgement path, which is what bounds the sent-packet
// map on a live connection. Without it the map would grow to b.N entries and
// this would measure map growth, which is the failure mode resetSend was written
// to avoid in the first place.
//
// Read it against BenchmarkQUICSend, not on its own: the gap between the two is
// the per-distinct-packet bookkeeping cost, and it is the only place that gap is
// visible.
func BenchmarkQUICSend_MonotonicPN(b *testing.B) {
	requireSendBench(b)
	c, s, pc := benchSendConn(b)
	req := []byte("GET / HTTP/3\r\nhost: h3.example\r\naccept: */*\r\n\r\n")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetSendKeepingPN(c, s)
		if _, err := s.Send(req, false); err != nil {
			b.Fatal(err)
		}
		// Retire the packet that just left the window. sendPN was already
		// incremented past the packet this iteration sent.
		if pn := c.sendPN[spaceApp]; pn > benchInFlight {
			delete(c.sent[spaceApp].packets, pn-benchInFlight-1)
		}
	}
	b.StopTimer()
	// A window that drifted means the prune fell behind the sends — several
	// packets per op, say — and the run measured map growth rather than
	// steady-state insertion. That is the exact confusion this benchmark exists
	// to remove, so it fails loudly instead of reporting a number.
	if got, want := len(c.sent[spaceApp].packets), min(b.N, benchInFlight+1); got > want {
		b.Fatalf("sent-packet map holds %d entries, want at most %d: the window "+
			"drifted, so this measured growth and not per-packet bookkeeping", got, want)
	}
	b.ReportMetric(float64(pc.datagrams.Load())/float64(b.N), "datagrams/op")
}

// BenchmarkQUICSend_Stream16KiB sends a 16 KiB body per op, which the send path
// splits into ~14 max-size 1-RTT datagrams (RFC 9000 §14, ~1200 bytes each) —
// one pc.Write per datagram, no GSO or batching. datagrams/op surfaces that
// per-request syscall count (the throughput-limiting proxy the perf analysis
// flagged) and allocs/op shows the per-datagram seal cost scaled by the split,
// so PART B's per-seal saving compounds across the datagrams of one request.
func BenchmarkQUICSend_Stream16KiB(b *testing.B) {
	requireSendBench(b)
	c, s, pc := benchSendConn(b)
	body := make([]byte, 16*1024)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetSend(c, s)
		if _, err := s.Send(body, false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	// Without GSO one datagram is one Write is one sendto, so writes/op == datagrams/op:
	// the ~15-syscall/request baseline the GSO variant collapses to ~1.
	b.ReportMetric(float64(pc.datagrams.Load())/float64(b.N), "writes/op")
	b.ReportMetric(float64(pc.datagrams.Load())/float64(b.N), "datagrams/op")
}

// BenchmarkQUICSend_Stream16KiB_GSO is BenchmarkQUICSend_Stream16KiB over a
// batched-write (gsoWriter) transport: the same ~15 datagrams per 16 KiB request
// now leave in ONE WriteGSO, so writes/op drops from ~15 to ~1 while datagrams/op
// is unchanged — the direct proxy for the sendto-per-packet throughput limiter this
// PR removes on Linux. datagrams/op stays ~15; writes/op is the figure that moves.
func BenchmarkQUICSend_Stream16KiB_GSO(b *testing.B) {
	requireSendBench(b)
	pc := &countingGSOPC{}
	c, s := benchSendConnPC(b, pc)
	body := make([]byte, 16*1024)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetSend(c, s)
		if _, err := s.Send(body, false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(pc.writes.Load())/float64(b.N), "writes/op")
	b.ReportMetric(float64(pc.datagrams.Load())/float64(b.N), "datagrams/op")
}
