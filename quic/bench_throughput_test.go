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

// benchSendConn builds a Conn with an installed 1-RTT sealer, huge flow-control
// windows, a counting PacketConn, and one open client bidi stream — the minimal
// rig that drives the real seal→PacketConn.Write send path (writeStreamFrame →
// writeAppFrames → sealPacket → pc.Write) without standing up a live handshake.
// Congestion control is left disabled (cwnd == 0, the struct-literal default),
// so grantable is bounded only by the per-datagram budget and the huge windows.
func benchSendConn(b *testing.B) (*Conn, *Stream, *countingPC) {
	b.Helper()
	dcid := []byte("benchdc0")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	if err != nil {
		b.Fatal(err)
	}
	pc := &countingPC{}
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
	return c, s, pc
}

// resetSend rewinds the per-op send state so each iteration re-seals from a fixed
// offset. Keeping the send offset and packet number at zero holds the
// sent-packet map at a reused key (no map growth per op) and keeps the AEAD
// confidentiality counter below its limit (RFC 9001 §6.6), so the steady-state
// allocation figure reflects the seal path alone rather than bookkeeping growth.
func resetSend(c *Conn, s *Stream) {
	s.sendOffset = 0
	s.finSent = false
	c.connSent = 0
	c.sendPN[spaceApp] = 0
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
func requireSendBench(b *testing.B) {
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
	b.ReportMetric(float64(pc.datagrams.Load())/float64(b.N), "datagrams/op")
}
