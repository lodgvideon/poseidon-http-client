package conn

// R0 baseline for the raw-frames design (docs/H2_RAW_FRAMES_DESIGN.md).
//
// The existing conn benchmarks measure a TLS round trip against an in-process
// net/http2 server. That is the wrong shape for a load generator on two counts:
// the peer is not h2c, and b.ReportAllocs() charges the benchmark every
// allocation net/http2 makes on the server side, so the client's own cost is
// unreadable underneath it.
//
// This harness is the load-generator shape instead: plaintext h2c, N concurrent
// streams on ONE connection, a small unary request, and a response the caller
// throws away. The peer is built on frame.Framer and allocates nothing per
// request — the response header block is pre-encoded stateless (so it is valid
// on every stream, not just the first) and the response body is one reused
// buffer.
//
// What to read from it:
//
//   - allocs/op and B/op are the client's, near enough, because the peer's are ~0.
//   - writes/req is the transport write count, which on h2c is the syscall count
//     and on TLS would additionally be the record count. It is the metric R1
//     (Conn.WriteFrames) exists to move.
//   - ns/op is NOISE. Six identical runs of the sibling gRPC harness spread 2x
//     with nothing changed; a real socket in the loop makes latency claims from
//     this harness worthless. Use the CPU profile for time attribution.
//
// The arbiter for R0's cost attribution is the CPU profile, not the component
// benchmarks below — those exist to put a hard number next to each profile
// bucket, not to replace it:
//
//	go test ./conn/ -run='^$' -bench=BenchmarkLoadGen_Unary_C64 -benchmem \
//	  -benchtime=20000x -cpuprofile=cpu.out -mutexprofile=mutex.out
//	go tool pprof -top -nodecount=40 cpu.out
//
// Buckets to attribute in that profile: hpack encode (Encoder.EncodeBlock and
// below), transport writes (flushWrite / bufio / syscall), the receive event
// path (OnData, dataBufPool, deliverEnd, chan send/recv), and the scheduler
// (runtime.schedule, mcall, chanrecv park/unpark) — the last is what a
// goroutine-per-request design pays and a sink design would not.

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// loadGenPrefaceLen is the length of the HTTP/2 client connection preface magic.
const loadGenPrefaceLen = 24

// statelessBlock encodes fields with a throwaway encoder, forcing IndexWithout
// so nothing enters the dynamic table. The result is a pure function of fields
// and is therefore valid for every stream on the connection, which is what lets
// the peer pre-encode once and write the same slice for every response.
//
// It is also, in miniature, the R3 proposal from the design doc: the same
// property that makes a response block cacheable here makes a *request* block
// cacheable for a generator.
func statelessBlock(fields ...hpack.HeaderField) []byte {
	for i := range fields {
		fields[i].Indexing = hpack.IndexWithout
	}
	return hpack.NewEncoder().EncodeBlock(nil, fields)
}

// loadGenPeer is a zero-alloc h2c peer that answers every stream with a fixed
// response: HEADERS, then respSize bytes of DATA carrying END_STREAM (or
// END_STREAM on the HEADERS when respSize is 0).
type loadGenPeer struct {
	ln       net.Listener
	headers  []byte
	respBody []byte
}

func newLoadGenPeer(tb testing.TB, respSize int) *loadGenPeer {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("loadGenPeer listen: %v", err)
	}
	p := &loadGenPeer{
		ln: ln,
		headers: statelessBlock(
			hpack.HeaderField{Name: []byte(":status"), Value: []byte("200")},
			hpack.HeaderField{Name: []byte("content-type"), Value: []byte("application/grpc")},
		),
		respBody: make([]byte, respSize),
	}
	tb.Cleanup(func() { _ = ln.Close() })
	go p.accept()
	return p
}

func (p *loadGenPeer) addr() string { return p.ln.Addr().String() }

func (p *loadGenPeer) accept() {
	for {
		tc, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.serveConn(tc)
	}
}

func (p *loadGenPeer) serveConn(tc net.Conn) {
	defer func() { _ = tc.Close() }()
	// Buffered so a whole response reaches the socket in one write, as a real
	// server's would; an unbuffered peer would put its own syscalls inside every
	// measured round trip.
	bw := bufio.NewWriterSize(tc, 64*1024)
	fr := frame.NewFramer(bw, bufio.NewReaderSize(tc, 64*1024)) // writer first
	defer fr.Close()

	if err := fr.WriteSettings(frame.SettingsParams{}); err != nil {
		return
	}
	if err := bw.Flush(); err != nil {
		return
	}
	var preface [loadGenPrefaceLen]byte
	if _, err := io.ReadFull(tc, preface[:]); err != nil {
		return
	}
	h := &loadGenHandler{peer: p, fr: fr, bw: bw}
	for !h.settingsSeen {
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			return
		}
	}
	if err := fr.WriteSettingsAck(); err != nil {
		return
	}
	if err := bw.Flush(); err != nil {
		return
	}
	for !h.clientAckSeen {
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			return
		}
	}
	h.serving = true
	for {
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			return
		}
	}
}

// loadGenHandler answers frames on the peer side. It holds no per-stream state:
// every stream is answered the moment its terminal frame arrives, so streams may
// interleave arbitrarily without the peer tracking any of them.
type loadGenHandler struct {
	peer          *loadGenPeer
	fr            *frame.Framer
	bw            *bufio.Writer
	settingsSeen  bool
	clientAckSeen bool
	serving       bool
}

// respond writes the whole response for one stream and flushes it.
func (h *loadGenHandler) respond(streamID uint32) error {
	body := h.peer.respBody
	if err := h.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      streamID,
		BlockFragment: h.peer.headers,
		EndHeaders:    true,
		EndStream:     len(body) == 0,
	}); err != nil {
		return err
	}
	if len(body) > 0 {
		if err := h.fr.WriteData(streamID, true, body); err != nil {
			return err
		}
	}
	return h.bw.Flush()
}

func (h *loadGenHandler) OnHeaders(fh frame.FrameHeader, _ frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	if !h.serving {
		return nil
	}
	if fh.Flags&frame.FlagHeadersEndStream != 0 {
		return h.respond(fh.StreamID)
	}
	return nil
}

func (h *loadGenHandler) OnData(fh frame.FrameHeader, _ []byte, _ uint8) error {
	if !h.serving {
		return nil
	}
	// The client's connection-level send window is 65535 bytes for the whole
	// connection lifetime, debited by every request body byte across every
	// stream. Without this refund the benchmark stalls permanently after the
	// first 64 KiB of request payload. Refunding at the connection scope alone is
	// enough: each stream gets a fresh 65535-byte window and the benchmark's
	// request bodies are far smaller than that.
	if fh.Length > 0 {
		if err := h.fr.WriteWindowUpdate(0, fh.Length); err != nil {
			return err
		}
	}
	if fh.Flags&frame.FlagDataEndStream != 0 {
		return h.respond(fh.StreamID)
	}
	return h.bw.Flush()
}

func (h *loadGenHandler) OnSettings(fh frame.FrameHeader, _ frame.SettingsParams) error {
	if fh.Flags&frame.FlagSettingsAck != 0 {
		h.clientAckSeen = true
	} else {
		h.settingsSeen = true
	}
	return nil
}

func (h *loadGenHandler) OnPing(fh frame.FrameHeader, data [8]byte) error {
	if fh.Flags&frame.FlagPingAck != 0 {
		return nil
	}
	if err := h.fr.WritePing(true, data); err != nil {
		return err
	}
	return h.bw.Flush()
}

func (h *loadGenHandler) OnPriority(frame.FrameHeader, frame.Priority) error { return nil }
func (h *loadGenHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error { return nil }
func (h *loadGenHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (h *loadGenHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
func (h *loadGenHandler) OnWindowUpdate(frame.FrameHeader, uint32) error                  { return nil }
func (h *loadGenHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error       { return nil }
func (h *loadGenHandler) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error           { return nil }
func (h *loadGenHandler) OnOrigin(frame.FrameHeader, []string) error                      { return nil }

var _ frame.Handler = (*loadGenHandler)(nil)

// lgWriteCounter tallies Write calls and bytes on the transport. One Write on
// h2c is one syscall; on TLS it is additionally one record. Both are what the
// send path is trying to spend less of, so this is a first-class metric here.
type lgWriteCounter struct {
	writes atomic.Int64
	bytes  atomic.Int64
}

type lgCountingConn struct {
	net.Conn
	c *lgWriteCounter
}

func (c *lgCountingConn) Write(p []byte) (int, error) {
	c.c.writes.Add(1)
	c.c.bytes.Add(int64(len(p)))
	return c.Conn.Write(p)
}

type lgCountingDialer struct {
	inner Dialer
	c     *lgWriteCounter
}

func (d *lgCountingDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	nc, err := d.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return &lgCountingConn{Conn: nc, c: d.c}, nil
}

// AssertsALPN mirrors the wrapped PlaintextDialer, which asserts nothing.
func (d *lgCountingDialer) AssertsALPN() string { return "" }

// dialLoadGenPeer opens one h2c connection to the peer. wc may be nil.
func dialLoadGenPeer(tb testing.TB, p *loadGenPeer, wc *lgWriteCounter) *Conn {
	tb.Helper()
	return dialLoadGenPeerOpts(tb, p, wc, false)
}

func dialLoadGenPeerOpts(tb testing.TB, p *loadGenPeer, wc *lgWriteCounter, groupCommit bool) *Conn {
	tb.Helper()
	var d Dialer = &PlaintextDialer{}
	if wc != nil {
		d = &lgCountingDialer{inner: d, c: wc}
	}
	c, err := Dial(context.Background(), p.addr(), ConnOptions{
		Dialer:      d,
		GroupCommit: groupCommit,
		// Our own gate on concurrent streams is min(what we advertise, what the
		// peer advertises); the peer advertises nothing, so this value is what
		// caps the benchmark's in-flight streams. The default 100 would turn a
		// 256-worker run into an ErrTooManyStreams storm.
		Settings: AdvertisedSettings{MaxConcurrentStreams: 16384},
	})
	if err != nil {
		tb.Fatalf("Dial: %v", err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// lgRequestFields is the field set a gRPC-shaped generator sends. Seven fields,
// which is what makes HPACK encode cost worth attributing at all.
func lgRequestFields(authority string) []hpack.HeaderField {
	return []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":scheme"), Value: []byte("http")},
		{Name: []byte(":authority"), Value: []byte(authority)},
		{Name: []byte(":path"), Value: []byte("/bench.Svc/Echo")},
		{Name: []byte("content-type"), Value: []byte("application/grpc")},
		{Name: []byte("te"), Value: []byte("trailers")},
		{Name: []byte("grpc-timeout"), Value: []byte("1000m")},
	}
}

// benchLoadGen runs workers concurrent request loops over one connection.
// Every response is drained and discarded, which is the generator's shape: it
// wants the completion signal and the status, never the body.
func benchLoadGen(b *testing.B, workers, reqSize, respSize int) {
	b.Helper()
	benchLoadGenOpts(b, workers, reqSize, respSize, false)
}

func benchLoadGenOpts(b *testing.B, workers, reqSize, respSize int, groupCommit bool) {
	b.Helper()
	p := newLoadGenPeer(b, respSize)
	wc := &lgWriteCounter{}
	c := dialLoadGenPeerOpts(b, p, wc, groupCommit)
	ctx := context.Background()
	hdrs := lgRequestFields("bench.local")
	body := make([]byte, reqSize)

	one := func() error {
		s, err := c.NewStream(ctx)
		if err != nil {
			return err
		}
		if err := s.SendHeaders(ctx, hdrs, len(body) == 0); err != nil {
			return err
		}
		if len(body) > 0 {
			if err := s.SendData(ctx, body, true); err != nil {
				return err
			}
		}
		for {
			ev, err := s.Recv(ctx)
			if err != nil {
				return err
			}
			// conn hands ownership of the pooled header and payload buffers to the
			// caller via Slab/DataSlab and does NOT reclaim them itself; the client
			// package returns them in Response.Reset / sr.Close. A generator written
			// straight against conn has to do the same, and one that forgets pays a
			// fresh 16 KiB allocation per DATA frame. Returning the POINTER, not the
			// slice, is what keeps it off the heap.
			if ev.Slab != nil {
				headerSlabPool.Put(ev.Slab)
			}
			if ev.DataSlab != nil {
				dataBufPool.Put(ev.DataSlab)
			}
			if ev.EndStream {
				break
			}
		}
		return s.Close()
	}

	// Warm up the stream pool, the HPACK dynamic table and the slab pools, then
	// zero the transport counters so warm-up writes are not charged to b.N.
	if err := one(); err != nil {
		b.Fatalf("warmup: %v", err)
	}
	wc.writes.Store(0)
	wc.bytes.Store(0)

	remaining := int64(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Fatalf is illegal off the test goroutine; Errorf + return is not.
			for atomic.AddInt64(&remaining, -1) >= 0 {
				if err := one(); err != nil {
					b.Errorf("request: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()

	n := float64(b.N)
	b.ReportMetric(float64(wc.writes.Load())/n, "writes/req")
	b.ReportMetric(float64(wc.bytes.Load())/n, "wirebytes/req")
}

// --- Macro benchmarks: the R0 baseline itself.
//
// The C-suffix is the number of concurrent request loops on the single
// connection. C1 isolates per-request cost with no wmu contention; C64 and C256
// are the regime a generator actually runs in, where wmu serialization and
// scheduler cost start to dominate.

func BenchmarkLoadGen_Unary_C1(b *testing.B)   { benchLoadGen(b, 1, 64, 64) }
func BenchmarkLoadGen_Unary_C16(b *testing.B)  { benchLoadGen(b, 16, 64, 64) }
func BenchmarkLoadGen_Unary_C64(b *testing.B)  { benchLoadGen(b, 64, 64, 64) }
func BenchmarkLoadGen_Unary_C256(b *testing.B) { benchLoadGen(b, 256, 64, 64) }

// Headers-only: no request body and no response body, so writes/req isolates
// what the HEADERS path alone costs. The gap between this and Unary_C64 is what
// a one-shot HEADERS+DATA send (backlog item 1) and R1 batching can reclaim.
func BenchmarkLoadGen_HeadersOnly_C64(b *testing.B) { benchLoadGen(b, 64, 0, 0) }

// A 1 KiB response is where the receive-side slab copy and event-channel hop
// start to be measurable against the rest of the request.
func BenchmarkLoadGen_Resp1KB_C64(b *testing.B) { benchLoadGen(b, 64, 64, 1024) }

// GroupCommit is the write batching the connection already has. It defers a
// HEADERS flush when another writer is queued on wmu, so the next holder batches
// both into one transport write. It is the closest existing thing to R1, and the
// writes/req delta against BenchmarkLoadGen_Unary_C64 is direct evidence for how
// much an explicit, caller-driven batch could take off — with the caveat from
// closed issue #360 that extending the same *waiting* mechanism to DATA cost
// p50 +81%. R1 must batch without waiting.
func BenchmarkLoadGen_Unary_C64_GroupCommit(b *testing.B) {
	benchLoadGenOpts(b, 64, 64, 64, true)
}

func BenchmarkLoadGen_HeadersOnly_C64_GroupCommit(b *testing.B) {
	benchLoadGenOpts(b, 64, 0, 0, true)
}

// --- Component benchmarks: hard numbers for each profile bucket.
//
// These touch no socket, so unlike the macro benchmarks their ns/op is
// meaningful. They are NOT a substitute for the CPU profile — a component that
// costs 200 ns in isolation may cost more in situ (cache pressure, lock
// contention) or less (branch prediction). Use them to sanity-check the profile,
// not to derive percentages.

// BenchmarkLoadGen_Component_HPACKEncode is the per-request encode a generator
// would skip entirely with a cached stateless block (R3). The encoder is warm:
// the dynamic table already holds every field, which is the steady state and the
// cheapest the encode ever gets.
func BenchmarkLoadGen_Component_HPACKEncode(b *testing.B) {
	enc := hpack.NewEncoder()
	hdrs := lgRequestFields("bench.local")
	buf := make([]byte, 0, 512)
	for i := 0; i < 8; i++ { // warm the dynamic table
		_ = enc.EncodeBlock(buf[:0], hdrs)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block := enc.EncodeBlock(buf[:0], hdrs)
		if len(block) == 0 {
			b.Fatal("empty block")
		}
	}
}

// BenchmarkLoadGen_Component_HPACKEncodeStateless is the R3 alternative: what a
// cached static-only block costs to produce once. Compare its OUTPUT SIZE
// against the warm dynamic encode above — that difference, times the request
// rate, is the bandwidth R3 trades away for the encode it removes.
func BenchmarkLoadGen_Component_HPACKEncodeStateless(b *testing.B) {
	hdrs := lgRequestFields("bench.local")
	warm := hpack.NewEncoder()
	for i := 0; i < 8; i++ {
		_ = warm.EncodeBlock(nil, hdrs)
	}
	dynLen := len(warm.EncodeBlock(nil, hdrs))
	statLen := len(statelessBlock(lgRequestFields("bench.local")...))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(statelessBlock(lgRequestFields("bench.local")...)) == 0 {
			b.Fatal("empty block")
		}
	}
	b.StopTimer()
	// ResetTimer DELETES user-reported metrics, so these have to be reported
	// after the loop, not before it.
	b.ReportMetric(float64(dynLen), "dyn-bytes")
	b.ReportMetric(float64(statLen), "static-bytes")
	b.ReportMetric(float64(statLen-dynLen), "extra-bytes/req")
}

// BenchmarkLoadGen_Component_DataEventHop is the per-DATA-frame receive cost
// that sink mode (R2) would remove: a pooled slab checkout, the payload copy out
// of the framer's read buffer, the channel send, the channel receive, and the
// slab return. It reproduces the sequence conn.OnData performs, minus the
// flow-control accounting and validation that a sink would still do.
//
// This is the number the R2 gate is decided on: measure it against the macro
// benchmark's per-request time, and if the DATA frames of a request account for
// under ~5% of it, R2 does not pay for its concurrency hazards.
func BenchmarkLoadGen_Component_DataEventHop(b *testing.B) {
	const payloadSize = 1024
	src := make([]byte, payloadSize)
	ch := make(chan StreamEvent, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bufPtr := dataBufPool.Get().(*[]byte)
		*bufPtr = append((*bufPtr)[:0], src...)
		ch <- StreamEvent{Type: EventData, Data: *bufPtr, DataSlab: bufPtr, EndStream: true}
		ev := <-ch
		dataBufPool.Put(ev.DataSlab)
	}
}

// BenchmarkLoadGen_Component_StreamLifecycle measures NewStream + Close on a
// stream that is NEVER SENT — the path a generator takes when it reserves a
// stream and then abandons it (rate limiter, GOAWAY, request build failure).
//
// It does NOT measure the normal lifecycle, and its allocation count is the
// point: a never-sent stream has no on-wire id, so markStreamDone never runs,
// so the appClosed/connDone rendezvous never completes and the struct is never
// returned to streamPool. Every abandoned stream therefore costs a fresh Stream
// plus its event channel. The macro benchmarks above settle at 2 allocs/op for a
// whole request, which is what the pool looks like when it does work.
func BenchmarkLoadGen_Component_StreamLifecycle(b *testing.B) {
	p := newLoadGenPeer(b, 0)
	c := dialLoadGenPeer(b, p, nil)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := c.NewStream(ctx)
		if err != nil {
			b.Fatalf("NewStream: %v", err)
		}
		// Never sent, so the stream has no on-wire identity and Close only has to
		// undo the local bookkeeping.
		if err := s.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}
