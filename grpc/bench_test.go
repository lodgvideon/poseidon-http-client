package grpc

// Benchmark harness for the gRPC layer.
//
// The peer is a minimal in-process H2C server built on the zero-alloc
// frame.Framer rather than net/http2, for the same reason
// client/bench_mock_test.go uses one: b.ReportAllocs() counts every goroutine
// in the test binary, so an httptest peer charges the benchmark ~27 allocs/op
// of server-side work and drowns out what this package actually costs. This
// peer allocates nothing per request — the response header and trailer blocks
// are pre-encoded once, and the echo writes the client's own DATA payload back
// verbatim.
//
// Read B/op and allocs/op from these, not ns/op. The round trip goes through a
// real loopback socket, and six identical runs of BenchmarkGRPC_Unary_1KB at
// benchtime=1000x spread from 126 to 258 microseconds — a 2x swing with nothing
// changed. Allocation counts over the same runs did not move by one. A latency
// claim needs a quieter harness than this; an allocation claim does not.
// BenchmarkGRPC_BuildHeaders is the exception: it touches no socket, so its
// ns/op is meaningful.
//
// Run with:
//
//	go test -run='^$' -bench=BenchmarkGRPC -benchmem ./grpc/...

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// benchPrefaceLen is the length of the HTTP/2 client connection preface magic.
const benchPrefaceLen = 24

// encodeStatelessBlock encodes fields with a throwaway encoder, forcing
// IndexWithout so nothing enters the dynamic table. The returned bytes are
// therefore valid for every response on the connection, not just the first —
// which is what lets the peer pre-encode once and write the same slice for
// every RPC.
func encodeStatelessBlock(fields ...hpack.HeaderField) []byte {
	for i := range fields {
		fields[i].Indexing = hpack.IndexWithout
	}
	return hpack.NewEncoder().EncodeBlock(nil, fields)
}

// mockGRPCPeer is a zero-alloc h2c peer that answers every stream with an echo:
// response HEADERS, the client's own message bytes back as DATA, then trailers
// carrying grpc-status: 0.
type mockGRPCPeer struct {
	ln       net.Listener
	headers  []byte
	trailers []byte
	// dataFrames counts every DATA frame the client sent, across all streams
	// and connections. It is what tells "the message and the half-close shared
	// a frame" apart from "they did not" — a distinction the byte counts cannot
	// make, since the extra frame carries no payload.
	dataFrames atomic.Int64
}

func newMockGRPCPeer(tb testing.TB) *mockGRPCPeer {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("mockGRPCPeer listen: %v", err)
	}
	p := &mockGRPCPeer{
		ln: ln,
		headers: encodeStatelessBlock(
			hpack.HeaderField{Name: []byte(":status"), Value: []byte("200")},
			hpack.HeaderField{Name: []byte("content-type"), Value: []byte("application/grpc")},
		),
		trailers: encodeStatelessBlock(
			hpack.HeaderField{Name: []byte("grpc-status"), Value: []byte("0")},
		),
	}
	tb.Cleanup(func() { _ = ln.Close() })
	go p.accept()
	return p
}

func (p *mockGRPCPeer) addr() string { return p.ln.Addr().String() }

func (p *mockGRPCPeer) accept() {
	for {
		tc, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.serveConn(tc)
	}
}

func (p *mockGRPCPeer) serveConn(tc net.Conn) {
	defer func() { _ = tc.Close() }()
	// Buffered so a whole response (HEADERS + DATA + trailers) reaches the
	// socket in one write, as a real server's would. An unbuffered peer would
	// put three syscalls of its own into every measured round trip.
	bw := bufio.NewWriterSize(tc, 16*1024)
	fr := frame.NewFramer(bw, bufio.NewReaderSize(tc, 16*1024)) // writer first
	defer fr.Close()

	if err := fr.WriteSettings(frame.SettingsParams{}); err != nil {
		return
	}
	if err := bw.Flush(); err != nil {
		return
	}
	var preface [benchPrefaceLen]byte
	if _, err := io.ReadFull(tc, preface[:]); err != nil {
		return
	}
	h := &mockGRPCHandler{peer: p, fr: fr, bw: bw}
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

// mockGRPCHandler answers frames on the peer side. It holds no per-stream state
// beyond the fact that response headers have been sent, which is enough because
// the benchmarks drive one stream at a time.
type mockGRPCHandler struct {
	peer          *mockGRPCPeer
	fr            *frame.Framer
	bw            *bufio.Writer
	settingsSeen  bool
	clientAckSeen bool
	serving       bool
}

// sendHeaders emits the response header block. A gRPC server is free to send it
// before reading the request body, and doing so here means every later DATA
// chunk can be echoed the moment it arrives.
func (h *mockGRPCHandler) sendHeaders(streamID uint32) error {
	return h.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      streamID,
		BlockFragment: h.peer.headers,
		EndHeaders:    true,
	})
}

// sendTrailers emits the trailer block that carries grpc-status and ends the
// stream, then flushes the whole response to the socket.
func (h *mockGRPCHandler) sendTrailers(streamID uint32) error {
	if err := h.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      streamID,
		BlockFragment: h.peer.trailers,
		EndHeaders:    true,
		EndStream:     true,
	}); err != nil {
		return err
	}
	return h.bw.Flush()
}

func (h *mockGRPCHandler) OnHeaders(fh frame.FrameHeader, _ frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	if !h.serving {
		return nil
	}
	if err := h.sendHeaders(fh.StreamID); err != nil {
		return err
	}
	if fh.Flags&frame.FlagHeadersEndStream != 0 {
		return h.sendTrailers(fh.StreamID)
	}
	return nil
}

// refund returns n bytes of receive window to the client at both scopes. A
// request larger than the initial 65535-byte window blocks in
// acquireSendCredits until this arrives, and the peer is the only thing that
// can unblock it — so the refund is immediate and unbatched rather than
// thresholded the way conn's own receive path does it.
func (h *mockGRPCHandler) refund(streamID uint32, n uint32) error {
	if n == 0 {
		return nil
	}
	if err := h.fr.WriteWindowUpdate(0, n); err != nil {
		return err
	}
	return h.fr.WriteWindowUpdate(streamID, n)
}

func (h *mockGRPCHandler) OnData(fh frame.FrameHeader, p []byte, _ uint8) error {
	if !h.serving {
		return nil
	}
	h.peer.dataFrames.Add(1)
	// The client's DATA payload is already a well-formed length-prefixed gRPC
	// message (or a fragment of one), so echoing the bytes verbatim reproduces
	// the request as the response with nothing allocated and nothing parsed.
	//
	// The echo ignores the peer's own send window, which a conforming server
	// could not do. It is safe only because conn refunds from its reader
	// goroutine as each frame is accounted, so the client's receive window is
	// replenished between frames and never goes negative — see
	// Conn.onDataReceived. Do not copy this into anything that has to talk to a
	// real client.
	if len(p) > 0 {
		if err := h.fr.WriteData(fh.StreamID, false, p); err != nil {
			return err
		}
		if err := h.refund(fh.StreamID, uint32(len(p))); err != nil {
			return err
		}
	}
	if fh.Flags&frame.FlagDataEndStream != 0 {
		return h.sendTrailers(fh.StreamID)
	}
	// Not the last frame: flush, or a client blocked on the credit this refund
	// carries would wait for a write that only the trailers would trigger.
	return h.bw.Flush()
}

func (h *mockGRPCHandler) OnSettings(fh frame.FrameHeader, _ frame.SettingsParams) error {
	if fh.Flags&frame.FlagSettingsAck != 0 {
		h.clientAckSeen = true
	} else {
		h.settingsSeen = true
	}
	return nil
}

func (h *mockGRPCHandler) OnPing(fh frame.FrameHeader, data [8]byte) error {
	if fh.Flags&frame.FlagSettingsAck != 0 { // an ACK needs no answer
		return nil
	}
	if err := h.fr.WritePing(true, data); err != nil {
		return err
	}
	return h.bw.Flush()
}

func (h *mockGRPCHandler) OnPriority(frame.FrameHeader, frame.Priority) error { return nil }
func (h *mockGRPCHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error { return nil }
func (h *mockGRPCHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (h *mockGRPCHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error {
	return nil
}
func (h *mockGRPCHandler) OnWindowUpdate(frame.FrameHeader, uint32) error            { return nil }
func (h *mockGRPCHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error { return nil }
func (h *mockGRPCHandler) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error     { return nil }
func (h *mockGRPCHandler) OnOrigin(frame.FrameHeader, []string) error                { return nil }

var _ frame.Handler = (*mockGRPCHandler)(nil)

// writeCounter tallies Write calls and bytes on a transport. One Write on a
// plaintext transport is one syscall; on a TLS transport it is additionally one
// TLS record, with its own ~22 bytes of header and AEAD tag. Both are what the
// send path is trying to spend less of, so the count is a first-class metric
// here rather than a debugging aid.
type writeCounter struct {
	writes atomic.Int64
	bytes  atomic.Int64
}

type countingConn struct {
	net.Conn
	c *writeCounter
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.c.writes.Add(1)
	c.c.bytes.Add(int64(len(p)))
	return c.Conn.Write(p)
}

// countingDialer wraps a Dialer so every connection it returns reports its
// writes into c.
type countingDialer struct {
	inner conn.Dialer
	c     *writeCounter
}

func (d *countingDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	nc, err := d.inner.Dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: nc, c: d.c}, nil
}

// AssertsALPN mirrors the wrapped PlaintextDialer, which asserts nothing.
func (d *countingDialer) AssertsALPN() string { return "" }

// dialMockPeer opens a gRPC client against the in-process peer. wc may be nil
// when the benchmark does not care about transport writes.
func dialMockPeer(tb testing.TB, p *mockGRPCPeer, wc *writeCounter) *ClientConn {
	tb.Helper()
	var d conn.Dialer = &conn.PlaintextDialer{}
	if wc != nil {
		d = &countingDialer{inner: d, c: wc}
	}
	cc, err := Dial(context.Background(), p.addr(), Options{
		Conn:      conn.ConnOptions{Dialer: d},
		Scheme:    "http",
		Authority: "bench.local",
	})
	if err != nil {
		tb.Fatalf("Dial: %v", err)
	}
	tb.Cleanup(func() { _ = cc.Close() })
	return cc
}

// benchUnary runs size-byte echo RPCs against the mock peer.
func benchUnary(b *testing.B, size int) {
	b.Helper()
	cc := dialMockPeer(b, newMockGRPCPeer(b), nil)
	ctx := context.Background()
	msg := make([]byte, size)
	if _, err := cc.Invoke(ctx, "/bench.Svc/Echo", msg, nil); err != nil {
		b.Fatalf("warmup: %v", err)
	}
	b.SetBytes(int64(size))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := cc.Invoke(ctx, "/bench.Svc/Echo", msg, nil); err != nil {
			b.Fatalf("Invoke: %v", err)
		}
	}
}

// BenchmarkGRPC_Unary_8B is the small-message case: latency and per-RPC
// overhead dominate, and the payload copies do not.
func BenchmarkGRPC_Unary_8B(b *testing.B) { benchUnary(b, 8) }

// BenchmarkGRPC_Unary_1KB is a typical RPC payload.
func BenchmarkGRPC_Unary_1KB(b *testing.B) { benchUnary(b, 1024) }

// BenchmarkGRPC_Unary_64KB spans several DATA frames, so it charges the
// per-frame receive path and the message reassembly copies rather than the
// fixed per-RPC cost.
func BenchmarkGRPC_Unary_64KB(b *testing.B) { benchUnary(b, 64*1024) }

// BenchmarkGRPC_ServerStream_64x1KB measures the receive path with one HEADERS
// and one trailer block amortised over 64 messages, which is where the
// reassembly and per-message copies show up without the per-RPC setup.
func BenchmarkGRPC_ServerStream_64x1KB(b *testing.B) {
	const msgs = 64
	cc := dialMockPeer(b, newMockGRPCPeer(b), nil)
	ctx := context.Background()
	// The peer echoes DATA verbatim, so one request carrying msgs messages
	// comes back as msgs messages.
	var req []byte
	for i := 0; i < msgs; i++ {
		var err error
		if req, err = AppendMessage(req, make([]byte, 1024)); err != nil {
			b.Fatalf("AppendMessage: %v", err)
		}
	}
	run := func() {
		s, err := cc.NewStream(ctx, "/bench.Svc/EchoStream", nil)
		if err != nil {
			b.Fatalf("NewStream: %v", err)
		}
		defer func() { _ = s.Close() }()
		// req already carries its own length prefixes, so it is written as raw
		// DATA rather than through Send (which would prefix it again).
		if err := s.s.SendData(ctx, req, true); err != nil {
			b.Fatalf("SendData: %v", err)
		}
		for i := 0; i < msgs; i++ {
			if _, err := s.Recv(ctx); err != nil {
				b.Fatalf("Recv %d: %v", i, err)
			}
		}
	}
	run()
	b.SetBytes(msgs * 1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		run()
	}
}

// BenchmarkGRPC_BuildHeaders isolates the request header block construction,
// which every RPC pays before any I/O happens.
func BenchmarkGRPC_BuildHeaders(b *testing.B) {
	cc := newClientConn(nil, Options{Authority: "bench.local"}.defaulted(), false)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sc := headerScratchPool.Get().(*headerScratch)
		sinkHeaders = cc.buildHeaders(ctx, "/bench.Svc/Echo", nil, nil, sc)
		putHeaderScratch(sc)
	}
}

// sinkHeaders keeps BenchmarkGRPC_BuildHeaders' result live so the call is not
// optimised away.
var sinkHeaders []conn.HeaderField
