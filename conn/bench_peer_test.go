package conn

// Benchmark peer for this package's benchmarks.
//
// The peer is a minimal in-process h2c server built on the zero-alloc
// frame.Framer, for the same reason grpc/bench_test.go and
// client/bench_mock_test.go each grew one: b.ReportAllocs() counts every
// goroutine in the test binary, so an httptest + net/http2 peer charges the
// benchmark with the server's work and drowns out what this package costs.
//
// conn was the last of the three to get one, and the gap was not cosmetic.
// Measured on 2026-08-12 against the old httptest harness,
// BenchmarkConn_Roundtrip_Concurrent reported 34 allocs/op, of which an exact
// -memprofilerate=1 profile attributed only 10.2 to conn and frame together.
// The other 23.6 were net/http2, the TLS handshake, and time.Time.Format with
// strconv.formatBits — the server rendering its own Date header. A number that
// is 70% somebody else's cannot gate anything.
//
// This peer allocates nothing per request: the 204 response block is encoded
// once at construction and the same bytes are written for every stream.
//
// Read B/op and allocs/op from these benchmarks, not ns/op — the round trip
// crosses a real loopback socket, whose latency is far noisier than the
// allocation counts.

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// benchPrefaceLen is the length of the HTTP/2 client connection preface magic.
const benchPrefaceLen = 24

// encodeBenchBlock encodes fields with a throwaway encoder, forcing
// IndexWithout so nothing enters the dynamic table. The returned bytes are
// therefore valid for every response on the connection rather than only the
// first, which is what lets the peer pre-encode once and write the same slice
// for every stream.
func encodeBenchBlock(fields ...hpack.HeaderField) []byte {
	for i := range fields {
		fields[i].Indexing = hpack.IndexWithout
	}
	return hpack.NewEncoder().EncodeBlock(nil, fields)
}

// benchPeer answers every stream with an empty 204 — the same response the
// httptest handler it replaced produced with w.WriteHeader(204), so the
// benchmarks measure the same exchange they always did.
type benchPeer struct {
	ln      net.Listener
	headers []byte
}

func newBenchPeer(tb testing.TB) *benchPeer {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("benchPeer listen: %v", err)
	}
	p := &benchPeer{
		ln: ln,
		headers: encodeBenchBlock(
			hpack.HeaderField{Name: []byte(":status"), Value: []byte("204")},
		),
	}
	tb.Cleanup(func() { _ = ln.Close() })
	go p.accept()
	return p
}

func (p *benchPeer) addr() string { return p.ln.Addr().String() }

func (p *benchPeer) accept() {
	for {
		tc, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.serveConn(tc)
	}
}

func (p *benchPeer) serveConn(tc net.Conn) {
	defer func() { _ = tc.Close() }()
	// Buffered so a whole response reaches the socket in one write, as a real
	// server's would. An unbuffered peer would put extra syscalls of its own
	// into every measured round trip.
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
	h := &benchPeerHandler{peer: p, fr: fr, bw: bw}
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

// benchPeerHandler answers frames on the peer side. It holds no per-stream
// state: every response is written from fh.StreamID the moment the request
// arrives, on this connection's single reader goroutine. That is what makes it
// safe under BenchmarkConn_Roundtrip_Concurrent, where many client streams are
// open at once — the frames still arrive, and are answered, one at a time.
type benchPeerHandler struct {
	peer          *benchPeer
	fr            *frame.Framer
	bw            *bufio.Writer
	settingsSeen  bool
	clientAckSeen bool
	serving       bool
}

// OnHeaders answers with the pre-encoded 204 and ends the stream. A 204 carries
// no body, so one HEADERS frame is the whole response.
func (h *benchPeerHandler) OnHeaders(fh frame.FrameHeader, _ frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	if !h.serving {
		return nil
	}
	if err := h.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      fh.StreamID,
		BlockFragment: h.peer.headers,
		EndHeaders:    true,
		EndStream:     true,
	}); err != nil {
		return err
	}
	return h.bw.Flush()
}

// OnData refunds the receive window at both scopes and, on END_STREAM, answers.
// The benchmarks here send END_STREAM on HEADERS and so never reach this, but a
// peer that silently swallowed a request body would turn any future upload
// benchmark into a deadlock rather than a failure.
func (h *benchPeerHandler) OnData(fh frame.FrameHeader, p []byte, _ uint8) error {
	if !h.serving {
		return nil
	}
	if n := uint32(len(p)); n > 0 {
		if err := h.fr.WriteWindowUpdate(0, n); err != nil {
			return err
		}
		if err := h.fr.WriteWindowUpdate(fh.StreamID, n); err != nil {
			return err
		}
	}
	if fh.Flags&frame.FlagDataEndStream != 0 {
		if err := h.fr.WriteHeaders(frame.WriteHeadersParams{
			StreamID:      fh.StreamID,
			BlockFragment: h.peer.headers,
			EndHeaders:    true,
			EndStream:     true,
		}); err != nil {
			return err
		}
	}
	return h.bw.Flush()
}

func (h *benchPeerHandler) OnSettings(fh frame.FrameHeader, _ frame.SettingsParams) error {
	if fh.Flags&frame.FlagSettingsAck != 0 {
		h.clientAckSeen = true
	} else {
		h.settingsSeen = true
	}
	return nil
}

func (h *benchPeerHandler) OnPing(fh frame.FrameHeader, data [8]byte) error {
	if fh.Flags&frame.FlagSettingsAck != 0 { // an ACK needs no answer
		return nil
	}
	if err := h.fr.WritePing(true, data); err != nil {
		return err
	}
	return h.bw.Flush()
}

func (h *benchPeerHandler) OnPriority(frame.FrameHeader, frame.Priority) error { return nil }
func (h *benchPeerHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error { return nil }
func (h *benchPeerHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (h *benchPeerHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error {
	return nil
}
func (h *benchPeerHandler) OnWindowUpdate(frame.FrameHeader, uint32) error            { return nil }
func (h *benchPeerHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error { return nil }
func (h *benchPeerHandler) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error     { return nil }
func (h *benchPeerHandler) OnOrigin(frame.FrameHeader, []string) error                { return nil }

var _ frame.Handler = (*benchPeerHandler)(nil)

// benchDrain reads a stream to its end and returns every pooled buffer the
// events carried, the way client.Response.Reset and StreamResponse.Close do in
// production. Skipping it is not a small inaccuracy: conn hands pooled buffers
// out through StreamEvent.Slab and StreamEvent.DataSlab and never reclaims them
// itself, so a benchmark that drops them misses headerSlabPool on every request
// and reports the pool's New function as if it were the path's own cost. The
// old harness did exactly that, for 2 allocations per request.
//
// It closes the stream too. Without Close the appClosed/connDone handshake
// never completes, the stream is never recycled, and streamPool's hit rate is
// zero — worth another 5 allocations per request, and again none of them real.
func benchDrain(ctx context.Context, s StreamRef) error {
	defer func() { _ = s.Close() }()
	for {
		ev, err := s.Recv(ctx)
		if err != nil {
			return err
		}
		// Return the buffers before inspecting anything that aliases them:
		// ev.Headers points into ev.Slab, so this must be the last use of both.
		if ev.Slab != nil {
			GetHeaderSlabPool().Put(ev.Slab)
		}
		if ev.DataSlab != nil {
			GetDataBufPool().Put(ev.DataSlab)
		}
		if ev.EndStream {
			return nil
		}
	}
}
