package client_test

// BenchmarkDo_MockTransport measures client-side allocations for a round-trip
// GET request using a minimal in-process H2C peer. Unlike BenchmarkDo_WithTrailers
// (which uses httptest), the mock peer runs zero-alloc frame I/O so that
// b.ReportAllocs() reflects only client-side allocations, not server goroutines.
//
// Run with:
//
//	go test -run='^$' -bench=BenchmarkDo_MockTransport -benchmem ./client/...

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// hpackStatus200 is the HPACK encoding of ":status: 200".
// RFC 7541 §C.2.4: indexed header field with index 8 → 0x88.
var hpackStatus200Bytes = []byte{0x88}

// mockH2Peer is a minimal zero-alloc H2C server for benchmarks.
type mockH2Peer struct {
	ln net.Listener
}

func newMockH2Peer(b *testing.B) *mockH2Peer {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("mockH2Peer listen: %v", err)
	}
	p := &mockH2Peer{ln: ln}
	b.Cleanup(func() { _ = ln.Close() })
	go p.accept()
	return p
}

func (p *mockH2Peer) addr() string { return p.ln.Addr().String() }

func (p *mockH2Peer) accept() {
	for {
		tc, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.serveConn(tc)
	}
}

// http2ClientPrefaceLen is the length of the HTTP/2 connection preface magic.
const http2ClientPrefaceLen = 24

func (p *mockH2Peer) serveConn(tc net.Conn) {
	defer tc.Close()
	fr := frame.NewFramer(tc, tc)
	defer fr.Close()

	// Step 1: send server SETTINGS first.
	if err := fr.WriteSettings(frame.SettingsParams{}); err != nil {
		return
	}

	// Step 2: consume the 24-byte client connection preface magic.
	var prefaceBuf [http2ClientPrefaceLen]byte
	if _, err := io.ReadFull(tc, prefaceBuf[:]); err != nil {
		return
	}

	// Step 3: exchange SETTINGS (client sends SETTINGS; we ACK; wait for client ACK).
	h := &mockPeerHandler{fr: fr}
	for !h.settingsSeen {
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			return
		}
	}
	if err := fr.WriteSettingsAck(); err != nil {
		return
	}
	for !h.clientAckSeen {
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			return
		}
	}

	// Step 4: serve requests.
	h.serving = true
	for {
		if _, err := fr.ReadFrame(context.Background(), h); err != nil {
			return
		}
	}
}

// mockPeerHandler processes frames on the mock H2C peer.
type mockPeerHandler struct {
	fr           *frame.Framer
	settingsSeen bool
	clientAckSeen bool
	serving       bool
	// pendingStreamID tracks a stream that received HEADERS without END_STREAM
	// (body follows). We respond after the final DATA or HEADERS with END_STREAM.
	pendingStreamID uint32
}

func (h *mockPeerHandler) respond(streamID uint32) error {
	return h.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      streamID,
		BlockFragment: hpackStatus200Bytes,
		EndStream:     true,
		EndHeaders:    true,
	})
}

func (h *mockPeerHandler) OnHeaders(fh frame.FrameHeader, _ frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	if !h.serving {
		return nil
	}
	if fh.Flags&frame.FlagHeadersEndStream != 0 {
		// GET-style or trailer HEADERS — respond immediately.
		return h.respond(fh.StreamID)
	}
	// POST-style initial HEADERS without END_STREAM — body will follow.
	h.pendingStreamID = fh.StreamID
	return nil
}

func (h *mockPeerHandler) OnData(fh frame.FrameHeader, _ []byte, _ uint8) error {
	if !h.serving {
		return nil
	}
	if fh.Flags&frame.FlagDataEndStream != 0 {
		return h.respond(fh.StreamID)
	}
	return nil
}

func (h *mockPeerHandler) OnSettings(fh frame.FrameHeader, _ frame.SettingsParams) error {
	if fh.Flags&frame.FlagSettingsAck != 0 {
		h.clientAckSeen = true
	} else {
		h.settingsSeen = true
	}
	return nil
}

func (h *mockPeerHandler) OnPriority(frame.FrameHeader, frame.Priority) error { return nil }
func (h *mockPeerHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error  { return nil }
func (h *mockPeerHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (h *mockPeerHandler) OnPing(fh frame.FrameHeader, data [8]byte) error {
	if fh.Flags&frame.FlagSettingsAck == 0 { // non-ACK PING: echo it
		return h.fr.WritePing(true, data)
	}
	return nil
}
func (h *mockPeerHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error {
	return nil
}
func (h *mockPeerHandler) OnWindowUpdate(frame.FrameHeader, uint32) error      { return nil }
func (h *mockPeerHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error { return nil }

var _ frame.Handler = (*mockPeerHandler)(nil)

// BenchmarkDo_MockTransport benchmarks a GET round-trip against a minimal
// in-process H2C peer. Server-side allocations are negligible (zero-alloc
// frame codec); b.ReportAllocs() reflects client-side allocations only.
func BenchmarkDo_MockTransport(b *testing.B) {
	peer := newMockH2Peer(b)

	c, err := client.NewClient(client.ClientOptions{
		Addr:          peer.addr(),
		DefaultScheme: "http",
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.PlaintextDialer{},
		},
	})
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })

	req := &client.Request{Method: "GET", Path: "/"}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var res client.Response
		if err := c.Do(ctx, req, &res); err != nil {
			b.Fatalf("Do: %v", err)
		}
		res.Reset()
	}
}
