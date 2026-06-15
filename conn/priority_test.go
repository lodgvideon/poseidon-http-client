package conn

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// capturePriorityHandler records every PRIORITY frame seen on the wire.
type capturePriorityHandler struct {
	mu         sync.Mutex
	priorities []frame.Priority
	streamIDs  []uint32
}

func (c *capturePriorityHandler) OnData(_ frame.FrameHeader, _ []byte, _ uint8) error  { return nil }
func (c *capturePriorityHandler) OnHeaders(_ frame.FrameHeader, _ frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	return nil
}
func (c *capturePriorityHandler) OnPriority(fh frame.FrameHeader, p frame.Priority) error {
	c.mu.Lock()
	c.priorities = append(c.priorities, p)
	c.streamIDs = append(c.streamIDs, fh.StreamID)
	c.mu.Unlock()
	return nil
}
func (c *capturePriorityHandler) OnContinuation(_ frame.FrameHeader, _ frame.HeaderBlock) error {
	return nil
}
func (c *capturePriorityHandler) OnSettings(_ frame.FrameHeader, _ frame.SettingsParams) error { return nil }
func (c *capturePriorityHandler) OnSettingsAck(_ frame.FrameHeader) error                     { return nil }
func (c *capturePriorityHandler) OnPing(_ frame.FrameHeader, _ [8]byte) error                 { return nil }
func (c *capturePriorityHandler) OnRSTStream(_ frame.FrameHeader, _ frame.ErrCode) error      { return nil }
func (c *capturePriorityHandler) OnGoAway(_ frame.FrameHeader, _ uint32, _ frame.ErrCode, _ []byte) error {
	return nil
}
func (c *capturePriorityHandler) OnWindowUpdate(_ frame.FrameHeader, _ uint32) error { return nil }
func (c *capturePriorityHandler) OnAltSvc(_ frame.FrameHeader, _ []frame.AltSvcEntry) error {
	return nil
}
func (c *capturePriorityHandler) OnOrigin(_ frame.FrameHeader, _ []string) error { return nil }
func (c *capturePriorityHandler) OnPushPromise(_ frame.FrameHeader, _ uint32, _ frame.HeaderBlock, _ uint8) error {
	return nil
}

// noopHandler discards all frames.
type noopHandler struct{}

func (noopHandler) OnData(_ frame.FrameHeader, _ []byte, _ uint8) error  { return nil }
func (noopHandler) OnHeaders(_ frame.FrameHeader, _ frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	return nil
}
func (noopHandler) OnPriority(_ frame.FrameHeader, _ frame.Priority) error { return nil }
func (noopHandler) OnContinuation(_ frame.FrameHeader, _ frame.HeaderBlock) error {
	return nil
}
func (noopHandler) OnSettings(_ frame.FrameHeader, _ frame.SettingsParams) error { return nil }
func (noopHandler) OnSettingsAck(_ frame.FrameHeader) error                     { return nil }
func (noopHandler) OnPing(_ frame.FrameHeader, _ [8]byte) error                 { return nil }
func (noopHandler) OnRSTStream(_ frame.FrameHeader, _ frame.ErrCode) error      { return nil }
func (noopHandler) OnGoAway(_ frame.FrameHeader, _ uint32, _ frame.ErrCode, _ []byte) error {
	return nil
}
func (noopHandler) OnWindowUpdate(_ frame.FrameHeader, _ uint32) error { return nil }
func (noopHandler) OnAltSvc(_ frame.FrameHeader, _ []frame.AltSvcEntry) error {
	return nil
}
func (noopHandler) OnOrigin(_ frame.FrameHeader, _ []string) error { return nil }
func (noopHandler) OnPushPromise(_ frame.FrameHeader, _ uint32, _ frame.HeaderBlock, _ uint8) error {
	return nil
}

// http2ClientPreface is the 24-byte client connection preface string.
const http2ClientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// TestFramer_WriteAndReadPriority verifies the round trip of a PRIORITY
// frame on a net.Pipe pair, with the receiver parsing payload correctly.
func TestFramer_WriteAndReadPriority(t *testing.T) {
	srv, cli := net.Pipe()
	defer cli.Close()

	capture := &capturePriorityHandler{}
	done := make(chan struct{})

	// Server side: read preface then read PRIORITY frame.
	go func() {
		defer close(done)
		buf := make([]byte, len(http2ClientPreface))
		if _, err := readN(srv, buf); err != nil {
			t.Logf("server preface: %v", err)
			return
		}
		fr := frame.NewFramer(srv, srv)
		// Read all incoming frames until peer closes.
		for {
			if _, err := fr.ReadFrame(context.Background(), capture); err != nil {
				return
			}
		}
	}()

	// Client side: write preface, settings, then PRIORITY.
	fr := frame.NewFramer(cli, cli)
	if _, err := cli.Write([]byte(http2ClientPreface)); err != nil {
		t.Fatalf("write preface: %v", err)
	}
	if err := fr.WriteSettings(frame.SettingsParams{}); err != nil {
		t.Fatalf("WriteSettings: %v", err)
	}
	if err := fr.WritePriority(1, frame.Priority{StreamDep: 13, Exclusive: false, Weight: 200}); err != nil {
		t.Fatalf("WritePriority: %v", err)
	}
	// Give the server a moment to read.
	_ = srv.Close()
	<-done

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.priorities) != 1 {
		t.Fatalf("priorities captured = %d, want 1", len(capture.priorities))
	}
	got := capture.priorities[0]
	if got.StreamDep != 13 {
		t.Errorf("StreamDep = %d, want 13", got.StreamDep)
	}
	if got.Weight != 200 {
		t.Errorf("Weight = %d, want 200", got.Weight)
	}
	if capture.streamIDs[0] != 1 {
		t.Errorf("streamID = %d, want 1", capture.streamIDs[0])
	}
}

// TestFramer_DispatchPriority_ExclusiveFlag verifies the exclusive bit.
func TestFramer_DispatchPriority_ExclusiveFlag(t *testing.T) {
	srv, cli := net.Pipe()
	defer cli.Close()

	capture := &capturePriorityHandler{}
	done := make(chan struct{})

	go func() {
		defer close(done)
		buf := make([]byte, len(http2ClientPreface))
		_, _ = readN(srv, buf)
		fr := frame.NewFramer(srv, srv)
		for {
			if _, err := fr.ReadFrame(context.Background(), capture); err != nil {
				return
			}
		}
	}()

	// Build PRIORITY payload with E=1 in high bit.
	payload := make([]byte, 5)
	binary.BigEndian.PutUint32(payload[0:4], 7|0x80000000)
	payload[4] = 32

	fr := frame.NewFramer(cli, cli)
	_, _ = cli.Write([]byte(http2ClientPreface))
	_ = fr.WriteSettings(frame.SettingsParams{})
	hdr := []byte{
		0, 0, 5,
		byte(frame.FramePriority), 0, 0, 0, 0, 1,
	}
	_, _ = cli.Write(hdr)
	_, _ = cli.Write(payload)
	_ = srv.Close()
	<-done

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.priorities) != 1 {
		t.Fatalf("priorities = %d, want 1", len(capture.priorities))
	}
	got := capture.priorities[0]
	if !got.Exclusive {
		t.Error("Exclusive = false, want true")
	}
	if got.StreamDep != 7 {
		t.Errorf("StreamDep = %d, want 7 (masked)", got.StreamDep)
	}
	if got.Weight != 32 {
		t.Errorf("Weight = %d, want 32", got.Weight)
	}
}

// TestFramer_DispatchPriority_WrongLength verifies the framer rejects
// a PRIORITY frame whose length is not 5 (RFC 7540 §6.3).
func TestFramer_DispatchPriority_WrongLength(t *testing.T) {
	srv, cli := net.Pipe()
	defer cli.Close()

	done := make(chan struct{})
	var readErr error
	go func() {
		defer close(done)
		buf := make([]byte, len(http2ClientPreface))
		_, _ = readN(srv, buf)
		fr := frame.NewFramer(srv, srv)
		_, readErr = fr.ReadFrame(context.Background(), noopHandler{})
	}()

	// Write a PRIORITY frame with length=4 (wrong).
	hdr := []byte{
		0, 0, 4, // length = 4
		byte(frame.FramePriority), 0, 0, 0, 0, 1,
	}
	_, _ = cli.Write(hdr)
	_, _ = cli.Write([]byte{1, 2, 3, 4})
	_ = cli.Close() // unblock server's ReadFrame

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("server didn't finish")
	}
	if readErr == nil {
		t.Error("expected error for wrong-length PRIORITY")
	}
}

// TestConn_HeadWithPriority_H2Server verifies that a real h2 server
// receives a request whose HEADERS frame carries the PRIORITY field.
// Go's net/http2 server doesn't expose priority, so we just verify
// the request completes successfully when the client embeds PRIORITY.
func TestConn_HeadWithPriority_H2Server(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	// Open a raw TLS conn to the h2 server.
	rawConn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer rawConn.Close()

	tlsConn := tls.Client(rawConn, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
		ServerName:         "example.com",
	})
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}
	defer tlsConn.Close()

	// HTTP/2 handshake.
	if _, err := tlsConn.Write([]byte(http2ClientPreface)); err != nil {
		t.Fatalf("preface: %v", err)
	}
	fr := frame.NewFramer(tlsConn, tlsConn)
	if err := fr.WriteSettings(frame.SettingsParams{}); err != nil {
		t.Fatalf("WriteSettings: %v", err)
	}

	// Read server SETTINGS, then ACK.
	if _, err := fr.ReadFrame(context.Background(), noopHandler{}); err != nil {
		t.Fatalf("read server settings: %v", err)
	}
	if err := fr.WriteSettingsAck(); err != nil {
		t.Fatalf("WriteSettingsAck: %v", err)
	}

	// Send HEADERS with PRIORITY embedded.
	enc := hpack.NewEncoder()
	hdrs := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	block := enc.EncodeBlock(nil, hdrs)
	prio := &frame.Priority{StreamDep: 0, Exclusive: false, Weight: 200}
	if err := fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      1,
		BlockFragment: block,
		EndHeaders:    true,
		EndStream:     true,
		Priority:      prio,
	}); err != nil {
		t.Fatalf("WriteHeaders: %v", err)
	}

	// Use a status-capturing handler to read response.
	var gotStatus int
	statusHandler := &statusCaptureHandler{status: &gotStatus}
	for i := 0; i < 5; i++ {
		fh, err := fr.ReadFrame(context.Background(), statusHandler)
		if err != nil {
			t.Fatalf("ReadFrame[%d]: %v", i, err)
		}
		if statusHandler.done {
			break
		}
		_ = fh
	}
	if gotStatus != 200 {
		t.Errorf("status = %d, want 200", gotStatus)
	}
}

// statusCaptureHandler is a frame.Handler that records the :status
// value from response HEADERS.
type statusCaptureHandler struct {
	status *int
	done   bool
}

func (s *statusCaptureHandler) OnData(_ frame.FrameHeader, _ []byte, _ uint8) error { return nil }
func (s *statusCaptureHandler) OnHeaders(fh frame.FrameHeader, hb frame.HeaderBlock, _ *frame.Priority, _ uint8) error {
	dec := hpack.NewDecoder()
	visit := func(h hpack.HeaderField) error {
		if string(h.Name) == ":status" {
			*s.status = atoi(string(h.Value))
			s.done = true
		}
		return nil
	}
	_ = dec.DecodeBlock(hb, visit)
	return nil
}
func (s *statusCaptureHandler) OnPriority(_ frame.FrameHeader, _ frame.Priority) error { return nil }
func (s *statusCaptureHandler) OnContinuation(_ frame.FrameHeader, _ frame.HeaderBlock) error {
	return nil
}
func (s *statusCaptureHandler) OnSettings(_ frame.FrameHeader, _ frame.SettingsParams) error { return nil }
func (s *statusCaptureHandler) OnSettingsAck(_ frame.FrameHeader) error                     { return nil }
func (s *statusCaptureHandler) OnPing(_ frame.FrameHeader, _ [8]byte) error                 { return nil }
func (s *statusCaptureHandler) OnRSTStream(_ frame.FrameHeader, _ frame.ErrCode) error      { return nil }
func (s *statusCaptureHandler) OnGoAway(_ frame.FrameHeader, _ uint32, _ frame.ErrCode, _ []byte) error {
	return nil
}
func (s *statusCaptureHandler) OnWindowUpdate(_ frame.FrameHeader, _ uint32) error { return nil }
func (s *statusCaptureHandler) OnAltSvc(_ frame.FrameHeader, _ []frame.AltSvcEntry) error {
	return nil
}
func (s *statusCaptureHandler) OnOrigin(_ frame.FrameHeader, _ []string) error { return nil }
func (s *statusCaptureHandler) OnPushPromise(_ frame.FrameHeader, _ uint32, _ frame.HeaderBlock, _ uint8) error {
	return nil
}

// atoi is a tiny helper for parsing 3-digit HTTP status codes.
func atoi(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return -1
		}
		n = n*10 + int(s[i]-'0')
	}
	return n
}
