package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// pipePeer simulates the server side of net.Pipe by responding to the
// preface + SETTINGS sequence. net.Pipe is synchronous (no buffer), so
// writes must run in a goroutine concurrently with reads to avoid
// deadlock when both sides write before either reads.
func pipePeer(t *testing.T, srv net.Conn) {
	t.Helper()
	defer srv.Close()
	preface := make([]byte, 24)
	if _, err := readN(srv, preface); err != nil {
		t.Logf("peer read preface: %v", err)
		return
	}
	srvFr := frame.NewFramer(srv, srv)
	var sp frame.SettingsParams
	sp.N = 1
	sp.Pairs[0] = frame.SettingPair{ID: frame.SettingMaxConcurrentStreams, Value: 100}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- srvFr.WriteSettings(sp)
	}()
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("peer read client settings: %v", err)
		return
	}
	if err := <-writeDone; err != nil {
		t.Logf("peer write settings: %v", err)
		return
	}
	go func() {
		writeDone <- srvFr.WriteSettingsAck()
	}()
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("peer read settings ack: %v", err)
		return
	}
	if err := <-writeDone; err != nil {
		t.Logf("peer write settings ack: %v", err)
		return
	}
}

func readN(c net.Conn, buf []byte) (int, error) {
	var read int
	for read < len(buf) {
		n, err := c.Read(buf[read:])
		read += n
		if err != nil {
			return read, err
		}
	}
	return read, nil
}

// nilHandler implements frame.Handler with no-ops.
type nilHandler struct{}

func (nilHandler) OnData(frame.FrameHeader, []byte, uint8) error { return nil }
func (nilHandler) OnHeaders(frame.FrameHeader, frame.HeaderBlock, *frame.Priority, uint8) error {
	return nil
}
func (nilHandler) OnPriority(frame.FrameHeader, frame.Priority) error       { return nil }
func (nilHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error       { return nil }
func (nilHandler) OnSettings(frame.FrameHeader, frame.SettingsParams) error { return nil }
func (nilHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (nilHandler) OnPing(frame.FrameHeader, [8]byte) error                         { return nil }
func (nilHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
func (nilHandler) OnWindowUpdate(frame.FrameHeader, uint32) error                  { return nil }
func (nilHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error       { return nil }

func TestHandshakeSettings_RoundTripsAgainstPipePeer(t *testing.T) {
	cli, srv := net.Pipe()
	go pipePeer(t, srv)

	defer cli.Close()
	fr := frame.NewFramer(cli, cli)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	peer, err := handshakeSettings(ctx, fr, AdvertisedSettings{}.defaulted())
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	// Peer announced MaxConcurrentStreams=100; we should observe that.
	var found bool
	for i := 0; i < peer.N; i++ {
		p := peer.Pairs[i]
		if p.ID == frame.SettingMaxConcurrentStreams && p.Value == 100 {
			found = true
		}
	}
	if !found {
		t.Fatalf("peer settings = %+v", peer)
	}
}
