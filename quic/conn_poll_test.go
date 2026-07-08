package quic

import "testing"

// onePacketPC hands back a single prepared datagram on the first Read, then
// times out; writes (client ACKs) are captured.
type onePacketPC struct {
	pkt     []byte
	read    bool
	written [][]byte
}

func (p *onePacketPC) Read(b []byte) (int, error) {
	if p.read {
		return 0, errPipeTimeout
	}
	p.read = true
	return copy(b, p.pkt), nil
}
func (p *onePacketPC) Write(b []byte) (int, error) {
	p.written = append(p.written, append([]byte(nil), b...))
	return len(b), nil
}
func (p *onePacketPC) Close() error { return nil }

// TestConn_Poll_DeliversStreamData drives the post-handshake receive path: a
// server-sealed 1-RTT packet carrying a STREAM frame is read by Poll, decrypted,
// dispatched, and delivered to the open stream (RFC 9000 §13).
func TestConn_Poll_DeliversStreamData(t *testing.T) {
	dcid := []byte("polltest")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(keys)
	if err != nil {
		t.Fatal(err)
	}

	// A server 1-RTT (short-header) packet with a STREAM frame + FIN on stream 0.
	frames := AppendStream(nil, 0, 0, true, []byte("response body"))
	pkt := sealServerPacket(t, sealer, PacketShort, nil, nil, 0, frames)

	pc := &onePacketPC{pkt: pkt}
	c := &Conn{
		pc:                pc,
		dcid:              dcid,
		oneRTTSealer:      sealer, // needed by flush to send the ACK
		handshakeComplete: true,
		peer:              TransportParams{InitialMaxStreamsBidi: 1},
	}
	c.keys.OneRTT = opener

	s, err := c.OpenStream() // stream 0 — the one the server replies on
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Poll(); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := string(s.Recv()); got != "response body" {
		t.Fatalf("Recv = %q, want %q", got, "response body")
	}
	if !s.Finished() {
		t.Fatal("stream should be finished")
	}
	if len(pc.written) == 0 {
		t.Fatal("Poll should have flushed an ACK for the ack-eliciting packet")
	}
}
