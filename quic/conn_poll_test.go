package quic

import (
	"testing"
	"time"
)

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

// drainingPC hands back a queue of prepared datagrams, one per Read, then
// reports a timeout. It implements SetReadDeadline so Poll's non-blocking drain
// engages and empties the queue in a single call.
type drainingPC struct {
	pkts    [][]byte
	i       int
	written [][]byte
}

func (p *drainingPC) Read(b []byte) (int, error) {
	if p.i >= len(p.pkts) {
		return 0, timeoutError{} // socket drained
	}
	n := copy(b, p.pkts[p.i])
	p.i++
	return n, nil
}
func (p *drainingPC) Write(b []byte) (int, error) {
	p.written = append(p.written, append([]byte(nil), b...))
	return len(b), nil
}
func (p *drainingPC) Close() error                    { return nil }
func (p *drainingPC) SetReadDeadline(time.Time) error { return nil }

// TestConn_Poll_DrainsBufferedBurst verifies a single Poll reads and processes
// every datagram already buffered in the socket — not just one — so a server's
// response burst is reassembled and acknowledged in one batch (G.6j). Draining
// one datagram per Poll cannot keep pace with a bulk transfer, so the kernel
// buffer overflows and the peer's congestion window collapses (RFC 9002 §7).
func TestConn_Poll_DrainsBufferedBurst(t *testing.T) {
	dcid := []byte("draintst")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(keys)
	if err != nil {
		t.Fatal(err)
	}

	// Four server 1-RTT packets carrying consecutive STREAM chunks on stream 0,
	// the last with FIN — as they would arrive back-to-back in the socket.
	chunks := []string{"aaaa", "bbbb", "cccc", "dddd"}
	pkts := make([][]byte, 0, len(chunks))
	var off uint64
	for i, ch := range chunks {
		fin := i == len(chunks)-1
		frames := AppendStream(nil, 0, off, fin, []byte(ch))
		pkts = append(pkts, sealServerPacket(t, sealer, PacketShort, nil, nil, uint64(i), frames))
		off += uint64(len(ch))
	}

	pc := &drainingPC{pkts: pkts}
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

	if err := c.Poll(); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got, want := string(s.Recv()), "aaaabbbbccccdddd"; got != want {
		t.Fatalf("Recv = %q, want %q (a single Poll must drain the whole burst)", got, want)
	}
	if !s.Finished() {
		t.Fatal("stream should be finished after draining the FIN packet")
	}
	if pc.i != len(pkts) {
		t.Fatalf("drained %d of %d datagrams", pc.i, len(pkts))
	}
	// The whole burst is acknowledged with one flush, not one ACK per datagram.
	if len(pc.written) != 1 {
		t.Fatalf("wrote %d packets, want 1 (single batched ACK)", len(pc.written))
	}
}
