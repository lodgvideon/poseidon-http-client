//go:build linux

package quic

import (
	"bytes"
	"net"
	"syscall"
	"testing"
	"time"
)

// realUDP returns a bound receive socket and a sender connected to it, with the
// raw file descriptors both offload paths need. A platform that cannot hand out
// a RawConn skips the test rather than silently exercising the fallback.
func realUDP(t *testing.T) (rx *net.UDPConn, rxRC syscall.RawConn, tx *net.UDPConn, txRC syscall.RawConn) {
	t.Helper()
	rx, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	t.Cleanup(func() { _ = rx.Close() })
	tx, err = net.DialUDP("udp", nil, rx.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	if rxRC, err = rx.SyscallConn(); err != nil {
		t.Skipf("no RawConn on the receive socket: %v", err)
	}
	if txRC, err = tx.SyscallConn(); err != nil {
		t.Skipf("no RawConn on the send socket: %v", err)
	}
	return rx, rxRC, tx, txRC
}

// TestGRO_RealSocketRoundTrip runs the offload primitives against real UDP
// sockets — the only test in this package that reaches syscall.SendmsgN and
// syscall.Recvmsg at all.
//
// Every other GRO/GSO test drives a fake PacketConn, so the code that builds the
// UDP_SEGMENT control message, hands it to sendmsg, and parses the UDP_GRO cmsg
// back out was covered only by the parts of it that are pure functions. That is
// the half most likely to break when the syscall plumbing is refactored, and it
// had no test.
//
// It is deliberately tolerant about coalescing: whether the kernel actually
// merges the datagrams depends on the kernel version, the NIC and, on loopback,
// on configuration this test does not control. What is asserted is what must hold
// either way — every byte arrives, in order, exactly once.
func TestGRO_RealSocketRoundTrip(t *testing.T) {
	rx, rxRC, tx, txRC := realUDP(t)

	const (
		segSize = 100
		segs    = 3
	)
	payload := make([]byte, segSize*segs)
	for i := range payload {
		payload[i] = byte(i)
	}

	_ = EnableGRO(rxRC) // best-effort: without it the read simply is not coalesced

	gso := NewGSOState()
	n, err := SendGSO(gso, txRC, tx, payload, segSize)
	if err != nil {
		t.Fatalf("SendGSO: %v", err)
	}
	if n != len(payload) {
		t.Errorf("SendGSO wrote %d bytes, want %d", n, len(payload))
	}

	gro := NewGROState(128)
	got := make([]byte, 0, len(payload))
	buf := make([]byte, 64*1024)
	if err := rx.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	for len(got) < len(payload) {
		rn, seg, rerr := RecvGRO(gro, rxRC, rx, buf)
		if rerr != nil {
			t.Fatalf("RecvGRO after %d of %d bytes: %v", len(got), len(payload), rerr)
		}
		if rn == 0 {
			t.Fatalf("RecvGRO returned 0 bytes after %d of %d", len(got), len(payload))
		}
		// A reported segment size must describe the read it came with: either it
		// is 0 (not coalesced) or it divides the read into datagrams.
		if seg != 0 && seg > rn {
			t.Errorf("segSize %d exceeds the %d bytes read", seg, rn)
		}
		got = append(got, buf[:rn]...)
		t.Logf("read %d bytes, segSize %d", rn, seg)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload round-tripped wrong: got %d bytes, want %d (equal=%v)",
			len(got), len(payload), bytes.Equal(got, payload))
	}
}

// TestGRO_RealSocketSingleDatagram covers the un-coalesced shape against a real
// socket: one datagram in, segSize 0 out, because there is nothing to segment.
func TestGRO_RealSocketSingleDatagram(t *testing.T) {
	rx, rxRC, tx, txRC := realUDP(t)
	_ = EnableGRO(rxRC)

	want := []byte("one datagram, no segmentation")
	// segSize >= len(buf) takes SendGSO's own single-datagram path, which is the
	// plain write — the shape a request smaller than the MTU takes in production.
	if _, err := SendGSO(NewGSOState(), txRC, tx, want, len(want)); err != nil {
		t.Fatalf("SendGSO: %v", err)
	}

	if err := rx.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 2048)
	n, seg, err := RecvGRO(NewGROState(128), rxRC, rx, buf)
	if err != nil {
		t.Fatalf("RecvGRO: %v", err)
	}
	if !bytes.Equal(buf[:n], want) {
		t.Errorf("read %q, want %q", buf[:n], want)
	}
	// Not coalesced, so the kernel reports no segment size — or one that covers
	// the whole read, which means the same thing to the caller.
	if seg != 0 && seg < n {
		t.Errorf("segSize %d splits a single %d-byte datagram", seg, n)
	}
}

// TestGRO_StateReusedAcrossReads pins that the hoisted state is reusable: the
// same GROState and GSOState drive several round trips, which is how a
// connection uses them. A state object that only worked once — a closure built
// per call, a buffer consumed on first use — would fail here rather than in
// production.
func TestGRO_StateReusedAcrossReads(t *testing.T) {
	rx, rxRC, tx, txRC := realUDP(t)
	_ = EnableGRO(rxRC)

	gso := NewGSOState()
	gro := NewGROState(128)
	buf := make([]byte, 4096)

	for i := 0; i < 5; i++ {
		want := bytes.Repeat([]byte{byte('a' + i)}, 64+i)
		if _, err := SendGSO(gso, txRC, tx, want, len(want)); err != nil {
			t.Fatalf("round %d SendGSO: %v", i, err)
		}
		if err := rx.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		n, _, err := RecvGRO(gro, rxRC, rx, buf)
		if err != nil {
			t.Fatalf("round %d RecvGRO: %v", i, err)
		}
		if !bytes.Equal(buf[:n], want) {
			t.Errorf("round %d: read %d bytes, want %d", i, n, len(want))
		}
	}
}

// TestGRO_StateReleasesCallerBuffers pins that neither state object keeps a
// reference to the caller's buffer once the call returns. Holding one would pin
// it for the life of the connection, and the read buffer here is 64 KiB.
func TestGRO_StateReleasesCallerBuffers(t *testing.T) {
	rx, rxRC, tx, txRC := realUDP(t)

	gso := NewGSOState()
	if _, err := SendGSO(gso, txRC, tx, []byte("payload"), len("payload")); err != nil {
		t.Fatalf("SendGSO: %v", err)
	}
	if gso.buf != nil {
		t.Errorf("GSOState still holds the caller's send buffer after the call")
	}

	gro := NewGROState(128)
	if err := rx.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, _, err := RecvGRO(gro, rxRC, rx, make([]byte, 2048)); err != nil {
		t.Fatalf("RecvGRO: %v", err)
	}
	if gro.buf != nil {
		t.Errorf("GROState still holds the caller's read buffer after the call")
	}
}
