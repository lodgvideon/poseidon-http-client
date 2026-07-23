package http1_test

import (
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// probePair returns a client Conn wired to a live TCP peer plus that peer, so a
// test can drive real socket events (write, close) under the real read-deadline
// semantics ProbeIdle relies on. net.Pipe is avoided: its deadline behaviour
// differs from a kernel socket's.
func probePair(t *testing.T) (*http1.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			accepted <- nil
			return
		}
		accepted <- c
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	srv := <-accepted
	if srv == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { _ = srv.Close() })
	return http1.NewConn(cli), srv
}

// probeBecomesFalse polls ProbeIdle for up to ~200ms, allowing a peer write/FIN
// to traverse the loopback. The stall is bounded per attempt, not by a fixed
// total sleep, so it stays stable on slow CI (see the repo's timing-test note).
func probeBecomesFalse(c *http1.Conn) bool {
	for i := 0; i < 100; i++ {
		if !c.ProbeIdle() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// TestProbeIdle_HealthySocketReusable pins that an open, silent idle connection
// probes as reusable — and repeatedly, proving the read deadline is cleared so a
// later real read is unaffected.
func TestProbeIdle_HealthySocketReusable(t *testing.T) {
	c, _ := probePair(t)
	for i := 0; i < 3; i++ {
		if !c.ProbeIdle() {
			t.Fatalf("probe %d: healthy idle conn reported not reusable", i)
		}
	}
}

// TestProbeIdle_UnsolicitedDataEvicts pins that an idle connection carrying
// unsolicited bytes probes as not reusable — RFC 9110: data on a connection with
// no outstanding request is not a valid response, so the next request must not
// consume it as its status line.
func TestProbeIdle_UnsolicitedDataEvicts(t *testing.T) {
	c, srv := probePair(t)
	if _, err := srv.Write([]byte("HTTP/1.1 200 OK\r\n\r\n")); err != nil {
		t.Fatalf("server write: %v", err)
	}
	if !probeBecomesFalse(c) {
		t.Fatal("unsolicited data on an idle conn was not detected")
	}
}

// TestProbeIdle_PeerCloseEvicts pins that a peer FIN on an idle connection probes
// as not reusable (RFC 9112 §9.6: monitor idle connections for a closure signal).
func TestProbeIdle_PeerCloseEvicts(t *testing.T) {
	c, srv := probePair(t)
	_ = srv.Close()
	if !probeBecomesFalse(c) {
		t.Fatal("peer FIN on an idle conn was not detected")
	}
}

// TestProbeIdle_ClosedConn pins that a locally closed conn probes false without
// touching the socket.
func TestProbeIdle_ClosedConn(t *testing.T) {
	c, _ := probePair(t)
	_ = c.Close()
	if c.ProbeIdle() {
		t.Fatal("a closed conn must not be reported reusable")
	}
}
