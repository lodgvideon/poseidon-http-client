package http3

import (
	"net"
	"sync"
	"testing"
	"time"
)

// TestUDPConn_ConcurrentReadGROWriteGSO_NoRace pins that the offloaded read and
// write paths share no mutable state.
//
// ReadGRO runs on the QUIC reader goroutine and WriteGSO on the sender, and the
// raw file descriptor is used by both. An earlier version of the GSO path
// resolved it through a lazily-populated cache, which made the sender's first
// write race the reader's — an unsynchronised write to the same field. Resolving
// it once at construction makes the field immutable, and this test is what would
// have caught the lazy version: it fails under -race, not by asserting anything.
func TestUDPConn_ConcurrentReadGROWriteGSO_NoRace(t *testing.T) {
	srv, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no UDP: %v", err)
	}
	defer srv.Close()

	c, err := net.Dial("udp", srv.LocalAddr().String())
	if err != nil {
		t.Skipf("dial: %v", err)
	}
	uc := newUDPConn(c.(*net.UDPConn))
	defer uc.Close()

	// A responder so the reader has something to read.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 2048)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = srv.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			n, addr, err := srv.ReadFrom(buf)
			if err != nil {
				continue
			}
			_, _ = srv.WriteTo(buf[:n], addr)
		}
	}()

	deadline := time.Now().Add(400 * time.Millisecond)
	wg.Add(2)
	go func() { // sender: WriteGSO
		defer wg.Done()
		payload := make([]byte, 1200*2)
		for time.Now().Before(deadline) {
			_, _ = uc.WriteGSO(payload, 1200)
		}
	}()
	go func() { // reader: ReadGRO
		defer wg.Done()
		buf := make([]byte, 64<<10)
		for time.Now().Before(deadline) {
			_ = uc.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
			_, _, _ = uc.ReadGRO(buf)
		}
	}()

	time.Sleep(450 * time.Millisecond)
	close(stop)
	wg.Wait()
}
