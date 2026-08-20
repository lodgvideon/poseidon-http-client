package http3

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// countingRawConn wraps the raw file descriptor and counts the operations that
// actually reached it. It is what turns "the test passed" into "the test
// exercised the path": quic.SendGSO and quic.RecvGRO fall back to a plain loop
// over the net.UDPConn whenever the offload is unavailable, and off Linux they
// ignore the raw fd altogether, so a green run says nothing on its own about
// whether the offloaded paths ran (#817).
type countingRawConn struct {
	rc     syscall.RawConn
	reads  atomic.Int64
	writes atomic.Int64
	ctrls  atomic.Int64
}

func (c *countingRawConn) Control(f func(uintptr)) error {
	c.ctrls.Add(1)
	return c.rc.Control(f)
}

func (c *countingRawConn) Read(f func(uintptr) bool) error {
	c.reads.Add(1)
	return c.rc.Read(f)
}

func (c *countingRawConn) Write(f func(uintptr) bool) error {
	c.writes.Add(1)
	return c.rc.Write(f)
}

// TestUDPConn_ConcurrentReadGROWriteGSO_NoRace pins that the offloaded read and
// write paths share no mutable state.
//
// ReadGRO runs on the QUIC reader goroutine and WriteGSO on the sender, and the
// raw file descriptor is used by both. An earlier version of the GSO path
// resolved it through a lazily-populated cache, which made the sender's first
// write race the reader's — an unsynchronised write to the same field. Resolving
// it once at construction makes the field immutable, and this test is what would
// have caught the lazy version: it fails under -race, not by asserting anything.
//
// Two things a concurrency test needs, both missing before (#817): evidence the
// path under test actually ran, and a precondition check on what makes it
// runnable. Every return value was discarded and nothing was asserted, so a run
// in which both loops failed on their first call passed exactly like a real one.
//
// The measured facts behind the shape of the fix:
//
//   - The test is inert without -race. Restoring the lazy cache passes 2/2 on a
//     plain go test and reports DATA RACE 2/2 under -race, so a non-race
//     `go test ./http3/` is not verification of this property. `make test-race`
//     is the repo default, so CI does cover it.
//   - Forcing newUDPConn's rc to nil does NOT blind the test to the lazy cache:
//     WriteGSO and ReadGRO read u.rc on every call whatever its value, so the
//     naive version still reports DATA RACE 2/2 with the offload off. What a nil
//     rc removes is the OFFLOADED path itself — quic.SendGSO and quic.RecvGRO
//     short-circuit to the fallback — which is why the raw fd is a skip
//     precondition rather than a hard requirement, and why the counters below
//     report what was engaged rather than merely that the test finished.
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
	if uc.rc == nil {
		t.Skip("SyscallConn is unavailable on this socket, so quic.SendGSO and quic.RecvGRO " +
			"both short-circuit to the unoffloaded loop and the offloaded paths this test " +
			"exists for never run")
	}
	// Written before either goroutine starts, so this is not itself the
	// unsynchronised write under test — it is how the run reports whether the
	// offloaded paths were reached at all.
	raw := &countingRawConn{rc: uc.rc}
	uc.rc = raw
	var writes, reads atomic.Int64

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
		payload := make([]byte, 1200*2) // > segSize, so SendGSO takes the raw-fd path
		for time.Now().Before(deadline) {
			if _, err := uc.WriteGSO(payload, 1200); err == nil {
				writes.Add(1)
			}
		}
	}()
	go func() { // reader: ReadGRO
		defer wg.Done()
		buf := make([]byte, 64<<10)
		for time.Now().Before(deadline) {
			_ = uc.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			if _, _, err := uc.ReadGRO(buf); err == nil {
				reads.Add(1)
			}
		}
	}()

	time.Sleep(450 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Engagement, printed whether the run passes or fails: without it a green tick
	// cannot be told from a run in which neither offloaded path was ever entered.
	rawWrites, rawReads := raw.writes.Load(), raw.reads.Load()
	t.Logf("offload engaged=%v on GOOS=%s: raw-fd writes=%d reads=%d control=%d; "+
		"WriteGSO ok=%d ReadGRO ok=%d",
		rawWrites > 0 && rawReads > 0, runtime.GOOS, rawWrites, rawReads, raw.ctrls.Load(),
		writes.Load(), reads.Load())

	assert.Positivef(t, writes.Load(),
		"no WriteGSO call succeeded, so the sender side never ran and the race this test "+
			"polices was never given a chance to happen")
	assert.Positivef(t, reads.Load(),
		"no ReadGRO call succeeded, so the reader side never ran; with only one of the two "+
			"goroutines doing work there is no concurrent access to detect")
	if runtime.GOOS == "linux" {
		// Only Linux has the offload: quic/gso_other.go and quic/gro_other.go ignore
		// rc by build tag, so counting raw-fd operations elsewhere would assert a
		// path that does not exist.
		assert.Positivef(t, rawWrites,
			"WriteGSO never reached the raw file descriptor (%d writes): SendGSO took the "+
				"per-datagram fallback the whole run, so the offloaded send path — the one "+
				"that shares u.rc with the reader — was never exercised", rawWrites)
		assert.Positivef(t, rawReads,
			"ReadGRO never reached the raw file descriptor (%d reads): RecvGRO took the plain "+
				"Read fallback the whole run, so the offloaded receive path was never exercised",
			rawReads)
	}
}
