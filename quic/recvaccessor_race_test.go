package quic

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Stream.Finished / ResetReceived / ResetCode read fields the reader goroutine
// mutates under conn.mu in OnStream and OnResetStream. They used to read them
// without the lock, while RecvState's own doc — three lines away — said a
// consumer "must read them under the same lock rather than via the individual
// lock-free accessors". The type offered both, and http3's control-stream
// servicing used the racy one.
//
// These run the accessors against a concurrent writer doing exactly what the
// reader does. Under -race they fail if the lock comes off; without -race they
// still assert the answers stay coherent.

// racingRecvConn builds a Conn with one stream, ready for a writer goroutine to
// drive its receive state while readers poll the accessors.
func racingRecvConn(t *testing.T) (*Conn, *Stream) {
	t.Helper()
	c := &Conn{streams: map[uint64]*Stream{}, isServer: false, localMaxStreamsUni: 100}
	s := &Stream{id: 0x3, conn: c}
	c.streams[s.id] = s
	return c, s
}

// TestStreamRecvAccessors_NoRaceWithReader is the gate. The writer mutates under
// conn.mu, as OnResetStream does; the readers call the accessors. With the
// accessors lock-free this is a textbook data race and -race reports it.
//
// The verdict is the race detector's, so the two counters matter: a run where the
// writer never mutated, or the readers never polled, reports "no race" for the
// same reason a correct implementation does.
func TestStreamRecvAccessors_NoRaceWithReader(t *testing.T) {
	c, s := racingRecvConn(t)
	const rounds = 500
	var mutations, polls int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// The reader goroutine's half: exactly the mutation OnResetStream performs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			c.mu.Lock()
			s.recvReset = true
			s.recvResetCode = uint64(i)
			c.mu.Unlock()
			c.mu.Lock()
			s.recvReset = false
			s.recvResetCode = 0
			c.mu.Unlock()
			atomic.AddInt64(&mutations, 2)
		}
		close(stop)
	}()
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = s.Finished()
				_ = s.ResetReceived()
				_ = s.ResetCode()
				_, _, _ = s.RecvState()
				atomic.AddInt64(&polls, 1)
			}
		}()
	}
	wg.Wait()

	assert.EqualValuesf(t, 2*rounds, atomic.LoadInt64(&mutations),
		"the writer performed %d mutations, want %d — with no injection a lock-free "+
			"accessor reports no race for the same reason a correct one does",
		atomic.LoadInt64(&mutations), 2*rounds)
	assert.NotZero(t, atomic.LoadInt64(&polls),
		"no reader goroutine polled an accessor, so nothing raced the writer at all")
}

// TestStreamRecvAccessors_AgreeWithRecvState pins that the accessors and the
// snapshot answer the same question, so fixing the race did not quietly change
// what they report.
func TestStreamRecvAccessors_AgreeWithRecvState(t *testing.T) {
	c, s := racingRecvConn(t)

	freshFin, freshReset, freshCode := s.RecvState()
	freshAccessors := [3]any{s.Finished(), s.ResetReceived(), s.ResetCode()}
	c.mu.Lock()
	s.recvReset = true
	s.recvResetCode = 42
	c.mu.Unlock()
	fin, reset, code := s.RecvState()
	resetAccessors := [3]any{s.Finished(), s.ResetReceived(), s.ResetCode()}

	assert.Equalf(t, freshAccessors, [3]any{freshFin, freshReset, freshCode},
		"fresh stream: RecvState (%v,%v,%d) disagrees with the accessors %v",
		freshFin, freshReset, freshCode, freshAccessors)
	assert.True(t, resetAccessors[0].(bool),
		"Finished() is false after a peer RESET_STREAM; a reset ends the receive side")
	assert.True(t, resetAccessors[1].(bool), "ResetReceived() is false after a peer RESET_STREAM")
	assert.Equalf(t, uint64(42), resetAccessors[2], "ResetCode() = %v, want 42", resetAccessors[2])
	assert.Equalf(t, [3]any{true, true, uint64(42)}, [3]any{fin, reset, code},
		"RecvState = (%v,%v,%d), want (true,true,42)", fin, reset, code)
}

// TestStreamRecvAccessors_DoNotDeadlockUnderPoll guards the risk the fix
// introduces: taking conn.mu inside an exported accessor deadlocks a caller that
// already holds it. The one production caller is http3's control-stream
// servicing, which runs on the reader goroutine after Poll returns — and Poll's
// documented postcondition is that it never returns holding c.mu. This pins that
// the accessors are callable from an unlocked context without help.
func TestStreamRecvAccessors_DoNotDeadlockUnderPoll(t *testing.T) {
	_, s := racingRecvConn(t)
	done := make(chan struct{})

	go func() {
		_ = s.Finished()
		_ = s.ResetReceived()
		_ = s.ResetCode()
		close(done)
	}()
	var returned bool
	select {
	case <-done:
		returned = true
	case <-time.After(10 * time.Second):
	}

	// Bounded rather than left to hang the binary: a deadlock that kills the whole
	// run on the package timeout prints no --- FAIL line for this test.
	require.True(t, returned,
		"the accessors did not return from an unlocked context — an exported accessor "+
			"that takes conn.mu must be callable by a caller that does not hold it")
}
