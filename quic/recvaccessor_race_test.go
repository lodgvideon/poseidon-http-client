package quic

import (
	"sync"
	"testing"
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
func TestStreamRecvAccessors_NoRaceWithReader(t *testing.T) {
	c, s := racingRecvConn(t)

	const rounds = 500
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
			}
		}()
	}
	wg.Wait()
}

// TestStreamRecvAccessors_AgreeWithRecvState pins that the accessors and the
// snapshot answer the same question, so fixing the race did not quietly change
// what they report.
func TestStreamRecvAccessors_AgreeWithRecvState(t *testing.T) {
	c, s := racingRecvConn(t)

	// Clean, nothing received.
	if fin, reset, code := s.RecvState(); fin != s.Finished() || reset != s.ResetReceived() || code != s.ResetCode() {
		t.Errorf("fresh stream: RecvState (%v,%v,%d) disagrees with the accessors (%v,%v,%d)",
			fin, reset, code, s.Finished(), s.ResetReceived(), s.ResetCode())
	}

	// After a peer reset.
	c.mu.Lock()
	s.recvReset = true
	s.recvResetCode = 42
	c.mu.Unlock()

	if !s.Finished() {
		t.Error("Finished() is false after a peer RESET_STREAM; a reset ends the receive side")
	}
	if !s.ResetReceived() {
		t.Error("ResetReceived() is false after a peer RESET_STREAM")
	}
	if got := s.ResetCode(); got != 42 {
		t.Errorf("ResetCode() = %d, want 42", got)
	}
	fin, reset, code := s.RecvState()
	if !fin || !reset || code != 42 {
		t.Errorf("RecvState = (%v,%v,%d), want (true,true,42)", fin, reset, code)
	}
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
	<-done // a deadlock here hangs the test binary, which is the failure
}
