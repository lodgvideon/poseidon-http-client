package conn

// Test-only seams for *Stream.
//
// These used to sit in stream.go with no production caller — #515 flagged them
// as "test-only mutators on the pooled struct", surface a future edit can wire
// into the wrong path. Moving them into a _test.go file is what makes that
// impossible rather than merely discouraged: the production build does not
// contain them, so a call from stream.go or conn.go does not compile.
//
// They are kept rather than deleted because the alternative is rewriting ~50
// call sites onto the production entry points, and those entry points carry
// guards a unit test deliberately wants to bypass — pushIfID refuses unless the
// stream is still the one the caller looked up, which is exactly the condition
// most of these tests are not trying to exercise.

// push delivers an event as the reader goroutine would. Non-blocking under the
// channel's capacity. On overflow it marks the stream closed, dispatches the RST
// send to a background goroutine (so the reader is never blocked on wmu), and
// signals via resetSignal so a blocked Recv unblocks immediately.
//
// Production reaches the same code through pushIfID, which adds the
// still-the-same-stream guard; this is that path without it.
func (s *Stream) push(e StreamEvent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pushLocked(e)
}
