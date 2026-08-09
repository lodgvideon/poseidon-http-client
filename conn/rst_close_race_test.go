package conn

import (
	"bytes"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestConn_PeerReset_ConcurrentCloseIsRaceFree is a -race regression guard for a
// peer reset (RST_STREAM or GOAWAY victim) delivered while the caller runs
// Stream.Close() concurrently.
//
// The teardown handler releases the stream's inflight slot by marking it ended,
// which makes it bothEnded — recycle-eligible. recycleStream (conn/stream.go)
// rewrites s.events and zeroes fields with NO s.mu held, so a Close() that reaches
// its recycle branch (!closed && bothEnded) while the handler still runs push /
// markStreamDone is a data race and can recycle the struct out from under the
// reader (dropping the reset, re-leaking the slot, or handing a live struct to a
// new request via the pool).
//
// The fix delivers the reset first and sets remoteEnded+localEnded+closed in ONE
// s.mu section, so bothEnded (== localEnded && remoteEnded) can only be observed
// once closed is also set — Close() then always hits its early `closed` return
// and never recycles. Both bothEnded inputs matter: localEnded is set by the send
// path (SendHeaders/SendData) when the upload completes, so a stream whose request
// is fully sent has localEnded==true BEFORE the peer reset. Setting remoteEnded in
// a section separate from closed left a window that raced exactly that (common)
// case; the localEnded==true rows below fail 3/3 under -race against that variant.
func TestConn_PeerReset_ConcurrentCloseIsRaceFree(t *testing.T) {
	rst := func(h *connHandler, _ *Conn, s *Stream) {
		_ = h.OnRSTStream(frame.FrameHeader{Type: frame.FrameRSTStream, StreamID: s.id}, frame.ErrCodeCancel)
	}
	goaway := func(_ *connHandler, c *Conn, s *Stream) {
		// lastStreamID below s.id so s is a drained victim (id > lastStreamID).
		c.onGoAwayReceived(s.id-1, frame.ErrCode(0))
	}

	for _, tc := range []struct {
		name          string
		streamID      uint32
		preLocalEnded bool // request upload already completed (localEnded set by send path)
		terminate     func(*connHandler, *Conn, *Stream)
	}{
		{"rst_half_open", 1, false, rst},
		{"rst_upload_complete", 1, true, rst},
		{"goaway_half_open", 3, false, goaway},
		{"goaway_upload_complete", 3, true, goaway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 800; i++ {
				c := newGoAwayConn()
				// A Framer wired to a buffer so Close's writeRSTStream path (taken
				// when Close observes the stream before the handler marks it closed)
				// has a real writer; c.wb stays nil, so flushWrite is a no-op.
				c.fr = frame.NewFramer(&bytes.Buffer{}, &bytes.Buffer{})
				h := newConnHandler(c, hpack.NewDecoder())

				s := newStream(tc.streamID, 8, c, 65535)
				s.id = tc.streamID
				if tc.preLocalEnded {
					s.localEnded = true
				}
				c.streams[tc.streamID] = s
				c.inflight++

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					tc.terminate(h, c, s)
				}()
				go func() {
					defer wg.Done()
					_ = s.ref().Close()
				}()
				wg.Wait()

				// The slot must be released and the stream evicted regardless of
				// which goroutine won the race.
				c.smu.Lock()
				inflight := c.inflight
				_, registered := c.streams[tc.streamID]
				c.smu.Unlock()
				if inflight != 0 {
					t.Fatalf("iter %d: inflight = %d, want 0 (slot leaked)", i, inflight)
				}
				if registered {
					t.Fatalf("iter %d: stream still registered, want evicted", i)
				}
			}
		})
	}
}
