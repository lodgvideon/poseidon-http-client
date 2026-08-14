package conn

import (
	"bytes"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestHandlePushPromise_RecycledParentIsNotAnnounced drives the real
// handlePushPromiseBlock, which the pushIfID unit tests do not reach: reverting
// the handler to the ungated push passes the entire conn suite without this.
//
// handlePushPromiseBlock resolves the parent by id and then reserves the
// promised stream, takes a header block from the pool and copies every field
// before delivering. A *Stream is pooled, so in that window the application can
// finish the parent, Close it, and the next NewStream can claim the struct. The
// promise must not be announced to whoever owns it now, and the promised stream
// must be refused rather than left reserved and unreachable.
func TestHandlePushPromise_RecycledParentIsNotAnnounced(t *testing.T) {
	var wire bytes.Buffer
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.fr = frame.NewFramer(&wire, nil) // writer first
	c.opts.EnablePush = true
	// Without a non-zero cap and buffer, reservePushedStream refuses and the
	// handler RSTs the promised stream from ITS error path — which looks exactly
	// like the refusal this test wants to observe. That false green is what
	// mutation-checking caught: the test passed with the id gate reverted.
	c.opts.Settings.MaxConcurrentStreams = 8
	c.opts.Settings.InitialWindowSize = 65535
	c.opts.StreamEventBuffer = 8

	parent := newStream(1, 8, c, 65535)
	// The authority the promise carries. Without it validatePushedRequest refuses
	// the promise on its own path — with an RST on the same stream — and the
	// handler never reaches the delivery this test is about.
	parent.authorityBuf = []byte("example.com")
	c.streams[1] = parent
	c.inflight = 1

	// The window this guards: handlePushPromiseBlock's lookup succeeds while the
	// stream is still registered under id 1, and only then does the application
	// finish it, Close it, and the next NewStream claim the struct. The registry
	// entry is what the handler already holds; the struct under it now carries a
	// different id. Modelled by mutating the id in place, so the property is
	// pinned deterministically rather than by racing the window.
	parent.id = 3

	h := &connHandler{streams: c, dec: hpack.NewDecoder()}
	enc := hpack.NewEncoder()
	if err := h.handlePushPromiseBlock(1, 2, promiseBlock(enc, "/a.css")); err != nil {
		t.Fatalf("handlePushPromiseBlock: %v", err)
	}

	select {
	case ev := <-parent.events:
		if ev.Type == EventPushPromise {
			t.Fatal("the recycled struct's new lifetime was announced a promise made to the previous one")
		}
		t.Fatalf("unexpected event %s on the new lifetime", ev.Type)
	default:
	}

	// The promise could not be announced, so the promised stream must be refused.
	if wire.Len() == 0 {
		t.Fatal("no frame written; an undeliverable promise left stream 2 reserved and unreachable")
	}
	b := wire.Bytes()
	ftype := b[3]
	streamID := (uint32(b[5])<<24 | uint32(b[6])<<16 | uint32(b[7])<<8 | uint32(b[8])) &^ (1 << 31)
	if ftype != 0x3 || streamID != 2 { // 0x3 = RST_STREAM
		t.Fatalf("wrote frame type %#x on stream %d, want RST_STREAM (0x3) on the promised stream 2", ftype, streamID)
	}
	// The code, not just the frame: two other paths in this handler also RST the
	// promised stream (a failed reservation, a rejected promised request), so
	// asserting only "an RST appeared" cannot tell them from this one. That weaker
	// assertion passed with the id gate reverted.
	code := uint32(b[9])<<24 | uint32(b[10])<<16 | uint32(b[11])<<8 | uint32(b[12])
	if code != uint32(frame.ErrCodeRefusedStream) {
		t.Fatalf("RST code = %d, want REFUSED_STREAM (%d)", code, frame.ErrCodeRefusedStream)
	}
}
