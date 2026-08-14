package conn

import (
	"bufio"
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ConnOptions.WriteBufferSize is what bounds a coalesced write, so it is the
// knob SendBatch is sized against. It used to be a constant, and one thing
// derived from that constant at compile time — the group-commit convoy
// threshold — has to follow it now that it varies per connection.

// TestWriteBufferSize_DefaultsAndClamps pins the three arms of the defaulting.
func TestWriteBufferSize_DefaultsAndClamps(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int
		want int
	}{
		{"zero takes the default", 0, defaultWriteBufferSize},
		{"negative takes the default", -1, defaultWriteBufferSize},
		{"below the floor is raised", 1, minWriteBufferSize},
		{"one below the floor is raised", minWriteBufferSize - 1, minWriteBufferSize},
		{"at the floor is kept", minWriteBufferSize, minWriteBufferSize},
		{"in range is kept", 64 * 1024, 64 * 1024},
		{"at the ceiling is kept", maxWriteBufferSize, maxWriteBufferSize},
		{"above the ceiling is lowered", maxWriteBufferSize + 1, maxWriteBufferSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (ConnOptions{WriteBufferSize: tc.in}).defaulted().WriteBufferSize; got != tc.want {
				t.Errorf("WriteBufferSize %d defaulted to %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// TestWriteBufferSize_FloorHoldsOneWholeFrame is the reason the floor is where
// it is rather than at some round number: below one maximum-size frame plus its
// header, the coalescing the buffer exists for stops working and every frame
// costs two writes again — the opposite of what a caller sets this for.
func TestWriteBufferSize_FloorHoldsOneWholeFrame(t *testing.T) {
	if minWriteBufferSize < int(frameSizeFloor)+9 {
		t.Fatalf("minWriteBufferSize = %d cannot hold a %d-byte frame plus its 9-byte header",
			minWriteBufferSize, frameSizeFloor)
	}
}

// TestWriteBufferSize_SizesTheWriter pins that the option reaches the writer,
// which nothing else observes: a Conn built with a large buffer must be able to
// hold more than the default before flushing.
func TestWriteBufferSize_SizesTheWriter(t *testing.T) {
	const want = 128 * 1024
	opts := ConnOptions{WriteBufferSize: want}.defaulted()
	wb := bufio.NewWriterSize(&countingSink{}, opts.WriteBufferSize)
	if got := wb.Available(); got != want {
		t.Errorf("buffered writer holds %d bytes, want %d", got, want)
	}
}

// TestGroupCommit_ConvoyThresholdTracksWriteBufferSize is the coupling the
// constant used to hide. groupCommitFlushBytes was `writeBufferSize / 2`,
// evaluated at compile time; with a per-connection buffer that would leave a
// 256 KiB-buffered connection convoying at 8 KiB, and a small-buffered one
// convoying past its own bufio auto-flush boundary — the exact hazard the
// threshold exists to avoid.
func TestGroupCommit_ConvoyThresholdTracksWriteBufferSize(t *testing.T) {
	for _, size := range []int{minWriteBufferSize, defaultWriteBufferSize, 256 * 1024} {
		opts := ConnOptions{WriteBufferSize: size, GroupCommit: true}.defaulted()
		wb := bufio.NewWriterSize(&countingSink{}, opts.WriteBufferSize)
		b := newWriteBatcher(true, &sync.Mutex{}, wb, opts.WriteBufferSize/2)
		if want := opts.WriteBufferSize / 2; b.flushBytes != want {
			t.Errorf("buffer %d: convoy threshold %d, want %d", size, b.flushBytes, want)
		}
		if b.flushBytes >= wb.Available() {
			t.Errorf("buffer %d: convoy threshold %d is not below the auto-flush boundary %d",
				size, b.flushBytes, wb.Available())
		}
	}
}

// TestSendBatch_HonoursWriteBufferSize is the end-to-end statement of the
// sizing contract: the same batch that splits against a small buffer is one
// write against a buffer large enough to hold it. Without this, "raise
// WriteBufferSize to batch more" is documentation with nothing behind it.
func TestSendBatch_HonoursWriteBufferSize(t *testing.T) {
	const (
		entries  = 8
		bodySize = 4096
	)
	writesFor := func(bufSize int) int {
		sink := &countingSink{}
		wb := bufio.NewWriterSize(sink, bufSize)
		c := &Conn{
			streams: map[uint32]*Stream{},
			opts:    ConnOptions{WriteBufferSize: bufSize}.defaulted(),
			nextID:  1,
			enc:     hpack.NewEncoder(),
			wb:      wb,
		}
		c.fr = frame.NewFramer(wb, bytes.NewReader(nil)) // writer first
		c.fcOutCond = sync.NewCond(&c.fcOutMu)
		c.peerConnSendWindow = entries * bodySize * 2
		c.wbatch = newWriteBatcher(false, &c.wmu, wb, bufSize/2)

		body := make([]byte, bodySize)
		batch := make([]BatchEntry, entries)
		for i := range batch {
			batch[i] = BatchEntry{
				Stream:    batchStream(c, 65535).ref(),
				Fields:    batchFields,
				Body:      body,
				EndStream: true,
			}
		}
		if err := c.SendBatch(context.Background(), batch); err != nil {
			t.Fatalf("SendBatch at buffer %d: %v", bufSize, err)
		}
		for i := range batch {
			if batch[i].Err != nil {
				t.Fatalf("buffer %d entry %d: %v", bufSize, i, batch[i].Err)
			}
		}
		return sink.writes
	}

	small := writesFor(minWriteBufferSize)
	large := writesFor(maxWriteBufferSize)
	if large != 1 {
		t.Errorf("a buffer large enough for the whole batch cost %d writes, want 1", large)
	}
	if small <= large {
		t.Errorf("a %d-byte buffer cost %d writes and a %d-byte one cost %d; the option "+
			"is not reaching the write path", minWriteBufferSize, small, maxWriteBufferSize, large)
	}
}
