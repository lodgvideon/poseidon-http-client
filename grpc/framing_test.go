package grpc

import (
	"bytes"
	"errors"
	"testing"
)

func TestAppendMessage_PrefixShape(t *testing.T) {
	got, err := AppendMessage(nil, []byte("hi"))
	if err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}
	want := []byte{0, 0, 0, 0, 2, 'h', 'i'}
	if !bytes.Equal(got, want) {
		t.Fatalf("AppendMessage = % x, want % x", got, want)
	}
}

func TestAppendMessage_EmptyMessage(t *testing.T) {
	got, err := AppendMessage(nil, nil)
	if err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}
	if !bytes.Equal(got, []byte{0, 0, 0, 0, 0}) {
		t.Fatalf("AppendMessage(nil) = % x, want a bare zero-length prefix", got)
	}
}

// TestDecoder_MessageSpansChunks pins the property the whole receive path
// depends on: HTTP/2 DATA boundaries have nothing to do with gRPC message
// boundaries, so a message split byte-by-byte across chunks must reassemble.
func TestDecoder_MessageSpansChunks(t *testing.T) {
	full, err := AppendMessage(nil, []byte("hello world"))
	if err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}
	var d decoder
	for i := 0; i < len(full)-1; i++ {
		d.Push(full[i : i+1])
		if _, ok, err := d.Next(); ok || err != nil {
			t.Fatalf("after %d/%d bytes: ok=%v err=%v, want incomplete", i+1, len(full), ok, err)
		}
	}
	d.Push(full[len(full)-1:])
	msg, ok, err := d.Next()
	if err != nil || !ok {
		t.Fatalf("Next = ok=%v err=%v, want a complete message", ok, err)
	}
	if string(msg) != "hello world" {
		t.Fatalf("msg = %q", msg)
	}
	if d.Pending() != 0 {
		t.Fatalf("Pending = %d, want 0", d.Pending())
	}
}

func TestDecoder_MultipleMessagesInOneChunk(t *testing.T) {
	var buf []byte
	for _, s := range []string{"a", "bb", "ccc"} {
		var err error
		buf, err = AppendMessage(buf, []byte(s))
		if err != nil {
			t.Fatalf("AppendMessage: %v", err)
		}
	}
	var d decoder
	d.Push(buf)
	for _, want := range []string{"a", "bb", "ccc"} {
		msg, ok, err := d.Next()
		if err != nil || !ok {
			t.Fatalf("Next(%q) = ok=%v err=%v", want, ok, err)
		}
		if string(msg) != want {
			t.Fatalf("msg = %q, want %q", msg, want)
		}
	}
	if _, ok, _ := d.Next(); ok {
		t.Fatal("Next returned a fourth message")
	}
}

func TestDecoder_CompressedFlagRejected(t *testing.T) {
	var d decoder
	d.Push([]byte{1, 0, 0, 0, 1, 'x'})
	if _, _, err := d.Next(); !errors.Is(err, ErrCompressed) {
		t.Fatalf("Next = %v, want ErrCompressed", err)
	}
}

// TestDecoder_OversizeRejectedBeforeBuffering pins that the limit is checked
// against the length prefix, not against what has arrived: a hostile peer that
// declares 4 GiB must be refused on the prefix alone.
func TestDecoder_OversizeRejectedBeforeBuffering(t *testing.T) {
	var d decoder
	d.max = 16
	d.Push([]byte{0, 0xFF, 0xFF, 0xFF, 0xFF})
	if _, _, err := d.Next(); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Next = %v, want ErrMessageTooLarge", err)
	}
	if d.Pending() != 5 {
		t.Fatalf("Pending = %d — the declared payload must not have been buffered", d.Pending())
	}
}

func TestDecoder_TruncatedMessageLeavesPending(t *testing.T) {
	full, err := AppendMessage(nil, []byte("abcdef"))
	if err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}
	var d decoder
	d.Push(full[:len(full)-2])
	if _, ok, err := d.Next(); ok || err != nil {
		t.Fatalf("Next = ok=%v err=%v, want incomplete", ok, err)
	}
	if d.Pending() == 0 {
		t.Fatal("Pending = 0 — a truncated message must remain visible to the caller")
	}
}

// TestDecoder_CompactKeepsBufferBounded pins that a long-lived stream does not
// grow the decoder's buffer without bound as messages are consumed.
func TestDecoder_CompactKeepsBufferBounded(t *testing.T) {
	msg := bytes.Repeat([]byte("x"), 1024)
	var d decoder
	for i := 0; i < 2000; i++ {
		chunk, err := AppendMessage(nil, msg)
		if err != nil {
			t.Fatalf("AppendMessage: %v", err)
		}
		d.Push(chunk)
		got, ok, err := d.Next()
		if err != nil || !ok {
			t.Fatalf("iteration %d: ok=%v err=%v", i, ok, err)
		}
		if len(got) != len(msg) {
			t.Fatalf("iteration %d: len = %d", i, len(got))
		}
	}
	if cap(d.buf) > 64<<10 {
		t.Fatalf("decoder buffer grew to %d bytes over 2000 consumed messages", cap(d.buf))
	}
}

// TestDecoder_CompactPreservesContent drives the one path that moves bytes in
// place: a partially-consumed buffer being slid to the front. Every other test
// either drains the buffer exactly — taking compact's fast path — or never
// drains before the next Push, so the slide itself was never executed. An
// off-by-one in that copy is silent payload corruption, not a crash.
func TestDecoder_CompactPreservesContent(t *testing.T) {
	var d decoder
	// Chunks deliberately misaligned with message boundaries, which is the
	// normal case on the wire: DATA frames know nothing about messages.
	var wire []byte
	want := []string{"alpha", "bravo-longer", "c", "delta-longest-of-them"}
	for _, m := range want {
		var err error
		wire, err = AppendMessage(wire, []byte(m))
		if err != nil {
			t.Fatalf("AppendMessage: %v", err)
		}
	}

	var got []string
	for off := 0; off < len(wire); off += 7 {
		end := off + 7
		if end > len(wire) {
			end = len(wire)
		}
		d.Push(wire[off:end])
		// Drain at most one message per Push, so the buffer is routinely left
		// partly consumed and the next Push has to compact around it.
		if msg, ok, err := d.Next(); err != nil {
			t.Fatalf("Next: %v", err)
		} else if ok {
			got = append(got, string(msg))
		}
	}
	for {
		msg, ok, err := d.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(msg))
	}

	if len(got) != len(want) {
		t.Fatalf("decoded %d messages, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message %d = %q, want %q — compaction moved the wrong bytes", i, got[i], want[i])
		}
	}
	if d.Pending() != 0 {
		t.Fatalf("Pending = %d", d.Pending())
	}
}

// TestDecoder_CompactBoundedWhilePartlyConsumed keeps the buffer permanently
// half-drained, which is the state the slide exists to bound.
func TestDecoder_CompactBoundedWhilePartlyConsumed(t *testing.T) {
	msg := bytes.Repeat([]byte("y"), 512)
	var d decoder
	for i := 0; i < 500; i++ {
		chunk, err := AppendMessage(nil, msg)
		if err != nil {
			t.Fatalf("AppendMessage: %v", err)
		}
		// Two messages in, one message out: the pending region never empties.
		d.Push(chunk)
		d.Push(chunk)
		if _, ok, err := d.Next(); err != nil || !ok {
			t.Fatalf("iteration %d: ok=%v err=%v", i, ok, err)
		}
	}
	if cap(d.buf) > 1<<20 {
		t.Fatalf("buffer grew to %d bytes while permanently half-consumed", cap(d.buf))
	}
}

func TestDecoder_Reset(t *testing.T) {
	var d decoder
	d.Push([]byte{0, 0, 0, 0, 9})
	d.Reset()
	if d.Pending() != 0 {
		t.Fatalf("Pending after Reset = %d", d.Pending())
	}
}
