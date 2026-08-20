package grpc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendMessage_PrefixShape(t *testing.T) {
	want := []byte{0, 0, 0, 0, 2, 'h', 'i'}

	got, err := AppendMessage(nil, []byte("hi"))

	require.NoError(t, err, "AppendMessage")
	require.Equalf(t, want, got, "AppendMessage = % x, want % x", got, want)
}

func TestAppendMessage_EmptyMessage(t *testing.T) {
	got, err := AppendMessage(nil, nil)

	require.NoError(t, err, "AppendMessage")
	require.Equalf(t, []byte{0, 0, 0, 0, 0}, got,
		"AppendMessage(nil) = % x, want a bare zero-length prefix", got)
}

// TestDecoder_MessageSpansChunks pins the property the whole receive path
// depends on: HTTP/2 DATA boundaries have nothing to do with gRPC message
// boundaries, so a message split byte-by-byte across chunks must reassemble.
func TestDecoder_MessageSpansChunks(t *testing.T) {
	full, err := AppendMessage(nil, []byte("hello world"))
	require.NoError(t, err, "AppendMessage")
	var d decoder

	for i := 0; i < len(full)-1; i++ {
		d.Push(full[i : i+1])
		_, ok, err := d.Next()

		require.Falsef(t, ok, "after %d/%d bytes: ok=%v err=%v, want incomplete", i+1, len(full), ok, err)
		require.NoErrorf(t, err, "after %d/%d bytes, want incomplete", i+1, len(full))
	}
	d.Push(full[len(full)-1:])
	msg, ok, err := d.Next()

	require.NoErrorf(t, err, "Next = ok=%v err=%v, want a complete message", ok, err)
	require.Truef(t, ok, "Next = ok=%v err=%v, want a complete message", ok, err)
	require.Equalf(t, "hello world", string(msg), "msg = %q", msg)
	require.Equalf(t, 0, d.Pending(), "Pending = %d, want 0", d.Pending())
}

func TestDecoder_MultipleMessagesInOneChunk(t *testing.T) {
	var buf []byte
	for _, s := range []string{"a", "bb", "ccc"} {
		var err error
		buf, err = AppendMessage(buf, []byte(s))
		require.NoError(t, err, "AppendMessage")
	}
	var d decoder

	d.Push(buf)

	for _, want := range []string{"a", "bb", "ccc"} {
		msg, ok, err := d.Next()
		require.NoErrorf(t, err, "Next(%q) = ok=%v err=%v", want, ok, err)
		require.Truef(t, ok, "Next(%q) = ok=%v err=%v", want, ok, err)
		require.Equalf(t, want, string(msg), "msg = %q, want %q", msg, want)
	}
	_, ok, _ := d.Next()
	require.False(t, ok, "Next returned a fourth message")
}

func TestDecoder_CompressedFlagRejected(t *testing.T) {
	var d decoder
	d.Push([]byte{1, 0, 0, 0, 1, 'x'})

	_, _, err := d.Next()

	require.ErrorIsf(t, err, ErrCompressed, "Next = %v, want ErrCompressed", err)
}

// TestDecoder_OversizeRejectedBeforeBuffering pins that the limit is checked
// against the length prefix, not against what has arrived: a hostile peer that
// declares 4 GiB must be refused on the prefix alone.
func TestDecoder_OversizeRejectedBeforeBuffering(t *testing.T) {
	var d decoder
	d.max = 16
	d.Push([]byte{0, 0xFF, 0xFF, 0xFF, 0xFF})

	_, _, err := d.Next()

	require.ErrorIsf(t, err, ErrMessageTooLarge, "Next = %v, want ErrMessageTooLarge", err)
	require.Equalf(t, 5, d.Pending(),
		"Pending = %d — the declared payload must not have been buffered", d.Pending())
}

func TestDecoder_TruncatedMessageLeavesPending(t *testing.T) {
	full, err := AppendMessage(nil, []byte("abcdef"))
	require.NoError(t, err, "AppendMessage")
	var d decoder

	d.Push(full[:len(full)-2])

	_, ok, err := d.Next()
	require.Falsef(t, ok, "Next = ok=%v err=%v, want incomplete", ok, err)
	require.NoErrorf(t, err, "Next = ok=%v err=%v, want incomplete", ok, err)
	require.NotZero(t, d.Pending(),
		"Pending = 0 — a truncated message must remain visible to the caller")
}

// TestDecoder_CompactKeepsBufferBounded pins that a long-lived stream does not
// grow the decoder's buffer without bound as messages are consumed.
func TestDecoder_CompactKeepsBufferBounded(t *testing.T) {
	msg := bytes.Repeat([]byte("x"), 1024)
	var d decoder

	for i := 0; i < 2000; i++ {
		chunk, err := AppendMessage(nil, msg)
		require.NoError(t, err, "AppendMessage")
		d.Push(chunk)
		got, ok, err := d.Next()
		require.NoErrorf(t, err, "iteration %d: ok=%v err=%v", i, ok, err)
		require.Truef(t, ok, "iteration %d: ok=%v err=%v", i, ok, err)
		require.Lenf(t, got, len(msg), "iteration %d: len = %d", i, len(got))
	}

	require.LessOrEqualf(t, cap(d.buf), 64<<10,
		"decoder buffer grew to %d bytes over 2000 consumed messages", cap(d.buf))
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
		require.NoError(t, err, "AppendMessage")
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
		msg, ok, err := d.Next()
		require.NoError(t, err, "Next")
		if ok {
			got = append(got, string(msg))
		}
	}
	for {
		msg, ok, err := d.Next()
		require.NoError(t, err, "Next")
		if !ok {
			break
		}
		got = append(got, string(msg))
	}

	require.Lenf(t, got, len(want), "decoded %d messages, want %d: %q", len(got), len(want), got)
	for i := range want {
		require.Equalf(t, want[i], got[i],
			"message %d = %q, want %q — compaction moved the wrong bytes", i, got[i], want[i])
	}
	require.Equalf(t, 0, d.Pending(), "Pending = %d", d.Pending())
}

// TestDecoder_CompactBoundedWhilePartlyConsumed keeps the buffer permanently
// half-drained, which is the state the slide exists to bound.
//
// Both halves of this fixture are load-bearing, and the first version got each
// one wrong.
//
// The consumed prefix has to be MORE than half the buffer, or compact takes its
// `off == len(own)` fast path and the slide never runs. Every iteration here
// leaves a three-byte stub of the next message pending, so off is one whole
// message against a buffer three bytes longer.
//
// And exactly one message may be delivered per message pushed. Pushing two and
// draining one — what this test used to do — grows the pending region by a
// message every iteration whether or not the slide exists, so the only ceiling
// the correct code passes is one loose enough for the slide-less form to pass
// too. That is why this used to assert 1 MiB and could not fail. Here the
// steady state is one message plus a stub, so a few chunks separate the two:
// with the slide the buffer stays around 520 bytes, without it it reaches 500
// messages, ~259 kB.
func TestDecoder_CompactBoundedWhilePartlyConsumed(t *testing.T) {
	const iters = 500
	const stub = 3 // an incomplete prefix: enough to keep the region non-empty
	one, err := AppendMessage(nil, bytes.Repeat([]byte("y"), 512))
	require.NoError(t, err, "AppendMessage")
	var wire []byte
	for i := 0; i <= iters; i++ {
		wire = append(wire, one...)
	}
	var d decoder

	// Prime with one whole message plus the head of the next.
	d.Push(wire[:len(one)+stub])
	cursor := len(one) + stub
	for i := 0; i < iters; i++ {
		_, ok, err := d.Next()
		require.NoErrorf(t, err, "iteration %d: ok=%v err=%v", i, ok, err)
		require.Truef(t, ok, "iteration %d: ok=%v err=%v", i, ok, err)
		require.Equalf(t, stub, d.Pending(),
			"iteration %d left %d bytes pending, want %d — the fixture must keep the "+
				"buffer partly consumed, or compact takes its drained-exactly fast "+
				"path and the slide is never reached", i, d.Pending(), stub)
		// Completes the pending message and leaves a fresh stub behind it.
		d.Push(wire[cursor : cursor+len(one)])
		cursor += len(one)
	}

	require.LessOrEqualf(t, cap(d.buf), 16*len(one),
		"buffer grew to %d bytes while permanently partly consumed, want at most %d "+
			"— the slide is not running, so a long-lived stream accumulates every "+
			"chunk it has ever been handed", cap(d.buf), 16*len(one))
}

// TestDecoder_EmptyChunkIsNotBorrowed pins the first half of PushBorrowed's
// guard. The borrow tests exercise its second half (d.Pending() != 0)
// thoroughly and this half not at all.
//
// An empty DATA frame is legal on the wire and carries no message. Borrowing on
// one would take ownership of the caller's slab in order to alias nothing —
// and because a borrow only ends at the NEXT push, that slab would sit out of
// circulation until a chunk carrying bytes arrived.
func TestDecoder_EmptyChunkIsNotBorrowed(t *testing.T) {
	var d decoder

	borrowed := d.PushBorrowed(nil, nil)

	require.Falsef(t, borrowed,
		"PushBorrowed(nil) = true — a true return transfers slab ownership to the "+
			"decoder, so an empty DATA frame would park a pooled buffer indefinitely")
	assert.Falsef(t, d.borrowing,
		"a refused borrow still armed the decoder: borrowing=%v", d.borrowing)
}

// TestDecoder_EmptyPushDoesNotEndABorrow pins the matching guard on Push, which
// pump does reach: it falls back to Push whenever PushBorrowed declines, and an
// empty ev.Data always declines.
//
// Ending the borrow there would not be wrong, it would be waste — the
// undelivered remainder of the borrowed chunk copied into the decoder's own
// buffer, and the slab handed back, on a frame that delivered nothing.
func TestDecoder_EmptyPushDoesNotEndABorrow(t *testing.T) {
	whole, err := AppendMessage(nil, []byte("borrowed"))
	require.NoError(t, err, "AppendMessage")
	var d decoder
	require.True(t, d.PushBorrowed(whole, nil), "an empty decoder refused to borrow")

	d.Push(nil)

	require.Truef(t, d.borrowing,
		"an empty DATA frame ended a live borrow — the undelivered remainder was "+
			"copied into the decoder's own buffer for a chunk carrying no bytes")
	msg, ok, err := d.Next()
	require.NoErrorf(t, err, "Next = (ok=%v, err=%v)", ok, err)
	require.Truef(t, ok, "Next = (ok=%v, err=%v)", ok, err)
	assert.Equalf(t, "borrowed", string(msg), "message = %q after an empty push", msg)
}

func TestDecoder_Reset(t *testing.T) {
	var d decoder
	d.Push([]byte{0, 0, 0, 0, 9})

	d.Reset()

	assert.Equalf(t, 0, d.Pending(), "Pending after Reset = %d", d.Pending())
}

// Reset drops all pending bytes, keeping the allocated buffer for reuse.
//
// Test-only. It lived in framing.go with no production caller, and as a method
// on the real decoder it was a footgun: it clears buf and off without ending a
// borrow, so calling it mid-borrow would strand a pooled slab and truncate a
// chunk the decoder does not own. Nothing in the package wants that; the tests
// that use it never borrow.
func (d *decoder) Reset() {
	d.buf = d.buf[:0]
	d.off = 0
}
