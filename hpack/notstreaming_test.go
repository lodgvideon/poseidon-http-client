package hpack

import (
	"errors"
	"testing"
)

// Feed and Finish used to report "you never called Begin" through
// ErrInvalidPrefix — the sentinel for a malformed representation byte from the
// peer. The two say opposite things about who is at fault: one is the caller's
// own sequencing, entirely local, and the other is a peer's bytes, which RFC 7541
// §5 makes a connection error. Anyone mapping sentinels to RFC sections was told
// a local bug came off the wire (#517).

// TestDecoder_FeedWithoutBegin_IsNotAWireError pins the distinction in both
// directions: the API-misuse sentinel is returned, and it is NOT the wire one.
//
// The second assertion is the one that matters. A dedicated sentinel that still
// reported itself as ErrInvalidPrefix under errors.Is would look like a fix and
// mislead exactly the caller this is for.
func TestDecoder_FeedWithoutBegin_IsNotAWireError(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(d *Decoder) error
	}{
		{"Feed", func(d *Decoder) error {
			return d.Feed([]byte{0x82}, func(HeaderField) error { return nil })
		}},
		{"Finish", func(d *Decoder) error { return d.Finish() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDecoder()
			err := tc.call(d)
			if !errors.Is(err, ErrNotStreaming) {
				t.Errorf("%s without Begin returned %v, want ErrNotStreaming", tc.name, err)
			}
			if errors.Is(err, ErrInvalidPrefix) {
				t.Errorf("%s without Begin still reports ErrInvalidPrefix — a caller "+
					"sequencing error is being attributed to a malformed byte from the peer",
					tc.name)
			}
		})
	}
}

// TestDecoder_FinishAfterFinish_IsNotStreaming covers the other way to reach it:
// a session closed by Finish is closed, so the next Finish is misuse rather than
// a truncated block.
func TestDecoder_FinishAfterFinish_IsNotStreaming(t *testing.T) {
	d := NewDecoder()
	d.Begin()
	if err := d.Finish(); err != nil {
		t.Fatalf("first Finish: %v", err)
	}
	if err := d.Finish(); !errors.Is(err, ErrNotStreaming) {
		t.Errorf("second Finish returned %v, want ErrNotStreaming", err)
	}
}

// TestDecoder_TruncatedBlockIsStillAWireError is the over-correction guard: a
// session that IS open but whose field block ended mid-representation must still
// report ErrTruncated. Renaming the misuse case must not reclassify the peer's.
func TestDecoder_TruncatedBlockIsStillAWireError(t *testing.T) {
	d := NewDecoder()
	d.Begin()
	// A literal-with-incremental-indexing prefix promising a name that never
	// arrives: the block ends mid-representation.
	if err := d.Feed([]byte{0x40, 0x05, 'a'}, func(HeaderField) error { return nil }); err != nil {
		t.Fatalf("Feed of a partial representation should buffer, not fail: %v", err)
	}
	err := d.Finish()
	if !errors.Is(err, ErrTruncated) {
		t.Errorf("Finish on a truncated block returned %v, want ErrTruncated", err)
	}
	if errors.Is(err, ErrNotStreaming) {
		t.Error("a truncated block is being reported as caller misuse")
	}
}
