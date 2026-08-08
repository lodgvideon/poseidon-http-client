package qpack

import (
	"errors"
	"testing"
)

// RFC 9204 §4.5.1 / §4.5.1.1 — the Required Insert Count rules on the DECODE
// path, i.e. on bytes a peer chose.
//
// A guard-removal sweep scored this package 3/6: every one of the three misses
// was a Required Insert Count check, and deleting any of them broke no test.
// The package's Appendix B replay is genuinely strong, but it only ever walks
// well-formed encoder output, so nothing was driving the rejection arms.
//
// hpack, the sibling codec, scored 10/10 on the same sweep. That gap is the
// shape this repo produces most often: a concept is developed in one stack and
// silently not carried to the other.

// TestConformance_RFC9204_Sec451_RequiredInsertCountBeyondTableRejected pins
// that a field section referring to more insertions than the table has actually
// performed is rejected rather than decoded against whatever is present.
//
// This decoder is synchronous: it never blocks waiting for the encoder stream to
// catch up, so a Required Insert Count it cannot satisfy is unresolvable by
// definition. Accepting it would mean resolving dynamic references against the
// wrong entries — the peer's header values silently becoming other values.
func TestConformance_RFC9204_Sec451_RequiredInsertCountBeyondTableRejected(t *testing.T) {
	// maxCapacity 1024 -> maxEntries 32, so the prefix below decodes to a
	// Required Insert Count of 1 while the table has performed 0 insertions.
	dt := NewDynamicTable(1024)
	if dt.InsertCount() != 0 {
		t.Fatalf("fixture: InsertCount = %d, want 0", dt.InsertCount())
	}

	// Encoded Insert Count 2 (8-bit prefix) -> RIC 1; Delta Base 0, Sign 0.
	src := []byte{0x02, 0x00}

	d := NewDecoder()
	err := d.DecodeFieldSection(src, dt, func(_, _ []byte) error {
		t.Fatal("a field line was emitted from a section whose Required Insert Count is unsatisfiable")
		return nil
	})
	if !errors.Is(err, ErrDecompressionFailed) {
		t.Fatalf("RIC 1 against a table with 0 insertions: err = %v, want ErrDecompressionFailed", err)
	}
}

// TestConformance_RFC9204_Sec4511_EncodedInsertCountOutOfRangeRejected pins the
// two rejection arms of the §4.5.1.1 decoding algorithm, which the Appendix B
// vectors never reach because a conforming encoder never emits these.
//
// The algorithm maps an Encoded Insert Count back into a 2*MaxEntries
// wraparound window. Two results are not decodable and must be refused rather
// than wrapped into a plausible-looking value:
//
//   - a value that exceeds MaxValue but is not far enough past the window to be
//     unwrapped without underflowing, and
//   - a non-zero encoding that resolves to 0.
//
// Both would otherwise produce a Required Insert Count the peer never meant.
func TestConformance_RFC9204_Sec4511_EncodedInsertCountOutOfRangeRejected(t *testing.T) {
	// maxEntries 2 -> fullRange 4; totalInserts 0 -> maxValue 2, maxWrapped 0.
	const maxEntries, totalInserts = 2, 0

	t.Run("past_the_window_but_unwrappable", func(t *testing.T) {
		// enc 4 -> ric = 0 + 4 - 1 = 3, which is > maxValue 2 but <= fullRange 4,
		// so subtracting fullRange would underflow. Refuse.
		if _, err := reqInsertCountFromEncoded(4, maxEntries, totalInserts); !errors.Is(err, ErrDecompressionFailed) {
			t.Fatalf("encoded insert count 4 (maxEntries %d, inserts %d): err = %v, want ErrDecompressionFailed",
				maxEntries, totalInserts, err)
		}
	})

	t.Run("nonzero_encoding_resolving_to_zero", func(t *testing.T) {
		// enc 1 -> ric = 0 + 1 - 1 = 0. Zero is how "no dynamic references" is
		// spelled, and it has its own encoding (0); reaching it from a non-zero
		// encoding means the value did not survive the round trip.
		if _, err := reqInsertCountFromEncoded(1, maxEntries, totalInserts); !errors.Is(err, ErrDecompressionFailed) {
			t.Fatalf("encoded insert count 1 (maxEntries %d, inserts %d): err = %v, want ErrDecompressionFailed",
				maxEntries, totalInserts, err)
		}
	})

	// Controls, so the two above pin a rejection rather than a function that
	// refuses everything.
	t.Run("accepts_a_valid_encoding", func(t *testing.T) {
		// enc 0 always means "no dynamic references".
		if got, err := reqInsertCountFromEncoded(0, maxEntries, totalInserts); err != nil || got != 0 {
			t.Fatalf("encoded insert count 0 = (%d, %v), want (0, nil)", got, err)
		}
		// enc 2 -> ric = 0 + 2 - 1 = 1, within maxValue 2.
		if got, err := reqInsertCountFromEncoded(2, maxEntries, totalInserts); err != nil || got != 1 {
			t.Fatalf("encoded insert count 2 = (%d, %v), want (1, nil)", got, err)
		}
	})
}
