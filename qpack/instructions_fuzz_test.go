package qpack

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// This file fuzzes the two QPACK instruction streams (RFC 9204 §4.3, §4.4). Both
// parsers consume a raw, unframed, fully peer-controlled byte stream for the life
// of the connection, and both are driven here STATEFULLY — a sequence of chunks
// against one long-lived object — because a single buffer against a fresh table
// never reaches eviction, capacity changes, insert-count growth, or the
// absolute-index arithmetic that those states unlock.

// fuzzMaxTableCapacity bounds the maxCapacity a fuzz input may pick for a table.
// It is well above the 4096 the http3 client actually advertises, but small enough
// that eviction and the capacity-bounded storage invariants stay reachable within
// one fuzz input instead of needing megabytes of instructions to provoke.
const fuzzMaxTableCapacity = 1 << 16

// dtState is the logical (encoder-observable) state of a dynamic table: everything
// a peer's view of our table depends on. Scratch buffers and arena layout are
// deliberately excluded — they are private, and two tables fed the same
// instructions in different chunkings may legitimately differ there.
type dtState struct {
	capacity, size, insertCount uint64
	count                       int
	entries                     []string
}

// snapshotDT captures dt's logical state, walking every live absolute index so the
// entry bytes themselves — not just the counters — take part in a comparison.
func snapshotDT(dt *DynamicTable) dtState {
	s := dtState{
		capacity:    dt.capacity,
		size:        dt.size,
		insertCount: dt.insertCount,
		count:       dt.count,
	}
	for abs := dt.oldestAbs(); dt.count > 0 && abs < dt.insertCount; abs++ {
		name, value, ok := dt.at(abs)
		if !ok {
			s.entries = append(s.entries, "<unresolvable>")
			continue
		}
		s.entries = append(s.entries, string(name)+"\x00"+string(value))
	}
	return s
}

// sameDTState compares two logical snapshots.
func sameDTState(a, b dtState) bool {
	if a.capacity != b.capacity || a.size != b.size ||
		a.insertCount != b.insertCount || a.count != b.count {
		return false
	}
	if len(a.entries) != len(b.entries) {
		return false
	}
	for i := range a.entries {
		if a.entries[i] != b.entries[i] {
			return false
		}
	}
	return true
}

// checkDTInvariants asserts the RFC 9204 §3.2 dynamic-table invariants that every
// encoder instruction a peer sends must preserve. prevInsertCount is the insert
// count from before the instructions under test were applied.
func checkDTInvariants(t *testing.T, dt *DynamicTable, prevInsertCount uint64) {
	t.Helper()

	// §3.2.3: the capacity the encoder sets MUST NOT exceed the maximum the decoder
	// advertised — setCapacity is the only writer and must reject a larger one.
	if dt.capacity > dt.maxCapacity {
		t.Fatalf("capacity %d exceeds the advertised maxCapacity %d", dt.capacity, dt.maxCapacity)
	}
	// §3.2.2: entries are evicted until the table fits, so the live size can never
	// stand above the capacity. This is the bound the whole memory story rests on.
	if dt.size > dt.capacity {
		t.Fatalf("size %d exceeds capacity %d (eviction did not run)", dt.size, dt.capacity)
	}
	// §3.2.4: absolute indices are permanent, so the insert count only ever grows.
	if dt.insertCount < prevInsertCount {
		t.Fatalf("insert count went backwards: %d -> %d", prevInsertCount, dt.insertCount)
	}
	if dt.count < 0 || uint64(dt.count) > dt.insertCount {
		t.Fatalf("count %d is not within [0, insertCount %d]", dt.count, dt.insertCount)
	}

	// The counters must agree with the entries actually reachable: recompute the
	// live size by walking the window. Drift here is a silent partial apply — an
	// instruction that mutated one field and bailed before the others.
	var recomputed uint64
	for abs := dt.oldestAbs(); dt.count > 0 && abs < dt.insertCount; abs++ {
		name, value, ok := dt.at(abs)
		if !ok {
			t.Fatalf("live absolute index %d in [%d,%d) does not resolve",
				abs, dt.oldestAbs(), dt.insertCount)
		}
		recomputed += entrySize(len(name), len(value))
	}
	if recomputed != dt.size {
		t.Fatalf("size bookkeeping drift: recomputed %d from live entries, tracked %d",
			recomputed, dt.size)
	}

	// §3.2.5/§3.2.6 index arithmetic: nothing outside the live window may resolve.
	// An out-of-window index that resolves is the classic QPACK bug class — it hands
	// a peer an entry, or arena bytes, it never inserted.
	if _, _, ok := dt.at(dt.insertCount); ok {
		t.Fatalf("absolute index %d resolved past the insert count", dt.insertCount)
	}
	if oldest := dt.oldestAbs(); dt.count > 0 && oldest > 0 {
		if _, _, ok := dt.at(oldest - 1); ok {
			t.Fatalf("evicted absolute index %d still resolves", oldest-1)
		}
	}
	if _, _, ok := dt.at(^uint64(0)); ok {
		t.Fatalf("absolute index 2^64-1 resolved")
	}
	// §3.2.5: an encoder-stream relative index resolves to InsertCount-relIndex-1,
	// so it is live exactly while relIndex < count, and MUST NOT resolve at or past
	// the insert count, where that subtraction would wrap.
	for rel := uint64(0); rel < uint64(dt.count)+2; rel++ {
		_, _, ok := dt.atInsertCountRelative(rel)
		if want := rel < uint64(dt.count); ok != want {
			t.Fatalf("relative index %d resolvable=%v, want %v (count %d, insertCount %d)",
				rel, ok, want, dt.count, dt.insertCount)
		}
	}
	if _, _, ok := dt.atInsertCountRelative(^uint64(0)); ok {
		t.Fatalf("relative index 2^64-1 resolved (index arithmetic wrapped)")
	}

	// The capacity is the decoder's memory commitment (§3.2.2), so the table's
	// storage must stay proportional to IT. Every live entry costs >=32 bytes
	// (§3.2.1), so at most maxCapacity/32 can be live however many instructions
	// arrive: the backing ring and arena must track the capacity, never the
	// instruction count.
	maxLive := dt.maxCapacity/32 + 1
	if uint64(len(dt.entries)) > 2*maxLive+32 {
		t.Fatalf("entry ring grew to %d slots for a table bounded at %d live entries "+
			"(maxCapacity %d, count %d, insertCount %d): storage tracks the instruction "+
			"count, not the capacity",
			len(dt.entries), maxLive, dt.maxCapacity, dt.count, dt.insertCount)
	}
	if uint64(len(dt.arena)) > 4*dt.maxCapacity+1024 {
		t.Fatalf("arena grew to %d bytes for a table bounded at %d bytes (count %d, insertCount %d)",
			len(dt.arena), dt.maxCapacity, dt.count, dt.insertCount)
	}
}

// FuzzParseEncoderInstructions drives a SEQUENCE of QPACK encoder instructions
// (RFC 9204 §4.3) against ONE long-lived dynamic table, exactly the way http3's
// readQPACKEncoder does it: bytes arrive in chunks, whole instructions are applied
// and dropped, and a trailing partial instruction is retained for the next pass.
// The server's encoder stream is fully attacker-controlled and is the richest
// surface in the stack — bit-prefix dispatch over four instruction types, prefix
// varints, Huffman literals, insertion with eviction, and absolute-index
// arithmetic.
//
// Beyond "does not panic" it asserts, after every chunk: the consumed count is in
// bounds, any error is the typed QPACK_ENCODER_STREAM_ERROR, the RFC 9204 §3.2
// table invariants hold (checkDTInvariants), and — the differential property —
// feeding the same bytes in chunks reaches EXACTLY the state that feeding them in
// one call does. That last one is what "consume only whole instructions" means: an
// instruction half-applied before returning needMore, or a chunk boundary that
// changes the outcome, surfaces as a state mismatch instead of needing a crash.
func FuzzParseEncoderInstructions(f *testing.F) {
	// Valid encodings first, built by the package's own encoder, so the fuzzer
	// starts from bytes that already reach the insert paths rather than plateauing
	// on a malformed first byte.
	enc, err := NewDynamicEncoder(4096, 4096)
	if err != nil {
		f.Fatalf("NewDynamicEncoder: %v", err)
	}
	enc.EncodeFieldSection(nil, []hpack.HeaderField{
		{Name: []byte("x-custom"), Value: []byte("value")},
		{Name: []byte("cookie"), Value: []byte("a=b")},
	})
	live := enc.DrainEncoderInstructions(nil) // Set Capacity + two Inserts
	f.Add(uint64(4096), live, []byte(nil), []byte(nil))
	// The same stream split mid-instruction, so the corpus already holds a needMore
	// boundary for the mutator to work outward from.
	f.Add(uint64(4096), live[:len(live)/2], live[len(live)/2:], []byte(nil))

	setCap := func(c uint64) []byte { return hpack.EncodeInteger(nil, 5, 0x20, c) }
	// Set Capacity, Insert With Literal Name ("",""), Duplicate of relative index 0.
	f.Add(uint64(4096), setCap(4096), []byte{0x40, 0x00}, []byte{0x00})
	f.Add(uint64(4096), setCap(64), []byte{0x40, 0x01, 'a', 0x01, 'b'}, []byte{0x00})
	// Insert With Name Reference into the static table.
	f.Add(uint64(4096), setCap(4096), []byte{0xc0 | 15, 0x03, 'a', 'b', 'c'}, []byte(nil))
	f.Add(uint64(4096), setCap(4097), []byte(nil), []byte(nil)) // capacity above the maximum -> reject
	f.Add(uint64(4096), []byte{0x80}, []byte(nil), []byte(nil)) // dynamic name ref into an empty table
	f.Add(uint64(4096), []byte{0x00}, []byte(nil), []byte(nil)) // Duplicate of a nonexistent entry
	f.Add(uint64(4096), []byte{0x40, 0x7f}, []byte(nil), []byte(nil)) // literal name, truncated length varint
	// A literal name declaring a length near the top of the prefix-integer space.
	f.Add(uint64(4096), []byte{0x40, 0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}, []byte(nil), []byte(nil))
	f.Add(uint64(4096), []byte{0x60, 0x02, 0xff, 0xff}, []byte(nil), []byte(nil)) // Huffman literal name, invalid code
	f.Add(uint64(0), []byte{0x40, 0x00}, []byte(nil), []byte(nil))                // insert into a zero-capacity table
	// Repeated eviction at the minimum entry size.
	f.Add(uint64(32), setCap(32), []byte{0x40, 0x00, 0x40, 0x00, 0x40, 0x00}, []byte(nil))
	f.Add(uint64(4096), []byte{}, []byte{}, []byte{})

	f.Fuzz(func(t *testing.T, maxCap uint64, c1, c2, c3 []byte) {
		maxCap %= fuzzMaxTableCapacity
		chunks := [][]byte{c1, c2, c3}

		dt := NewDynamicTable(maxCap)
		var (
			buf           []byte // the retained partial-instruction tail, as http3 keeps it
			applied       []byte // every byte handed to the chunked table, in order
			totalConsumed int
			prevInsert    uint64
			chunkedErr    error
		)
		for _, chunk := range chunks {
			buf = append(buf, chunk...)
			applied = append(applied, chunk...)

			n, err := dt.ParseEncoderInstructions(buf)
			if n < 0 || n > len(buf) {
				t.Fatalf("consumed %d bytes of a %d-byte buffer", n, len(buf))
			}
			if err != nil && !errors.Is(err, ErrEncoderStream) {
				t.Fatalf("untyped error %v, want ErrEncoderStream (QPACK_ENCODER_STREAM_ERROR)", err)
			}
			totalConsumed += n
			checkDTInvariants(t, dt, prevInsert)
			prevInsert = dt.InsertCount()
			if err != nil {
				chunkedErr = err
				break // http3 kills the connection here, so stop feeding it bytes
			}
			buf = append(buf[:0], buf[n:]...) // drop the applied bytes, keep the tail
		}

		// Differential: one table, one call, the same bytes. A chunk boundary is a
		// buffering artifact and MUST NOT be observable in the resulting table.
		whole := NewDynamicTable(maxCap)
		wholeN, wholeErr := whole.ParseEncoderInstructions(applied)
		if (chunkedErr != nil) != (wholeErr != nil) {
			t.Fatalf("chunked err=%v but one-shot err=%v for the same %d bytes",
				chunkedErr, wholeErr, len(applied))
		}
		if wholeN != totalConsumed {
			t.Fatalf("one-shot consumed %d bytes, chunked consumed %d", wholeN, totalConsumed)
		}
		if got, want := snapshotDT(dt), snapshotDT(whole); !sameDTState(got, want) {
			t.Fatalf("chunked state %+v != one-shot state %+v", got, want)
		}
	})
}

// FuzzParseDecoderInstructions drives a SEQUENCE of QPACK decoder instructions
// (RFC 9204 §4.4) against ONE long-lived encoder, the way http3's readQPACKDecoder
// does. These are the acknowledgments the server's decoder sends about OUR table;
// believing one is what makes an entry referenceable, so a server that
// acknowledges more inserts than we ever made must be rejected rather than
// believed — an over-large Known Received Count would let the encoder reference an
// entry the peer does not have and wedge every later request on that connection.
//
// Beyond "does not panic" it asserts, after every chunk: the consumed count is in
// bounds, any error is the typed QPACK_DECODER_STREAM_ERROR, the Known Received
// Count never exceeds the insert count and never moves backwards (§2.1.4), decoder
// instructions never move the insert count, and the same chunked-vs-one-shot
// differential property.
func FuzzParseDecoderInstructions(f *testing.F) {
	f.Add(uint64(2), AppendInsertCountIncrement(nil, 1), []byte(nil), []byte(nil))
	f.Add(uint64(2), AppendInsertCountIncrement(nil, 1), AppendInsertCountIncrement(nil, 1), []byte(nil))
	f.Add(uint64(2), AppendSectionAcknowledgment(nil, 0), []byte(nil), []byte(nil))
	// A stream id large enough to need a multi-byte prefix integer.
	f.Add(uint64(2), AppendSectionAcknowledgment(nil, 1<<20), []byte(nil), []byte(nil))
	f.Add(uint64(2), AppendStreamCancellation(nil, 4), []byte(nil), []byte(nil))
	f.Add(uint64(2), AppendInsertCountIncrement(nil, 0), []byte(nil), []byte(nil))     // zero increment -> reject (§4.4.3)
	f.Add(uint64(2), AppendInsertCountIncrement(nil, 1<<40), []byte(nil), []byte(nil)) // ack past our inserts -> reject
	f.Add(uint64(2), []byte{0x3f, 0xff, 0xff}, []byte(nil), []byte(nil))               // truncated increment varint
	f.Add(uint64(2), []byte{0x3f}, []byte{0xff, 0xff, 0x00}, []byte(nil))              // the same, split at the boundary
	f.Add(uint64(0), AppendInsertCountIncrement(nil, 1), []byte(nil), []byte(nil))     // ack with nothing inserted
	f.Add(uint64(2), []byte{}, []byte{}, []byte{})

	f.Fuzz(func(t *testing.T, nInserts uint64, c1, c2, c3 []byte) {
		chunks := [][]byte{c1, c2, c3}
		// newEnc builds an encoder that has already inserted nInserts entries, so an
		// Insert Count Increment has a real ceiling to be checked against. Two are
		// built from the same recipe so the differential comparison starts equal.
		newEnc := func() *Encoder {
			e, err := NewDynamicEncoder(4096, 4096)
			if err != nil {
				t.Fatalf("NewDynamicEncoder: %v", err)
			}
			for i := uint64(0); i < nInserts%16; i++ {
				e.EncodeFieldSection(nil, []hpack.HeaderField{
					{Name: []byte("x-fuzz"), Value: []byte{byte('a' + i)}},
				})
			}
			e.DrainEncoderInstructions(nil)
			return e
		}

		enc := newEnc()
		insertCount := enc.InsertCount()
		var (
			buf           []byte
			applied       []byte
			totalConsumed int
			prevKnown     uint64
			chunkedErr    error
		)
		for _, chunk := range chunks {
			buf = append(buf, chunk...)
			applied = append(applied, chunk...)

			n, err := enc.ParseDecoderInstructions(buf)
			if n < 0 || n > len(buf) {
				t.Fatalf("consumed %d bytes of a %d-byte buffer", n, len(buf))
			}
			if err != nil && !errors.Is(err, ErrDecoderStream) {
				t.Fatalf("untyped error %v, want ErrDecoderStream (QPACK_DECODER_STREAM_ERROR)", err)
			}
			totalConsumed += n

			// §2.1.4: the Known Received Count counts OUR insertions the peer has
			// acknowledged, so it can never stand above them.
			known := enc.KnownReceivedCount()
			if known > insertCount {
				t.Fatalf("Known Received Count %d exceeds insert count %d: the peer was believed",
					known, insertCount)
			}
			if known < prevKnown {
				t.Fatalf("Known Received Count went backwards: %d -> %d", prevKnown, known)
			}
			if enc.InsertCount() != insertCount {
				t.Fatalf("decoder instructions moved the insert count %d -> %d",
					insertCount, enc.InsertCount())
			}
			prevKnown = known
			if err != nil {
				chunkedErr = err
				break // http3 kills the connection here
			}
			buf = append(buf[:0], buf[n:]...)
		}

		whole := newEnc()
		wholeN, wholeErr := whole.ParseDecoderInstructions(applied)
		if (chunkedErr != nil) != (wholeErr != nil) {
			t.Fatalf("chunked err=%v but one-shot err=%v for the same %d bytes",
				chunkedErr, wholeErr, len(applied))
		}
		if wholeN != totalConsumed {
			t.Fatalf("one-shot consumed %d bytes, chunked consumed %d", wholeN, totalConsumed)
		}
		if enc.KnownReceivedCount() != whole.KnownReceivedCount() {
			t.Fatalf("chunked Known Received Count %d != one-shot %d",
				enc.KnownReceivedCount(), whole.KnownReceivedCount())
		}
	})
}
