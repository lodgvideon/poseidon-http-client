package qpack

import (
	"strconv"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/require"
)

// These tests exercise the ENCODE-side dynamic table (RFC 9204 §2.1, Q5): the
// client inserts repeated request-header entries into its own encoder dynamic
// table — the table the peer maintains as decoder — and references them once the
// peer acknowledges them. The golden proof is a round-trip: the encoder-stream
// Insert instructions and the request field sections are decoded back with the Q1
// decoder against a mirror of the peer's decode table, and must reproduce the
// original headers. A wrong Required Insert Count, Base, or index would fail the
// decode here exactly as it would fail a real server on the wire.

// TestConformance_RFC9204_Sec21_EncodeReferenceAfterAck drives the conservative
// insert-then-reference-after-ack strategy: the first request inserts and stays
// static; after the peer's Insert Count Increment the second request references the
// dynamic entries. Both round-trip to the identical headers.
//
// The three steps share one encoder deliberately: reference-after-ack is a state
// machine, and step 3's expected output exists only because steps 1 and 2 ran.
func TestConformance_RFC9204_Sec21_EncodeReferenceAfterAck(t *testing.T) {
	const serverMax = 4096
	enc, err := NewDynamicEncoder(serverMax, serverMax)
	require.NoError(t, err, "NewDynamicEncoder with a capacity that fits entries must succeed")
	// A mirror of the dynamic table the peer maintains as our decoder, driven only
	// by the encoder-stream instructions we produce.
	server := NewDynamicTable(serverMax)
	mustApply(t, server, enc.DrainEncoderInstructions(nil)) // Set Dynamic Table Capacity (§4.3.1)
	require.Equal(t, uint64(serverMax), server.Capacity(),
		"the first encoder-stream instruction must size the peer's table, or every later insert is measured against the wrong capacity")

	fields := []header.Field{
		hf(":method", "GET"),
		hf(":scheme", "https"),
		hf(":authority", "example.com"),
		hf(":path", "/"),
		hf("cookie", "sess=abc"),
	}

	// --- Request 1: nothing acknowledged, so the section is static-only and the two
	// non-static fields are inserted for future requests (RFC 9204 §2.1). ---
	sec1 := enc.EncodeFieldSection(nil, fields)
	insts1 := enc.DrainEncoderInstructions(nil)

	require.Equal(t, NewEncoder().EncodeFieldSection(nil, fields), sec1,
		"with nothing acknowledged the encoder may reference nothing, so request 1 must be byte-identical to the static-only profile — anything else is a reference the peer cannot resolve yet")
	require.NotEmpty(t, insts1,
		"request 1 must emit Insert instructions for the repeated headers, or the dynamic table is never populated and the whole strategy is inert")
	// The first insert is Insert With Name Reference to the static table (§4.3.2):
	// name ":authority" is static index 0, so the byte is 0xc0 (1T=11, index 0).
	require.Equalf(t, byte(0xc0), insts1[0],
		"first Insert instruction = %#x, want Insert With Name Reference static idx 0: re-encoding a name the static table already carries wastes encoder-stream bytes on every connection", insts1[0])
	mustApply(t, server, insts1)
	require.Equal(t, uint64(2), server.InsertCount(),
		"exactly the two non-static fields (:authority, cookie) are insert candidates")
	wantEntry(t, server, 0, ":authority", "example.com")
	wantEntry(t, server, 1, "cookie", "sess=abc")
	assertDecodeDyn(t, NewDecoder(), sec1, server, fields) // RIC 0 (static)

	// --- Peer acknowledges the two inserts with an Insert Count Increment (§4.4.3). ---
	_, aerr := enc.ParseDecoderInstructions(AppendInsertCountIncrement(nil, 2))

	require.NoError(t, aerr, "ParseDecoderInstructions(ICI+2)")
	require.Equal(t, uint64(2), enc.KnownReceivedCount(),
		"the Known Received Count (§2.1.4) is the only thing that makes an entry referenceable; if it does not advance, the encoder stays static forever")

	// --- Request 2: the same headers now reference the dynamic table. ---
	sec2 := enc.EncodeFieldSection(nil, fields)

	require.Empty(t, enc.DrainEncoderInstructions(nil),
		"the entries are already in the peer's table, so re-inserting them would duplicate state and burn encoder-stream bytes")
	ric, rerr := RequiredInsertCount(sec2, server)
	require.NoError(t, rerr, "RequiredInsertCount on a section this encoder just produced")
	require.Equal(t, uint64(2), ric,
		"both acknowledged entries are referenced, so §2.1.2 fixes the Required Insert Count at the largest referenced absolute index plus one")
	assertDecodeDyn(t, NewDecoder(), sec2, server, fields) // dynamic references, must round-trip
	require.Lessf(t, len(sec2), len(sec1),
		"dynamic section (%d bytes) not smaller than static (%d bytes): referencing the table is the entire reason to maintain it", len(sec2), len(sec1))
}

// TestConformance_RFC9204_Sec214_ReferenceOnlyAcknowledged proves the encoder
// references only entries at an absolute index below the Known Received Count: with
// a partial acknowledgment (only the first insert) the second entry stays static,
// so the Required Insert Count never runs ahead of what the peer has received.
func TestConformance_RFC9204_Sec214_ReferenceOnlyAcknowledged(t *testing.T) {
	const serverMax = 4096
	enc, err := NewDynamicEncoder(serverMax, serverMax)
	require.NoError(t, err, "NewDynamicEncoder")
	server := NewDynamicTable(serverMax)
	mustApply(t, server, enc.DrainEncoderInstructions(nil))
	fields := []header.Field{
		hf(":authority", "example.com"),
		hf("cookie", "sess=abc"),
	}
	_ = enc.EncodeFieldSection(nil, fields) // inserts abs 0 and abs 1
	mustApply(t, server, enc.DrainEncoderInstructions(nil))
	// Acknowledge only the first insert.
	_, err = enc.ParseDecoderInstructions(AppendInsertCountIncrement(nil, 1))
	require.NoError(t, err, "ParseDecoderInstructions(ICI+1)")

	sec := enc.EncodeFieldSection(nil, fields)

	ric, rerr := RequiredInsertCount(sec, server)
	require.NoError(t, rerr, "RequiredInsertCount")
	require.Equal(t, uint64(1), ric,
		"only abs 0 is acknowledged; a Required Insert Count of 2 would name an insertion the peer has not confirmed receiving, and that section blocks its request stream")
	assertDecodeDyn(t, NewDecoder(), sec, server, fields)
}

// TestConformance_RFC9204_Sec411_Integer62Bit pins the §4.1.1 MUST that a QPACK
// implementation decode prefixed integers up to and including 62 bits long. The
// shared HPACK reader stops at 2^32-1, which is enough for HTTP/2 but false-rejects
// a conformant server here: a Section Acknowledgment or Stream Cancellation carries
// a QUIC stream ID, legal up to 2^62-1.
func TestConformance_RFC9204_Sec411_Integer62Bit(t *testing.T) {
	const max62 = uint64(1)<<62 - 1
	for _, prefix := range []uint8{3, 4, 5, 6, 7, 8} {
		t.Run(strconv.Itoa(int(prefix)), func(t *testing.T) {
			t.Run("at_the_ceiling", func(t *testing.T) {
				buf := hpack.EncodeInteger(nil, prefix, 0x00, max62)

				got, n, err := decodeInt(buf, prefix)

				require.NoErrorf(t, err, "decodeInt(%x, %d): 2^62-1 is a legal QUIC stream ID, so rejecting it false-rejects a conformant peer", buf, prefix)
				require.Equalf(t, max62, got, "decodeInt(%x, %d) value", buf, prefix)
				require.Equalf(t, len(buf), n, "decodeInt(%x, %d) must consume the whole encoding, or the next instruction is parsed from the middle of this one", buf, prefix)
			})
			t.Run("past_the_ceiling", func(t *testing.T) {
				over := hpack.EncodeInteger(nil, prefix, 0x00, uint64(1)<<62)

				_, _, err := decodeInt(over, prefix)

				require.ErrorIs(t, err, hpack.ErrIntegerOverflow,
					"2^62 is past what §4.1.1 requires and past what QUIC can carry; accepting it would let a peer drive an unbounded value into an index or length")
			})
		})
	}
}

// TestConformance_RFC9204_Sec71_SensitiveNeverIndexed pins the RFC 9114 §10.3 MUST
// NOT against the dynamic encode path: a field marked sensitive must never enter
// the connection-wide compression context, however often it repeats, and must go
// out as a literal with the N bit set (RFC 9204 §7.1). The dynamic table is shared
// by every request on the connection, so an indexed secret is the BREACH opening.
func TestConformance_RFC9204_Sec71_SensitiveNeverIndexed(t *testing.T) {
	enc, err := NewDynamicEncoder(4096, 4096)
	require.NoError(t, err, "NewDynamicEncoder")
	enc.DrainEncoderInstructions(nil) // drop the Set Dynamic Table Capacity instruction
	secret := []header.Field{{Name: []byte("authorization"), Value: []byte("Bearer s3cr3t"), Indexing: hpack.IndexNever}}

	// Encode it three times: without the sensitive flag the second pass would
	// insert it and the third would reference it Base-relative.
	for round := 0; round < 3; round++ {
		buf := enc.EncodeFieldSection(nil, secret)

		require.Equalf(t, uint64(0), enc.InsertCount(),
			"round %d: the secret entered the dynamic table, which every request on this connection shares — that is the BREACH opening §10.3 forbids", round)
		require.Emptyf(t, enc.DrainEncoderInstructions(nil),
			"round %d: an encoder-stream Insert would put the secret in the peer's table too", round)
		require.Equalf(t, []byte{0x00, 0x00}, buf[:2],
			"round %d: prefix must stay Required Insert Count 0, Base 0 — a dynamic reference here means the secret is indexed somewhere", round)
		// "authorization" is static index 84 (name-only match), so the field line is
		// a Literal with static Name Reference: 01 N=1 T=1 -> the N bit must be set.
		require.Equalf(t, byte(0x70), buf[2]&0xf0,
			"round %d: field line %#x must carry N=1, or an intermediary is free to index the secret into its own table", round, buf[2])
	}

	// Control: the same field without the flag is inserted and then referenced, so
	// the three rounds above pin the sensitive flag rather than an encoder that
	// never inserts anything.
	plain := []header.Field{{Name: []byte("authorization"), Value: []byte("Bearer s3cr3t")}}
	enc2, err := NewDynamicEncoder(4096, 4096)
	require.NoError(t, err, "NewDynamicEncoder")

	enc2.EncodeFieldSection(nil, plain)

	require.NotZero(t, enc2.InsertCount(),
		"control: a non-sensitive repeated field was never inserted, so the rounds above prove nothing about the sensitive flag")
}

// TestConformance_RFC9204_Sec44_ParseDecoderInstructions covers the encode-side
// consumption of the peer's decoder stream (§4.4): an Insert Count Increment
// advances the Known Received Count, a zero increment or one past the inserts made
// is a QPACK_DECODER_STREAM_ERROR, Section Acknowledgment / Stream Cancellation are
// consumed as no-ops, and a partial instruction is left for the next call.
func TestConformance_RFC9204_Sec44_ParseDecoderInstructions(t *testing.T) {
	newEnc := func(t *testing.T, inserts int) *Encoder {
		t.Helper()
		enc, err := NewDynamicEncoder(4096, 4096)
		require.NoError(t, err, "NewDynamicEncoder")
		enc.DrainEncoderInstructions(nil) // discard the Set Capacity instruction
		if inserts > 0 {
			fields := make([]header.Field, inserts)
			for i := range fields {
				fields[i] = hf("x-h"+strconv.Itoa(i), "v")
			}
			enc.EncodeFieldSection(nil, fields)
			enc.DrainEncoderInstructions(nil)
			require.Equalf(t, uint64(inserts), enc.InsertCount(),
				"setup inserted %d, want %d (raise capacity)", enc.InsertCount(), inserts)
		}
		return enc
	}
	t.Run("ICI_advances_known", func(t *testing.T) {
		enc := newEnc(t, 3)

		n, err := enc.ParseDecoderInstructions(AppendInsertCountIncrement(nil, 3))

		require.NoError(t, err, "ICI+3")
		require.NotZero(t, n, "ICI+3 must be consumed, or it is re-applied on the next call and double-counts")
		require.Equal(t, uint64(3), enc.KnownReceivedCount(),
			"the Known Received Count gates every dynamic reference; if it lags, the encoder never references anything it inserted")
	})
	t.Run("zero_increment_error", func(t *testing.T) {
		enc := newEnc(t, 1)

		_, err := enc.ParseDecoderInstructions([]byte{0x00})

		require.ErrorIs(t, err, ErrDecoderStream,
			"§4.4.3 forbids a zero increment; accepting it hides a peer whose decoder stream is desynchronised from ours")
	})
	t.Run("increment_past_inserts_error", func(t *testing.T) {
		enc := newEnc(t, 1)

		_, err := enc.ParseDecoderInstructions(AppendInsertCountIncrement(nil, 2))

		require.ErrorIs(t, err, ErrDecoderStream,
			"the peer cannot have received more insertions than we sent; taking its word for it would let it mark unsent entries referenceable")
	})
	t.Run("section_ack_and_cancel_noop", func(t *testing.T) {
		enc := newEnc(t, 2)
		buf := AppendSectionAcknowledgment(nil, 4)
		buf = AppendStreamCancellation(buf, 8)

		n, err := enc.ParseDecoderInstructions(buf)

		require.NoError(t, err, "SectionAck+Cancel")
		require.Equal(t, len(buf), n,
			"both instructions must be consumed whole; a byte left behind is re-read as the start of a different instruction")
		require.Equal(t, uint64(0), enc.KnownReceivedCount(),
			"neither instruction acknowledges an insertion, so treating one as an increment would make unacknowledged entries referenceable")
	})
	t.Run("partial_instruction_retained", func(t *testing.T) {
		enc := newEnc(t, 64)
		full := AppendInsertCountIncrement(nil, 64) // 6-bit prefix overflows → 2 bytes
		require.GreaterOrEqualf(t, len(full), 2,
			"fixture: expected a multi-byte increment, got %x — with a single byte there is no partial form to test", full)

		nPartial, errPartial := enc.ParseDecoderInstructions(full[:len(full)-1])
		nFull, errFull := enc.ParseDecoderInstructions(full)

		require.NoError(t, errPartial, "a truncated instruction is not malformed; the decoder stream arrives in arbitrary pieces")
		require.Zero(t, nPartial, "no byte of an incomplete instruction may be consumed, or the retained prefix is lost")
		require.NoError(t, errFull, "resumed ICI")
		require.Equal(t, len(full), nFull, "the complete instruction must be consumed whole on the retry")
		require.Equal(t, uint64(64), enc.KnownReceivedCount(),
			"the increment must apply exactly once across the split, not zero times and not twice")
	})
}

// TestConformance_RFC9204_Sec322_EncoderNeverEvicts confirms the encode side stays
// eviction-free: given more distinct headers than the table can hold, it inserts
// only those that fit and leaves the rest literal, so the peer's mirror never
// evicts (live entries equal the insert count) and its size stays within capacity.
func TestConformance_RFC9204_Sec322_EncoderNeverEvicts(t *testing.T) {
	const capacity = 256
	enc, err := NewDynamicEncoder(capacity, capacity)
	require.NoError(t, err, "NewDynamicEncoder")
	server := NewDynamicTable(capacity)
	mustApply(t, server, enc.DrainEncoderInstructions(nil))
	fields := make([]header.Field, 50)
	for i := range fields {
		fields[i] = hf("x-key-"+strconv.Itoa(i), "value-"+strconv.Itoa(i))
	}

	sec := enc.EncodeFieldSection(nil, fields)

	mustApply(t, server, enc.DrainEncoderInstructions(nil)) // fails if any insert forced an eviction
	require.NotZero(t, enc.InsertCount(),
		"at least one header must fit and be inserted, or this test would pass against an encoder that never uses the table at all")
	require.Equal(t, int(server.InsertCount()), server.Len(),
		"an eviction occurred: an entry an in-flight request already references would then be gone from the peer's table, and that request fails to decode")
	require.LessOrEqual(t, server.Size(), uint64(capacity),
		"the mirror must stay within the capacity it was told about")
	assertDecodeDyn(t, NewDecoder(), sec, server, fields) // still round-trips (RIC 0, nothing acked)
}

// TestNewDynamicEncoder_RejectsTinyCapacity keeps the dynamic encoder off when the
// peer advertises a capacity too small to hold any entry (below 32, MaxEntries 0):
// the caller falls back to the static-only NewEncoder.
func TestNewDynamicEncoder_RejectsTinyCapacity(t *testing.T) {
	for _, maxCapacity := range []uint64{0, 16, 31} {
		t.Run(strconv.FormatUint(maxCapacity, 10), func(t *testing.T) {
			_, err := NewDynamicEncoder(maxCapacity, maxCapacity)

			require.ErrorIsf(t, err, ErrEncoderStream,
				"NewDynamicEncoder(%d): no entry fits below 32 bytes and MaxEntries would be 0, so the Required Insert Count encoding has no window — the caller must fall back to NewEncoder instead of getting a table it can never use", maxCapacity)
		})
	}
}
