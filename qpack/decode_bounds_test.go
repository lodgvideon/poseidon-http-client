package qpack

// decode_bounds_test.go — the two peer-input guards on the Literal Field Line
// paths that no test reached (#757, #759). The rejecting halves live with their
// siblings in TestConformance_RFC9204_Sec45_DecodeErrors; the accepting halves
// are here, so each guard is pinned as a bound rather than as a function that
// refuses everything.

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQPACK_LiteralNameRef_LastStaticIndexDecodes is the accepting partner of the
// name_ref_static_index_out_of_range case: index 98 is the last Appendix A row
// and must resolve. The rejecting case alone is satisfied by a guard that refuses
// every index, which would be the worse failure of the two — it turns a
// conformant server's field section into a connection error.
func TestQPACK_LiteralNameRef_LastStaticIndexDecodes(t *testing.T) {
	// Prefix 00 00 | Literal with Name Reference, static (01 N=0 T=1, 4-bit index):
	// 0x50|15 = 0x5f then 98-15 = 83 (0x53) | value literal H=0, length 1, "1".
	in := []byte{0x00, 0x00, 0x5f, 0x53, 0x01, '1'}
	want := []header.Field{hf("x-frame-options", "1")}

	assertDecode(t, in, want)
}

// TestQPACK_EncoderStream_MalformedHuffmanLiteralIsEncoderStreamError drives the
// error arm of readInstrString, which nothing executed: Huffman literals are
// decoded all over this suite, but none of them ever fails to decode. The arm
// matters because the two string readers map their failures to DIFFERENT
// connection error codes — QPACK_ENCODER_STREAM_ERROR here, QPACK_DECOMPRESSION_
// FAILED on the field-section path — and the code the caller closes the
// connection with is decided by the arm that never ran.
func TestQPACK_EncoderStream_MalformedHuffmanLiteralIsEncoderStreamError(t *testing.T) {
	// The capacity has to be set first: with the default capacity of 0 the insert
	// would be refused for lack of room and produce ErrEncoderStream anyway, so
	// the test would pass without the Huffman decode ever deciding anything.
	newTable := func(t *testing.T) *DynamicTable {
		t.Helper()
		dt := NewDynamicTable(4096)
		mustApply(t, dt, hpack.EncodeInteger(nil, 5, 0x20, 4096))
		require.Equal(t, uint64(4096), dt.Capacity(),
			"fixture: the table must have room for the insert, or the rejection below comes from the capacity rather than from the Huffman decode")
		return dt
	}

	t.Run("undecodable Huffman name is refused", func(t *testing.T) {
		dt := newTable(t)
		// Insert With Literal Name (01Hxxxxx) with H=1 and a 1-octet name of 0xff.
		// Eight 1-bits are the leading bits of EOS and emit no symbol, so RFC 7541
		// §5.2 — the string encoding RFC 9204 §4.1.2 reuses — makes them a decoding
		// error. The value literal after it is well formed, so an implementation
		// that accepted the name would go on and complete the insert.
		instr := []byte{0x61, 0xff, 0x00}

		n, err := dt.ParseEncoderInstructions(instr)

		require.ErrorIs(t, err, ErrEncoderStream,
			"a Huffman literal on the ENCODER stream that does not decode is QPACK_ENCODER_STREAM_ERROR (§4.1.2, §6); reporting anything else closes the connection with the wrong code")
		assert.NotErrorIs(t, err, ErrDecompressionFailed,
			"this is the encoder stream, not a field section: the two failures carry different HTTP/3 error codes and a caller cannot distinguish them if both arms return the same sentinel")
		assert.Zero(t, n,
			"a malformed instruction consumes nothing, or the caller resumes the stream in the middle of the instruction that just failed")
		assert.Zero(t, dt.InsertCount(),
			"nothing may be inserted from an instruction that failed to parse; an entry built from undecoded bytes is a name the peer's encoder never sent")
	})

	t.Run("control: a well-formed Huffman name inserts", func(t *testing.T) {
		dt := newTable(t)
		hn := hpack.HuffmanEncode(nil, []byte("a"))
		instr := append(hpack.EncodeInteger(nil, 5, 0x60, uint64(len(hn))), hn...) // 01 H=1
		instr = append(instr, 0x00)                                                // empty value literal

		n, err := dt.ParseEncoderInstructions(instr)

		require.NoError(t, err,
			"control: the same instruction shape with a DECODABLE Huffman name must be accepted, or the case above pins the H bit rather than the decode failure")
		require.Equal(t, len(instr), n, "control: the whole well-formed instruction must be consumed")
		require.Equal(t, uint64(1), dt.InsertCount(), "control: the well-formed instruction must actually insert")
		name, _, ok := dt.at(0)
		require.True(t, ok, "control: the inserted entry must resolve")
		assert.Equal(t, "a", string(name), "control: the Huffman-coded name must decode to what was encoded")
	})
}
