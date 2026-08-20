package qpack

// encode_contracts_test.go — four encode-side contracts with no assertion behind
// them (#767, #768). Each is byte-visible on the wire or decides whether a
// constructor succeeds, and each survived being mutated away.

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQPACK_LiteralName_SensitiveNBit pins the §7.1 N bit on the Literal Field
// Line with LITERAL NAME (§4.5.6) — the representation used when the name is not
// in Appendix A, which is exactly where a secret-bearing custom header lands:
// x-api-key, x-session-token, a bearer in a vendor header.
//
// The existing §7.1 test pins the N bit only for `authorization`, whose name IS
// in the static table and which therefore takes the Name-Reference branch and a
// different prefix byte. Losing the N bit on the literal-name branch lets an
// intermediary index the value into its own dynamic table, which is the BREACH
// exposure RFC 9114 §10.3 forbids — and the branch that was pinned is the one
// where a secret is least likely to appear.
func TestQPACK_LiteralName_SensitiveNBit(t *testing.T) {
	const custom = "x-api-key"
	_, _, nameMatch := lookupStatic([]byte(custom), []byte("s3cr3t"))
	require.Falsef(t, nameMatch,
		"fixture: %q must be absent from RFC 9204 Appendix A, or the encoder takes the Name-Reference branch and this test pins the byte the other test already pins", custom)

	for _, tc := range []struct {
		name     string
		indexing header.IndexingMode
		wantTop  byte
	}{
		{"sensitive", header.IndexNever, 0x30},
		{"not sensitive", header.IndexIncremental, 0x20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := []header.Field{{Name: []byte(custom), Value: []byte("s3cr3t"), Indexing: tc.indexing}}

			buf := NewEncoder().EncodeFieldSection(nil, fields)

			require.Greater(t, len(buf), 2,
				"the section must carry a field line after the two-byte prefix, or there is no representation byte to inspect")
			assert.Equalf(t, tc.wantTop, buf[2]&0xf0,
				"field line %#x: §4.5.6 puts the N bit at 001N_Hxxx, so the top nibble must be %#x here — the pair of cases pins the flag rather than a constant, and a lost N bit lets an intermediary index the value",
				buf[2], tc.wantTop)
		})
	}
}

// TestQPACK_EncodeRequiredInsertCount_WrapsAtTwiceMaxEntries pins the modulo in
// the §4.5.1.1 Required Insert Count encoding. Through Encoder the wrap is
// unreachable — the encode-side table never evicts, so RIC <= MaxEntries — but
// the function is documented as the general inverse of reqInsertCountFromEncoded
// and is called directly by FuzzRequiredInsertCount, whose committed seeds all
// land below 2*MaxEntries. Dropping the modulo left the package green.
//
// Each case asserts the encoded value AND the round trip, because the encoding
// is only meaningful if the peer's decoder recovers the same absolute count from
// it: an encoded count outside the 2*MaxEntries window is rejected outright.
func TestQPACK_EncodeRequiredInsertCount_WrapsAtTwiceMaxEntries(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		ric, maxEntries, totalInserts uint64
		wantEncoded                   uint64
	}{
		{"inside the window", 5, 6, 5, 6},
		{"at the window", 12, 6, 12, 1},
		{"past the window", 20, 3, 20, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := encodeRequiredInsertCount(tc.ric, tc.maxEntries)

			assert.Equalf(t, tc.wantEncoded, enc,
				"§4.5.1.1 encodes the Required Insert Count as (RIC mod 2*MaxEntries) + 1; %d with MaxEntries %d must go out as %d, and an unwrapped value is one the peer's decoder refuses outright",
				tc.ric, tc.maxEntries, tc.wantEncoded)
			got, err := reqInsertCountFromEncoded(enc, tc.maxEntries, tc.totalInserts)
			require.NoErrorf(t, err,
				"the peer must be able to decode what we encode; an encoded count outside the 2*MaxEntries window is a QPACK_DECOMPRESSION_FAILED on a section we produced")
			assert.Equalf(t, tc.ric, got,
				"the wrap must be reversible against TotalNumberOfInserts %d: a recovered count that differs resolves every dynamic reference in the section against the wrong anchor", tc.totalInserts)
		})
	}
}

// TestNewDynamicEncoder_ClampsTableCapacityToPeerMaximum pins the §3.2.3 rule
// that the DECODER controls the table size. A caller asking for a larger table
// than the peer advertised must quietly get the peer's ceiling; without the clamp
// the constructor fails with ErrEncoderStream and the caller silently falls back
// to the static-only profile for the whole connection. Every call site in the
// suite passes tableCapacity == maxCapacity, so the clamp was never exercised.
func TestNewDynamicEncoder_ClampsTableCapacityToPeerMaximum(t *testing.T) {
	const peerMax = 4096

	enc, err := NewDynamicEncoder(peerMax, 2*peerMax)

	require.NoError(t, err,
		"§3.2.3: asking for more than the peer advertised is not an error, it is a request the peer's ceiling answers; refusing it costs the connection its dynamic table entirely")
	require.NotNil(t, enc, "a nil encoder with a nil error would be a worse failure than the refusal")
	assert.Equal(t, uint64(peerMax), enc.dt.Capacity(),
		"the effective capacity must be the peer's maximum, not the requested one; a table larger than the peer's evicts at a different point and the two sides disagree about which entries are live")
	// Set Dynamic Table Capacity (001xxxxx, 5-bit prefix) for 4096: 0x20|31 =
	// 0x3f, then 4096-31 = 4065 as a continuation (0xe1, 0x1f).
	assert.Equal(t, []byte{0x3f, 0xe1, 0x1f}, enc.DrainEncoderInstructions(nil),
		"the queued instruction must announce the CLAMPED capacity: it is what tells the peer's decoder how large our table is, and announcing the unclamped value would be refused as above its own maximum")
}

// TestLookupStatic_NameOnlyMatchReturnsTheLowestIndex pins the tie-break that
// lookupStatic's doc comment promises and nothing checked. The wire stays
// decodable either way, which is why no round-trip test can see it — but it is a
// documented, byte-visible contract, and the convention every QPACK encoder
// follows is the lowest index carrying the name.
func TestLookupStatic_NameOnlyMatchReturnsTheLowestIndex(t *testing.T) {
	// ":method" carries seven Appendix A rows, 15 (CONNECT) through 21 (PUT).
	// "PATCH" matches none of their values, so this is a name-only match.
	idx, exact, nameMatch := lookupStatic([]byte(":method"), []byte("PATCH"))

	assert.False(t, exact,
		"a name match with an unlisted value is not a full match; emitting an Indexed Field Line would send a method the caller never asked for")
	require.True(t, nameMatch,
		"the name IS in Appendix A, so the encoder must reference it rather than spelling it out on every request")
	assert.Equal(t, 15, idx,
		"a name-only match must return the LOWEST index carrying that name (index 15, ':method: CONNECT'); any other row is a different, equally valid-looking reference that this codec's own doc comment promises not to emit")

	got := NewEncoder().EncodeFieldSection(nil, []header.Field{hf(":method", "PATCH")})

	require.Greater(t, len(got), 3, "the section must carry the name reference and its value literal")
	assert.Equal(t, []byte{0x5f, 0x00}, got[2:4],
		"the §4.5.4 name reference goes on the wire as 01 N=0 T=1 with a 4-bit index prefix: 0x5f then 15-15 = 0; a different index here is a different byte string the peer reads literally")
}
