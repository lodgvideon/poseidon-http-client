package hpack

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// #518 asks whether hpack's encoder never emitting Huffman is policy or a gap,
// given that the sibling qpack codec applies a shorter-wins heuristic. The
// answer has to be measured rather than argued: Huffman trades CPU per request
// for bytes on the wire, and for a load generator both sides of that trade are
// the product.
//
// The measurement that decides it is PER REQUEST ON A WARM CONNECTION, which is
// the regime a load generator spends essentially all of its time in. Measuring
// a pile of literals in isolation answers a different and much more flattering
// question, because on a warm connection almost every field is a static- or
// dynamic-table index and never reaches encodeStringLiteral at all — #438
// measured a warm encode of a 7-field gRPC request set at **7 bytes** total.
//
// So the honest split is by what a request actually sends literally:
//
//   - fixed path: nothing. Every field is indexed after the first request, and
//     Huffman has nothing to compress.
//   - varying path (/users/{id}, a query string): the :path value, every
//     request, because it is never the same twice and never becomes an index.
//
// The second case is the one worth costing, and it is common.

// h2Literals are values an H2 request can send literally. Kept as a set for the
// cold-connection figure; the per-request figures below use only what a warm
// connection still emits.
var h2Literals = []struct {
	what string
	s    string
}{
	{":authority", "api.example.com"},
	{":path fixed", "/v1/users"},
	{":path varying", "/v1/users/12345?filter=active&sort=created_at&page=7"},
	{"user-agent", "poseidon-loadgen/1.0 (+https://github.com/lodgvideon/poseidon-http-client)"},
	{"authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0"},
	{"content-type", "application/grpc+proto"},
	{"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	{"cookie", "session=8f14e45fceea167a5a36dedd4bea2543; theme=dark; lang=en-US"},
}

// varyingPath is what a load generator with per-request path cardinality sends:
// the one field that stays literal forever on a warm connection.
const varyingPath = "/v1/users/12345?filter=active&sort=created_at&page=7"

// TestHuffmanPolicy_MeasuredSavings reports what shorter-wins would buy, split
// into the cold-connection set and the warm per-request case that actually
// decides the policy. It asserts only the property the heuristic depends on —
// that the encoded form is never longer — and prints the rest, because the size
// question is a judgement recorded in #518, not a threshold to freeze here.
func TestHuffmanPolicy_MeasuredSavings(t *testing.T) {
	var plain, huff int
	for _, tc := range h2Literals {
		s := []byte(tc.s)
		hl := HuffmanEncodedLen(s)
		plain += len(s)
		best := len(s)
		if hl < len(s) {
			best = hl
		}
		huff += best
		t.Logf("%-16s plain=%3d huffman=%3d  %+.1f%%", tc.what, len(s), hl,
			-100.0*float64(len(s)-hl)/float64(len(s)))

		// "Use Huffman only when strictly shorter" means the encoded form can
		// never exceed the plain one. TestHuffmanEncodedLenMatchesEncode owns
		// the oracle in general; here it is the premise of the policy.
		assert.Equalf(t, hl, len(HuffmanEncode(nil, s)),
			"%s: HuffmanEncodedLen=%d but HuffmanEncode produced %d — the shorter-wins heuristic decides from the oracle, so a disagreement makes the decision on a length that never lands on the wire",
			tc.what, hl, len(HuffmanEncode(nil, s)))
	}
	t.Logf("cold connection, all literals: plain=%d shorter-wins=%d (%.1f%% saved)",
		plain, huff, 100.0*float64(plain-huff)/float64(plain))

	p := []byte(varyingPath)
	t.Logf("WARM connection, per request: fixed path saves 0 bytes (no literals at all); "+
		"varying path saves %d bytes (%d->%d)", len(p)-HuffmanEncodedLen(p), len(p), HuffmanEncodedLen(p))
}

// The CPU half of the trade, at the two granularities that matter. Both arms
// encode into a reused buffer, so the difference is the Huffman coding and not
// allocation — and both must stay at 0 allocs/op, which is what makes adopting
// the heuristic possible at all under this repo's bench gate.

// BenchmarkHuffmanPolicy_WarmRequest_Plain is the decisive pair: one varying
// :path value, which is everything a warm connection still sends literally.
func BenchmarkHuffmanPolicy_WarmRequest_Plain(b *testing.B) {
	benchOne(b, []byte(varyingPath), false)
}

func BenchmarkHuffmanPolicy_WarmRequest_ShorterWins(b *testing.B) {
	benchOne(b, []byte(varyingPath), true)
}

// BenchmarkHuffmanPolicy_ColdBlock_* is the whole literal set, i.e. the first
// request on a fresh connection.
func BenchmarkHuffmanPolicy_ColdBlock_Plain(b *testing.B) {
	benchAll(b, false)
}

func BenchmarkHuffmanPolicy_ColdBlock_ShorterWins(b *testing.B) {
	benchAll(b, true)
}

func benchOne(b *testing.B, v []byte, shorterWins bool) {
	dst := make([]byte, 0, 512)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = dst[:0]
		huff := shorterWins && HuffmanEncodedLen(v) < len(v)
		dst = encodeStringLiteral(dst, v, huff)
	}
	_ = fmt.Sprint()
}

func benchAll(b *testing.B, shorterWins bool) {
	vals := make([][]byte, len(h2Literals))
	for i, tc := range h2Literals {
		vals[i] = []byte(tc.s)
	}
	dst := make([]byte, 0, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = dst[:0]
		for _, v := range vals {
			huff := shorterWins && HuffmanEncodedLen(v) < len(v)
			dst = encodeStringLiteral(dst, v, huff)
		}
	}
}
