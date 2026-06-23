package hpack

// realalloc_test.go measures HPACK allocations under REALISTIC browser-like
// traffic rather than pure static-table hits. The static-table benches in
// encoder_test.go / decoder_test.go exercise only indexed representations
// (0 alloc by construction); these benches force Huffman string literals and
// dynamic-table inserts on first touch, which is what real request headers
// actually look like on the wire.
//
// Only the WARM (steady-state) benches live here: they reuse a primed
// Encoder/Decoder, model a long-lived connection (the load-generator case),
// and are 0 alloc / 0 B per op — so they pass the absolute bench-gate while
// proving the hot path stays alloc-free even under Huffman-forcing traffic.
//
// The COLD per-connection cost (fresh codec each request) is NOT a hot-path
// benchmark — it is one-time codec construction (encoder dynamic-table arena
// + decoder scratch/table arenas, ~6 allocs / ~13 KB total, amortized over the
// connection lifetime). Those numbers are recorded in docs/BENCH_BASELINE.md
// rather than gated here, where a non-zero result would be a false positive.
//
// Caveat: this is PURE hpack. There are no per-request HeaderField slice
// allocations from a conn/stream layer, no map building, and the decode
// visitor deliberately does NOT copy out the aliased name/value slices
// (they alias d.scratch and are only valid for the visit call). A real
// client that retains decoded headers would add a copy per field on top.

import (
	"testing"
)

// realRequestFields returns a ~12-field browser-like request header set.
// Pseudo-headers (:method/:scheme/:authority/:path) and most regular header
// NAMES exist in the static table, but the VALUES below are deliberately not
// static-table values (the long cookie, the specific user-agent, the path,
// the authority, etc.), so the encoder must emit Huffman string literals and
// insert into the dynamic table on the first pass.
func realRequestFields() []HeaderField {
	return []HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},   // static value (idx 2)
		{Name: []byte(":scheme"), Value: []byte("https")}, // static value (idx 7)
		{Name: []byte(":authority"), Value: []byte("www.example.com")},
		{Name: []byte(":path"), Value: []byte("/api/v2/users/12345/profile?include=avatar,settings&lang=en")},
		{Name: []byte("user-agent"), Value: []byte("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")},
		{Name: []byte("accept"), Value: []byte("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")},
		{Name: []byte("accept-encoding"), Value: []byte("gzip, deflate, br")},
		{Name: []byte("accept-language"), Value: []byte("en-US,en;q=0.9,fr;q=0.8")},
		{Name: []byte("referer"), Value: []byte("https://www.example.com/dashboard")},
		{Name: []byte("cache-control"), Value: []byte("no-cache")},
		{Name: []byte("cookie"), Value: []byte("session_id=a3f8c1d29b4e7f60a1b2c3d4e5f6a7b8; csrf_token=9f8e7d6c5b4a39281706f5e4d3c2b1a0; theme=dark; consent=granted; _ga=GA1.2.345678901.1234567890; locale=en_US")},
		{Name: []byte("priority"), Value: []byte("u=0, i")}, // name not in static table -> literal name + value
	}
}

// touchField forces the decoder's aliased scratch slices to actually be read
// so the compiler can't optimize the visit away, WITHOUT copying them out.
// This models the conn layer reading header bytes during the visit call.
var sinkLen int

func touchField(f HeaderField) error {
	sinkLen += len(f.Name) + len(f.Value)
	return nil
}

// BenchmarkEncoder_RealRequest_Warm: SAME encoder reused across iterations.
// After the first pass the dynamic table holds every literal, so subsequent
// encodes resolve to indexed representations — steady-state cost on a
// long-lived connection sending repeated header sets. Must stay 0-alloc.
func BenchmarkEncoder_RealRequest_Warm(b *testing.B) {
	fields := realRequestFields()
	enc := NewEncoder()
	dst := make([]byte, 0, 512)
	enc.EncodeBlock(dst[:0], fields) // prime the dynamic table once
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = enc.EncodeBlock(dst[:0], fields)
	}
	_ = dst
}

// BenchmarkDecoder_RealRequest_Warm: SAME decoder reused. DecodeBlock resets
// scratch to len 0 keeping capacity; the warm block is almost entirely indexed
// references that need no scratch. Steady-state decode cost. Must stay 0-alloc.
func BenchmarkDecoder_RealRequest_Warm(b *testing.B) {
	fields := realRequestFields()
	enc := NewEncoder()
	cold := enc.EncodeBlock(make([]byte, 0, 512), fields) // primes encoder table
	warm := enc.EncodeBlock(make([]byte, 0, 512), fields) // mostly indexed refs

	d := NewDecoder()
	if err := d.DecodeBlock(cold, touchField); err != nil { // prime decoder table + scratch cap
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d.DecodeBlock(warm, touchField); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRoundtrip_RealRequest_Warm: encode then decode the realistic set
// each iteration, both codecs warmed — the real per-request HPACK cost a load
// generator pays once the connection is warmed up. Must stay 0-alloc.
func BenchmarkRoundtrip_RealRequest_Warm(b *testing.B) {
	fields := realRequestFields()
	enc := NewEncoder()
	dec := NewDecoder()
	dst := make([]byte, 0, 512)

	dst = enc.EncodeBlock(dst[:0], fields) // warm both codecs
	if err := dec.DecodeBlock(dst, touchField); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = enc.EncodeBlock(dst[:0], fields)
		if err := dec.DecodeBlock(dst, touchField); err != nil {
			b.Fatal(err)
		}
	}
	_ = dst
}
