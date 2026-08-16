package hpack

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// staticindex_bench_test.go measures staticIndex against the input distribution
// the encoder actually feeds it, rather than against one pinned field.
//
// staticIndex is called once per field on every encode, before the dynamic
// table is consulted (encoder.go, writeFieldAlreadyFlushedSize), so its input
// distribution is exactly the distribution of field names this client sends.
//
// Two facts about the table (RFC 7541 App. A) decide what that costs:
//
//   - Only rows 2..16 carry a non-empty value; every row from 17 on carries an
//     empty one. So for a field with a non-empty value a FULL match is
//     impossible past index 16.
//   - The linear scan this package used until #459 returned early ONLY on a
//     full match. A name-only match or a miss walked all 61 rows, because the
//     scan could not know that a later row would not also match on value.
//
// Together those mean the scan exited early for `:method`, for `:scheme`, and
// for `:path` only when the path is exactly `/` — and for nothing else a client
// sends. `:authority`, a real `:path`, `user-agent`, `content-type`, `cookie`,
// `accept-*`, `te`, `grpc-timeout` — every field whose value is per-request,
// and every name absent from the table — cost the full 61 rows. The client's
// own request sets are two early exits against seven to ten full walks:
//
//	grpc.buildHeaders   9 fields: 2 full matches, 4 name-only, 3 misses
//	realRequestFields  12 fields: 2 full matches, 9 name-only, 1 miss
//
// Every count above is pinned by TestStaticIndex_FixtureDistribution, so this
// comment fails CI rather than going stale.
//
// BenchmarkStaticIndex_Hit pinned `:method`/`GET` — index 2, one of the two
// early exits, the single most favourable input for the implementation #459
// replaced, and so the input on which a map can only lose. It is kept below
// under a name that says so. BenchmarkStaticIndex_RequestSet is the number to
// read (#685).
//
// The `impl=` axis benchmarks the map against refStaticIndex, the pre-#459 scan
// kept verbatim in staticindex_test.go as the correctness oracle. Both arms run
// in one binary in one process, so the comparison does not depend on this
// host's release-to-release noise floor:
//
//	go test ./hpack/ -run '^$' -bench 'StaticIndex_(ScanVsMap|RequestSet)' \
//	  -count=24 > out.txt && benchstat -col /impl out.txt

// staticProbe is one field lookup, with the result both implementations must
// produce. wantIdx/wantFull are asserted, so a label can never drift from the
// table position it names.
type staticProbe struct {
	label    string
	name     []byte
	value    []byte
	wantIdx  uint64 // index staticIndex returns; 0 means no name match
	wantFull bool   // name AND value matched
}

// staticProbes spans the distribution: the early full matches a request
// carries, name-only matches at the front and back of the table, the only
// multi-value name, and a miss. Every entry is a field this client really
// sends.
var staticProbes = []staticProbe{
	// The early exits — the only inputs on which the scan can beat the map, and
	// spaced 2/3/4/7 so the crossover between the two falls between two
	// measured points rather than being extrapolated. `:method`/`GET` is the old
	// pinned fixture, found by the scan in two rows; `:method`/`POST` is what
	// gRPC sends; `:path`/`/` is the one full match a request gets beyond the
	// two pseudo-headers, and only when the path is exactly the root.
	{"FullMatch_Idx2", []byte(":method"), []byte("GET"), 2, true},
	{"FullMatch_Idx3", []byte(":method"), []byte("POST"), 3, true},
	{"FullMatch_Idx4", []byte(":path"), []byte("/"), 4, true},
	{"FullMatch_Idx7", []byte(":scheme"), []byte("https"), 7, true},

	// Name-only matches: the scan walks all 61 rows for each of these
	// regardless of where the name sits, because it cannot stop until it has
	// ruled out a later full match. The map's cost tracks the name instead.
	{"NameOnly_Idx1", []byte(":authority"), []byte("www.example.com"), 1, false},
	{"NameOnly_Idx4", []byte(":path"), []byte("/api/v2/users/12345/profile?include=avatar"), 4, false},
	{"NameOnly_Idx31", []byte("content-type"), []byte("application/grpc"), 31, false},
	{"NameOnly_Idx58", []byte("user-agent"), []byte("poseidon-grpc/1.0"), 58, false},

	// `:status` is the only name with more than two rows — seven — so it is the
	// longest inner value walk the map can be made to do. A client encodes it
	// only when acting as a server, but it is the shape most at risk if the
	// lookup is ever restructured, which is why #459 pinned its ordering.
	{"NameOnly_Idx8_SevenValues", []byte(":status"), []byte("599"), 8, false},

	// A miss. Both implementations must rule out the whole table; the scan does
	// it in 61 comparisons, the map in one hash. Three of gRPC's nine fields
	// land here (`te`, `grpc-accept-encoding`, `grpc-timeout`).
	{"Miss", []byte("grpc-timeout"), []byte("999999u"), 0, false},
}

// grpcRequestFields mirrors the field set grpc.buildHeaders sends on every
// unary RPC (grpc/conn.go), values included, since the values are what decide
// full-match versus name-only.
func grpcRequestFields() []HeaderField {
	return []HeaderField{
		{Name: []byte(":method"), Value: []byte("POST")},  // static value (idx 3)
		{Name: []byte(":scheme"), Value: []byte("https")}, // static value (idx 7)
		{Name: []byte(":path"), Value: []byte("/helloworld.Greeter/SayHello")},
		{Name: []byte(":authority"), Value: []byte("api.example.com:443")},
		{Name: []byte("content-type"), Value: []byte("application/grpc")},
		{Name: []byte("user-agent"), Value: []byte("poseidon-grpc/1.0")},
		{Name: []byte("te"), Value: []byte("trailers")},                   // not in the table
		{Name: []byte("grpc-accept-encoding"), Value: []byte("identity")}, // not in the table
		{Name: []byte("grpc-timeout"), Value: []byte("999999u")},          // not in the table
	}
}

// TestStaticIndex_FixtureDistribution pins every factual claim the file comment
// above makes, so the rationale for these benchmarks cannot quietly stop being
// true — which is the failure mode #685 is a record of.
func TestStaticIndex_FixtureDistribution(t *testing.T) {
	// Claim: only rows 2..16 carry a non-empty value, so a field with a
	// non-empty value can never full-match past index 16. This is what makes
	// the scan's early exit unreachable for almost every real field.
	const lastValued = 16
	for i := lastValued + 1; i <= staticTableLen; i++ {
		assert.Emptyf(t, staticTable[i].value,
			"row %d (%q) carries value %q — the benchmark rationale assumes "+
				"no row past %d has one", i, staticTable[i].name, staticTable[i].value, lastValued)
	}

	// Claim: each probe's label names the position it actually resolves to.
	for _, p := range staticProbes {
		idx, full := staticIndex(p.name, p.value)

		assert.Equalf(t, p.wantIdx, idx,
			"probe %s: staticIndex(%q, %q) index = %d, want %d", p.label, p.name, p.value, idx, p.wantIdx)
		assert.Equalf(t, p.wantFull, full,
			"probe %s: staticIndex(%q, %q) full = %v, want %v", p.label, p.name, p.value, full, p.wantFull)
	}

	// Claim: the two request sets are two early exits against seven to ten full
	// table walks. If a fixture is edited, these counts move and the file
	// comment has to move with them.
	sets := []struct {
		name                             string
		fields                           []HeaderField
		wantFull, wantNameOnly, wantMiss int
	}{
		{"grpc.buildHeaders", grpcRequestFields(), 2, 4, 3},
		{"realRequestFields", realRequestFields(), 2, 9, 1},
	}
	for _, s := range sets {
		var gotFull, gotNameOnly, gotMiss int
		for i := range s.fields {
			switch idx, full := staticIndex(s.fields[i].Name, s.fields[i].Value); {
			case full:
				gotFull++
			case idx != 0:
				gotNameOnly++
			default:
				gotMiss++
			}
		}
		assert.Equalf(t, s.wantFull, gotFull,
			"%s: %d full / %d name-only / %d miss, want %d / %d / %d",
			s.name, gotFull, gotNameOnly, gotMiss, s.wantFull, s.wantNameOnly, s.wantMiss)
		assert.Equalf(t, s.wantNameOnly, gotNameOnly,
			"%s: %d full / %d name-only / %d miss, want %d / %d / %d",
			s.name, gotFull, gotNameOnly, gotMiss, s.wantFull, s.wantNameOnly, s.wantMiss)
		assert.Equalf(t, s.wantMiss, gotMiss,
			"%s: %d full / %d name-only / %d miss, want %d / %d / %d",
			s.name, gotFull, gotNameOnly, gotMiss, s.wantFull, s.wantNameOnly, s.wantMiss)
	}
}

// BenchmarkStaticIndex_Hit_BestCaseForScan is the fixture #685 is about, kept
// and renamed rather than deleted. `:method`/`GET` is static-table index 2,
// which the pre-#459 linear scan resolved in two comparisons — the cheapest
// input there is for a scan and the most expensive relative case for a map, so
// this benchmark reports a regression for a change that is a large net win on
// every real field set. It went 5.18 -> 7.86 ns/op across v0.12.0 -> v0.13.0,
// and that number is correct and unrepresentative at the same time. Read
// BenchmarkStaticIndex_RequestSet for the cost a request actually pays.
//
// The body is unchanged from BenchmarkStaticIndex_Hit so the historical numbers
// still compare, and `-bench StaticIndex_Hit` still selects it.
func BenchmarkStaticIndex_Hit_BestCaseForScan(b *testing.B) {
	name := []byte(":method")
	value := []byte("GET")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = staticIndex(name, value)
	}
}

// BenchmarkStaticIndex_ScanVsMap runs each probe through both implementations,
// so the position at which the map overtakes the scan can be read off directly
// instead of inferred from two releases measured hours apart.
func BenchmarkStaticIndex_ScanVsMap(b *testing.B) {
	for _, p := range staticProbes {
		b.Run("case="+p.label, func(b *testing.B) {
			// scan first: it is the implementation that was replaced, so
			// `benchstat -col /impl` takes it as the baseline and the ratio
			// column reads as the answer to "did the map help here".
			b.Run("impl=scan", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_, _ = refStaticIndex(p.name, p.value)
				}
			})
			b.Run("impl=map", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_, _ = staticIndex(p.name, p.value)
				}
			})
		})
	}
}

// BenchmarkStaticIndex_RequestSet is the representative measurement: one
// iteration is one request's worth of lookups, in the order the encoder
// performs them, so ns/op is per REQUEST and not per lookup.
//
// `browser` is realRequestFields, the same 12-field set
// BenchmarkEncoder_RealRequest_Warm encodes, so the two numbers relate directly.
// `grpc` is the 9-field set grpc.buildHeaders sends, which is the set #459 was
// measured against.
func BenchmarkStaticIndex_RequestSet(b *testing.B) {
	sets := []struct {
		label  string
		fields []HeaderField
	}{
		{"browser", realRequestFields()},
		{"grpc", grpcRequestFields()},
	}
	for _, s := range sets {
		b.Run("set="+s.label, func(b *testing.B) {
			b.Run("impl=scan", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					for j := range s.fields {
						_, _ = refStaticIndex(s.fields[j].Name, s.fields[j].Value)
					}
				}
			})
			b.Run("impl=map", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					for j := range s.fields {
						_, _ = staticIndex(s.fields[j].Name, s.fields[j].Value)
					}
				}
			})
		})
	}
}
