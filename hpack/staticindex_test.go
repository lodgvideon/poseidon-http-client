package hpack

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staticIndex went from a linear scan of all 61 rows to a name-keyed map. The
// contract is subtle enough that "it still passes the suite" is not evidence:
// a name-only match must return the LOWEST index carrying that name, and a
// full match must be the lowest index matching both. `:status` alone has seven
// rows, so an implementation that returned any-match-wins would look right on
// most inputs and be wrong on the one header every response carries.

// refStaticIndex is the pre-map implementation, kept verbatim as the oracle.
func refStaticIndex(name, value []byte) (uint64, bool) {
	var nameOnly uint64
	for i := 1; i <= staticTableLen; i++ {
		e := staticTable[i]
		if !bytes.Equal(e.name, name) {
			continue
		}
		if bytes.Equal(e.value, value) {
			return uint64(i), true
		}
		if nameOnly == 0 {
			nameOnly = uint64(i)
		}
	}
	return nameOnly, false
}

// TestStaticIndex_MatchesTheLinearScan drives every row of the table through
// both implementations, plus the cases that are not rows: a known name with an
// unknown value, an unknown name, and empty inputs.
func TestStaticIndex_MatchesTheLinearScan(t *testing.T) {
	type probe struct{ name, value []byte }
	probes := make([]probe, 0, staticTableLen*3+28)

	// Every row exactly as it appears.
	for i := 1; i <= staticTableLen; i++ {
		probes = append(probes, probe{staticTable[i].name, staticTable[i].value})
		// The same name with a value the table does not carry — the name-only path.
		probes = append(probes, probe{staticTable[i].name, []byte("\x00no-such-value")})
		// The same name with an empty value: distinct from the above because
		// several rows genuinely carry an empty value.
		probes = append(probes, probe{staticTable[i].name, nil})
	}
	// Names that are not in the table at all.
	for _, n := range []string{"x-custom", "", ":metho", ":methodx", "content-type"} {
		probes = append(probes, probe{[]byte(n), []byte("v")})
		probes = append(probes, probe{[]byte(n), nil})
	}
	// Cross-product of a few names against a few values, to catch a lookup that
	// pairs the right name with another name's value.
	names := [][]byte{[]byte(":status"), []byte(":method"), []byte("accept-encoding")}
	values := [][]byte{[]byte("200"), []byte("404"), []byte("GET"), []byte("POST"), []byte("gzip, deflate"), []byte("")}
	for _, n := range names {
		for _, v := range values {
			probes = append(probes, probe{n, v})
		}
	}

	for _, p := range probes {
		wantIdx, wantFull := refStaticIndex(p.name, p.value)

		gotIdx, gotFull := staticIndex(p.name, p.value)

		assert.Equalf(t, wantIdx, gotIdx,
			"staticIndex(%q, %q) index disagrees with the linear scan the map replaced", p.name, p.value)
		assert.Equalf(t, wantFull, gotFull,
			"staticIndex(%q, %q) full-match flag disagrees with the linear scan the map replaced", p.name, p.value)
	}
}

// TestStaticIndex_LowestIndexWins pins the two ordering rules directly, on the
// name that actually has duplicates, so a failure names the rule rather than
// just disagreeing with an oracle.
func TestStaticIndex_LowestIndexWins(t *testing.T) {
	// :status appears seven times (200, 204, 206, 304, 400, 404, 500). A
	// name-only match must return the first of them.
	first := uint64(0)
	for i := 1; i <= staticTableLen; i++ {
		if string(staticTable[i].name) == ":status" {
			first = uint64(i)
			break
		}
	}
	require.NotZero(t, first, ":status not in the static table — test premise broken")

	t.Run("name-only match returns the lowest index", func(t *testing.T) {
		idx, full := staticIndex([]byte(":status"), []byte("599"))

		assert.False(t, full, "staticIndex(:status, 599) reported a full match; 599 is not a value the table carries")
		assert.Equalf(t, first, idx,
			"staticIndex(:status, 599) = %d, want %d — a name-only match must return the LOWEST index carrying the name", idx, first)
	})

	t.Run("full match returns that value's own row", func(t *testing.T) {
		idx, full := staticIndex([]byte(":status"), []byte("404"))

		require.True(t, full, "staticIndex(:status, 404) reported no full match")
		assert.Equalf(t, "404", string(staticTable[idx].value),
			"staticIndex(:status, 404) = index %d, which holds %q — a full match must land on the row carrying that value, not on the first row sharing the name",
			idx, staticTable[idx].value)
	})
}

// TestStaticIndex_DoesNotAllocate pins what forced the design: a name+value key
// would have to be built per lookup, and concatenating two slices allocates.
// This package's bench gate is absolute, so a regression here fails CI — but it
// fails it in a benchmark, where the cause is far less obvious than here.
func TestStaticIndex_DoesNotAllocate(t *testing.T) {
	name, value := []byte(":status"), []byte("404")
	miss := []byte("x-not-in-table")

	// The closure stays free of testify: it reflects and allocates, and
	// AllocsPerRun charges the whole process. Assert outside.
	n := testing.AllocsPerRun(200, func() {
		_, _ = staticIndex(name, value)
		_, _ = staticIndex(name, []byte("599")) // name-only path
		_, _ = staticIndex(miss, value)         // miss path
	})

	assert.Zerof(t, n, "staticIndex allocates %.1f times per lookup set, want 0", n)
}
