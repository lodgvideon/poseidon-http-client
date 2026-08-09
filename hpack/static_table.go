package hpack

import "bytes"

// staticEntry holds one row of the HPACK static table (RFC 7541 App. A).
// Names and values are []byte to avoid string conversions on the hot path.
type staticEntry struct {
	name, value []byte
}

// staticTable is indexed 1..61; index 0 is unused (HPACK uses 1-based indices).
var staticTable = [62]staticEntry{
	0:  {nil, nil}, // unused
	1:  {[]byte(":authority"), nil},
	2:  {[]byte(":method"), []byte("GET")},
	3:  {[]byte(":method"), []byte("POST")},
	4:  {[]byte(":path"), []byte("/")},
	5:  {[]byte(":path"), []byte("/index.html")},
	6:  {[]byte(":scheme"), []byte("http")},
	7:  {[]byte(":scheme"), []byte("https")},
	8:  {[]byte(":status"), []byte("200")},
	9:  {[]byte(":status"), []byte("204")},
	10: {[]byte(":status"), []byte("206")},
	11: {[]byte(":status"), []byte("304")},
	12: {[]byte(":status"), []byte("400")},
	13: {[]byte(":status"), []byte("404")},
	14: {[]byte(":status"), []byte("500")},
	15: {[]byte("accept-charset"), nil},
	16: {[]byte("accept-encoding"), []byte("gzip, deflate")},
	17: {[]byte("accept-language"), nil},
	18: {[]byte("accept-ranges"), nil},
	19: {[]byte("accept"), nil},
	20: {[]byte("access-control-allow-origin"), nil},
	21: {[]byte("age"), nil},
	22: {[]byte("allow"), nil},
	23: {[]byte("authorization"), nil},
	24: {[]byte("cache-control"), nil},
	25: {[]byte("content-disposition"), nil},
	26: {[]byte("content-encoding"), nil},
	27: {[]byte("content-language"), nil},
	28: {[]byte("content-length"), nil},
	29: {[]byte("content-location"), nil},
	30: {[]byte("content-range"), nil},
	31: {[]byte("content-type"), nil},
	32: {[]byte("cookie"), nil},
	33: {[]byte("date"), nil},
	34: {[]byte("etag"), nil},
	35: {[]byte("expect"), nil},
	36: {[]byte("expires"), nil},
	37: {[]byte("from"), nil},
	38: {[]byte("host"), nil},
	39: {[]byte("if-match"), nil},
	40: {[]byte("if-modified-since"), nil},
	41: {[]byte("if-none-match"), nil},
	42: {[]byte("if-range"), nil},
	43: {[]byte("if-unmodified-since"), nil},
	44: {[]byte("last-modified"), nil},
	45: {[]byte("link"), nil},
	46: {[]byte("location"), nil},
	47: {[]byte("max-forwards"), nil},
	48: {[]byte("proxy-authenticate"), nil},
	49: {[]byte("proxy-authorization"), nil},
	50: {[]byte("range"), nil},
	51: {[]byte("referer"), nil},
	52: {[]byte("refresh"), nil},
	53: {[]byte("retry-after"), nil},
	54: {[]byte("server"), nil},
	55: {[]byte("set-cookie"), nil},
	56: {[]byte("strict-transport-security"), nil},
	57: {[]byte("transfer-encoding"), nil},
	58: {[]byte("user-agent"), nil},
	59: {[]byte("vary"), nil},
	60: {[]byte("via"), nil},
	61: {[]byte("www-authenticate"), nil},
}

// staticTableLen is the number of valid entries (1..staticTableLen inclusive).
const staticTableLen = 61

// staticNameEntry is every static-table row sharing one field name.
type staticNameEntry struct {
	// first is the lowest index carrying this name — what a name-only match
	// returns, per HPACK encoder convention.
	first uint64
	// vals holds each (value, index) for this name in ASCENDING index order, so
	// the first value match found is also the lowest-numbered one.
	vals []staticVal
}

type staticVal struct {
	value []byte
	idx   uint64
}

// staticByName indexes the static table by field name, built once at init.
//
// It replaces a linear scan of all 61 rows per field. A gRPC request carries
// nine fields, so that scan cost ~549 bytes.Equal calls per request and was the
// bulk of the warm encode (#452).
//
// Keyed by name only, deliberately. A name+value key would have to be built per
// lookup, and concatenating two byte slices allocates — which this package's
// absolute zero-alloc gate forbids. Looking up `m[string(name)]` does NOT
// allocate (the compiler elides the conversion for a map read), and the
// remaining value comparison walks a list that is one or two entries for every
// name except `:status`, which has seven.
var staticByName map[string]staticNameEntry

func init() {
	staticByName = make(map[string]staticNameEntry, staticTableLen)
	for i := 1; i <= staticTableLen; i++ {
		e := staticTable[i]
		key := string(e.name)
		ent, seen := staticByName[key]
		if !seen {
			ent.first = uint64(i)
		}
		ent.vals = append(ent.vals, staticVal{value: e.value, idx: uint64(i)})
		staticByName[key] = ent
	}
}

// staticIndex looks up a field in the static table.
// Returns (idx, fullMatch) where idx == 0 means no name match;
// fullMatch == true means name AND value match.
//
// For a name-only match, returns the FIRST entry whose name matches (lowest
// index), per HPACK encoder convention.
func staticIndex(name, value []byte) (uint64, bool) {
	ent, ok := staticByName[string(name)]
	if !ok {
		return 0, false
	}
	for i := range ent.vals {
		if bytes.Equal(ent.vals[i].value, value) {
			return ent.vals[i].idx, true
		}
	}
	return ent.first, false
}


