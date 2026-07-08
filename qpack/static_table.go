package qpack

// staticEntry is one row of the QPACK static table (RFC 9204 Appendix A).
type staticEntry struct {
	name  string
	value string
}

// staticNameB / staticValueB are []byte views of each static entry, filled once
// at init so the decoder can emit an indexed field without allocating.
var (
	staticNameB  [len(staticTable)][]byte
	staticValueB [len(staticTable)][]byte
)

func init() {
	for i := range staticTable {
		staticNameB[i] = []byte(staticTable[i].name)
		staticValueB[i] = []byte(staticTable[i].value)
	}
}

// staticTable is the QPACK static table: 99 entries indexed 0..98 (RFC 9204
// Appendix A). It is a DIFFERENT table from HPACK's (61 entries, 1-based) — do
// not substitute one for the other.
var staticTable = [99]staticEntry{
	{":authority", ""},
	{":path", "/"},
	{"age", "0"},
	{"content-disposition", ""},
	{"content-length", "0"},
	{"cookie", ""},
	{"date", ""},
	{"etag", ""},
	{"if-modified-since", ""},
	{"if-none-match", ""},
	{"last-modified", ""},
	{"link", ""},
	{"location", ""},
	{"referer", ""},
	{"set-cookie", ""},
	{":method", "CONNECT"},
	{":method", "DELETE"},
	{":method", "GET"},
	{":method", "HEAD"},
	{":method", "OPTIONS"},
	{":method", "POST"},
	{":method", "PUT"},
	{":scheme", "http"},
	{":scheme", "https"},
	{":status", "103"},
	{":status", "200"},
	{":status", "304"},
	{":status", "404"},
	{":status", "503"},
	{"accept", "*/*"},
	{"accept", "application/dns-message"},
	{"accept-encoding", "gzip, deflate, br"},
	{"accept-ranges", "bytes"},
	{"access-control-allow-headers", "cache-control"},
	{"access-control-allow-headers", "content-type"},
	{"access-control-allow-origin", "*"},
	{"cache-control", "max-age=0"},
	{"cache-control", "max-age=2592000"},
	{"cache-control", "max-age=604800"},
	{"cache-control", "no-cache"},
	{"cache-control", "no-store"},
	{"cache-control", "public, max-age=31536000"},
	{"content-encoding", "br"},
	{"content-encoding", "gzip"},
	{"content-type", "application/dns-message"},
	{"content-type", "application/javascript"},
	{"content-type", "application/json"},
	{"content-type", "application/x-www-form-urlencoded"},
	{"content-type", "image/gif"},
	{"content-type", "image/jpeg"},
	{"content-type", "image/png"},
	{"content-type", "text/css"},
	{"content-type", "text/html; charset=utf-8"},
	{"content-type", "text/plain"},
	{"content-type", "text/plain;charset=utf-8"},
	{"range", "bytes=0-"},
	{"strict-transport-security", "max-age=31536000"},
	{"strict-transport-security", "max-age=31536000; includesubdomains"},
	{"strict-transport-security", "max-age=31536000; includesubdomains; preload"},
	{"vary", "accept-encoding"},
	{"vary", "origin"},
	{"x-content-type-options", "nosniff"},
	{"x-xss-protection", "1; mode=block"},
	{":status", "100"},
	{":status", "204"},
	{":status", "206"},
	{":status", "302"},
	{":status", "400"},
	{":status", "403"},
	{":status", "421"},
	{":status", "425"},
	{":status", "500"},
	{"accept-language", ""},
	{"access-control-allow-credentials", "FALSE"},
	{"access-control-allow-credentials", "TRUE"},
	{"access-control-allow-headers", "*"},
	{"access-control-allow-methods", "get"},
	{"access-control-allow-methods", "get, post, options"},
	{"access-control-allow-methods", "options"},
	{"access-control-expose-headers", "content-length"},
	{"access-control-request-headers", "content-type"},
	{"access-control-request-method", "get"},
	{"access-control-request-method", "post"},
	{"alt-svc", "clear"},
	{"authorization", ""},
	{"content-security-policy", "script-src 'none'; object-src 'none'; base-uri 'none'"},
	{"early-data", "1"},
	{"expect-ct", ""},
	{"forwarded", ""},
	{"if-range", ""},
	{"origin", ""},
	{"purpose", "prefetch"},
	{"server", ""},
	{"timing-allow-origin", "*"},
	{"upgrade-insecure-requests", "1"},
	{"user-agent", ""},
	{"x-forwarded-for", ""},
	{"x-frame-options", "deny"},
	{"x-frame-options", "sameorigin"},
}

// staticByName maps a header name to every static-table index that carries it,
// in ascending index order. Built once at init; lookups use the
// map[string(b)] no-alloc special case.
var staticByName = func() map[string][]int {
	m := make(map[string][]int, len(staticTable))
	for i := range staticTable {
		m[staticTable[i].name] = append(m[staticTable[i].name], i)
	}
	return m
}()

// lookupStatic finds the best static-table entry for (name, value): exact when
// both match (→ Indexed Field Line), otherwise a name-only match (→ Literal
// Field Line with Name Reference) at the lowest such index. It allocates
// nothing (the map key and value comparison both use the []byte fast paths).
func lookupStatic(name, value []byte) (idx int, exact, nameMatch bool) {
	idxs, ok := staticByName[string(name)]
	if !ok {
		return 0, false, false
	}
	for _, i := range idxs {
		if staticTable[i].value == string(value) {
			return i, true, true
		}
	}
	return idxs[0], false, true
}
