package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func TestAppendMetadata_TextAndBinary(t *testing.T) {
	md, err := AppendMetadata(nil, "X-Request-Id", []byte("abc"))
	require.NoError(t, err, "AppendMetadata")
	require.Equalf(t, "x-request-id", string(md[0].Name), "name = %q, want it lowercased", md[0].Name)
	require.Equalf(t, "abc", string(md[0].Value), "value = %q, want it sent verbatim", md[0].Value)

	md, err = AppendMetadata(md, "trace-bin", []byte{0x00, 0xff, 0x10})

	require.NoError(t, err, "AppendMetadata(-bin)")
	require.Equalf(t, "AP8Q", string(md[1].Value), "binary value = %q, want base64", md[1].Value)
	got, ok, err := MetadataValue(md, "trace-bin")
	require.NoErrorf(t, err, "MetadataValue(-bin): ok=%v", ok)
	require.Truef(t, ok, "MetadataValue(-bin): ok=%v err=%v", ok, err)
	require.Equalf(t, "\x00\xff\x10", string(got), "decoded = % x", got)
}

func TestMetadataValue_UnpaddedBinaryAccepted(t *testing.T) {
	// gRPC permits a peer to omit base64 padding; the read side must cope.
	padded := []conn.HeaderField{{Name: []byte("k-bin"), Value: []byte("AP8Q")}}
	unpadded := []conn.HeaderField{{Name: []byte("k-bin"), Value: []byte("YQ")}}

	_, paddedOK, paddedErr := MetadataValue(padded, "k-bin")
	got, ok, err := MetadataValue(unpadded, "k-bin")

	require.NoErrorf(t, paddedErr, "padded form rejected: ok=%v", paddedOK)
	require.Truef(t, paddedOK, "padded form rejected: ok=%v err=%v", paddedOK, paddedErr)
	require.NoErrorf(t, err, "unpadded decode = %q ok=%v", got, ok)
	require.Truef(t, ok && string(got) == "a", "unpadded decode = %q ok=%v err=%v", got, ok, err)
}

// TestMetadataValue_MalformedBinaryIsNotAbsent pins the difference between "the
// peer sent nothing" and "the peer sent something undecodable". Folding the
// second into the first makes an application that reads a signature or a
// capability out of metadata take its nothing-to-check branch on a value the
// peer corrupted on purpose — fail-open on peer input.
func TestMetadataValue_MalformedBinaryIsNotAbsent(t *testing.T) {
	md := []conn.HeaderField{{Name: []byte("sig-bin"), Value: []byte("!!!not base64!!!")}}

	v, ok, err := MetadataValue(md, "sig-bin")
	missingV, missingOK, missingErr := MetadataValue(md, "missing-bin")

	require.True(t, ok, "ok = false — a present-but-corrupt value must not read as absent")
	require.Error(t, err, "err = nil, want a decode failure")
	require.Nilf(t, v, "value = %q, want nil alongside the error", v)
	assert.Falsef(t, missingOK, "absent key = ok=%v err=%v, want false/nil", missingOK, missingErr)
	assert.NoErrorf(t, missingErr, "absent key = ok=%v err=%v, want false/nil", missingOK, missingErr)
	assert.Nil(t, missingV, "an absent key must yield no value")
}

func TestAppendMetadata_RejectsReservedAndPseudo(t *testing.T) {
	// Owned by the transport, forbidden in HTTP/2 outright, or inside the
	// grpc- namespace the protocol reserves for itself.
	reserved := []string{
		"content-type", "TE", "grpc-timeout", "user-agent", "grpc-accept-encoding",
		"connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "host",
		"grpc-status", "grpc-message", "grpc-anything-future",
	}
	// Not a legal field name at all. A pseudo-header's colon is not a token
	// character, so these fail the syntax gate before the reserved-name gate.
	malformed := []string{":path", ":authority", "", "bad key", "x\r\ny: 1", "tab\tkey"}

	for _, k := range reserved {
		_, err := AppendMetadata(nil, k, []byte("x"))

		assert.ErrorIsf(t, err, ErrReservedMetadata,
			"AppendMetadata(%q) = %v, want ErrReservedMetadata", k, err)
	}
	for _, k := range malformed {
		_, err := AppendMetadata(nil, k, []byte("x"))

		assert.ErrorIsf(t, err, ErrInvalidMetadata,
			"AppendMetadata(%q) = %v, want ErrInvalidMetadata", k, err)
	}
}

// TestAppendMetadata_RejectsInjectionValues pins the send-side gate. HPACK
// length-prefixes a field value, so these bytes cannot split the HTTP/2 wire
// itself; the exposure is an HTTP/2-to-HTTP/1.1 downgrading intermediary, where
// CR and LF *are* the delimiters and a value carrying them becomes several
// fields and, past a blank line, an injected request.
func TestAppendMetadata_RejectsInjectionValues(t *testing.T) {
	bad := []string{
		"bob\r\nx-admin: true", "a\nb", "cr\rhere", "nul\x00byte", " leading", "trailing ", "\ttab",
	}

	for _, v := range bad {
		_, err := AppendMetadata(nil, "x-user", []byte(v))

		assert.ErrorIsf(t, err, ErrInvalidMetadata,
			"AppendMetadata(value=%q) = %v, want ErrInvalidMetadata", v, err)
	}
	_, err := AppendMetadata(nil, "x-user", []byte("plain-value"))

	assert.NoErrorf(t, err, "AppendMetadata(legal value) = %v, want nil", err)
}

// TestDefaultSensitiveField pins the credential list this package refuses to let
// into the HPACK dynamic table.
func TestDefaultSensitiveField(t *testing.T) {
	sensitive := []string{"authorization", "proxy-authorization", "cookie"}
	ordinary := []string{"x-request-id", "authorization-scheme", "", "auth"}

	for _, n := range sensitive {
		assert.Truef(t, defaultSensitiveField([]byte(n)), "%q not treated as sensitive", n)
	}
	for _, n := range ordinary {
		assert.Falsef(t, defaultSensitiveField([]byte(n)), "%q wrongly treated as sensitive", n)
	}
}

// TestEncodeTimeout_RoundsUp pins the direction of rounding: a server deadline
// shorter than what the caller asked for would cancel work the caller still
// considers live, so any remainder must round the value up.
func TestEncodeTimeout_RoundsUp(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "0n"},
		{-time.Second, "0n"},
		{1 * time.Nanosecond, "1n"},
		{1500 * time.Nanosecond, "1500n"},
		// 2s is 2e9ns, past the 8-digit ceiling, so the unit steps up to µs.
		{2 * time.Second, "2000000u"},
		// 100s is 1e8µs — one digit too many — so the unit steps up to ms.
		{100 * time.Second, "100000m"},
		{time.Hour, "3600000m"},
		// Every case above divides its chosen unit exactly, so none of them
		// reaches the rounding branch. These do — the remainder only exists
		// once the value is too large for nanoseconds, which is the atom.
		// Drop the round-up and each one loses its last digit, handing the
		// server a deadline shorter than the caller's.
		{time.Hour + time.Nanosecond, "3600001m"},
		{100*time.Second + time.Nanosecond, "100001m"},
		{100*time.Millisecond + time.Nanosecond, "100001u"},
		{40 * time.Hour, "144000S"},
		{100000 * time.Hour, "6000000M"},
	}

	for _, c := range cases {
		got := encodeTimeout(c.in)

		assert.Equalf(t, c.want, got, "encodeTimeout(%v) = %q, want %q", c.in, got, c.want)
	}
}

func TestEncodeTimeout_RoundTrip(t *testing.T) {
	durations := []time.Duration{
		time.Nanosecond, time.Microsecond, 250 * time.Millisecond,
		30 * time.Second, 5 * time.Minute, 3 * time.Hour,
	}

	for _, d := range durations {
		v := encodeTimeout(d)
		got, err := decodeTimeout(v)

		require.NoErrorf(t, err, "decodeTimeout(%q)", v)
		require.GreaterOrEqualf(t, got, d,
			"encodeTimeout(%v) = %q decoded to %v — shorter than requested", d, v, got)
	}
}

func TestEncodeTimeout_ValueNeverExceedsEightDigits(t *testing.T) {
	durations := []time.Duration{time.Nanosecond, time.Second, time.Hour, 1 << 62}

	for _, d := range durations {
		v := encodeTimeout(d)

		require.LessOrEqualf(t, len(v)-1, 8,
			"encodeTimeout(%v) = %q — value part exceeds the 8 digits the spec allows", d, v)
	}
}

func TestDecodeTimeout_Malformed(t *testing.T) {
	malformed := []string{"", "S", "abcS", "10x", "-5S"}

	for _, v := range malformed {
		_, err := decodeTimeout(v)

		assert.Errorf(t, err, "decodeTimeout(%q) = nil error, want failure", v)
	}
}

// encodeTimeout renders d as a grpc-timeout field value. Test-only: the
// production path appends into a caller's buffer (appendTimeout) precisely to
// avoid this allocation, so a string-returning twin belongs with the tests that
// want the convenience.
func encodeTimeout(d time.Duration) string {
	return string(appendTimeout(nil, d))
}

// TestMetadataValue_KeyIsCaseInsensitive pins the read side of the rule
// AppendMetadata already implements on the write side. gRPC metadata keys are
// case-insensitive and travel on the wire lowercased, so a caller reading back
// with the capitalisation they wrote must find the field they wrote.
//
// AppendMetadata's lowercasing is covered — TestAppendMetadata_TextAndBinary
// passes "X-Request-Id" — and MetadataValue's was not. Losing it does not raise
// an error: the lookup simply reads as absent, which is the fail-open shape
// TestMetadataValue_MalformedBinaryIsNotAbsent guards against one door along.
func TestMetadataValue_KeyIsCaseInsensitive(t *testing.T) {
	md, err := AppendMetadata(nil, "X-Request-Id", []byte("abc"))
	require.NoError(t, err, "AppendMetadata")
	md, err = AppendMetadata(md, "Trace-Bin", []byte{0x00, 0xff, 0x10})
	require.NoError(t, err, "AppendMetadata(-bin)")

	text, textOK, textErr := MetadataValue(md, "X-Request-Id")
	bin, binOK, binErr := MetadataValue(md, "TRACE-BIN")

	require.NoErrorf(t, textErr, "MetadataValue(\"X-Request-Id\"): ok=%v", textOK)
	require.Truef(t, textOK,
		"MetadataValue(\"X-Request-Id\") read as absent — the key the caller wrote "+
			"with no longer finds the field they wrote")
	assert.Equalf(t, "abc", string(text), "value = %q", text)
	require.NoErrorf(t, binErr, "MetadataValue(\"TRACE-BIN\"): ok=%v", binOK)
	require.Truef(t, binOK, "MetadataValue(\"TRACE-BIN\") read as absent")
	assert.Equalf(t, "\x00\xff\x10", string(bin),
		"decoded = % x — the -bin suffix is matched on the lowercased key too, so a "+
			"mixed-case one would hand the caller the base64 back verbatim", bin)
}
