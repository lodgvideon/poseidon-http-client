package grpc

import (
	"errors"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func TestAppendMetadata_TextAndBinary(t *testing.T) {
	md, err := AppendMetadata(nil, "X-Request-Id", []byte("abc"))
	if err != nil {
		t.Fatalf("AppendMetadata: %v", err)
	}
	if string(md[0].Name) != "x-request-id" {
		t.Fatalf("name = %q, want it lowercased", md[0].Name)
	}
	if string(md[0].Value) != "abc" {
		t.Fatalf("value = %q, want it sent verbatim", md[0].Value)
	}

	md, err = AppendMetadata(md, "trace-bin", []byte{0x00, 0xff, 0x10})
	if err != nil {
		t.Fatalf("AppendMetadata(-bin): %v", err)
	}
	if string(md[1].Value) != "AP8Q" {
		t.Fatalf("binary value = %q, want base64", md[1].Value)
	}
	got, ok, err := MetadataValue(md, "trace-bin")
	if !ok || err != nil {
		t.Fatalf("MetadataValue(-bin): ok=%v err=%v", ok, err)
	}
	if string(got) != "\x00\xff\x10" {
		t.Fatalf("decoded = % x", got)
	}
}

func TestMetadataValue_UnpaddedBinaryAccepted(t *testing.T) {
	// gRPC permits a peer to omit base64 padding; the read side must cope.
	md := []conn.HeaderField{{Name: []byte("k-bin"), Value: []byte("AP8Q")}}
	if _, ok, err := MetadataValue(md, "k-bin"); !ok || err != nil {
		t.Fatalf("padded form rejected: ok=%v err=%v", ok, err)
	}
	md = []conn.HeaderField{{Name: []byte("k-bin"), Value: []byte("YQ")}}
	got, ok, err := MetadataValue(md, "k-bin")
	if !ok || err != nil || string(got) != "a" {
		t.Fatalf("unpadded decode = %q ok=%v err=%v", got, ok, err)
	}
}

// TestMetadataValue_MalformedBinaryIsNotAbsent pins the difference between "the
// peer sent nothing" and "the peer sent something undecodable". Folding the
// second into the first makes an application that reads a signature or a
// capability out of metadata take its nothing-to-check branch on a value the
// peer corrupted on purpose — fail-open on peer input.
func TestMetadataValue_MalformedBinaryIsNotAbsent(t *testing.T) {
	md := []conn.HeaderField{{Name: []byte("sig-bin"), Value: []byte("!!!not base64!!!")}}
	v, ok, err := MetadataValue(md, "sig-bin")
	if !ok {
		t.Fatal("ok = false — a present-but-corrupt value must not read as absent")
	}
	if err == nil {
		t.Fatal("err = nil, want a decode failure")
	}
	if v != nil {
		t.Fatalf("value = %q, want nil alongside the error", v)
	}
	if _, ok, err := MetadataValue(md, "missing-bin"); ok || err != nil {
		t.Fatalf("absent key = ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestAppendMetadata_RejectsReservedAndPseudo(t *testing.T) {
	// Owned by the transport, forbidden in HTTP/2 outright, or inside the
	// grpc- namespace the protocol reserves for itself.
	for _, k := range []string{
		"content-type", "TE", "grpc-timeout", "user-agent", "grpc-accept-encoding",
		"connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "host",
		"grpc-status", "grpc-message", "grpc-anything-future",
	} {
		if _, err := AppendMetadata(nil, k, []byte("x")); !errors.Is(err, ErrReservedMetadata) {
			t.Errorf("AppendMetadata(%q) = %v, want ErrReservedMetadata", k, err)
		}
	}
	// Not a legal field name at all. A pseudo-header's colon is not a token
	// character, so these fail the syntax gate before the reserved-name gate.
	for _, k := range []string{":path", ":authority", "", "bad key", "x\r\ny: 1", "tab\tkey"} {
		if _, err := AppendMetadata(nil, k, []byte("x")); !errors.Is(err, ErrInvalidMetadata) {
			t.Errorf("AppendMetadata(%q) = %v, want ErrInvalidMetadata", k, err)
		}
	}
}

// TestAppendMetadata_RejectsInjectionValues pins the send-side gate. HPACK
// length-prefixes a field value, so these bytes cannot split the HTTP/2 wire
// itself; the exposure is an HTTP/2-to-HTTP/1.1 downgrading intermediary, where
// CR and LF *are* the delimiters and a value carrying them becomes several
// fields and, past a blank line, an injected request.
func TestAppendMetadata_RejectsInjectionValues(t *testing.T) {
	for _, v := range []string{
		"bob\r\nx-admin: true", "a\nb", "cr\rhere", "nul\x00byte", " leading", "trailing ", "\ttab",
	} {
		if _, err := AppendMetadata(nil, "x-user", []byte(v)); !errors.Is(err, ErrInvalidMetadata) {
			t.Errorf("AppendMetadata(value=%q) = %v, want ErrInvalidMetadata", v, err)
		}
	}
	if _, err := AppendMetadata(nil, "x-user", []byte("plain-value")); err != nil {
		t.Errorf("AppendMetadata(legal value) = %v, want nil", err)
	}
}

// TestDefaultSensitiveField pins the credential list this package refuses to let
// into the HPACK dynamic table.
func TestDefaultSensitiveField(t *testing.T) {
	for _, n := range []string{"authorization", "proxy-authorization", "cookie"} {
		if !defaultSensitiveField([]byte(n)) {
			t.Errorf("%q not treated as sensitive", n)
		}
	}
	for _, n := range []string{"x-request-id", "authorization-scheme", "", "auth"} {
		if defaultSensitiveField([]byte(n)) {
			t.Errorf("%q wrongly treated as sensitive", n)
		}
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
		if got := encodeTimeout(c.in); got != c.want {
			t.Errorf("encodeTimeout(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestEncodeTimeout_RoundTrip(t *testing.T) {
	for _, d := range []time.Duration{
		time.Nanosecond, time.Microsecond, 250 * time.Millisecond,
		30 * time.Second, 5 * time.Minute, 3 * time.Hour,
	} {
		v := encodeTimeout(d)
		got, err := decodeTimeout(v)
		if err != nil {
			t.Fatalf("decodeTimeout(%q): %v", v, err)
		}
		if got < d {
			t.Fatalf("encodeTimeout(%v) = %q decoded to %v — shorter than requested", d, v, got)
		}
	}
}

func TestEncodeTimeout_ValueNeverExceedsEightDigits(t *testing.T) {
	for _, d := range []time.Duration{
		time.Nanosecond, time.Second, time.Hour, 1 << 62,
	} {
		v := encodeTimeout(d)
		if len(v)-1 > 8 {
			t.Fatalf("encodeTimeout(%v) = %q — value part exceeds the 8 digits the spec allows", d, v)
		}
	}
}

func TestDecodeTimeout_Malformed(t *testing.T) {
	for _, v := range []string{"", "S", "abcS", "10x", "-5S"} {
		if _, err := decodeTimeout(v); err == nil {
			t.Errorf("decodeTimeout(%q) = nil error, want failure", v)
		}
	}
}

// encodeTimeout renders d as a grpc-timeout field value. Test-only: the
// production path appends into a caller's buffer (appendTimeout) precisely to
// avoid this allocation, so a string-returning twin belongs with the tests that
// want the convenience.
func encodeTimeout(d time.Duration) string {
	return string(appendTimeout(nil, d))
}
