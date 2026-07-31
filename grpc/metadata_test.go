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
	got, ok := MetadataValue(md, "trace-bin")
	if !ok {
		t.Fatal("MetadataValue(-bin) not found")
	}
	if string(got) != "\x00\xff\x10" {
		t.Fatalf("decoded = % x", got)
	}
}

func TestMetadataValue_UnpaddedBinaryAccepted(t *testing.T) {
	// gRPC permits a peer to omit base64 padding; the read side must cope.
	md := []conn.HeaderField{{Name: []byte("k-bin"), Value: []byte("AP8Q")}}
	if _, ok := MetadataValue(md, "k-bin"); !ok {
		t.Fatal("padded form rejected")
	}
	md = []conn.HeaderField{{Name: []byte("k-bin"), Value: []byte("YQ")}}
	got, ok := MetadataValue(md, "k-bin")
	if !ok || string(got) != "a" {
		t.Fatalf("unpadded decode = %q ok=%v", got, ok)
	}
}

func TestAppendMetadata_RejectsReservedAndPseudo(t *testing.T) {
	for _, k := range []string{":path", "content-type", "TE", "grpc-timeout", "user-agent", ""} {
		if _, err := AppendMetadata(nil, k, []byte("x")); !errors.Is(err, ErrReservedMetadata) {
			t.Errorf("AppendMetadata(%q) = %v, want ErrReservedMetadata", k, err)
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
