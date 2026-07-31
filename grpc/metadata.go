package grpc

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// binSuffix marks a metadata key whose value is binary and therefore
// base64-encoded on the wire.
const binSuffix = "-bin"

// ErrReservedMetadata is reported when caller-supplied metadata would
// overwrite a header the transport owns.
var ErrReservedMetadata = errors.New("grpc: metadata key is reserved by the transport")

// binEncoding is the padded standard base64 alphabet. gRPC permits unpadded
// input on the read side but this client always writes padded output, which
// every implementation accepts.
var binEncoding = base64.StdEncoding

// AppendMetadata appends one metadata entry to md. A key ending in "-bin"
// carries a binary value and is base64-encoded; any other key carries an ASCII
// value and is sent verbatim. The key is lowercased, because HTTP/2 field
// names must be lowercase (RFC 9113 §8.2.1) and gRPC metadata keys are
// case-insensitive.
//
// It returns ErrReservedMetadata for pseudo-headers and for the headers the
// transport sets itself, so a caller cannot silently break the RPC by, say,
// overriding content-type.
func AppendMetadata(md []conn.HeaderField, key string, value []byte) ([]conn.HeaderField, error) {
	k := strings.ToLower(key)
	if err := checkMetadataKey(k); err != nil {
		return nil, err
	}
	if strings.HasSuffix(k, binSuffix) {
		enc := make([]byte, binEncoding.EncodedLen(len(value)))
		binEncoding.Encode(enc, value)
		return append(md, conn.HeaderField{Name: []byte(k), Value: enc}), nil
	}
	return append(md, conn.HeaderField{Name: []byte(k), Value: value}), nil
}

// reservedKeys are the request headers the transport owns. A caller that needs
// to change one of these configures it through Options instead.
var reservedKeys = map[string]struct{}{
	"content-type":         {},
	"te":                   {},
	"grpc-timeout":         {},
	"grpc-encoding":        {},
	"grpc-accept-encoding": {},
	"user-agent":           {},
}

// checkMetadataKey rejects keys the transport owns and keys that are not
// valid HTTP/2 field names for this position.
func checkMetadataKey(k string) error {
	if k == "" {
		return fmt.Errorf("%w: empty key", ErrReservedMetadata)
	}
	if k[0] == ':' {
		return fmt.Errorf("%w: pseudo-header %q", ErrReservedMetadata, k)
	}
	if _, bad := reservedKeys[k]; bad {
		return fmt.Errorf("%w: %q", ErrReservedMetadata, k)
	}
	return nil
}

// MetadataValue returns the value of the first entry named key, decoding
// base64 when key ends in "-bin". ok is false when no such entry exists.
// The returned slice aliases md for text keys and is freshly allocated for
// binary keys.
func MetadataValue(md []conn.HeaderField, key string) (value []byte, ok bool) {
	k := strings.ToLower(key)
	for i := range md {
		if string(md[i].Name) != k {
			continue
		}
		if !strings.HasSuffix(k, binSuffix) {
			return md[i].Value, true
		}
		dec, err := decodeBin(md[i].Value)
		if err != nil {
			return nil, false
		}
		return dec, true
	}
	return nil, false
}

// decodeBin base64-decodes a -bin metadata value, accepting both the padded
// and unpadded forms a peer may send.
func decodeBin(v []byte) ([]byte, error) {
	enc := binEncoding
	if len(v)%4 != 0 {
		enc = base64.RawStdEncoding
	}
	out := make([]byte, enc.DecodedLen(len(v)))
	n, err := enc.Decode(out, v)
	if err != nil {
		return nil, err
	}
	return out[:n], nil
}

// timeoutUnits pairs each grpc-timeout unit suffix with its duration, ordered
// finest-first so encodeTimeout picks the most precise unit that fits in the
// eight digits the specification allows.
var timeoutUnits = [...]struct {
	suffix byte
	size   time.Duration
}{
	{'n', time.Nanosecond},
	{'u', time.Microsecond},
	{'m', time.Millisecond},
	{'S', time.Second},
	{'M', time.Minute},
	{'H', time.Hour},
}

// maxTimeoutDigits is the largest grpc-timeout value the specification allows:
// "at most 8 digits".
const maxTimeoutDigits = 99999999

// encodeTimeout renders d as a grpc-timeout field value. Values are rounded up
// so the server's deadline is never tighter than the caller's, and a d past
// what eight digits of hours can express is clamped to that maximum.
func encodeTimeout(d time.Duration) string {
	if d <= 0 {
		return "0n"
	}
	for _, u := range timeoutUnits {
		n := int64(d / u.size)
		if d%u.size != 0 {
			n++ // round up: a shorter deadline than asked for would be wrong
		}
		if n <= maxTimeoutDigits {
			return strconv.FormatInt(n, 10) + string(u.suffix)
		}
	}
	return strconv.Itoa(maxTimeoutDigits) + "H"
}

// decodeTimeout parses a grpc-timeout field value. It exists for tests and for
// callers that inspect what was sent; the client never receives this header.
func decodeTimeout(v string) (time.Duration, error) {
	if len(v) < 2 {
		return 0, fmt.Errorf("grpc: malformed grpc-timeout %q", v)
	}
	n, err := strconv.ParseInt(v[:len(v)-1], 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("grpc: malformed grpc-timeout %q", v)
	}
	for _, u := range timeoutUnits {
		if u.suffix == v[len(v)-1] {
			return time.Duration(n) * u.size, nil
		}
	}
	return 0, fmt.Errorf("grpc: unknown grpc-timeout unit in %q", v)
}
