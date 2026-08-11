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
// overriding content-type, and ErrInvalidMetadata for a name or value that
// would not be a legal HTTP/2 field.
//
// A text value is stored aliasing the caller's slice — it is not copied — so a
// caller reusing a scratch buffer must not overwrite it before NewStream has
// written the headers. A binary value is base64-encoded into fresh memory and
// carries no such constraint.
func AppendMetadata(md []conn.HeaderField, key string, value []byte) ([]conn.HeaderField, error) {
	// nil allowlist: the package-level entry point cannot see a connection's
	// Options, so it stays strict. Use (*ClientConn).AppendMetadata to build a
	// field whose name is exempted there.
	return appendMetadata(md, key, value, nil)
}

func appendMetadata(md []conn.HeaderField, key string, value []byte, allowReserved map[string]struct{}) ([]conn.HeaderField, error) {
	k := strings.ToLower(key)
	name := []byte(k)
	if err := validMetadataName(name); err != nil {
		return nil, err
	}
	if err := checkMetadataKey(k, allowReserved); err != nil {
		return nil, err
	}
	if strings.HasSuffix(k, binSuffix) {
		// Validate what reaches the wire, not what the caller passed: a binary
		// value legitimately contains NUL and CR, and it is the base64 of it —
		// which never can — that becomes the field value.
		enc := make([]byte, binEncoding.EncodedLen(len(value)))
		binEncoding.Encode(enc, value)
		return append(md, conn.HeaderField{Name: name, Value: enc}), nil
	}
	if err := validMetadataValue(name, value); err != nil {
		return nil, err
	}
	return append(md, conn.HeaderField{Name: name, Value: value}), nil
}

// reservedAllowSet turns Options.AllowReservedMetadata into the lowercase set
// checkMetadataKey consults. Returns nil for an empty list, which reads as
// "nothing exempted" on lookup.
func reservedAllowSet(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[strings.ToLower(n)] = struct{}{}
	}
	return set
}

// AppendMetadata is AppendMetadata bound to this connection, so a name listed in
// Options.AllowReservedMetadata is accepted here too.
//
// It exists because the package-level function cannot see the allowlist, and
// the one thing a caller enabling it needs is the very thing that function does:
// base64-encoding a "-bin" value. Without this a caller would have to hand-build
// the conn.HeaderField and reimplement that encoding.
func (cc *ClientConn) AppendMetadata(md []conn.HeaderField, key string, value []byte) ([]conn.HeaderField, error) {
	return appendMetadata(md, key, value, cc.allowReserved)
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
	// Connection-specific fields, forbidden in HTTP/2 outright (RFC 9113
	// §8.2.2). "host" is not connection-specific, but the transport derives
	// :authority itself and the pair must agree (RFC 9113 §8.3.1), so a caller
	// copy can only disagree. Same list client/request.go refuses.
	"connection":        {},
	"keep-alive":        {},
	"proxy-connection":  {},
	"transfer-encoding": {},
	"upgrade":           {},
	"host":              {},
}

// checkMetadataKey rejects keys the transport owns. Name syntax is validated
// separately by validMetadataName; callers should reach both through
// validMetadata rather than calling this alone.
func checkMetadataKey(k string, allowReserved map[string]struct{}) error {
	if k == "" {
		return fmt.Errorf("%w: empty key", ErrReservedMetadata)
	}
	if k[0] == ':' {
		return fmt.Errorf("%w: pseudo-header %q", ErrReservedMetadata, k)
	}
	if _, bad := reservedKeys[k]; bad {
		return fmt.Errorf("%w: %q", ErrReservedMetadata, k)
	}
	// The gRPC protocol reserves the whole grpc- namespace for itself, not just
	// the names in use today: "Header names starting with 'grpc-' but not
	// listed here are reserved for future GRPC use and should not be used by
	// applications." Refusing the prefix is what keeps a caller's header from
	// colliding with a future protocol field.
	if strings.HasPrefix(k, "grpc-") {
		// Options.AllowReservedMetadata exempts specific names from THIS check
		// only. The pseudo-header and reservedKeys gates above have already run,
		// so an allowlist cannot be used to forge content-type, te or
		// grpc-timeout however it is spelled.
		if _, ok := allowReserved[k]; !ok {
			return fmt.Errorf("%w: the grpc- namespace is reserved by the protocol: %q "+
				"(Options.AllowReservedMetadata exempts specific names)",
				ErrReservedMetadata, k)
		}
	}
	return nil
}

// MetadataValue returns the value of the first entry named key, decoding
// base64 when key ends in "-bin". The returned slice aliases md for text keys
// and is freshly allocated for binary keys.
//
// ok reports whether an entry named key was present; err reports that one was
// present but could not be decoded. The two are separate on purpose. Folding a
// malformed "-bin" value into ok=false would make it indistinguishable from
// "the peer sent nothing", and an application that reads a signature or
// capability out of metadata would take its no-credential-required branch on a
// value the peer deliberately corrupted — fail-open on peer input.
func MetadataValue(md []conn.HeaderField, key string) (value []byte, ok bool, err error) {
	k := strings.ToLower(key)
	for i := range md {
		if string(md[i].Name) != k {
			continue
		}
		if !strings.HasSuffix(k, binSuffix) {
			return md[i].Value, true, nil
		}
		dec, derr := decodeBin(md[i].Value)
		if derr != nil {
			return nil, true, fmt.Errorf("grpc: metadata %q is not valid base64: %w", k, derr)
		}
		return dec, true, nil
	}
	return nil, false, nil
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

// appendTimeout appends the grpc-timeout field value for d to dst. Values are
// rounded up so the server's deadline is never tighter than the caller's, and a
// d past what eight digits of hours can express is clamped to that maximum.
//
// It appends rather than returning a string because it runs on every RPC that
// carries a deadline: building the value with string concatenation costs two
// allocations, and appending into the caller's buffer costs none.
func appendTimeout(dst []byte, d time.Duration) []byte {
	if d <= 0 {
		return append(dst, '0', 'n')
	}
	for _, u := range timeoutUnits {
		n := int64(d / u.size)
		if d%u.size != 0 {
			n++ // round up: a shorter deadline than asked for would be wrong
		}
		if n <= maxTimeoutDigits {
			return append(strconv.AppendInt(dst, n, 10), u.suffix)
		}
	}
	return append(strconv.AppendInt(dst, maxTimeoutDigits, 10), 'H')
}
