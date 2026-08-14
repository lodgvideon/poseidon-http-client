package trace

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// EnvVar is the environment variable FromEnv reads.
//
// A build tag cannot be turned on in a binary somebody already shipped, which
// is the whole complaint behind -tags poseidondebug carrying the Close()-leak
// finalizer and nothing else. This is the runtime equivalent.
const EnvVar = "POSEIDON_DEBUG"

// Category selects what a tracer reports. It is a bitmask so POSEIDON_DEBUG can
// name several.
type Category uint8

const (
	// CatFrames reports every frame crossing the wire.
	CatFrames Category = 1 << iota
)

// CatAll is every category this build implements.
const CatAll = CatFrames

// Has reports whether every category in want is selected.
func (c Category) Has(want Category) bool { return c&want == want }

// ErrUnknownCategory is returned by ParseCategories for a token it does not
// recognise. It is an error rather than a silent skip because the whole point
// of this knob is to be reached for once, under pressure, by someone who then
// reads the log and believes what it does not say.
var ErrUnknownCategory = errors.New("poseidon/trace: unknown POSEIDON_DEBUG category")

// ParseCategories parses a POSEIDON_DEBUG value: a comma-separated list of
// category names. "all" selects everything, and an empty or whitespace-only
// value selects nothing. Names are case-insensitive and surrounding spaces are
// ignored.
//
// The categories this build understands are "frames" and "all". "streams" and
// "flow" are named in the issue that asked for this and are accepted as
// reserved-but-unimplemented: they parse, and select nothing, so a command line
// written for them keeps working when the connection-level seam lands rather
// than failing at startup today.
func ParseCategories(s string) (Category, error) {
	var c Category
	for _, tok := range strings.Split(s, ",") {
		switch strings.ToLower(strings.TrimSpace(tok)) {
		case "":
			continue
		case "all", "1", "true":
			c |= CatAll
		case "frames", "frame":
			c |= CatFrames
		case "streams", "stream", "flow":
			// Reserved: the conn-level seam these name is not built yet.
			continue
		default:
			return 0, fmt.Errorf("%w: %q", ErrUnknownCategory, strings.TrimSpace(tok))
		}
	}
	return c, nil
}

// FromEnv builds a Tracer from POSEIDON_DEBUG, writing to w.
//
// It returns a nil Tracer when the variable is unset, empty, or names only
// categories this build does not implement — and a nil Tracer is exactly what
// the emit sites check for, so the off case stays free. The returned closer is
// non-nil whenever the Tracer is, and must be called to flush.
//
// A malformed value is an error, not a fallback to off.
func FromEnv(w io.Writer) (Tracer, io.Closer, error) {
	v, ok := os.LookupEnv(EnvVar)
	if !ok {
		return nil, nil, nil
	}
	cats, err := ParseCategories(v)
	if err != nil {
		return nil, nil, err
	}
	if !cats.Has(CatFrames) {
		return nil, nil, nil
	}
	t := NewTextTracer(w)
	return t, t, nil
}
