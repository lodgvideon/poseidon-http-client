package trace

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCategories(t *testing.T) {
	tests := []struct {
		in      string
		want    Category
		wantErr bool
	}{
		{in: "", want: 0},
		{in: "frames", want: CatFrames},
		{in: "frame", want: CatFrames},
		{in: "FRAMES", want: CatFrames},
		{in: "  frames  ", want: CatFrames},
		{in: "all", want: CatAll},
		{in: "1", want: CatAll},
		{in: "true", want: CatAll},
		{in: "frames,frames", want: CatFrames},
		{in: "frames,,", want: CatFrames},
		// Reserved for the connection-level seam: accepted so a command line
		// written for them keeps working when that lands, selecting nothing today.
		{in: "streams", want: 0},
		{in: "flow", want: 0},
		{in: "frames,streams,flow", want: CatFrames},
		// A typo is loud. The whole point of this knob is to be reached for once,
		// under pressure, by someone who then believes what the log does not say.
		{in: "framse", wantErr: true},
		{in: "frames,nonsense", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			in := tc.in

			got, err := ParseCategories(in)

			if tc.wantErr {
				require.ErrorIsf(t, err, ErrUnknownCategory,
					"err = %v, want ErrUnknownCategory: a typo must fail at startup rather than quietly select nothing, because the person who typed it then reads the log and believes what it does not say",
					err)
				return
			}
			require.NoErrorf(t, err, "ParseCategories(%q) must parse", in)
			assert.Equalf(t, tc.want, got,
				"categories = %b, want %b", got, tc.want)
		})
	}
}

func TestFromEnv(t *testing.T) {
	t.Run("unset yields no tracer", func(t *testing.T) {
		var buf bytes.Buffer

		tr, closer, err := FromEnv(&buf)

		require.NoErrorf(t, err, "FromEnv with %s unset", EnvVar)
		// Deliberately not assert.Nil: it reflects, so it also accepts a typed nil
		// held in the interface -- and that is precisely the value the emit sites'
		// `if tracer != nil` check would treat as present and then dereference.
		assert.Truef(t, tr == nil,
			"tracer = %v, want an untyped nil with %s unset: the cost of tracing being off must be one nil check per frame, not a live tracer",
			tr, EnvVar)
		assert.Truef(t, closer == nil,
			"closer = %v, want nil with %s unset", closer, EnvVar)
	})

	t.Run("frames yields a text tracer", func(t *testing.T) {
		t.Setenv(EnvVar, "frames")
		var buf bytes.Buffer

		tr, closer, err := FromEnv(&buf)
		require.NoError(t, err, "FromEnv with a well-formed value")
		require.NotNil(t, tr, "POSEIDON_DEBUG=frames produced no tracer")
		require.NotNil(t, closer,
			"the closer is non-nil whenever the tracer is; without it the final batch never reaches the writer")
		tr.TraceFrame(FrameInfo{Proto: ProtoH2, Dir: DirOut, TypeName: "PING", Length: 8})
		closeErr := closer.Close()

		require.NoError(t, closeErr, "Close flushes what the tracer buffered")
		assert.NotZero(t, buf.Len(),
			"the tracer FromEnv built wrote nothing: a build tag cannot be turned on in a binary somebody already shipped, and this is the runtime equivalent")
	})

	t.Run("categories this build cannot serve yield no tracer", func(t *testing.T) {
		t.Setenv(EnvVar, "streams")

		tr, closer, err := FromEnv(&bytes.Buffer{})

		require.NoError(t, err,
			"a reserved category parses; it is accepted so a command line written for it keeps working when the conn-level seam lands")
		assert.Truef(t, tr == nil,
			"streams alone started a frame tracer (%v); selecting a category this build does not implement must stay as free as tracing being off",
			tr)
		assert.Truef(t, closer == nil,
			"streams alone produced a closer (%v)", closer)
	})

	t.Run("a malformed value is an error, not off", func(t *testing.T) {
		t.Setenv(EnvVar, "framse")

		_, _, err := FromEnv(&bytes.Buffer{})

		require.ErrorIsf(t, err, ErrUnknownCategory,
			"err = %v, want ErrUnknownCategory: a malformed value must not fall back to off, or the operator who reached for this knob under pressure reads a silent log as a quiet connection",
			err)
	})
}
