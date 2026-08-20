package trace

import (
	"bytes"
	"os"
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
		// The falsy spellings are an error, NOT a silent off. "1" and "true"
		// are accepted as on, so flipping one to "0" is the obvious way to turn
		// the knob back off without unsetting it — and it fails at startup
		// instead. That is this package's stated rule ("a malformed value is an
		// error, not a fallback to off") applied consistently, and the choice
		// is pinned here rather than left to be rediscovered: silently
		// selecting nothing for "0" is exactly the outcome ErrUnknownCategory
		// exists to prevent, because the operator then reads an empty log as a
		// quiet connection. Unset the variable to turn tracing off.
		{in: "0", wantErr: true},
		{in: "false", wantErr: true},
		{in: "off", wantErr: true},
		{in: "no", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			in := tc.in

			got, err := ParseCategories(in)

			if tc.wantErr {
				require.ErrorIsf(t, err, ErrUnknownCategory,
					"err = %v, want ErrUnknownCategory: a typo must fail at startup rather than quietly select nothing, because the person who typed it then reads the log and believes what it does not say",
					err)
				assert.Equalf(t, Category(0), got,
					"categories = %b alongside the error, want 0: a caller that logs the error and carries on would then be tracing after a typo, the precise outcome ErrUnknownCategory exists to prevent",
					got)
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
		// There is no t.Unsetenv, and this subtest used to rely on the variable
		// being absent from the ambient environment — which it is in CI and is
		// not for anyone who exports the knob while debugging the thing the
		// knob is for. t.Setenv records the original state, including "was
		// unset", and restores it in cleanup, so removing the variable
		// afterwards is hermetic in both directions.
		//
		// Hygiene, not a hole: deleting the !ok guard in FromEnv outright
		// survives this package's suite either way, because an unset variable
		// reads as "" and ParseCategories("") selects nothing.
		t.Setenv(EnvVar, "")
		require.NoError(t, os.Unsetenv(EnvVar), "unset %s for this subtest", EnvVar)
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

	// Set-but-empty is its own value class: LookupEnv reports ok, so the unset
	// guard does not fire and the value has to be parsed and then rejected by
	// the category gate. No test reached FromEnv with it before.
	t.Run("set but empty yields no tracer", func(t *testing.T) {
		t.Setenv(EnvVar, "")

		tr, closer, err := FromEnv(&bytes.Buffer{})

		require.NoErrorf(t, err, "%s= is empty, not malformed; an empty value selects nothing and must not fail at startup", EnvVar)
		assert.Truef(t, tr == nil,
			"tracer = %v, want nil with %s set to the empty string: the cost of tracing being off must be one nil check per frame, not a live tracer nobody asked for",
			tr, EnvVar)
		assert.Truef(t, closer == nil,
			"closer = %v, want nil with %s set to the empty string", closer, EnvVar)
	})

	t.Run("a malformed value is an error, not off", func(t *testing.T) {
		t.Setenv(EnvVar, "framse")

		_, _, err := FromEnv(&bytes.Buffer{})

		require.ErrorIsf(t, err, ErrUnknownCategory,
			"err = %v, want ErrUnknownCategory: a malformed value must not fall back to off, or the operator who reached for this knob under pressure reads a silent log as a quiet connection",
			err)
	})
}
