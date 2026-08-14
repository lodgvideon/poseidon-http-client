package trace

import (
	"bytes"
	"errors"
	"testing"
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
			got, err := ParseCategories(tc.in)
			if tc.wantErr {
				if !errors.Is(err, ErrUnknownCategory) {
					t.Fatalf("err = %v, want ErrUnknownCategory", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("categories = %b, want %b", got, tc.want)
			}
		})
	}
}

func TestFromEnv(t *testing.T) {
	t.Run("unset yields no tracer", func(t *testing.T) {
		var buf bytes.Buffer
		tr, closer, err := FromEnv(&buf)
		if err != nil {
			t.Fatalf("FromEnv: %v", err)
		}
		if tr != nil || closer != nil {
			t.Fatalf("tracer = %v closer = %v, want both nil with %s unset", tr, closer, EnvVar)
		}
	})

	t.Run("frames yields a text tracer", func(t *testing.T) {
		t.Setenv(EnvVar, "frames")
		var buf bytes.Buffer
		tr, closer, err := FromEnv(&buf)
		if err != nil {
			t.Fatalf("FromEnv: %v", err)
		}
		if tr == nil || closer == nil {
			t.Fatal("POSEIDON_DEBUG=frames produced no tracer")
		}
		tr.TraceFrame(FrameInfo{Proto: ProtoH2, Dir: DirOut, TypeName: "PING", Length: 8})
		if err := closer.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if buf.Len() == 0 {
			t.Error("tracer wrote nothing")
		}
	})

	t.Run("categories this build cannot serve yield no tracer", func(t *testing.T) {
		t.Setenv(EnvVar, "streams")
		tr, closer, err := FromEnv(&bytes.Buffer{})
		if err != nil {
			t.Fatalf("FromEnv: %v", err)
		}
		if tr != nil || closer != nil {
			t.Error("streams alone started a frame tracer")
		}
	})

	t.Run("a malformed value is an error, not off", func(t *testing.T) {
		t.Setenv(EnvVar, "framse")
		if _, _, err := FromEnv(&bytes.Buffer{}); !errors.Is(err, ErrUnknownCategory) {
			t.Fatalf("err = %v, want ErrUnknownCategory", err)
		}
	})
}
