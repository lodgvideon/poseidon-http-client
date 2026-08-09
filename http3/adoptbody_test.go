package http3

import (
	"bytes"
	"testing"
)

// The buffered Do path adopts the first DATA frame's payload as the response
// body instead of copying it (dispatchFrame, the FrameData case). That is only
// sound because of two FrameReader properties, and neither is obvious from the
// call site — a later change to either would corrupt response bodies silently,
// with no test failing. These pin both.

// TestFrameReader_PayloadCapIsItsLength pins the first property: ReadFrame hands
// back a payload whose capacity equals its length, so appending to it MUST
// allocate rather than write into the reader's remaining buffered frames.
//
// dispatchFrame relies on this the moment a response carries a second DATA
// frame: the body then holds the first payload, and `append(rb.body, ...)` would
// scribble over whatever the reader has buffered next if there were spare
// capacity.
func TestFrameReader_PayloadCapIsItsLength(t *testing.T) {
	var fr FrameReader
	wire := AppendData(nil, []byte("first"))
	wire = AppendData(wire, []byte("second"))
	fr.Feed(wire)

	typ, payload, err := fr.ReadFrame()
	if err != nil || typ != FrameData {
		t.Fatalf("ReadFrame = (%#x, %v), want a DATA frame", typ, err)
	}
	if cap(payload) != len(payload) {
		t.Fatalf("payload has len %d cap %d — appending to an adopted body would write "+
			"into the reader's next buffered frame", len(payload), cap(payload))
	}
}

// TestFrameReader_FeedDoesNotRewriteConsumedBytes pins the second property: Feed
// only appends at the tail and ReadFrame only slides the window forward, so a
// payload already handed out is never written again.
//
// A compacting Feed — copying the live window back to the head of the array to
// reclaim consumed capacity — is a natural-looking optimisation that would break
// this, and it would break it invisibly: the body would be overwritten by
// whatever arrived next, with every existing test still green.
func TestFrameReader_FeedDoesNotRewriteConsumedBytes(t *testing.T) {
	var fr FrameReader
	body := bytes.Repeat([]byte{'A'}, 512)
	fr.Feed(AppendData(nil, body))

	_, payload, err := fr.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	// Hold the payload the way dispatchFrame now holds the body, then keep
	// feeding — enough to force the reader's buffer to grow several times.
	adopted := payload
	for i := 0; i < 8; i++ {
		fr.Feed(bytes.Repeat([]byte{'Z'}, 4096))
	}
	if !bytes.Equal(adopted, body) {
		t.Errorf("an adopted payload changed under later Feeds: got %d bytes of %q..., want %d of 'A'",
			len(adopted), adopted[:min(8, len(adopted))], len(body))
	}
}

// TestRespBuilder_AdoptedBodyIsSafeToAlias drives the real dispatchFrame over the
// shapes that exercise the adoption: one DATA frame, several, and DATA followed
// by trailers. The body must come out byte-identical in every case.
func TestRespBuilder_AdoptedBodyIsSafeToAlias(t *testing.T) {
	c := &Client{}

	t.Run("single frame", func(t *testing.T) {
		want := bytes.Repeat([]byte{'x'}, 1024)
		rb := &respBuilder{resp: &Response{Status: 200}}
		if err := c.dispatchFrame(rb, FrameData, want); err != nil {
			t.Fatalf("dispatchFrame: %v", err)
		}
		if !bytes.Equal(rb.body, want) {
			t.Fatalf("body = %d bytes, want %d", len(rb.body), len(want))
		}
	})

	t.Run("three frames concatenate", func(t *testing.T) {
		// Fed through a real FrameReader so the payloads are genuine aliases of
		// its buffer, which is the whole point — passing freshly made slices
		// would not exercise the aliasing at all.
		parts := [][]byte{
			bytes.Repeat([]byte{'a'}, 700),
			bytes.Repeat([]byte{'b'}, 1300),
			bytes.Repeat([]byte{'c'}, 900),
		}
		var wire []byte
		var want []byte
		for _, p := range parts {
			wire = AppendData(wire, p)
			want = append(want, p...)
		}

		var fr FrameReader
		rb := &respBuilder{resp: &Response{Status: 200}}
		// Feed in small bursts so frames straddle, as they do on the wire.
		for off := 0; off < len(wire); off += 512 {
			end := off + 512
			if end > len(wire) {
				end = len(wire)
			}
			fr.Feed(wire[off:end])
			for {
				typ, payload, err := fr.ReadFrame()
				if err != nil {
					break
				}
				if derr := c.dispatchFrame(rb, typ, payload); derr != nil {
					t.Fatalf("dispatchFrame: %v", derr)
				}
			}
		}
		if !bytes.Equal(rb.body, want) {
			t.Fatalf("body = %d bytes, want %d — the adopted first frame was corrupted "+
				"by the appends that followed", len(rb.body), len(want))
		}
	})

	t.Run("body survives frames read after it", func(t *testing.T) {
		// The adopted body must not change while the reader keeps parsing. This is
		// the trailers shape without needing a QPACK section: any later frame that
		// makes the reader grow its buffer will do.
		want := bytes.Repeat([]byte{'q'}, 2048)
		wire := AppendData(nil, want)
		wire = AppendData(wire, bytes.Repeat([]byte{'r'}, 2048))

		var fr FrameReader
		fr.Feed(wire[:1000]) // first DATA incomplete
		if _, _, err := fr.ReadFrame(); err != ErrNeedMore {
			t.Fatalf("ReadFrame on a partial frame = %v, want ErrNeedMore", err)
		}
		fr.Feed(wire[1000:])

		rb := &respBuilder{resp: &Response{Status: 200}}
		typ, payload, err := fr.ReadFrame()
		if err != nil || typ != FrameData {
			t.Fatalf("first ReadFrame = (%#x, %v)", typ, err)
		}
		if derr := c.dispatchFrame(rb, typ, payload); derr != nil {
			t.Fatalf("dispatchFrame: %v", derr)
		}
		adoptedLen := len(rb.body)

		// Consume the rest; the body already handed to rb must be untouched.
		for {
			typ, payload, err := fr.ReadFrame()
			if err != nil {
				break
			}
			if derr := c.dispatchFrame(rb, typ, payload); derr != nil {
				t.Fatalf("dispatchFrame: %v", derr)
			}
		}
		if !bytes.Equal(rb.body[:adoptedLen], want) {
			t.Fatalf("the adopted first %d bytes changed while later frames were parsed", adoptedLen)
		}
	})
}
