package http3

import (
	"bytes"
	"context"
	"testing"
)

// The buffered Do path copies each response body out of the frame reader and
// recycles the reader's array between requests (roundTrip's acquire/release).
// Neither half is obvious from its call site, and getting either wrong corrupts
// response bodies silently, with no other test failing. These pin both.
//
// This file used to pin the opposite arrangement: the body ADOPTED the reader's
// first DATA payload, which made the array caller-owned and therefore
// per-request, and required that Feed never rewrite a consumed region. That
// bought one copy per response and paid for it with a growth chain per response
// — several times the body's own size for a body arriving one QUIC packet at a
// time (BenchmarkRecvPath_BufferedBody). The copy is the cheaper half of that
// trade once the array can be pooled, so the invariants are now the mirror of
// what they were.

// TestFrameReader_PayloadCapIsItsLength pins that ReadFrame hands back a payload
// whose capacity equals its length, so appending to it MUST allocate rather than
// write into the reader's remaining buffered frames.
//
// dispatchFrame relies on this the moment a response carries a second DATA
// frame: `append(rb.body, payload...)` on a first payload with spare capacity
// would scribble over whatever the reader has buffered next.
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
		t.Fatalf("payload has len %d cap %d — appending to a body built on it would write "+
			"into the reader's next buffered frame", len(payload), cap(payload))
	}
}

// TestRespBuilder_BodyDoesNotAliasReaderBuffer pins the property that makes
// recycling the reader's array sound: the body dispatchFrame builds owns its
// bytes. If it ever aliases the reader again, the array handed back to
// frameBufPool is still live and the next request overwrites a body that has
// already been returned to a caller.
func TestRespBuilder_BodyDoesNotAliasReaderBuffer(t *testing.T) {
	c := &Client{}
	want := bytes.Repeat([]byte{'x'}, 1024)

	var fr FrameReader
	fr.Feed(AppendData(nil, want))
	typ, payload, err := fr.ReadFrame()
	if err != nil || typ != FrameData {
		t.Fatalf("ReadFrame = (%#x, %v), want a DATA frame", typ, err)
	}
	rb := &respBuilder{resp: &Response{Status: 200}}
	if derr := c.dispatchFrame(rb, typ, payload); derr != nil {
		t.Fatalf("dispatchFrame: %v", derr)
	}

	// Scribble over the reader's whole array the way a recycled buffer's next
	// user does. A body that aliased it would change under this.
	for i := range fr.buf[:cap(fr.buf)] {
		fr.buf[:cap(fr.buf)][i] = 'Z'
	}
	if !bytes.Equal(rb.body, want) {
		t.Fatalf("the body changed when the reader's array was overwritten — it aliases the "+
			"reader, so pooling that array corrupts bodies across requests: got %d bytes "+
			"starting %q", len(rb.body), rb.body[:min(8, len(rb.body))])
	}
}

// TestRespBuilder_BodySurvivesReaderRecycling drives the real dispatchFrame over
// the shapes that exercise body accumulation — one DATA frame, several, and DATA
// followed by another frame — and then recycles the reader's array through the
// pool and runs a second response over it. Every body must still read back
// byte-identical.
func TestRespBuilder_BodySurvivesReaderRecycling(t *testing.T) {
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
		feedInBursts(&fr, wire, 512, func(typ uint64, payload []byte) {
			if derr := c.dispatchFrame(rb, typ, payload); derr != nil {
				t.Fatalf("dispatchFrame: %v", derr)
			}
		})
		if !bytes.Equal(rb.body, want) {
			t.Fatalf("body = %d bytes, want %d — an earlier frame's bytes were corrupted "+
				"by the feeds that followed", len(rb.body), len(want))
		}
	})

	t.Run("body survives the reader being recycled", func(t *testing.T) {
		// The full lifecycle roundTrip runs: a pooled array, a response read off
		// it, the array returned, then a second request reusing it. The first
		// body must be untouched by the second response.
		first := bytes.Repeat([]byte{'q'}, 2048)
		second := bytes.Repeat([]byte{'r'}, 2048)

		var fr FrameReader
		fr.acquire()
		rb := &respBuilder{resp: &Response{Status: 200}}
		feedInBursts(&fr, AppendData(nil, first), 512, func(typ uint64, payload []byte) {
			if derr := c.dispatchFrame(rb, typ, payload); derr != nil {
				t.Fatalf("dispatchFrame: %v", derr)
			}
		})
		fr.release()

		var fr2 FrameReader
		fr2.acquire()
		defer fr2.release()
		rb2 := &respBuilder{resp: &Response{Status: 200}}
		feedInBursts(&fr2, AppendData(nil, second), 512, func(typ uint64, payload []byte) {
			if derr := c.dispatchFrame(rb2, typ, payload); derr != nil {
				t.Fatalf("dispatchFrame: %v", derr)
			}
		})

		if !bytes.Equal(rb.body, first) {
			t.Fatalf("the first response's body changed once its reader's array was recycled "+
				"and reused: got %d bytes starting %q", len(rb.body), rb.body[:min(8, len(rb.body))])
		}
		if !bytes.Equal(rb2.body, second) {
			t.Fatalf("the second response's body is wrong: got %d bytes", len(rb2.body))
		}
	})
}

// feedInBursts plays wire through fr in bursts of n bytes, handing every frame
// that completes to onFrame — the loop consumeFrames runs, minus the transport.
func feedInBursts(fr *FrameReader, wire []byte, n int, onFrame func(typ uint64, payload []byte)) {
	for off := 0; off < len(wire); off += n {
		end := min(off+n, len(wire))
		fr.Feed(wire[off:end])
		for {
			typ, payload, err := fr.ReadFrame()
			if err != nil {
				break
			}
			onFrame(typ, payload)
		}
	}
}

// TestClient_PooledFrameBuffer drives whole requests through the real Do path
// back to back and keeps every response. Each one reads its frames off an array
// the request before it returned to frameBufPool, so anything a response still
// aliases when roundTrip releases — the body, a header name or value, a trailer —
// shows up here as one response wearing another's bytes.
//
// The unit tests above pin the individual invariants; this pins that nothing
// ELSE on the response holds a piece of the reader.
func TestClient_PooledFrameBuffer(t *testing.T) {
	const requests = 6
	type got struct {
		resp *Response
		body []byte
	}
	results := make([]got, 0, requests)
	fills := []byte("abcdefghij")

	for i := 0; i < requests; i++ {
		// A body big enough that the reader's array is genuinely reused rather
		// than fitting in whatever slack a fresh one has, and a header value long
		// enough that an aliased one would be visibly overwritten.
		body := bytes.Repeat(fills[i:i+1], 6000+i)
		etag := string(bytes.Repeat(fills[i:i+1], 200))
		headers := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("etag", etag)))
		conn := &fakeConn{req: &fakeStream{
			recvChunks: [][]byte{headers, AppendData(nil, body)},
			fin:        true,
		}}
		client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 0}})
		if err != nil {
			t.Fatalf("request %d: NewClientFake: %v", i, err)
		}
		resp, gotBody, err := client.Do(context.Background(),
			&Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"})
		if err != nil {
			t.Fatalf("request %d: Do: %v", i, err)
		}
		results = append(results, got{resp: resp, body: gotBody})
	}

	for i, r := range results {
		wantBody := bytes.Repeat(fills[i:i+1], 6000+i)
		if !bytes.Equal(r.body, wantBody) {
			t.Errorf("request %d: body = %d bytes starting %q, want %d of %q — a later "+
				"request overwrote it through the recycled frame buffer",
				i, len(r.body), r.body[:min(8, len(r.body))], len(wantBody), fills[i:i+1])
		}
		wantEtag := bytes.Repeat(fills[i:i+1], 200)
		var etag []byte
		for _, f := range r.resp.Headers {
			if string(f.Name) == "etag" {
				etag = f.Value
			}
		}
		if !bytes.Equal(etag, wantEtag) {
			t.Errorf("request %d: etag = %d bytes starting %q, want %d of %q — the header "+
				"value aliases the recycled frame buffer",
				i, len(etag), etag[:min(8, len(etag))], len(wantEtag), fills[i:i+1])
		}
	}
}

// TestFrameReader_FeedReclaimsConsumedCapacity pins the win: once a frame has
// been consumed, its bytes are reusable, so a reader fed a long stream of frames
// settles on one array instead of climbing a fresh growth chain.
//
// Before this, ReadFrame re-sliced the buffer past each frame it handed out,
// which abandoned the capacity behind the window — every response reallocated
// its way back up from nothing.
func TestFrameReader_FeedReclaimsConsumedCapacity(t *testing.T) {
	var fr FrameReader
	fr.acquire()
	defer fr.release()

	frame := AppendData(nil, bytes.Repeat([]byte{'a'}, 4096))
	var array *byte
	for i := 0; i < 64; i++ {
		feedInBursts(&fr, frame, 1200, func(typ uint64, payload []byte) {
			if typ != FrameData || len(payload) != 4096 {
				t.Fatalf("round %d: frame = (%#x, %d bytes)", i, typ, len(payload))
			}
		})
		if i == 0 {
			array = &fr.buf[:cap(fr.buf)][0]
			continue
		}
		if got := &fr.buf[:cap(fr.buf)][0]; got != array {
			t.Fatalf("round %d reallocated the reader's array (cap %d) — consumed capacity "+
				"is not being reclaimed", i, cap(fr.buf))
		}
	}
}

// TestFrameReader_CompactionPreservesAStraddlingFrame covers the case compaction
// has to get right: a frame that is only half received when the array's tail
// runs out, so the live window slides back over consumed bytes with a partial
// frame header inside it.
func TestFrameReader_CompactionPreservesAStraddlingFrame(t *testing.T) {
	var fr FrameReader
	fr.acquire()
	defer fr.release()

	// Fill the array with small frames first so the tail is nearly spent and the
	// large frame below is guaranteed to straddle a compaction.
	filler := AppendData(nil, bytes.Repeat([]byte{'.'}, 64))
	for i := 0; i < 8; i++ {
		fr.Feed(filler)
		if _, _, err := fr.ReadFrame(); err != nil {
			t.Fatalf("filler %d: %v", i, err)
		}
	}

	for _, size := range []int{100, 4096, frameBufSize * 3} {
		want := bytes.Repeat([]byte{'w'}, size)
		var got []byte
		feedInBursts(&fr, AppendData(nil, want), 7, func(typ uint64, payload []byte) {
			if typ != FrameData {
				t.Fatalf("frame type %#x, want DATA", typ)
			}
			got = append(got, payload...)
		})
		if !bytes.Equal(got, want) {
			t.Fatalf("size %d: reassembled %d bytes, want %d — compaction moved the live "+
				"window incorrectly", size, len(got), len(want))
		}
	}
}

// TestFrameReader_ReleaseIsIdempotent pins that release can be deferred next to
// an early return without risking the same array being handed to two readers.
func TestFrameReader_ReleaseIsIdempotent(t *testing.T) {
	var fr FrameReader
	fr.acquire()
	fr.Feed(AppendData(nil, []byte("body")))
	fr.release()
	if fr.Buffered() != 0 || fr.buf != nil || fr.bufp != nil {
		t.Fatalf("release left the reader non-empty: buffered=%d buf=%v bufp=%v",
			fr.Buffered(), fr.buf != nil, fr.bufp != nil)
	}
	fr.release() // must not put the array back a second time
	fr.release()

	var a, b FrameReader
	a.acquire()
	b.acquire()
	if a.bufp == b.bufp {
		t.Fatal("two live readers acquired the same array")
	}
	a.release()
	b.release()
}

// TestFrameReader_OutsizedBufferIsNotPooled pins that an array grown past
// maxPooledFrameBuf is dropped rather than circulated, so one large response
// cannot pin that footprint in the pool for every small request after it.
func TestFrameReader_OutsizedBufferIsNotPooled(t *testing.T) {
	var fr FrameReader
	fr.acquire()
	fr.Feed(make([]byte, maxPooledFrameBuf+1))
	grown := &fr.buf[0]
	fr.release()

	// A pooled buffer is not reachable by identity, so check the next few the
	// pool hands out: none may be the outsized array.
	for i := 0; i < 8; i++ {
		var next FrameReader
		next.acquire()
		if cap(next.buf) > 0 && &next.buf[:cap(next.buf)][0] == grown {
			t.Fatal("the outsized array came back out of the pool")
		}
		if cap(next.buf) > maxPooledFrameBuf {
			t.Fatalf("pool handed out a %d-byte array, over the %d cap", cap(next.buf), maxPooledFrameBuf)
		}
		next.release()
	}
}
