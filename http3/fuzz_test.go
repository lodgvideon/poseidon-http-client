package http3

import (
	"bytes"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// FuzzQPACKEncoderStreamBuffer drives arbitrary server QPACK encoder-stream bytes
// (RFC 9204 §4.3) through readQPACKEncoder in CHUNKS, the way the reader goroutine
// does, and asserts the one property the whole memory story rests on: whenever the
// client keeps the connection alive, the partial instruction it retained is within
// the cap derived from the table capacity it advertised. The encoder stream is
// unframed and wholly server-controlled, and readInstrString parks on `needMore`
// until a declared literal fully arrives, so an unbounded retained tail is the
// natural failure — it is what this target exists to catch.
//
// maxCap is fuzzed, and the cap tracks it (qpackEncoderInstrCap), so small
// capacities put the cap within reach of ordinary fuzz-sized inputs instead of
// needing the 16 KB a 4096-byte table's cap would demand.
func FuzzQPACKEncoderStreamBuffer(f *testing.F) {
	// Insert With Literal Name declaring a 2 GiB name, then dribbling it: the shape
	// that grew the buffer without bound before the cap existed. A seed that stays
	// UNDER the cap would prove nothing, so these carry enough bytes to cross it at
	// the small advertised capacities — at maxCap 0 the cap is its floor of 64.
	huge := hpack.EncodeInteger(nil, 5, 0x40, 1<<31)
	dribbleA, dribbleB := make([]byte, 64), make([]byte, 64)
	f.Add(uint64(0), huge, dribbleA, dribbleB)
	f.Add(uint64(32), huge, dribbleA, dribbleB)
	f.Add(uint64(4096), huge, dribbleA, dribbleB)
	// A declared length just under a small cap, dribbled past it.
	f.Add(uint64(64), hpack.EncodeInteger(nil, 5, 0x40, 4000), dribbleA, dribbleB)
	// Whole instructions, which must be applied and dropped, never retained.
	f.Add(uint64(4096), hpack.EncodeInteger(nil, 5, 0x20, 4096), []byte{0x41, 'a', 0x01, 'b'}, []byte(nil))
	f.Add(uint64(4096), []byte{0x40, 0x00}, []byte(nil), []byte(nil))
	f.Add(uint64(0), []byte{}, []byte{}, []byte{})

	f.Fuzz(func(t *testing.T, maxCap uint64, c1, c2, c3 []byte) {
		maxCap %= 1 << 16

		// A Client wired for exactly this path — no reader goroutine, so a fuzz
		// iteration costs nothing but the parse.
		conn := &fakeConn{}
		stream := &fakeStream{id: 3, conn: conn, directRead: true, recvChunks: [][]byte{c1, c2, c3}}
		c := &Client{
			conn:           conn,
			qpackDyn:       qpack.NewDynamicTable(maxCap),
			qpackEncBufMax: qpackEncoderInstrCap(maxCap),
			qpackReady:     make(chan struct{}),
			qpackEnc:       stream,
		}

		for i := 0; i < len(stream.recvChunks); i++ {
			err := c.readQPACKEncoder()
			if err == nil {
				// Alive: the retained tail is a partial instruction and MUST be capped.
				if uint64(len(c.qpackEncBuf)) > c.qpackEncBufMax {
					t.Fatalf("chunk %d: retained %d bytes of a partial instruction, cap %d "+
						"(advertised capacity %d): the encoder stream buffer is unbounded",
						i, len(c.qpackEncBuf), c.qpackEncBufMax, maxCap)
				}
				continue
			}
			// Dead: the only error this path may produce is the typed connection error.
			if !errors.Is(err, ErrH3Control) {
				t.Fatalf("chunk %d: untyped error %v, want ErrH3Control", i, err)
			}
			if !conn.closed {
				t.Fatalf("chunk %d: returned %v without closing the connection", i, err)
			}
			break
		}
	})
}

// FuzzFrameReader streams arbitrary bytes through the HTTP/3 frame reader
// (RFC 9114 §7.1) via Feed + a ReadFrame loop, the shape a real stream reader
// drives. A peer controls every stream byte, so the reader must never panic or
// hang and must bound its buffering: with a frame-length cap set, a header
// announcing a huge length is rejected with ErrH3FrameTooLarge rather than
// buffered. Every successful ReadFrame consumes at least two bytes (the type and
// length varints), so the loop always terminates.
func FuzzFrameReader(f *testing.F) {
	f.Add(AppendData(nil, []byte("body")))
	f.Add(AppendHeaders(nil, []byte{0x00, 0x00, 0xd1})) // wraps a QPACK field section
	f.Add(AppendSettings(nil, []Setting{{ID: SettingQPACKMaxTableCapacity, Value: 4096}}))
	f.Add(AppendGoaway(nil, 0))
	// Two frames back to back, to exercise the consume-and-advance path.
	f.Add(AppendData(AppendData(nil, []byte("a")), []byte("bb")))
	f.Add([]byte{})                                                     // empty
	f.Add([]byte{0x00})                                                 // type varint only, no length
	f.Add([]byte{0x00, 0x40})                                           // truncated length varint
	f.Add([]byte{0x00, 0xc0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // header declaring a huge length
	f.Add([]byte{0x40})                                                 // truncated type varint

	f.Fuzz(func(t *testing.T, data []byte) {
		want := frameSeq(data, 0) // fed whole
		// The same bytes cut into pieces, drained between each, must yield the same
		// frames. Chopping is what the peer controls — a QUIC stream delivers
		// whatever the packets happened to carry — and it is also what drives Feed's
		// compaction, which moves the live window back over already-consumed bytes.
		// A compaction that mis-measures the window loses or duplicates stream bytes,
		// and this is the property that catches it.
		for _, chunk := range []int{1, 3, 64} {
			got := frameSeq(data, chunk)
			if len(got) != len(want) {
				t.Fatalf("chunk=%d yielded %d frames, feeding whole yielded %d", chunk, len(got), len(want))
			}
			for i := range got {
				if got[i].typ != want[i].typ || !bytes.Equal(got[i].payload, want[i].payload) {
					t.Fatalf("chunk=%d frame %d = (%#x, %d bytes), want (%#x, %d bytes)",
						chunk, i, got[i].typ, len(got[i].payload), want[i].typ, len(want[i].payload))
				}
			}
		}
	})
}

// fuzzFrame is one frame FrameReader handed out, with its payload copied so it
// survives the feeds that follow.
type fuzzFrame struct {
	typ     uint64
	payload []byte
}

// frameSeq plays data through a reader — all at once when chunk <= 0, otherwise
// in chunk-byte pieces with a drain after each — and returns the frames read. It
// stops at the first rejected frame (ErrH3FrameTooLarge), as a real stream reader
// does. The frame count is capped as a belt-and-suspenders guard on top of the
// guaranteed >=2-byte-per-frame progress.
func frameSeq(data []byte, chunk int) []fuzzFrame {
	var r FrameReader
	r.SetMaxFrameLen(1 << 20) // bound buffering so a huge declared length cannot OOM
	var out []fuzzFrame
	// drain reports whether reading may continue: false once a frame is rejected.
	drain := func() bool {
		for len(out) < 4096 {
			typ, payload, err := r.ReadFrame()
			if err != nil {
				return errors.Is(err, ErrNeedMore)
			}
			out = append(out, fuzzFrame{typ: typ, payload: append([]byte(nil), payload...)})
		}
		return false
	}
	if chunk <= 0 {
		r.Feed(data)
		drain()
		return out
	}
	for off := 0; off < len(data); off += chunk {
		r.Feed(data[off:min(off+chunk, len(data))])
		if !drain() {
			break
		}
	}
	return out
}

// FuzzParseSettings feeds arbitrary bytes to the HTTP/3 SETTINGS payload parser
// (RFC 9114 §7.2.4). A malformed or hostile SETTINGS payload — truncated pair,
// repeated or reserved identifier — must be rejected with an error, never a
// panic. The huge-count defense is implicit: the loop advances by whole varints
// and terminates when the payload is exhausted.
func FuzzParseSettings(f *testing.F) {
	f.Add([]byte{})                       // empty (valid: no settings)
	f.Add([]byte{0x01, 0x40, 0x00})       // id=1, value=0 (2-byte value)
	f.Add([]byte{0x06, 0x44, 0x00})       // MAX_FIELD_SECTION_SIZE
	f.Add([]byte{0x40})                   // truncated identifier varint
	f.Add([]byte{0x01})                   // identifier present, value missing
	f.Add([]byte{0x02, 0x00})             // reserved h2 identifier -> H3_SETTINGS_ERROR
	f.Add([]byte{0x01, 0x00, 0x01, 0x00}) // duplicate identifier

	f.Fuzz(func(_ *testing.T, payload []byte) {
		_, _ = ParseSettings(payload)
	})
}
