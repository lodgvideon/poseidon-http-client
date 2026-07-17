package http3

import (
	"errors"
	"io"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// This file fuzzes the peer-controlled parsers that sit ABOVE the QUIC layer: the
// two field-section decoders that turn a server's HEADERS payload into a Response,
// and the two varint readers that frame every byte a peer sends on a stream.

// fuzzPopulatedTable builds a dynamic table holding two entries, the way a live
// connection reaches that state: by applying real encoder-stream instructions
// (RFC 9204 §4.3). It is the table the dynamic-reference decode paths need — with a
// nil table every dynamic reference is rejected at the prefix and the decoder's
// index arithmetic is never reached.
func fuzzPopulatedTable(tb testing.TB) *qpack.DynamicTable {
	tb.Helper()
	dt := qpack.NewDynamicTable(4096)
	instr := appendSetCapacity(nil, 4096)
	instr = appendInsertLiteral(instr, "x-dyn", "yes")
	instr = appendInsertLiteral(instr, "x-other", "value")
	n, err := dt.ParseEncoderInstructions(instr)
	if err != nil || n != len(instr) {
		tb.Fatalf("seeding the dynamic table: consumed %d/%d, err %v", n, len(instr), err)
	}
	return dt
}

// fuzzResponseSection builds a valid response field section (:status 200 plus one
// regular header) with the package's own encoder, so the corpus starts from bytes
// that already decode rather than from malformed noise the decoder rejects at byte
// zero.
func fuzzResponseSection() []byte {
	return qpack.NewEncoder().EncodeFieldSection(nil, []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
		{Name: []byte("content-type"), Value: []byte("text/plain")},
	})
}

// FuzzDecodeResponseHeaders feeds arbitrary bytes to the response field-section
// decoder (RFC 9114 §4.1.2, §4.3.2). Every byte of a response HEADERS payload is
// peer-controlled, and this is where those bytes first become a structured
// Response the rest of the client trusts.
//
// Beyond "does not panic" it asserts: a rejected section yields NO Response at all
// (never a half-built one a caller could act on), every error is one of the two
// typed errors the caller maps to different error codes and scopes (ErrH3Message →
// a stream error, qpack.ErrDecompressionFailed → a connection error), and an
// ACCEPTED section satisfies every message rule the decoder is supposed to
// enforce — :status present and in range, no pseudo-header among the regular
// fields, and no field that §4.2/§5.5 forbids. It runs against both profiles: a nil
// table (static-only, where the Required Insert Count MUST come back 0) and a
// populated one that unlocks the dynamic index arithmetic.
func FuzzDecodeResponseHeaders(f *testing.F) {
	f.Add(fuzzResponseSection())
	f.Add(dynRefSection()) // references dynamic absolute index 0
	f.Add([]byte{0x00, 0x00, 0xd9}) // static Indexed Field Line: :status 200
	f.Add([]byte{0x00, 0x00})       // well-formed prefix, no fields -> no :status
	f.Add([]byte{})                 // empty: missing the prefix
	f.Add([]byte{0x00})             // Required Insert Count only, Base byte missing
	f.Add([]byte{0x00, 0x00, 0xd1}) // :method GET — a request pseudo-header in a response
	f.Add([]byte{0x00, 0x00, 0x80}) // dynamic reference against an empty table
	f.Add([]byte{0xff, 0x00})       // non-zero encoded Required Insert Count
	// :status with a value that is not three digits in 100..599.
	f.Add([]byte{0x00, 0x00, 0x5f, 0x00, 0x02, '9', '9'})
	// A literal field line whose name is empty.
	f.Add([]byte{0x00, 0x00, 0x20, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, src []byte) {
		for _, dt := range []*qpack.DynamicTable{nil, fuzzPopulatedTable(t)} {
			resp, ric, err := DecodeResponseHeaders(qpack.NewDecoder(), dt, src)
			if err != nil {
				// A rejected section must not leak a partially-built Response: a caller
				// that reads resp on the error path would be acting on attacker bytes
				// that failed validation.
				if resp != nil {
					t.Fatalf("error %v returned alongside a non-nil Response %+v", err, resp)
				}
				if ric != 0 {
					t.Fatalf("error %v returned alongside Required Insert Count %d", err, ric)
				}
				if !errors.Is(err, ErrH3Message) && !errors.Is(err, qpack.ErrDecompressionFailed) {
					t.Fatalf("untyped error %v: the caller maps ErrH3Message and "+
						"qpack.ErrDecompressionFailed to different codes and scopes", err)
				}
				continue
			}
			if resp == nil {
				t.Fatal("nil Response with a nil error")
			}
			// §4.1.2: a response the decoder accepts carries a :status it validated.
			if resp.Status < 100 || resp.Status > 599 {
				t.Fatalf("accepted :status %d, outside the valid 100..599 range", resp.Status)
			}
			// §4.3: every field that survived into Headers is a regular header. A
			// pseudo-header here means the ':' gate let one through into the set the
			// client hands to its caller as ordinary headers.
			for _, h := range resp.Headers {
				if len(h.Name) == 0 {
					t.Fatalf("accepted an empty field name")
				}
				if h.Name[0] == ':' {
					t.Fatalf("accepted pseudo-header %q among the regular headers", h.Name)
				}
				if !validFieldName(h.Name) || !validFieldValue(h.Value) {
					t.Fatalf("accepted field %q: %q, which fails §4.2 validation", h.Name, h.Value)
				}
				if forbiddenResponseField(h.Name) {
					t.Fatalf("accepted §4.2-forbidden field %q: %q", h.Name, h.Value)
				}
			}
			// The static-only profile cannot reference the dynamic table, so a section
			// it accepts always has a Required Insert Count of 0 — a non-zero one would
			// make the client acknowledge inserts that never happened (RFC 9204 §4.4.1).
			if dt == nil && ric != 0 {
				t.Fatalf("static-only profile returned Required Insert Count %d, want 0", ric)
			}
		}
	})
}

// FuzzDecodeTrailers feeds arbitrary bytes to the trailing field-section decoder
// (RFC 9114 §4.1). A trailer section arrives after the body, when the caller has
// often already begun acting on the response, and it carries NO pseudo-headers at
// all — so the ':' gate here is absolute rather than the ":status only" gate a
// header section uses.
//
// Beyond "does not panic" it asserts: a rejected section yields no fields, errors
// are typed (ErrH3Message / qpack.ErrDecompressionFailed), and every accepted
// field is a valid, non-pseudo, non-forbidden regular header.
func FuzzDecodeTrailers(f *testing.F) {
	f.Add(qpack.NewEncoder().EncodeFieldSection(nil, []hpack.HeaderField{
		{Name: []byte("x-checksum"), Value: []byte("abc123")},
	}))
	f.Add([]byte{0x00, 0x00})       // well-formed prefix, no fields (a valid empty trailer section)
	f.Add([]byte{0x00, 0x00, 0xd9}) // :status 200 — a pseudo-header in trailers
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x00, 0x00, 0x80})             // dynamic reference against an empty table
	f.Add([]byte{0xff, 0x00})                   // non-zero encoded Required Insert Count
	f.Add([]byte{0x00, 0x00, 0x20, 0x00, 0x00}) // literal field line, empty name
	f.Add(dynRefSection())

	f.Fuzz(func(t *testing.T, src []byte) {
		for _, dt := range []*qpack.DynamicTable{nil, fuzzPopulatedTable(t)} {
			fields, ric, err := DecodeTrailers(qpack.NewDecoder(), dt, src)
			if err != nil {
				if fields != nil {
					t.Fatalf("error %v returned alongside %d fields", err, len(fields))
				}
				if ric != 0 {
					t.Fatalf("error %v returned alongside Required Insert Count %d", err, ric)
				}
				if !errors.Is(err, ErrH3Message) && !errors.Is(err, qpack.ErrDecompressionFailed) {
					t.Fatalf("untyped error %v", err)
				}
				continue
			}
			for _, h := range fields {
				// §4.3: trailers carry no pseudo-headers, so ANY ':' name is malformed.
				if len(h.Name) == 0 {
					t.Fatalf("accepted an empty trailer field name")
				}
				if h.Name[0] == ':' {
					t.Fatalf("accepted pseudo-header %q in trailers", h.Name)
				}
				if !validFieldName(h.Name) || !validFieldValue(h.Value) {
					t.Fatalf("accepted trailer %q: %q, which fails §4.2 validation", h.Name, h.Value)
				}
				if forbiddenResponseField(h.Name) {
					t.Fatalf("accepted §4.2-forbidden trailer %q: %q", h.Name, h.Value)
				}
			}
			if dt == nil && ric != 0 {
				t.Fatalf("static-only profile returned Required Insert Count %d, want 0", ric)
			}
		}
	})
}

// FuzzReadStreamType feeds arbitrary bytes to the unidirectional stream-type reader
// (RFC 9114 §6.2). This varint is the FIRST thing a peer sends on every uni stream
// it opens, and its value picks the stream's role — control, push, or one of the
// two QPACK instruction streams — so it is the dispatch that routes attacker bytes
// to one parser or another.
//
// Beyond "does not panic" it asserts: the consumed count never runs past the input,
// a need-more is the typed ErrNeedMore and consumes nothing (a partial varint MUST
// stay unconsumed for the next Feed), the value stays inside the QUIC varint range,
// and the type round-trips — re-encoding it and re-reading yields the same value in
// no more bytes than the original spent.
func FuzzReadStreamType(f *testing.F) {
	f.Add(appendV(nil, StreamTypeControl))
	f.Add(appendV(nil, StreamTypeQPACKEncoder))
	f.Add(appendV(nil, StreamTypeQPACKDecoder))
	f.Add(appendV(nil, StreamTypePush))
	f.Add(appendV(nil, 0x1f*7+0x21))       // a GREASE stream type (§6.2)
	f.Add(appendV(nil, bytesx.MaxVarint))  // the top of the varint space
	f.Add([]byte{})                        // nothing yet
	f.Add([]byte{0x40})                    // 2-byte varint, one byte present
	f.Add([]byte{0xc0, 0x00})              // 8-byte varint, two bytes present
	f.Add([]byte{0x00, 0xff, 0xff})        // control type plus trailing stream bytes
	f.Add([]byte{0x40, 0x00})              // 0 in non-minimal 2-byte form

	f.Fuzz(func(t *testing.T, b []byte) {
		typ, n, err := ReadStreamType(b)
		if err != nil {
			if !errors.Is(err, ErrNeedMore) {
				t.Fatalf("untyped error %v, want ErrNeedMore", err)
			}
			// ErrNeedMore means "retry once more bytes arrive", so consuming any of
			// them would silently drop stream bytes the retry still needs.
			if n != 0 || typ != 0 {
				t.Fatalf("ErrNeedMore returned typ=%d n=%d, want both zero", typ, n)
			}
			return
		}
		if n <= 0 || n > len(b) {
			t.Fatalf("consumed %d bytes of a %d-byte buffer", n, len(b))
		}
		if typ > bytesx.MaxVarint {
			t.Fatalf("stream type %d exceeds the varint maximum %d", typ, bytesx.MaxVarint)
		}
		// Round-trip: the value must survive re-encoding. The wire form is not
		// canonical (a value may arrive in a longer-than-minimal varint), so the
		// re-encode may be shorter — but it must never be LONGER, and must re-read
		// to the same type.
		re := appendV(nil, typ)
		if len(re) > n {
			t.Fatalf("stream type %d read from %d bytes re-encodes to %d", typ, n, len(re))
		}
		typ2, n2, err2 := ReadStreamType(re)
		if err2 != nil || typ2 != typ || n2 != len(re) {
			t.Fatalf("re-reading the re-encoded type %d gave (%d, %d, %v)", typ, typ2, n2, err2)
		}
	})
}

// FuzzParseFrameHeader feeds arbitrary bytes to the HTTP/3 frame-header parser
// (RFC 9114 §7.1). Every frame on every stream begins with these two varints, and
// the parser deliberately does NOT bound-check the declared Length against the
// bytes present — that is the caller's job — so its contract is narrow and worth
// pinning exactly: it must consume only bytes that are there, and report a length
// the caller can safely compare against its cap.
//
// Beyond "does not panic" it asserts: a truncated header is the typed
// io.ErrUnexpectedEOF with nothing consumed and no half-parsed values handed back,
// a success consumed at least two bytes (two varints) and never more than the input
// holds, both varints stay in range, and the parse depends only on the header bytes
// — re-parsing just the consumed prefix yields the identical result, so trailing
// payload bytes can never change how a header is read.
func FuzzParseFrameHeader(f *testing.F) {
	f.Add(AppendData(nil, []byte("body")))
	f.Add(AppendHeaders(nil, []byte{0x00, 0x00, 0xd9}))
	f.Add(AppendSettings(nil, []Setting{{ID: SettingQPACKMaxTableCapacity, Value: 4096}}))
	f.Add(AppendGoaway(nil, 0))
	f.Add([]byte{})
	f.Add([]byte{0x00})                                                     // type only, length missing
	f.Add([]byte{0x40})                                                     // truncated type varint
	f.Add([]byte{0x00, 0x40})                                               // truncated length varint
	f.Add([]byte{0x00, 0xc0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})     // a huge declared length
	f.Add([]byte{0xc0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00}) // a huge type, length 0

	f.Fuzz(func(t *testing.T, b []byte) {
		typ, length, n, err := ParseFrameHeader(b)
		if err != nil {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("untyped error %v, want io.ErrUnexpectedEOF", err)
			}
			if n != 0 || typ != 0 || length != 0 {
				t.Fatalf("truncated header returned typ=%d length=%d n=%d, want all zero",
					typ, length, n)
			}
			return
		}
		// Two varints, each at least one byte, neither reaching past the input.
		if n < 2 || n > len(b) {
			t.Fatalf("consumed %d bytes of a %d-byte buffer for two varints", n, len(b))
		}
		if typ > bytesx.MaxVarint || length > bytesx.MaxVarint {
			t.Fatalf("type %d / length %d exceed the varint maximum %d", typ, length, bytesx.MaxVarint)
		}
		// The header must be a pure function of its own bytes: a parser that let
		// trailing payload bytes change the type or length would let a peer smuggle a
		// frame past a caller that bound-checked the header it saw.
		typ2, length2, n2, err2 := ParseFrameHeader(b[:n])
		if err2 != nil || typ2 != typ || length2 != length || n2 != n {
			t.Fatalf("re-parsing the %d-byte header prefix gave (%d, %d, %d, %v), "+
				"want (%d, %d, %d, nil)", n, typ2, length2, n2, err2, typ, length, n)
		}
	})
}
