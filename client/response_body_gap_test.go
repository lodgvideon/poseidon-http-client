package client

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// ————————————————————————————————————————————————————————————————
// Response and body-reader gaps (#845, #850, #889). Each is a branch the suite
// names and cannot reach: a status guard both of whose tests stop one guard
// earlier, a pool double-Put no single-goroutine test can observe, and a
// body-reader arm no httptest peer can produce.
// ————————————————————————————————————————————————————————————————

// TestParseThreeDigitInt_DigitValidity is the guard TestParseStatus_NotNumeric
// and TestParseStatus_Negative both stop short of (#845).
//
// Both of those use a TWO-byte :status ("OK", "-1"), so both return at the
// length guard; they are one equivalence class wearing two names, and deleting
// the digit-validity guard leaves the whole batch green. A peer controls
// :status verbatim and RFC 9113 §8.3.2 makes it exactly three digits, so the
// three-byte non-numeric case is the one a hostile peer actually sends.
//
// Note the low-side half of the guard as written: b[i] is a byte, so b[0]-'0'
// WRAPS for anything below '0' — "/00" gives d0 == 255, caught by `d0 > 9`, and
// `d0 < 0` can never be true. The boundary cases below are what makes that
// visible; they are the reason the guard is not dead even though half of its
// condition is.
func TestParseThreeDigitInt_DigitValidity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want int
		ok   bool
	}{
		// Accepted: the boundaries of the three-digit domain.
		{"lowest three-digit value", "000", 0, true},
		{"highest three-digit value", "999", 999, true},
		{"an ordinary status", "204", 204, true},
		// Refused at the LENGTH guard — the class both existing tests are in.
		{"two bytes", "OK", 0, false},
		{"four bytes", "2000", 0, false},
		{"empty", "", 0, false},
		// Refused at the DIGIT-VALIDITY guard — the class nothing reached.
		{"three letters", "abc", 0, false},
		{"letter O for zero", "2O0", 0, false},
		{"leading minus", "-12", 0, false},
		{"embedded space", "1 2", 0, false},
		{"leading space", " 20", 0, false},
		{"byte just below '0' (0x2f)", "/00", 0, false},
		{"byte just above '9' (0x3a)", "0:0", 0, false},
		{"non-ASCII digits", "\xd9\xa2\xd9", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseThreeDigitInt([]byte(c.in))

			if !c.ok {
				assert.Errorf(t, err,
					"parseThreeDigitInt(%q) = %d with no error; a peer picks this byte string "+
						"and a status that is not three digits must not be read as a number",
					c.in, got)
				return
			}
			require.NoErrorf(t, err, "parseThreeDigitInt(%q) rejected a well-formed status", c.in)
			assert.Equalf(t, c.want, got,
				"parseThreeDigitInt(%q) = %d, want %d — over-rejecting or misreading a valid "+
					"status changes what every caller branches on", c.in, got, c.want)
		})
	}
}

// TestCompress_DoubleRelease_DoesNotHandOneReaderToTwoRequests is the hazard
// TestCompress_ReleaseIsIdempotent names and cannot observe (#850).
//
// compressingReader.release opens with `if c.w == nil { return }`, and that is
// what makes it idempotent. Delete it and a second release runs
// compressingReaderPool.Put(c) AGAIN, so the same reader sits in the pool twice
// and two requests can be handed it at once — same c.buf, same c.src, same
// latched c.err, their compressed output interleaved into each other's body.
//
// The existing test releases three times and then does ONE
// prepareCompressedRequest on the same goroutine, and a duplicated pool entry is
// harmless when it is drawn one at a time: every Get resets buf, src, done and
// err. The damage needs TWO LIVE USERS OF THE SAME READER, which is what this
// builds — and it asserts identity first, so a failure says which mechanism
// broke rather than only that the bytes were wrong.
//
// A concurrent arm (N goroutines each compressing a distinct payload, under
// -race) was written first and DELETED: with the guard removed it still passed,
// under -race included. sync.Pool's per-P private slot means concurrent Gets
// rarely draw the duplicated entry, so the concurrent form is the one that
// cannot fail. Two sequential Gets on one goroutine hit it every time.
func TestCompress_DoubleRelease_DoesNotHandOneReaderToTwoRequests(t *testing.T) {
	seed := &Request{
		Method: "POST", Path: "/",
		BodyReader:   bytes.NewReader([]byte("seed payload")),
		CompressBody: EncodingGzip,
	}
	eff, release, err := prepareCompressedRequest(seed)
	require.NoError(t, err, "prepare the seed request")
	_, err = io.ReadAll(eff.BodyReader)
	require.NoError(t, err, "drain the seed request body")
	release()
	release() // the double release the guard must absorb

	first, err := getCompressingReader(EncodingGzip, bytes.NewReader([]byte("FIRST")))
	require.NoError(t, err, "first reader out of the pool")
	defer first.Release()
	second, err := getCompressingReader(EncodingGzip, bytes.NewReader([]byte("SECOND")))
	require.NoError(t, err, "second reader out of the pool")
	defer second.Release()

	require.NotSamef(t, first, second,
		"two concurrent requests were handed the SAME *compressingReader (%p); the double "+
			"release put it back twice, so both share one c.buf and one c.src and their "+
			"compressed bodies interleave", first)
	firstOut, err := io.ReadAll(first)
	require.NoError(t, err, "read the first request's compressed body")
	secondOut, err := io.ReadAll(second)
	require.NoError(t, err, "read the second request's compressed body")
	assert.Equalf(t, "FIRST", string(decodeWith(t, EncodingGzip, firstOut)),
		"the first request's body decoded to somebody else's payload")
	assert.Equalf(t, "SECOND", string(decodeWith(t, EncodingGzip, secondOut)),
		"the second request's body decoded to somebody else's payload")
}

// interleavedInterimServer replies on the first client HEADERS with a final 200,
// some body, then a 1xx block AFTER the final status, then the rest of the body.
//
// That ordering is what no httptest fixture can produce: net/http's HTTP/2
// server emits its 103s before WriteHeader(200), so recvFinalHeaders has already
// consumed and dropped every interim block by the time Do hands the caller a
// BodyReader. RFC 7540 §8.1 does not forbid the interleaving, and body.go has an
// arm for it — this is the peer that exercises it.
func interleavedInterimServer(pre, post string, interimStatus string) func(*frame.Framer) {
	return func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			sid, ok := capH.firstHeadersStreamID()
			if !ok {
				continue
			}
			enc := hpack.NewEncoder()
			final := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte("200")},
			})
			if err := srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID: sid, BlockFragment: final, EndHeaders: true,
			}); err != nil {
				return
			}
			if err := srvFr.WriteData(sid, false, []byte(pre)); err != nil {
				return
			}
			mid := enc.EncodeBlock(nil, []hpack.HeaderField{
				{Name: []byte(":status"), Value: []byte(interimStatus)},
				{Name: []byte("link"), Value: []byte("</s.css>; rel=preload")},
			})
			if err := srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID: sid, BlockFragment: mid, EndHeaders: true,
			}); err != nil {
				return
			}
			_ = srvFr.WriteData(sid, true, []byte(post))
			return
		}
	}
}

// TestConformance_RFC7540_Sec8_1_HeadersAfterFinalStatusIsRefusedNotSkipped
// settles #889 by measurement rather than by adding the skip test the issue
// sketched.
//
// #889 asks for a peer that sends HEADERS(1xx) AFTER the final status while the
// body streams, so body.go's `case conn.EventInterimHeaders: continue` arm is
// exercised. This is that peer — and the arm is never entered. conn's
// classifyHeaderBlock latches headersReceived on the final block, and a later
// HEADERS without END_STREAM is a stream error of type PROTOCOL_ERROR
// (RFC 7540 §8.1, §8.1.2.6 routing malformed to §5.4.2). So a body reader can
// only ever see EventTrailers or that reset; EventInterimHeaders and the
// spurious-EventHeaders arm beside it are unreachable on every transport —
// h3_transport does not replay resp.Interim as an event, and the HTTP/1.1
// exchange synthesises EventHeaders exactly once, for Do itself.
//
// That makes the mutation #889 proposes an EQUIVALENT one: no test can
// distinguish it through the public surface. What IS worth pinning is the
// answer the client actually gives, because the alternative failure — treating
// the block as the end of the body — hands the caller a short response and a
// clean io.EOF, indistinguishable from a complete one.
func TestConformance_RFC7540_Sec8_1_HeadersAfterFinalStatusIsRefusedNotSkipped(t *testing.T) {
	cases := []struct {
		name   string
		status string
	}{
		{"103 Early Hints after the final status", "103"},
		{"100 Continue after the final status", "100"},
		{"a spurious non-1xx HEADERS after the final status", "200"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := &fakeDialer{srvAfter: interleavedInterimServer("abc", "def", c.status)}
			cl, err := NewClient(ClientOptions{Addr: "fake:0", ConnOpts: conn.ConnOptions{Dialer: d}})
			require.NoError(t, err, "NewClient")
			defer func() { _ = cl.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var res Response

			err = cl.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyStream}, &res)

			require.NoError(t, err, "Do: the final HEADERS and the first DATA are well formed")
			require.Truef(t, res.BodyReader != nil, "BodyStream must hand back a BodyReader")
			defer func() { _ = res.BodyReader.Close() }()
			body, rerr := io.ReadAll(res.BodyReader)
			var rst *StreamResetError
			require.ErrorAsf(t, rerr, &rst,
				"reading past a HEADERS block that arrived after the final status returned "+
					"%v, want a *StreamResetError — io.EOF here would report a truncated body "+
					"as a complete one, which a streaming caller cannot tell from success", rerr)
			assert.Equalf(t, frame.ErrCodeProtocolError, rst.Code,
				"reset code %v, want PROTOCOL_ERROR: RFC 7540 §8.1 makes a non-END_STREAM "+
					"HEADERS after a final status malformed, and §8.1.2.6 routes malformed to "+
					"a STREAM error so the connection survives", rst.Code)
			assert.Equalf(t, "abc", string(body),
				"the caller was handed %q before the reset; the DATA that did arrive before "+
					"the malformed block must still reach it", body)
			assert.Equalf(t, 200, res.Status,
				"a header block after the final status overwrote the caller-visible status")
		})
	}
}
