package http1_test

// Conformance tests for the receive-side grammar of RFC 9112: the status line
// (§4), line termination (§2.2), field lines (§5.1) and chunked framing (§7.1).
//
// These all share one failure shape. Each lenient parse accepted a construct the
// grammar forbids, so this client and a stricter recipient of the same octets
// disagreed about where the message ended — which is the precondition §11.1
// (response splitting) and §11.2 (request smuggling) are written about. §4 says
// it outright: "lenient parsing can result in response splitting security
// vulnerabilities if there are multiple recipients of the message and each has
// its own unique interpretation of robustness".
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// wantHeadRejected asserts the two halves of the remedy for a malformed head:
// the response is not returned to the caller, and the connection is not offered
// back to the pool. It differs from wantRejected only in not pinning the error
// to ErrInvalidContentLength — these defects are grammar, not framing values.
func wantHeadRejected(t *testing.T, ex *http1.Exchange, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s accepted; want the response discarded", what)
	}
	if !errors.Is(err, http1.ErrInvalidHeaderBlock) {
		t.Errorf("%s: error = %v, want it to wrap ErrInvalidHeaderBlock so a caller can classify it", what, err)
	}
	if ex.KeepAlive() {
		t.Errorf("%s: KeepAlive() = true; the head is not a well-formed field sequence, "+
			"so the stream position cannot be trusted and the connection must not be pooled", what)
	}
}

// TestConformance_RFC9112_Sec4_StatusCodeMustBe3DIGIT pins `status-code = 3DIGIT`.
//
// The parse used strconv.Atoi, which is a superset of that ABNF in the one
// direction that changes control flow: it accepts a sign, any digit count and
// leading zeros. Every accepted value below 200 fell into ReadResponse's 1xx
// interim branch, which discards the header block and reads the NEXT line off
// the socket as another status line — so "HTTP/1.1 -5 x" was a server-supplied
// instruction to go read one more response. The fixture makes that concrete: the
// second response is complete and well formed, and under the old parse it was
// what the caller got back.
func TestConformance_RFC9112_Sec4_StatusCodeMustBe3DIGIT(t *testing.T) {
	// A whole fabricated response waiting behind the malformed status line.
	const fabricated = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED"

	// Values Atoi accepted that 3DIGIT does not. The sub-200 ones are the
	// dangerous half — they reached the interim-drain loop.
	//
	// 3DIGIT is the whole rule. "000", "600" and "900" are well-formed status
	// lines and now belong in the ACCEPT set below: an earlier version of this
	// test demanded they be refused, which encoded a first-digit restriction this
	// client had invented on top of the grammar — and which broke every request
	// against a host that answers 999.
	for _, code := range []string{"-5", "+99", "99", "1", "0", "0000200", "1234", "2e2", "0x64", " 200", "２００"} {
		t.Run(code, func(t *testing.T) {
			ex, err := readResponseErr(t, "HTTP/1.1 "+code+" x\r\n\r\n"+fabricated)
			if err == nil {
				t.Fatalf("status code %q accepted; want a parse error — 3DIGIT is the whole grammar, "+
					"and anything under 200 that gets through is a free 'read another response' step", code)
			}
			if ex.KeepAlive() {
				t.Errorf("status code %q: KeepAlive() = true; a status line that is not a status line "+
					"leaves the stream position indeterminate, so the connection must not be pooled", code)
			}
		})
	}
}

// TestConformance_RFC9112_Sec4_ValidStatusCodesStillParse is the control for the
// test above: a parse that simply rejected everything would pass it. 100 also
// pins that the interim path — the one the malformed codes were abusing — still
// works for the status class that actually defines it.
func TestConformance_RFC9112_Sec4_ValidStatusCodesStillParse(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
		want int
	}{
		{"200", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", 200},
		{"599", "HTTP/1.1 599 Whatever\r\nContent-Length: 0\r\n\r\n", 599},
		{"100 then 200", "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", 200},
		{"no reason phrase", "HTTP/1.1 204 \r\n\r\n", 204},
		// Unrecognised classes are processed, not refused. RFC 9110 §15.1 has a
		// recipient treat an unrecognised status as the x00 of its class, and the
		// repo's own HTTP1_CLIENT_CHECKLIST spells out "do not hard-fail the
		// parse". 999 is not hypothetical — it is deployed as a bot-block
		// response, and refusing it means 0% success against such a host.
		{"999", "HTTP/1.1 999 Request denied\r\nContent-Length: 0\r\n\r\n", 999},
		{"600", "HTTP/1.1 600 Odd\r\nContent-Length: 0\r\n\r\n", 600},
		{"000", "HTTP/1.1 000 Zero\r\nContent-Length: 0\r\n\r\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, "GET", tc.wire)
			code, _, err := ex.ReadResponse(context.Background())
			if err != nil {
				t.Fatalf("ReadResponse = %v, want a clean parse", err)
			}
			if code != tc.want {
				t.Errorf("status = %d, want %d", code, tc.want)
			}
		})
	}
}

// TestConformance_RFC9110_Sec15_1_UnrecognisedClassIsFinal pins the other half
// of accepting 6xx-9xx: they must be treated as FINAL responses, never drained
// as interim.
//
// The interim-drain loop discards a header block and reads the next line off the
// socket as another status line, so what admits a message to it decides whether
// a peer can make this client go read one more response. Gating it on "not
// final" (code >= 200) rather than on the 1xx range itself is what made the
// grammar load-bearing there in the first place, and what tempted this client
// into inventing a first-digit restriction the grammar does not have. With the
// gate stated positively — 100..199 — an unrecognised class cannot reach the
// loop whatever the parse allows, so the fabricated response parked behind these
// is never returned.
func TestConformance_RFC9110_Sec15_1_UnrecognisedClassIsFinal(t *testing.T) {
	const fabricated = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED"

	for _, code := range []string{"000", "099", "600", "999"} {
		t.Run(code, func(t *testing.T) {
			ex := wireExchange(t, "GET",
				"HTTP/1.1 "+code+" x\r\nContent-Length: 2\r\n\r\nok"+fabricated)
			got, _, err := ex.ReadResponse(context.Background())
			if err != nil {
				t.Fatalf("ReadResponse = %v; §15.1 has an unrecognised status processed, not refused", err)
			}
			want := 0
			for i := 0; i < 3; i++ {
				want = want*10 + int(code[i]-'0')
			}
			if got != want {
				t.Errorf("status = %d, want %d — the response behind this one was drained "+
					"into and returned instead", got, want)
			}
		})
	}
}

// TestConformance_RFC9112_Sec2_2_BareCRRejected pins the bare-CR rule.
//
// §2.2: "A recipient of such a bare CR MUST consider that element to be invalid
// or replace each bare CR with SP before processing the element or forwarding
// the message." The line reader did neither — TrimRight(line, "\r\n") DELETED
// the run — so "Content-Length: 5\r\r\n" was a clean Content-Length of 5 here
// and an invalid field line to anyone applying §2.2. The trailing EXTRA is what
// that disagreement costs: a recipient that rejects the field never frames the
// body at 5.
func TestConformance_RFC9112_Sec2_2_BareCRRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
	}{
		{"in a field line", "HTTP/1.1 200 OK\r\nContent-Length: 5\r\r\n\r\nHELLOEXTRA"},
		{"in the status line", "HTTP/1.1 200 OK\r\r\nContent-Length: 5\r\n\r\nHELLOEXTRA"},
		{"run of CRs", "HTTP/1.1 200 OK\r\nContent-Length: 5\r\r\r\n\r\nHELLOEXTRA"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex, err := readResponseErr(t, tc.wire)
			wantHeadRejected(t, ex, err, "a bare CR")
		})
	}
}

// TestConformance_RFC9112_Sec2_2_LoneLFStillTerminates is the control: §2.2 also
// says a recipient "MAY recognize a single LF as a line terminator and ignore
// any preceding CR", so tightening the bare-CR case must not turn LF-terminated
// lines into errors.
func TestConformance_RFC9112_Sec2_2_LoneLFStillTerminates(t *testing.T) {
	ex := wireExchange(t, "GET", "HTTP/1.1 200 OK\nContent-Length: 5\n\nHELLO")
	code, _, err := ex.ReadResponse(context.Background())
	if err != nil {
		t.Fatalf("ReadResponse = %v; §2.2 permits a lone LF as the line terminator", err)
	}
	if code != 200 {
		t.Errorf("status = %d, want 200", code)
	}
}

// TestConformance_RFC9112_Sec5_1_NoWhitespaceBeforeColon pins the rule §5.1
// states and then explains: "No whitespace is allowed between the field name and
// colon. In the past, differences in the handling of such whitespace have led to
// security vulnerabilities in request routing and response handling."
//
// The name was TrimSpace'd before validation, which silently normalised
// "Content-Length : 5" into a Content-Length this client framed the body by. The
// hop in front of us is obliged by the same section to "remove any such
// whitespace from a response message before forwarding" — so it may have
// forwarded a message with no Content-Length at all, and the two of us then
// disagree about where the body ends.
func TestConformance_RFC9112_Sec5_1_NoWhitespaceBeforeColon(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
	}{
		{"SP before colon", "HTTP/1.1 200 OK\r\nContent-Length : 5\r\n\r\nHELLOEXTRA"},
		{"HTAB before colon", "HTTP/1.1 200 OK\r\nContent-Length\t: 5\r\n\r\nHELLOEXTRA"},
		{"two SP before colon", "HTTP/1.1 200 OK\r\nContent-Length  : 5\r\n\r\nHELLOEXTRA"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex, err := readResponseErr(t, tc.wire)
			wantHeadRejected(t, ex, err, "whitespace between field name and colon")
		})
	}
}

// TestConformance_RFC9112_Sec5_1_FieldValueTrimIsOWSOnly pins that the
// whitespace stripped from a field value is OWS — SP and HTAB (RFC 9110 §5.6.3)
// — and nothing else.
//
// strings.TrimSpace is Unicode-aware and also removed VT, FF and NEL. §2.2
// requires a recipient to "parse an HTTP message as a sequence of octets", and
// names Unicode-level string processing as the reason: "Parsing an HTTP message
// as a stream of Unicode characters, without regard for the specific encoding,
// creates security vulnerabilities". Concretely, "Content-Length: 5\v" was a
// valid 5 here and an invalid field value to an octet parser.
func TestConformance_RFC9112_Sec5_1_FieldValueTrimIsOWSOnly(t *testing.T) {
	for _, ws := range []string{"\v", "\f", "\u0085"} {
		t.Run(strings.TrimSpace(quoteWS(ws)), func(t *testing.T) {
			ex, err := readResponseErr(t, "HTTP/1.1 200 OK\r\nContent-Length: 5"+ws+"\r\n\r\nHELLOEXTRA")
			wantRejected(t, ex, err, "a non-OWS octet in the Content-Length value")
		})
	}

	t.Run("OWS is still trimmed", func(t *testing.T) {
		ex := wireExchange(t, "GET", "HTTP/1.1 200 OK\r\nContent-Length: \t5 \t\r\n\r\nHELLO")
		if _, _, err := ex.ReadResponse(context.Background()); err != nil {
			t.Fatalf("ReadResponse = %v; §5.1 excludes leading and trailing OWS from the value", err)
		}
	})
}

// quoteWS names a whitespace octet for a subtest title.
func quoteWS(s string) string {
	switch s {
	case "\v":
		return "VT"
	case "\f":
		return "FF"
	case "\u0085":
		return "NEL"
	}
	return s
}

// TestConformance_RFC9112_Sec7_1_ChunkSizeGrammar pins `chunk-size = 1*HEXDIG`
// with no surrounding whitespace.
//
// The size line was TrimSpace'd wholesale, so " 5", "5 " and "5\v" all parsed as
// 5. A chunk-size line is the worst place for a framing disagreement: every
// octet after it either is or is not body depending on who read it. The one
// whitespace the grammar does allow is the BWS in §7.1.1's `chunk-ext = *( BWS
// ";" BWS chunk-ext-name ... )`, which exists only when a ";" follows — covered
// by the accept case below.
func TestConformance_RFC9112_Sec7_1_ChunkSizeGrammar(t *testing.T) {
	const tail = "hello\r\n0\r\n\r\n"

	for _, size := range []string{" 5", "5 ", "5\v", "5\f", "\t5", "5 5"} {
		t.Run("reject "+quoteWS(size), func(t *testing.T) {
			ex := wireExchange(t, "GET",
				"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"+size+"\r\n"+tail)
			if _, _, err := ex.ReadResponse(context.Background()); err != nil {
				t.Fatalf("ReadResponse = %v, want the head to parse", err)
			}
			buf := make([]byte, 64)
			if _, _, err := ex.ReadBodyChunk(buf); err == nil {
				t.Fatalf("chunk-size %q accepted; 1*HEXDIG admits no surrounding whitespace", size)
			}
			if ex.KeepAlive() {
				t.Error("KeepAlive() = true after a malformed chunk-size; the stream position is " +
					"indeterminate, so the connection must not be pooled")
			}
		})
	}

	// The control, and the one legal whitespace: BWS before the ";" of a
	// chunk-ext. A parse that answered "no whitespace ever" would break this.
	for _, ok := range []string{"5;a=b", "5 ;a=b", "5\t;a=b", "5 ; a = b"} {
		t.Run("accept "+ok, func(t *testing.T) {
			ex := wireExchange(t, "GET",
				"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"+ok+"\r\n"+tail)
			if _, _, err := ex.ReadResponse(context.Background()); err != nil {
				t.Fatalf("ReadResponse = %v", err)
			}
			if got := readAllTolerant(ex); got != "hello" {
				t.Errorf("body = %q, want %q — BWS before the chunk-ext ';' is §7.1.1 grammar", got, "hello")
			}
		})
	}
}

// TestConformance_RFC9112_Sec7_1_ChunkDataTerminatorMustBeCRLF pins the second
// CRLF of `chunk = chunk-size [ chunk-ext ] CRLF chunk-data CRLF`.
//
// The terminator was read as a line and discarded whatever it held, which made
// the real delimiter "everything up to the next LF". A recipient measuring
// chunk-data by chunk-size — as the grammar says to — reads those parked octets
// as the next chunk-size line instead. Same body, two framings.
func TestConformance_RFC9112_Sec7_1_ChunkDataTerminatorMustBeCRLF(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
	}{
		{"junk before the CRLF", "5\r\nhelloJUNK\r\n0\r\n\r\n"},
		{"a whole size line parked there", "5\r\nhello5\r\nWORLD\r\n0\r\n\r\n"},
		{"single space", "5\r\nhello \r\n0\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := wireExchange(t, "GET",
				"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"+tc.wire)
			if _, _, err := ex.ReadResponse(context.Background()); err != nil {
				t.Fatalf("ReadResponse = %v", err)
			}
			var err error
			buf := make([]byte, 64)
			for i := 0; i < 4 && err == nil; i++ {
				var done bool
				_, done, err = ex.ReadBodyChunk(buf)
				if done {
					break
				}
			}
			if err == nil {
				t.Fatalf("chunk-data terminated by %q accepted; §7.1 requires exactly CRLF", tc.wire)
			}
			if ex.KeepAlive() {
				t.Error("KeepAlive() = true after a malformed chunk terminator")
			}
		})
	}
}

// TestConformance_RFC9112_Sec6_WriteBodyWithoutFramingRefused pins that octets
// cannot be written after a head that declared no body.
//
// A head sent with endStream promised the peer a message ending at the blank
// line. Anything written after it is not a body — it is whatever the peer's
// parser makes of it, and what it makes of it is the next request-line. The
// fixture asserts on the wire because that is where the damage is: the peer sees
// two requests where the caller issued one, which is §11.2 request smuggling
// built from the caller's own bytes.
//
// GET, HEAD, DELETE and OPTIONS are the reachable set: they get no synthesized
// "Content-Length: 0" on a bodyless head, so the declared length stayed unset
// and the over-run check keyed on it never fired. POST, PUT and PATCH were
// already covered for that reason and are included as the control.
func TestConformance_RFC9112_Sec6_WriteBodyWithoutFramingRefused(t *testing.T) {
	const smuggled = "GET /admin HTTP/1.1\r\nHost: evil\r\n\r\n"

	for _, m := range []string{"GET", "HEAD", "DELETE", "OPTIONS", "POST", "PUT", "PATCH"} {
		t.Run(m, func(t *testing.T) {
			ex, capture := rawCapture(t)
			if err := ex.WriteRequest(context.Background(), reqCL(m), true); err != nil {
				t.Fatalf("WriteRequest: %v", err)
			}
			err := ex.WriteBody(context.Background(), []byte(smuggled), true)
			if err == nil {
				t.Errorf("WriteBody after a bodyless head = nil, want a refusal")
			}
			if wire := capture(); strings.Contains(wire, "/admin") {
				t.Errorf("the unframed octets reached the wire: %q\n"+
					"the peer parses them as a second request the caller never issued", wire)
			}
			if ex.KeepAlive() {
				t.Error("KeepAlive() = true after an unframed body write")
			}
		})
	}
}

// TestConformance_RFC9112_Sec6_WriteBodyWithFramingStillWorks is the control for
// the refusal above: a head that DID declare a length must still accept exactly
// that many octets.
func TestConformance_RFC9112_Sec6_WriteBodyWithFramingStillWorks(t *testing.T) {
	ex, capture := rawCapture(t)
	fields := reqCL("POST", hpack.HeaderField{Name: []byte("content-length"), Value: []byte("5")})
	if err := ex.WriteRequest(context.Background(), fields, false); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if err := ex.WriteBody(context.Background(), []byte("HELLO"), true); err != nil {
		t.Fatalf("WriteBody = %v, want the declared 5 octets to be accepted", err)
	}
	if wire := capture(); !strings.HasSuffix(wire, "\r\n\r\nHELLO") {
		t.Errorf("wire = %q, want it to end with the declared body", wire)
	}
}
