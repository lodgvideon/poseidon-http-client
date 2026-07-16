package http1_test

// Repeated Transfer-Encoding field lines are ONE list.
//
// RFC 9110 §5.3 defines repeated field lines by appending each value "to the
// initial field line value in order, separated by a comma (",") and optional
// whitespace" — so two field lines and one comma-separated line are the same
// message, and must be framed the same way. They were not: each line overwrote
// the verdict, so the last one won.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// teFramingLines reports whether the client chunk-framed the body for a given
// raw block of Transfer-Encoding field lines.
func teFramingLines(t *testing.T, lines string) (chunked bool, body string) {
	t.Helper()
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\n"+lines+"\r\n5\r\nhello\r\n0\r\n\r\nLEFTOVER")
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	body = drainTolerant(ex)
	return body == "hello", body
}

func drainTolerant(ex *http1.Exchange) string {
	buf := make([]byte, 512)
	var out []byte
	for {
		n, done, err := ex.ReadBodyChunk(buf)
		out = append(out, buf[:n]...)
		if err != nil || done {
			return string(out)
		}
	}
}

// TestConformance_RFC9110_Sec5_3_RepeatedTELinesAreOneList pins that the same
// message framed two ways is framed the same way.
//
// The pairs below are identical messages by §5.3. Before the fix the two-line
// spelling of each disagreed with the one-line spelling — an empty final line
// erased chunked framing and the raw chunk bytes were returned as the body.
func TestConformance_RFC9110_Sec5_3_RepeatedTELinesAreOneList(t *testing.T) {
	for _, tc := range []struct {
		name        string
		twoLines    string
		oneLine     string
		wantChunked bool
	}{
		{
			// §5.6.1.2: a recipient must ignore empty list elements, so the
			// final coding here is chunked, not "".
			name:        "chunked_then_empty",
			twoLines:    "Transfer-Encoding: chunked\r\nTransfer-Encoding: \r\n",
			oneLine:     "Transfer-Encoding: chunked, \r\n",
			wantChunked: true,
		},
		{
			name:        "gzip_then_chunked",
			twoLines:    "Transfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n",
			oneLine:     "Transfer-Encoding: gzip, chunked\r\n",
			wantChunked: true,
		},
		{
			name:        "chunked_then_gzip",
			twoLines:    "Transfer-Encoding: chunked\r\nTransfer-Encoding: gzip\r\n",
			oneLine:     "Transfer-Encoding: chunked, gzip\r\n",
			wantChunked: false,
		},
		{
			name:        "empty_then_chunked",
			twoLines:    "Transfer-Encoding: \r\nTransfer-Encoding: chunked\r\n",
			oneLine:     "Transfer-Encoding: , chunked\r\n",
			wantChunked: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			two, twoBody := teFramingLines(t, tc.twoLines)
			one, _ := teFramingLines(t, tc.oneLine)
			if two != one {
				t.Errorf("two field lines and the equivalent one-line list disagree: "+
					"two-line chunked=%v, one-line chunked=%v — RFC 9110 §5.3 makes them "+
					"the same message, so they must frame the same way (two-line body=%.40q)",
					two, one, twoBody)
			}
			if two != tc.wantChunked {
				t.Errorf("chunked=%v, want %v for:\n%s", two, tc.wantChunked, tc.twoLines)
			}
		})
	}
}

// TestConformance_RFC9110_Sec5_3_EmptyTELineAloneStillPresent is the boundary
// the fix must not cross: a Transfer-Encoding line that contributes no codings
// still means the FIELD is present, which RFC 9112 §6.3 rule 3 keys on (it
// overrides Content-Length) and rule 5 keys on (its fatal case is scoped to a
// message "received without Transfer-Encoding").
//
// So skipping the line's effect on the framing verdict must not also make the
// field invisible — respTE stays set.
func TestConformance_RFC9110_Sec5_3_EmptyTELineAloneStillPresent(t *testing.T) {
	// TE present (empty) + an invalid Content-Length. Rule 5 cannot fire because
	// Transfer-Encoding IS present; rule 4 frames the body by reading to close.
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: \r\nContent-Length: not-a-number\r\n\r\nhello")
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Errorf("ReadResponse err = %v, want nil — an empty Transfer-Encoding line still "+
			"makes the field PRESENT, and RFC 9112 §6.3 rule 5 is scoped to a message "+
			"received WITHOUT Transfer-Encoding", err)
	}
	if ex.KeepAlive() {
		t.Error("KeepAlive() = true — rule 3: a message with both Transfer-Encoding and " +
			"Content-Length ought to be handled as an error, so the socket must not be reused")
	}
}
