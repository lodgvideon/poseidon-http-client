package http1_test

import (
	"context"
	"testing"
)

// Two truncation positions that the existing suite steps over.
//
// The chunked tests all stop somewhere else: one truncates AFTER the terminal
// `0\r\n` (terminal chunk seen, trailer section missing), another truncates
// INSIDE chunk data. Neither lands on the position where the parser sits at a
// perfectly legal inter-chunk boundary and must still refuse to call the body
// complete — which is the position a real connection reset is most likely to
// produce, since it is the quiet moment between writes.
//
// The identity-framed case is the opposite problem and is pinned here precisely
// because it CANNOT be detected: with no Content-Length and no
// Transfer-Encoding the body runs until the connection closes (RFC 9112 §6.3
// rule 7), so a truncated body and a complete one are byte-identical on the
// wire. Reporting an error there would reject valid responses. The test exists
// so that property is a decision on the record rather than an accident a future
// framing change could flip unnoticed.

// readAll drains the body and reports the bytes read and the terminal error.
func readAll(t *testing.T, ex interface {
	ReadBodyChunk([]byte) (int, bool, error)
}) (int, error) {
	t.Helper()
	total := 0
	for {
		n, done, err := ex.ReadBodyChunk(make([]byte, 64))
		total += n
		if err != nil {
			return total, err
		}
		if done {
			return total, nil
		}
	}
}

// TestConformance_RFC9112_Sec6_3_ChunkedEOFAtChunkBoundary covers a close that
// lands between two well-formed chunks: the last chunk received is complete, so
// nothing is malformed, and the terminal `0\r\n\r\n` simply never arrives. The
// parser is at a legal boundary and must still report the body as incomplete —
// treating "no error so far" as "done" would hand the caller a short body with
// no indication anything was lost.
func TestConformance_RFC9112_Sec6_3_ChunkedEOFAtChunkBoundary(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"+
			"5\r\nHELLO\r\n") // one complete chunk, then EOF: no terminal 0-chunk
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	n, err := readAll(t, ex)
	if err == nil {
		t.Fatalf("a chunked body cut at a chunk boundary reported complete after %d bytes; "+
			"the terminal 0-chunk never arrived, so the caller cannot tell this from a "+
			"whole response", n)
	}
	if ex.KeepAlive() {
		t.Error("KeepAlive() = true after a truncated chunked body, want false — the " +
			"stream position is indeterminate, so the connection must not be reused " +
			"(RFC 9112 §6.3 rule 6)")
	}
}

// TestConformance_RFC9112_Sec6_3_ChunkedEOFAfterTerminalChunkIsComplete is the
// control that gives the test above its meaning. The same framing, one chunk
// further: with the terminal chunk present the body IS complete, and an
// implementation that simply errored on every chunked EOF would pass the test
// above for the wrong reason and fail here.
func TestConformance_RFC9112_Sec6_3_ChunkedEOFAfterTerminalChunkIsComplete(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"+
			"5\r\nHELLO\r\n0\r\n\r\n")
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	n, err := readAll(t, ex)
	if err != nil {
		t.Fatalf("a complete chunked body reported an error after %d bytes: %v", n, err)
	}
	if n != 5 {
		t.Errorf("read %d body bytes, want 5", n)
	}
}

// TestConformance_RFC9112_Sec6_3_IdentityFramedTruncationIsIndistinguishable
// pins the one framing where truncation cannot be detected at all. With neither
// Content-Length nor Transfer-Encoding the body is delimited by the close
// itself (§6.3 rule 7), so the bytes on the wire are identical whether the
// origin finished or the connection died mid-write.
//
// err == nil is therefore the conformant answer, not a gap: raising an error
// here would reject every valid close-delimited response. The hazard is real
// for a load generator and belongs in the docs, not in the parser — this test
// exists so the behaviour cannot change silently.
func TestConformance_RFC9112_Sec6_3_IdentityFramedTruncationIsIndistinguishable(t *testing.T) {
	ex := wireExchange(t, "GET",
		"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n"+
			"partial")
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	n, err := readAll(t, ex)
	if err != nil {
		t.Fatalf("a close-delimited body reported an error after %d bytes: %v\n"+
			"§6.3 rule 7 makes the close the delimiter, so this is a COMPLETE response "+
			"as far as the protocol can tell; erroring rejects valid responses", n, err)
	}
	if n != len("partial") {
		t.Errorf("read %d body bytes, want %d", n, len("partial"))
	}
	if ex.KeepAlive() {
		t.Error("KeepAlive() = true after a close-delimited body, want false — the " +
			"connection is the delimiter, so there is nothing left to reuse")
	}
}
