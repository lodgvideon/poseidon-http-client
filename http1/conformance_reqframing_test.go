package http1_test

// Conformance tests for the framing this client puts on the wire when it sends
// a request, keyed to RFC 9112 §6.1 and RFC 9110 §5.1.
//
// The response direction has the larger suite because a hostile server is the
// obvious threat. These cover the other direction: the client emitting framing
// that contradicts itself. A request carrying both a Content-Length and a
// Transfer-Encoding is the request-smuggling primitive (RFC 9112 §11.2) — a
// front end honouring one and a back end honouring the other disagree about
// where the request ends — and nothing downstream can undo a client that sends
// it.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// requestHead runs one WriteRequest against a pipe and returns the bytes the
// client actually put on the wire, up to and including the blank line.
func requestHead(t *testing.T, method string, endStream bool, extra ...hpack.HeaderField) string {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })

	headCh := make(chan string, 1)
	go func() {
		defer server.Close()
		br := bufio.NewReader(server)
		var sb strings.Builder
		for {
			line, err := br.ReadString('\n')
			sb.WriteString(line)
			if err != nil || strings.TrimRight(line, "\r\n") == "" {
				break
			}
		}
		headCh <- sb.String()
	}()

	fields := append([]hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte(method)},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}, extra...)

	ex := http1.NewConn(client).NewExchange()
	if err := ex.WriteRequest(context.Background(), fields, endStream); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	return <-headCh
}

// countFieldLines counts field lines whose name folds to want.
func countFieldLines(head, want string) int {
	n := 0
	for _, line := range strings.Split(head, "\r\n") {
		colon := strings.IndexByte(line, ':')
		if colon > 0 && strings.EqualFold(strings.TrimSpace(line[:colon]), want) {
			n++
		}
	}
	return n
}

// TestConformance_RFC9112_Sec6_1_NoContentLengthWithTransferEncoding pins that
// this client never emits both framing headers, whatever spelling the caller
// used for Content-Length.
//
// RFC 9110 §5.1: "Field names are case-insensitive", so every spelling below is
// the same request. RFC 9112 §6.1: "A sender MUST NOT send a Content-Length
// header field in any message that contains a Transfer-Encoding header field."
//
// The detection loop matched "content-length" exactly while the emit loop
// lower-cased the caller's name and wrote it regardless, so the canonical
// "Content-Length" was written to the wire yet left hasContentLength false —
// and the client added Transfer-Encoding: chunked on top of it.
func TestConformance_RFC9112_Sec6_1_NoContentLengthWithTransferEncoding(t *testing.T) {
	for _, spelling := range []string{
		"content-length", "Content-Length", "CONTENT-LENGTH", "Content-length",
	} {
		t.Run(spelling, func(t *testing.T) {
			head := requestHead(t, "POST", false,
				hpack.HeaderField{Name: []byte(spelling), Value: []byte("5")})
			low := strings.ToLower(head)
			hasCL := strings.Contains(low, "content-length:")
			hasTE := strings.Contains(low, "transfer-encoding:")
			if hasCL && hasTE {
				t.Errorf("this client emitted BOTH framing headers for a caller-supplied %q:\n%s\n"+
					"RFC 9112 §6.1 forbids a sender from doing this; the pair is the "+
					"request-smuggling primitive of §11.2", spelling, head)
			}
			if !hasCL {
				t.Errorf("the caller's Content-Length did not reach the wire at all:\n%s", head)
			}
		})
	}
}

// TestConformance_RFC9112_Sec6_1_SingleContentLengthOnBodylessPost pins that a
// bodyless POST carrying a caller-supplied Content-Length: 0 emits exactly one.
//
// The client appends its own "Content-Length: 0" for POST/PUT/PATCH so strict
// servers accept a bodyless request, but did so without checking whether the
// caller had already supplied one — which the field loop had already written.
// Two disagreeing Content-Length lines is the CL.CL desync (RFC 9112 §11.2,
// and §6.3 rule 5 on the receiving end) emitted by us.
//
// The caller value is 0 because that is the only Content-Length that agrees with
// a bodyless request: a non-zero Content-Length with no body is itself a §8.6
// desync and is now refused outright (see
// TestConformance_RFC9110_Sec8_6_ContentLengthWithoutBodyRejected), so a caller
// "0" is the case where the append-vs-caller de-duplication still has to hold.
func TestConformance_RFC9112_Sec6_1_SingleContentLengthOnBodylessPost(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "PATCH"} {
		t.Run(method, func(t *testing.T) {
			head := requestHead(t, method, true,
				hpack.HeaderField{Name: []byte("Content-Length"), Value: []byte("0")})
			if n := countFieldLines(head, "content-length"); n != 1 {
				t.Errorf("%d Content-Length field lines, want 1:\n%s\n"+
					"the client must not append its own Content-Length: 0 when the "+
					"caller already supplied one", n, head)
			}
		})
	}
}

// TestConformance_RFC9112_Sec6_1_BodylessPostStillGetsContentLengthZero is the
// control for the fix above: when the caller supplies no Content-Length, the
// client must still add its own, or strict servers reject the request. It guards
// against "fixing" the duplicate by dropping the header altogether.
func TestConformance_RFC9112_Sec6_1_BodylessPostStillGetsContentLengthZero(t *testing.T) {
	head := requestHead(t, "POST", true)
	if !strings.Contains(strings.ToLower(head), "content-length: 0") {
		t.Errorf("bodyless POST lost its Content-Length: 0:\n%s", head)
	}
}
