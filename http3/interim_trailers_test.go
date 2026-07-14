package http3

import (
	"context"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// TestDecodeTrailers checks that a trailer field section decodes its regular
// headers and rejects any pseudo-header (RFC 9114 §4.1/§4.3).
func TestDecodeTrailers(t *testing.T) {
	var dec qpack.Decoder
	fields, _, err := DecodeTrailers(&dec, nil, encodeSection(hf("x-checksum", "abc"), hf("x-count", "3")))
	if err != nil {
		t.Fatalf("DecodeTrailers: %v", err)
	}
	if len(fields) != 2 || string(fields[0].Name) != "x-checksum" || string(fields[1].Value) != "3" {
		t.Fatalf("trailer fields = %+v", fields)
	}
	if _, _, err := DecodeTrailers(&dec, nil, encodeSection(hf(":status", "200"))); err != ErrH3Message {
		t.Fatalf("pseudo-header in trailers: err = %v, want ErrH3Message", err)
	}
}

// TestClient_InterimAndTrailers drives the full response message sequence
// (RFC 9114 §4.1): a 1xx informational response, the final response, a DATA body,
// and a trailer section.
func TestClient_InterimAndTrailers(t *testing.T) {
	interim := AppendHeaders(nil, encodeSection(hf(":status", "103"), hf("link", "</s.css>; rel=preload")))
	final := AppendHeaders(nil, encodeSection(hf(":status", "200"), hf("content-type", "text/plain")))
	data := AppendData(nil, []byte("hi"))
	trailers := AppendHeaders(nil, encodeSection(hf("x-checksum", "abc")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{interim, final, data, trailers}, fin: true}}
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, body, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("status = %d, want 200", resp.Status)
	}
	if string(body) != "hi" {
		t.Fatalf("body = %q, want %q", body, "hi")
	}
	if len(resp.Interim) != 1 || resp.Interim[0].Status != 103 {
		t.Fatalf("interim = %+v, want one 103", resp.Interim)
	}
	if len(resp.Trailers) != 1 || string(resp.Trailers[0].Name) != "x-checksum" {
		t.Fatalf("trailers = %+v", resp.Trailers)
	}
}

// TestClient_MessageOrderErrors checks the §4.1 ordering rules: DATA cannot
// precede the final response or a 1xx body, and nothing may follow the trailers.
// An invalid frame sequence is a connection error of type H3_FRAME_UNEXPECTED.
func TestClient_MessageOrderErrors(t *testing.T) {
	final := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	data := AppendData(nil, []byte("x"))
	trailers := AppendHeaders(nil, encodeSection(hf("x-a", "b")))
	onexx := AppendHeaders(nil, encodeSection(hf(":status", "100")))

	cases := []struct {
		name   string
		chunks [][]byte
	}{
		{"data before final headers", [][]byte{data, final}},
		{"data after a 1xx (no body allowed)", [][]byte{onexx, data}},
		{"headers after trailers", [][]byte{final, trailers, trailers}},
		{"data after trailers", [][]byte{final, data, trailers, data}},
	}
	for _, tc := range cases {
		conn := &fakeConn{req: &fakeStream{recvChunks: tc.chunks, fin: true}}
		client, err := NewClientFake(conn, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"})
		if err != ErrH3Control {
			t.Fatalf("%s: err = %v, want ErrH3Control (connection error)", tc.name, err)
		}
		if conn.closeCode != H3FrameUnexpected {
			t.Fatalf("%s: close code = %#x, want H3_FRAME_UNEXPECTED", tc.name, conn.closeCode)
		}
	}
}

// TestClient_InterimWithoutFinal checks that a stream carrying only a 1xx and no
// final response is a malformed message (RFC 9114 §4.1).
func TestClient_InterimWithoutFinal(t *testing.T) {
	onexx := AppendHeaders(nil, encodeSection(hf(":status", "100")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{onexx}, fin: true}}
	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"}); err != ErrH3Message {
		t.Fatalf("1xx without final: err = %v, want ErrH3Message", err)
	}
}
