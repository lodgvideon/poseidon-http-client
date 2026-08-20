package http3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// TestDecodeTrailers checks that a trailer field section decodes its regular
// headers and rejects any pseudo-header (RFC 9114 §4.1/§4.3).
func TestDecodeTrailers(t *testing.T) {
	var dec qpack.Decoder
	regular := encodeSection(hf("x-checksum", "abc"), hf("x-count", "3"))
	pseudo := encodeSection(hf(":status", "200"))

	fields, _, err := DecodeTrailers(&dec, nil, regular)
	_, _, pseudoErr := DecodeTrailers(&dec, nil, pseudo)

	require.NoError(t, err, "DecodeTrailers over a well-formed trailer section")
	require.Lenf(t, fields, 2, "trailer fields = %+v, want the two the section carried", fields)
	assert.Equalf(t, "x-checksum", string(fields[0].Name), "trailer fields = %+v", fields)
	assert.Equalf(t, "3", string(fields[1].Value), "trailer fields = %+v", fields)
	assert.Equalf(t, ErrH3Message, pseudoErr,
		"pseudo-header in trailers: err = %v, want ErrH3Message — §4.3 admits no "+
			"pseudo-header in a trailer section", pseudoErr)
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
	require.NoError(t, err, "NewClientFake over the fake transport")

	resp, body, doErr := client.Do(context.Background(),
		&Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"})

	require.NoError(t, doErr, "Do over the full §4.1 response sequence")
	require.NotNil(t, resp, "Do returned no response alongside a nil error")
	assert.Equalf(t, 200, resp.Status,
		"status = %d, want 200: the final response, not the 103, is what Do returns", resp.Status)
	assert.Equalf(t, "hi", string(body), "body = %q, want %q", body, "hi")
	require.Lenf(t, resp.Interim, 1, "interim = %+v, want one 103", resp.Interim)
	assert.Equalf(t, 103, resp.Interim[0].Status,
		"interim = %+v, want one 103: an informational response must be surfaced, not dropped",
		resp.Interim)
	require.Lenf(t, resp.Trailers, 1, "trailers = %+v, want the one field the section carried", resp.Trailers)
	assert.Equalf(t, "x-checksum", string(resp.Trailers[0].Name), "trailers = %+v", resp.Trailers)
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
		require.NoErrorf(t, err, "%s: NewClientFake over the fake transport", tc.name)

		_, _, doErr := client.Do(context.Background(),
			&Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"})

		assert.ErrorIsf(t, doErr, ErrH3Control,
			"%s: err = %v, want ErrH3Control (connection error) — an invalid frame "+
				"sequence must kill the connection, not just the request", tc.name, doErr)
		assert.Equalf(t, H3FrameUnexpected, conn.closeCode,
			"%s: close code = %#x, want H3_FRAME_UNEXPECTED", tc.name, conn.closeCode)
	}
}

// TestClient_InterimWithoutFinal checks that a stream carrying only a 1xx and no
// final response is a malformed message (RFC 9114 §4.1).
func TestClient_InterimWithoutFinal(t *testing.T) {
	onexx := AppendHeaders(nil, encodeSection(hf(":status", "100")))
	conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{onexx}, fin: true}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	_, _, doErr := client.Do(context.Background(),
		&Request{Method: "GET", Scheme: "https", Authority: "e.com", Path: "/"})

	assert.Equalf(t, ErrH3Message, doErr,
		"1xx without final: err = %v, want ErrH3Message — a stream that ends after only "+
			"an informational response carries no response for the caller to return", doErr)
}

// TestDecodeTrailers_PseudoHeaderRuleIsSeparable makes the §4.3 rule TestDecodeTrailers
// names testable on its own.
//
// DecodeTrailers refuses a trailer pseudo-header twice over: the explicit
// `name[0] == ':'` check, and validFieldName, whose token alphabet has no ':' in
// it. Either rule alone keeps TestDecodeTrailers green, so it cannot say which one
// fired — and deleting the §4.3 rule it exists to pin left the suite green 2/2
// (#797).
//
// Through the PUBLIC surface the two are redundant and no input can separate
// them: every name a pseudo-header can have starts with ':', which validFieldName
// refuses anyway. Removing the §4.3 rule is therefore an equivalent mutant today.
// What is NOT equivalent is relaxing validFieldName — for uppercase interop, say —
// which would quietly make §4.3's rule the only guard. So the mechanism is pinned
// white-box, directly on the predicate, alongside the table of decode outcomes.
func TestDecodeTrailers_PseudoHeaderRuleIsSeparable(t *testing.T) {
	// The equivalence classes of a trailer field name: a response pseudo-header,
	// a request pseudo-header, and an ordinary token.
	cases := []struct {
		name    string
		field   header.Field
		wantErr error
	}{
		{"response pseudo-header", hf(":status", "200"), ErrH3Message},
		{"request pseudo-header", hf(":path", "/"), ErrH3Message},
		{"ordinary token", hf("x-ok", "1"), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var dec qpack.Decoder

			_, _, err := DecodeTrailers(&dec, nil, encodeSection(tc.field))

			assert.Equalf(t, tc.wantErr, err,
				"DecodeTrailers with a %q trailer = %v, want %v — §4.3 admits no "+
					"pseudo-header in a trailer section", tc.field.Name, err, tc.wantErr)
		})
	}

	t.Run("validFieldName refuses a colon on its own", func(t *testing.T) {
		got := validFieldName([]byte(":status"))

		assert.Falsef(t, got,
			"validFieldName(\":status\") = %v, want false. This is the SECOND of the two "+
				"rules that reject a trailer pseudo-header, and while it holds, §4.3's own "+
				"check is unobservable through DecodeTrailers. Relaxing the token alphabet "+
				"to admit ':' makes §4.3's rule the only guard — which is fine, but it must "+
				"be a decision, not a side effect nobody noticed.", got)
	})
}
