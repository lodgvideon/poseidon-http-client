package conn

// Conformance tests for response PSEUDO-HEADER validation, RFC 7540 §8.1.2.1 and
// §8.1.2.4. The receive path checked a ':' field's value but nothing else, so a
// response with an undefined pseudo-header, a duplicated :status, a pseudo-header
// after a regular one, a pseudo-header in a trailer section, or no :status at all
// was accepted and delivered to the caller as a success. §8.1.2.6 routes each to
// a stream error of type PROTOCOL_ERROR. The HTTP/3 sibling (http3/response.go)
// has always enforced these; this is the HTTP/2 path catching up.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func hf(name, value string) hpack.HeaderField {
	return hpack.HeaderField{Name: []byte(name), Value: []byte(value)}
}

// TestConformance_RFC7540_Sec8_1_2_1_ResponsePseudoHeaders_Malformed pins the
// four §8.1.2.1 rules on a response header block: only :status, at most once,
// before every regular field, and no undefined pseudo-header. Plus §8.1.2.4:
// :status is mandatory.
func TestConformance_RFC7540_Sec8_1_2_1_ResponsePseudoHeaders_Malformed(t *testing.T) {
	cases := []struct {
		name   string
		fields []hpack.HeaderField
	}{
		{"duplicate :status", []hpack.HeaderField{hf(":status", "200"), hf(":status", "500"), hf("content-type", "text/plain")}},
		{"undefined pseudo :authority", []hpack.HeaderField{hf(":status", "200"), hf(":authority", "evil.example")}},
		{"undefined pseudo :path", []hpack.HeaderField{hf(":status", "200"), hf(":path", "/")}},
		{"pseudo after regular", []hpack.HeaderField{hf("content-type", "text/plain"), hf(":status", "200")}},
		{"missing :status", []hpack.HeaderField{hf("content-type", "text/plain")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeStreamMap()
			h := newConnHandler(m, hpack.NewDecoder())
			m.addStream(1)

			err := deliverBlock(t, h, 1, tc.fields, false)

			wantStreamProtocolError(t, err, tc.name)
		})
	}
}

// TestConformance_RFC7540_Sec8_1_2_1_PseudoHeaderInTrailer_Malformed pins
// §8.1.2.1: "Pseudo-header fields MUST NOT appear in trailers." A valid header
// block opens the stream; the trailing block then carries a pseudo-header and
// must be rejected.
func TestConformance_RFC7540_Sec8_1_2_1_PseudoHeaderInTrailer_Malformed(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	require.NoError(t, deliverBlock(t, h, 1, []hpack.HeaderField{hf(":status", "200")}, false),
		"valid header block rejected")
	<-s.events // consume the EventHeaders

	// Trailer section carrying :status — a pseudo-header, forbidden in trailers.
	err := deliverBlock(t, h, 1, []hpack.HeaderField{hf(":status", "200"), hf("x-checksum", "abc")}, true)

	wantStreamProtocolError(t, err, "pseudo-header in trailer")
}

// TestConformance_RFC7540_Sec8_1_2_1_WellFormedResponseAccepted is the
// over-rejection guard: a single :status before regular fields, and a trailer
// section with only regular fields, are both legal and must be accepted.
func TestConformance_RFC7540_Sec8_1_2_1_WellFormedResponseAccepted(t *testing.T) {
	t.Run("header block", func(t *testing.T) {
		m := newFakeStreamMap()
		h := newConnHandler(m, hpack.NewDecoder())
		m.addStream(1)

		err := deliverBlock(t, h, 1, []hpack.HeaderField{hf(":status", "200"), hf("content-type", "text/plain")}, false)

		require.NoError(t, err, "well-formed response rejected")
	})
	t.Run("trailer without pseudo", func(t *testing.T) {
		m := newFakeStreamMap()
		h := newConnHandler(m, hpack.NewDecoder())
		s := m.addStream(1)
		require.NoError(t, deliverBlock(t, h, 1, []hpack.HeaderField{hf(":status", "200")}, false),
			"header block rejected")
		<-s.events // consume the EventHeaders

		err := deliverBlock(t, h, 1, []hpack.HeaderField{hf("x-checksum", "abc")}, true)

		require.NoError(t, err, "well-formed trailer rejected")
	})
}
