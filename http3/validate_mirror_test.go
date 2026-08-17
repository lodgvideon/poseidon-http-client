package http3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// conn/validate.go and this package's validate.go claim to be deliberate
// mirrors, "rule for rule". That claim went stale: HTTP/2 grew the
// edge-whitespace rule and HTTP/3 did not, so " x " was a stream error on one
// transport and a value handed to the caller on the other — the same
// reserialization-smuggling hazard the peer-input policy exists for, answered
// differently depending on which protocol negotiated.
//
// A comment cannot hold that property, and neither can this file on its own:
// conn's validator is unexported and a shared one is its own refactor (#492).
// What this does instead is give the HTTP/3 side the table the HTTP/2 side
// already had — TestConformance_RFC9113_Sec8_2_1_ResponseFieldValueEdgeWhitespace_Malformed
// in conn/conformance_respfields_test.go — so the rule is now pinned on BOTH
// sides rather than on one. The divergence happened because only one had a test.

// mirrorCases are the field values whose treatment must not depend on the
// protocol.
var mirrorCases = []struct {
	name  string
	value string
	valid bool
}{
	{"plain", "text/plain", true},
	{"inner spaces", "a b c", true},
	{"inner tab", "a\tb", true},
	{"empty", "", true},
	{"single visible char", "x", true},

	{"leading space", " x", false},
	{"trailing space", "x ", false},
	{"leading tab", "\tx", false},
	{"trailing tab", "x\t", false},
	{"both ends", " x ", false},
	{"only a space", " ", false},
	{"only a tab", "\t", false},

	{"NUL", "a\x00b", false},
	{"CR", "a\rb", false},
	{"LF", "a\nb", false},
	{"CRLF injection", "a\r\nX-Evil: 1", false},
}

// TestValidateMirror_FieldValueAgreesWithHTTP2 is the gate on the mirror claim.
func TestValidateMirror_FieldValueMatchesTheHTTP2Table(t *testing.T) {
	for _, tc := range mirrorCases {
		t.Run(tc.name, func(t *testing.T) {
			got := validFieldValue([]byte(tc.value))

			assert.Equalf(t, tc.valid, got,
				"%q: validFieldValue = %v, want %v — HTTP/2 answers %v for the same "+
					"bytes, and which servers work must not depend on which protocol "+
					"negotiated", tc.value, got, tc.valid, tc.valid)
		})
	}
}

// TestConformance_RFC9114_Sec412_EdgeWhitespaceIsMalformed states the HTTP/3
// requirement on its own terms, so the rule survives even if the HTTP/2 mirror
// is ever retired.
//
// RFC 9110 §5.5: "A field value does not include leading or trailing
// whitespace", and its ABNF admits SP/HTAB only between field-vchars. RFC 9114
// §4.1.2 makes "the inclusion of invalid characters in field names or values"
// malformed and says "Clients MUST NOT accept a malformed response."
func TestConformance_RFC9114_Sec412_EdgeWhitespaceIsMalformed(t *testing.T) {
	malformed := []string{" x", "x ", "\tx", "x\t", " ", "\t", " x "}
	legal := []string{"x", "a b", "a\tb", ""}

	gotMalformed := make([]bool, len(malformed))
	for i, v := range malformed {
		gotMalformed[i] = validFieldValue([]byte(v))
	}
	gotLegal := make([]bool, len(legal))
	for i, v := range legal {
		gotLegal[i] = validFieldValue([]byte(v))
	}

	for i, v := range malformed {
		assert.Falsef(t, gotMalformed[i],
			"%q accepted; a field value must not start or end with SP or HTAB", v)
	}
	for i, v := range legal {
		assert.Truef(t, gotLegal[i],
			"%q rejected; SP and HTAB are legal BETWEEN field-vchars", v)
	}
}
