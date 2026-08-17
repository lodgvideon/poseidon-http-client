package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestInterimCap_MatchesSiblings is a tripwire, not a behaviour test.
//
// http1.maxInterimResponses, http3.maxInterimResponses and this one bound the
// same thing — how many 1xx responses one exchange tolerates — and client.Do
// runs over all three stacks behind one public API, so they have to agree or the
// same application against the same origin succeeds or fails depending on which
// protocol ALPN picked.
//
// They cannot share a definition. Exporting one would create http1 -> conn and
// http3 -> conn edges: http1 is the minimal HTTP/1.1 codec that today depends on
// hpack alone, and the H3 stack is deliberately standalone. internal/bytesx is
// the wrong home too — it holds wire primitives, not per-exchange policy, and
// neither conn nor http1 imports it today.
//
// So the three stay independently declared and each package carries this test.
// Changing one turns exactly one test red, with a message naming the other two.
func TestInterimCap_MatchesSiblings(t *testing.T) {
	const agreedAcrossProtocols = 100

	got := maxInterimResponses

	assert.EqualValuesf(t, agreedAcrossProtocols, got,
		"conn.maxInterimResponses = %d, want %d.\n"+
			"This cap is shared by contract with http1.maxInterimResponses "+
			"(http1/conn.go) and http3.maxInterimResponses (http3/client.go). "+
			"If the new value is intended, change all three and update this test "+
			"in all three packages — client.Do must behave identically on H1, H2 and H3.",
		got, agreedAcrossProtocols)
}
