package http1

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestInterimCap_MatchesSiblings is a tripwire, not a behaviour test. See the
// identical test in conn/ and http3/ for the full reasoning: the three
// maxInterimResponses constants bound the same thing for one public API
// (client.Do), cannot share a definition without creating import edges the
// layering deliberately avoids, and so are pinned per package instead.
func TestInterimCap_MatchesSiblings(t *testing.T) {
	const agreedAcrossProtocols = 100

	got := maxInterimResponses

	assert.Equalf(t, agreedAcrossProtocols, got,
		"http1.maxInterimResponses = %d, want %d.\n"+
			"This cap is shared by contract with conn.maxInterimResponses "+
			"(conn/handler.go) and http3.maxInterimResponses (http3/client.go). "+
			"If the new value is intended, change all three and update this test "+
			"in all three packages — client.Do must behave identically on H1, H2 and H3.",
		got, agreedAcrossProtocols)
}
