package http1_test

// CONNECT over HTTP/1.1 — RFC 9112 §6.3 rule 2.
//
// Each test adds a row to docs/RFC_COVERAGE.md.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// TestConformance_RFC9112_Sec6_3_Rule2_ConnectRefused pins that a CONNECT
// request never reaches the wire on this exchange.
//
// Rule 2: "Any 2xx (Successful) response to a CONNECT request implies that the
// connection will become a tunnel immediately after the empty line that
// concludes the header fields. A client MUST ignore any Content-Length or
// Transfer-Encoding header fields received in such a message."
//
// The response path here frames every message by the fields the peer sent, so
// before this refusal a 2xx to CONNECT returned the tunnel's first octets as a
// message body, and the no-Content-Length variant fell to rule 4's
// read-until-close and blocked until the socket died. Implementing rule 2's
// framing instead would not help: there is no API on this Exchange to hand the
// caller the tunnelled socket, so a conformant CONNECT would succeed into a
// tunnel nobody can reach.
//
// Refusing at the send gate is what makes the tunnel unrepresentable — the
// request never leaves, so nothing is ever half-established. The wire assertion
// is the point of the test: an error alone would still permit a partially
// written request-line.
func TestConformance_RFC9112_Sec6_3_Rule2_ConnectRefused(t *testing.T) {
	ex, capture := rawCapture(t)
	err := ex.WriteRequest(context.Background(), reqCL("CONNECT"), true)
	if err == nil {
		t.Fatal("WriteRequest(CONNECT) = nil; a 2xx to CONNECT makes the connection a tunnel " +
			"(RFC 9112 §6.3 rule 2) and this exchange cannot honour that")
	}
	if !errors.Is(err, http1.ErrInvalidRequest) {
		t.Errorf("error = %v, want it to wrap ErrInvalidRequest so a caller can classify it", err)
	}
	if wire := capture(); wire != "" {
		t.Errorf("bytes reached the wire: %q — the refusal must happen before any octet "+
			"is written, or the peer sees a half-open tunnel request", wire)
	}
}

// TestConformance_RFC9112_Sec6_3_Rule2_OtherMethodsUnaffected is the control:
// the refusal is keyed on the exact method token and must not catch anything
// else, including methods whose names contain it.
func TestConformance_RFC9112_Sec6_3_Rule2_OtherMethodsUnaffected(t *testing.T) {
	for _, m := range []string{"GET", "POST", "OPTIONS", "CONNECTION", "XCONNECT", "connect"} {
		t.Run(m, func(t *testing.T) {
			ex, capture := rawCapture(t)
			if err := ex.WriteRequest(context.Background(), reqCL(m), true); err != nil {
				t.Fatalf("WriteRequest(%s) = %v, want it accepted — only the CONNECT token is refused", m, err)
			}
			if wire := capture(); !strings.HasPrefix(wire, m+" /") {
				t.Errorf("wire = %q, want it to start with %q", wire, m+" /")
			}
		})
	}
}
