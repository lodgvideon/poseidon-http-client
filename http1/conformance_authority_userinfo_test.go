package http1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/http1"
)

// TestConformance_RFC9110_Sec4_2_4_AuthorityUserinfoRejected pins that
// WriteRequest refuses a :authority carrying a userinfo "@" rather than emitting
// it verbatim into the Host field. RFC 9110 §4.2.4 deprecates the userinfo
// subcomponent and RFC 9112 §3.2 requires the Host field value to exclude it and
// its "@" delimiter — "user@host" must never become "Host: user@host". '@'
// cannot appear in a bare host[:port], so its presence is unconditionally an
// error. This guards direct http1 users; client.Do callers are refused earlier.
func TestConformance_RFC9110_Sec4_2_4_AuthorityUserinfoRejected(t *testing.T) {
	for _, auth := range []string{"user@example.com", "user:pass@example.com", "u@example.com:8443"} {
		t.Run(auth, func(t *testing.T) {
			ex, capture := rawCapture(t)
			err := ex.WriteRequest(context.Background(), []hpack.HeaderField{
				{Name: []byte(":method"), Value: []byte("GET")},
				{Name: []byte(":path"), Value: []byte("/")},
				{Name: []byte(":authority"), Value: []byte(auth)},
			}, true)
			if !errors.Is(err, http1.ErrInvalidRequest) {
				t.Fatalf("WriteRequest(:authority=%q) err = %v, want ErrInvalidRequest "+
					"(RFC 9110 §4.2.4, RFC 9112 §3.2)", auth, err)
			}
			if wire := capture(); wire != "" {
				t.Errorf("a rejected request must put no bytes on the wire, got:\n%q", wire)
			}
		})
	}
}

// TestConformance_RFC9110_Sec4_2_4_BareAuthorityAccepted is the over-rejection
// guard: a bare host[:port] (and an empty authority, per RFC 9112 §3.2) carries
// no userinfo and must pass.
func TestConformance_RFC9110_Sec4_2_4_BareAuthorityAccepted(t *testing.T) {
	for _, auth := range []string{"example.com", "example.com:8443", "[::1]:443", ""} {
		t.Run(auth, func(t *testing.T) {
			ex, _ := rawCapture(t)
			err := ex.WriteRequest(context.Background(), []hpack.HeaderField{
				{Name: []byte(":method"), Value: []byte("GET")},
				{Name: []byte(":path"), Value: []byte("/")},
				{Name: []byte(":authority"), Value: []byte(auth)},
			}, true)
			if err != nil {
				t.Fatalf("WriteRequest(:authority=%q) err = %v, want nil", auth, err)
			}
		})
	}
}
