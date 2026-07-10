package http3

import (
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// TestConformance_RFC9114_Sec422_FieldSectionSizeLimit checks that a request
// whose field section exceeds the peer's SETTINGS_MAX_FIELD_SECTION_SIZE is
// refused locally, and that one at exactly the limit is accepted. The size is the
// uncompressed name+value+32 cost of every field (RFC 9114 §4.2.2).
func TestConformance_RFC9114_Sec422_FieldSectionSizeLimit(t *testing.T) {
	var enc qpack.Encoder
	req := &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"}
	// :method/GET (7+3+32) + :scheme/https (7+5+32) + :authority/h (10+1+32) +
	// :path // (5+1+32) = 167.
	const size = (7 + 3 + 32) + (7 + 5 + 32) + (10 + 1 + 32) + (5 + 1 + 32)

	if _, err := req.EncodeHeaders(&enc, nil, size); err != nil {
		t.Fatalf("at exactly the limit: err = %v, want nil", err)
	}
	if _, err := req.EncodeHeaders(&enc, nil, size-1); err != ErrFieldSectionTooLarge {
		t.Fatalf("over the limit: err = %v, want ErrFieldSectionTooLarge", err)
	}
}

// TestConformance_RFC9114_Sec422_ExtraHeaderCounted checks that a regular header
// field contributes its name + value + 32 to the section size.
func TestConformance_RFC9114_Sec422_ExtraHeaderCounted(t *testing.T) {
	var enc qpack.Encoder
	req := &Request{
		Method: "GET", Scheme: "https", Authority: "h", Path: "/",
		Headers: []hpack.HeaderField{{Name: []byte("accept"), Value: []byte("text/html")}},
	}
	const base = (7 + 3 + 32) + (7 + 5 + 32) + (10 + 1 + 32) + (5 + 1 + 32) // 167
	const extra = 6 + 9 + 32                                                // accept: text/html = 47
	if _, err := req.EncodeHeaders(&enc, nil, base+extra); err != nil {
		t.Fatalf("at the limit including the extra header: err = %v, want nil", err)
	}
	if _, err := req.EncodeHeaders(&enc, nil, base+extra-1); err != ErrFieldSectionTooLarge {
		t.Fatalf("the extra header must count toward the size: err = %v, want ErrFieldSectionTooLarge", err)
	}
}

// TestConformance_RFC9114_Sec422_ApplyMaxFieldSection checks that the client
// records the peer's SETTINGS_MAX_FIELD_SECTION_SIZE, and that its absence leaves
// the no-limit default (RFC 9114 §4.2.2, §7.2.4.1).
func TestConformance_RFC9114_Sec422_ApplyMaxFieldSection(t *testing.T) {
	c := &Client{maxFieldSection: ^uint64(0)}
	c.applyServerSettings([]Setting{
		{ID: SettingQPACKMaxTableCapacity, Value: 0},
		{ID: SettingMaxFieldSectionSize, Value: 4096},
	})
	if c.maxFieldSection != 4096 {
		t.Fatalf("maxFieldSection = %d, want 4096", c.maxFieldSection)
	}

	c2 := &Client{maxFieldSection: ^uint64(0)}
	c2.applyServerSettings([]Setting{{ID: SettingQPACKBlockedStreams, Value: 0}})
	if c2.maxFieldSection != ^uint64(0) {
		t.Fatal("an absent SETTINGS_MAX_FIELD_SECTION_SIZE must leave the no-limit default")
	}
}

// TestConformance_RFC9114_Sec422_DoRefusesOversized checks that the limit is
// wired end to end: with a peer limit smaller than any request, Do refuses to
// send and surfaces ErrFieldSectionTooLarge (RFC 9114 §4.2.2).
func TestConformance_RFC9114_Sec422_DoRefusesOversized(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, _ := NewClientFake(conn, nil)
	client.maxFieldSection = 10 // below the smallest possible request field section

	_, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})
	if !errors.Is(err, ErrFieldSectionTooLarge) {
		t.Fatalf("Do err = %v, want ErrFieldSectionTooLarge", err)
	}
}
