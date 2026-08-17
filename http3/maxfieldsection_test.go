package http3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
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

	_, atLimit := req.EncodeHeaders(&enc, nil, size)
	_, overLimit := req.EncodeHeaders(&enc, nil, size-1)

	assert.NoErrorf(t, atLimit,
		"at exactly the limit: err = %v, want nil; a client that refuses the exact size the peer "+
			"advertised sends nothing at all against a strict server", atLimit)
	assert.Equalf(t, ErrFieldSectionTooLarge, overLimit,
		"over the limit: err = %v, want ErrFieldSectionTooLarge", overLimit)
}

// TestConformance_RFC9114_Sec422_ExtraHeaderCounted checks that a regular header
// field contributes its name + value + 32 to the section size.
func TestConformance_RFC9114_Sec422_ExtraHeaderCounted(t *testing.T) {
	var enc qpack.Encoder
	req := &Request{
		Method: "GET", Scheme: "https", Authority: "h", Path: "/",
		Headers: []header.Field{{Name: []byte("accept"), Value: []byte("text/html")}},
	}
	const base = (7 + 3 + 32) + (7 + 5 + 32) + (10 + 1 + 32) + (5 + 1 + 32) // 167
	const extra = 6 + 9 + 32                                                // accept: text/html = 47

	_, atLimit := req.EncodeHeaders(&enc, nil, base+extra)
	_, overLimit := req.EncodeHeaders(&enc, nil, base+extra-1)

	assert.NoErrorf(t, atLimit,
		"at the limit including the extra header: err = %v, want nil", atLimit)
	assert.Equalf(t, ErrFieldSectionTooLarge, overLimit,
		"the extra header must count toward the size: err = %v, want ErrFieldSectionTooLarge", overLimit)
}

// TestConformance_RFC9114_Sec422_ApplyMaxFieldSection checks that the client
// records the peer's SETTINGS_MAX_FIELD_SECTION_SIZE, and that its absence leaves
// the no-limit default (RFC 9114 §4.2.2, §7.2.4.1).
func TestConformance_RFC9114_Sec422_ApplyMaxFieldSection(t *testing.T) {
	t.Run("present", func(t *testing.T) {
		var c Client
		c.maxFieldSection.Store(^uint64(0))

		c.applyServerSettings([]Setting{
			{ID: SettingQPACKMaxTableCapacity, Value: 0},
			{ID: SettingMaxFieldSectionSize, Value: 4096},
		})

		assert.Equalf(t, uint64(4096), c.maxFieldSection.Load(),
			"maxFieldSection = %d, want 4096: the peer's limit never reaches the request path",
			c.maxFieldSection.Load())
	})

	t.Run("absent", func(t *testing.T) {
		var c Client
		c.maxFieldSection.Store(^uint64(0))

		c.applyServerSettings([]Setting{{ID: SettingQPACKBlockedStreams, Value: 0}})

		assert.Equal(t, ^uint64(0), c.maxFieldSection.Load(),
			"an absent SETTINGS_MAX_FIELD_SECTION_SIZE must leave the no-limit default")
	})
}

// TestConformance_RFC9114_Sec422_DoRefusesOversized checks that the limit is
// wired end to end: with a peer limit smaller than any request, Do refuses to
// send and surfaces ErrFieldSectionTooLarge (RFC 9114 §4.2.2).
func TestConformance_RFC9114_Sec422_DoRefusesOversized(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	client.maxFieldSection.Store(10) // below the smallest possible request field section

	_, _, doErr := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "h", Path: "/"})

	assert.ErrorIsf(t, doErr, ErrFieldSectionTooLarge,
		"Do err = %v, want ErrFieldSectionTooLarge: the peer's limit is recorded but never enforced "+
			"on the send path", doErr)
}
