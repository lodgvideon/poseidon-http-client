package conn

import (
	"strings"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdvertisedSettings_Defaulted_BoundsHeaderListSize asserts the
// decompressed header-list DoS gate is on by default (RFC 7540 §10.5.1).
// A zero value would leave HPACK expansion unbounded — a small compressed
// block of indexed references can decode to a very large field list.
func TestAdvertisedSettings_Defaulted_BoundsHeaderListSize(t *testing.T) {
	s := AdvertisedSettings{}.defaulted()

	require.NotZero(t, s.MaxHeaderListSize,
		"MaxHeaderListSize defaulted to 0 — decompressed-list DoS gate is off")
	require.EqualValuesf(t, defaultMaxHeaderListSize, s.MaxHeaderListSize,
		"MaxHeaderListSize = %d, want %d", s.MaxHeaderListSize, defaultMaxHeaderListSize)
}

// TestAdvertisedSettings_Defaulted_PreservesHeaderListSize confirms a caller
// override is not clobbered by the default.
func TestAdvertisedSettings_Defaulted_PreservesHeaderListSize(t *testing.T) {
	s := AdvertisedSettings{MaxHeaderListSize: 1 << 20}.defaulted()

	require.EqualValuesf(t, 1<<20, s.MaxHeaderListSize,
		"caller value lost: got %d, want %d", s.MaxHeaderListSize, 1<<20)
}

// TestEncodeAdvertised_AnnouncesHeaderListSize confirms the default cap is
// announced to the peer so a well-behaved server never sends a header list
// exceeding what we will accept (RFC 7540 §6.5.2).
func TestEncodeAdvertised_AnnouncesHeaderListSize(t *testing.T) {
	p := encodeAdvertised(AdvertisedSettings{}.defaulted(), false)

	v, ok := lookupPeerSetting(p, frame.SettingMaxHeaderListSize)

	require.True(t, ok, "SETTINGS_MAX_HEADER_LIST_SIZE not advertised")
	require.EqualValuesf(t, defaultMaxHeaderListSize, v,
		"advertised = %d, want %d", v, defaultMaxHeaderListSize)
}

// TestConformance_RFC7540_Sec10_5_1_HeaderListSizeCap_EnhanceYourCalm verifies
// that a decoded header list exceeding the local SETTINGS_MAX_HEADER_LIST_SIZE
// is rejected as a connection error of type ENHANCE_YOUR_CALM (RFC 7540
// §10.5.1), not the generic COMPRESSION_ERROR. This is the decompressed-size
// DoS gate: the compressed block is small but its decoded field list is not.
func TestConformance_RFC7540_Sec10_5_1_HeaderListSizeCap_EnhanceYourCalm(t *testing.T) {
	const cap = 128
	m := newFakeStreamMap()
	dec := hpack.NewDecoder()
	dec.SetMaxHeaderListSize(cap)
	h := newConnHandler(m, dec)
	m.addStream(1)
	// Each field's HeaderField.Size() = len(name)+len(value)+32.
	//   :status/200      -> 7+3+32   = 42   (total 42)
	//   x-a/<32 bytes>   -> 3+32+32  = 67   (total 109)
	//   x-b/<32 bytes>   -> 3+32+32  = 67   (total 176 > 128) -> reject
	big := strings.Repeat("a", 32)
	block := encodeBlock(t, []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
		{Name: []byte("x-a"), Value: []byte(big)},
		{Name: []byte("x-b"), Value: []byte(big)},
	})
	fh := frame.FrameHeader{
		Type:     frame.FrameHeaders,
		Length:   uint32(len(block)),
		Flags:    frame.FlagHeadersEndHeaders | frame.FlagHeadersEndStream,
		StreamID: 1,
	}

	err := h.OnHeaders(fh, frame.HeaderBlock(block), nil, 0)

	require.Error(t, err, "expected ConnError for over-limit decoded header list")
	ce, ok := err.(*ConnError)
	require.Truef(t, ok, "err type = %T, want *ConnError", err)
	require.Equalf(t, frame.ErrCodeEnhanceYourCalm, ce.Code,
		"code = %v, want ENHANCE_YOUR_CALM (RFC 7540 §10.5.1)", ce.Code)
}

// TestHeaderListSizeCap_WithinLimit_Succeeds is the negative control: a field
// list under the cap decodes normally and emits its event.
func TestHeaderListSizeCap_WithinLimit_Succeeds(t *testing.T) {
	m := newFakeStreamMap()
	dec := hpack.NewDecoder()
	dec.SetMaxHeaderListSize(defaultMaxHeaderListSize)
	h := newConnHandler(m, dec)
	s := m.addStream(1)
	block := encodeBlock(t, []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
	})
	fh := frame.FrameHeader{
		Type:     frame.FrameHeaders,
		Length:   uint32(len(block)),
		Flags:    frame.FlagHeadersEndHeaders | frame.FlagHeadersEndStream,
		StreamID: 1,
	}

	err := h.OnHeaders(fh, frame.HeaderBlock(block), nil, 0)

	require.NoErrorf(t, err, "OnHeaders under cap")
	select {
	case e := <-s.events:
		assert.Equalf(t, EventHeaders, e.Type, "event type = %v", e.Type)
	default:
		require.FailNow(t, "no event pushed")
	}
}
