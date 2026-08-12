package frame

// FrameType — RFC 7540 §11.2.
type FrameType uint8

const (
	FrameData         FrameType = 0x0
	FrameHeaders      FrameType = 0x1
	FramePriority     FrameType = 0x2
	FrameRSTStream    FrameType = 0x3
	FrameSettings     FrameType = 0x4
	FramePushPromise  FrameType = 0x5
	FramePing         FrameType = 0x6
	FrameGoAway       FrameType = 0x7
	FrameWindowUpdate FrameType = 0x8
	FrameContinuation FrameType = 0x9

	// Extension frame types (RFC 7838, RFC 8336).
	FrameAltSvc FrameType = 0x0a // ALTSVC, RFC 7838 §4
	FrameOrigin FrameType = 0x0c // ORIGIN, RFC 8336 §3
)

// Flags is a bitmask whose semantics depend on FrameType.
type Flags uint8

const (
	FlagDataEndStream          Flags = 0x1
	FlagDataPadded             Flags = 0x8
	FlagHeadersEndStream       Flags = 0x1
	FlagHeadersEndHeaders      Flags = 0x4
	FlagHeadersPadded          Flags = 0x8
	FlagHeadersPriority        Flags = 0x20
	FlagSettingsAck            Flags = 0x1
	FlagPingAck                Flags = 0x1
	FlagContinuationEndHeaders Flags = 0x4
	FlagPushPromiseEndHeaders  Flags = 0x4
	FlagPushPromisePadded      Flags = 0x8
)

// FrameHeader is the fixed 9-byte prefix of every frame (RFC 7540 §4.1).
type FrameHeader struct {
	Length   uint32 // 24-bit
	Type     FrameType
	Flags    Flags
	StreamID uint32 // 31-bit, R-bit masked
}

// Priority describes a PRIORITY field block (RFC 7540 §6.3).
type Priority struct {
	StreamDep uint32
	Exclusive bool
	Weight    uint8 // RFC weight = Weight + 1
}

// SettingID identifies a SETTINGS parameter (RFC 7540 §6.5.2).
type SettingID uint16

const (
	SettingHeaderTableSize      SettingID = 0x1
	SettingEnablePush           SettingID = 0x2
	SettingMaxConcurrentStreams SettingID = 0x3
	SettingInitialWindowSize    SettingID = 0x4
	SettingMaxFrameSize         SettingID = 0x5
	SettingMaxHeaderListSize    SettingID = 0x6

	// SettingEnableConnectProtocol (RFC 8441 §3) allows the client
	// to send extended-CONNECT requests (:protocol pseudo-header).
	SettingEnableConnectProtocol SettingID = 0x8
)

// SettingPair holds one SETTINGS entry.
type SettingPair struct {
	ID    SettingID
	Value uint32
}

// maxSettingsPairs bounds SettingsParams.Pairs, and settingsPairWireSize is what
// one pair costs on the wire — a 16-bit identifier and a 32-bit value (RFC 7540
// §6.5.1).
//
// Named because WriteSettings sizes its scratch array from their product. Both
// used to be literals in two files, 16 here and [96]byte there, with nothing
// saying 96 was 16*6. Raising the array without noticing the second one is an
// index-out-of-range in a wire writer; tying them together makes that
// unexpressible rather than a thing to remember.
const (
	maxSettingsPairs     = 16
	settingsPairWireSize = 6
)

// SettingsParams holds the current value of each DEFINED SETTINGS parameter
// (zero-alloc, no map), NOT one slot per wire occurrence: RFC 7540 §6.5 says a
// receiver keeps only "the current value of its parameters", so the decoder
// stores one slot per identifier with the last value seen.
//
// N is therefore bounded by the number of identifiers this implementation
// understands — seven today (0x1-0x6 and RFC 8441's 0x8) — and never by how many
// parameters, repeats included, a peer chooses to send. Pairs is
// maxSettingsPairs so that adding an identifier does not need this type resized;
// the comment here used to claim the array was sized to the identifier count,
// which it never was.
type SettingsParams struct {
	Pairs [maxSettingsPairs]SettingPair
	N     int
}

// set records id=value, replacing any existing value for id so the LAST value
// wins (RFC 7540 §6.5: "the value of a SETTINGS parameter is the last value that
// is seen by a receiver"). Because the decoder only ever calls this with defined
// identifiers, N is bounded by the number of those identifiers and Pairs cannot
// overflow, however many parameters — repeats included — the peer sends.
func (s *SettingsParams) set(id SettingID, value uint32) {
	for i := 0; i < s.N; i++ {
		if s.Pairs[i].ID == id {
			s.Pairs[i].Value = value
			return
		}
	}
	s.Pairs[s.N] = SettingPair{ID: id, Value: value}
	s.N++
}

// HeaderBlock is an opaque view over a HEADERS / PUSH_PROMISE / CONTINUATION
// header block fragment. Decode via hpack.Decoder.DecodeBlock(hb, visitor).
type HeaderBlock []byte

// AltSvcEntry represents one entry in an ALTSVC frame (RFC 7838 §4).
// Origin is the ASCII serialization of an origin (scheme://host[:port]).
// It MUST be non-empty on stream-0 frames and empty on non-zero-stream
// frames. AltValue is the alternative-service value (e.g.
// `h2="alt.example.com:443"`); an empty AltValue with empty Origin
// signals clearing of all alternative services for the stream.
type AltSvcEntry struct {
	Origin   string
	AltValue string
}
