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

	// Extension frame types (RFC 8336, RFC 9218).
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

// SettingsParams holds up to 16 SETTINGS pairs (zero-alloc, no map).
type SettingsParams struct {
	Pairs [16]SettingPair
	N     int
}

// HeaderBlock is an opaque view over a HEADERS / PUSH_PROMISE / CONTINUATION
// header block fragment. Decode via hpack.Decoder.DecodeBlock(hb, visitor).
type HeaderBlock []byte
