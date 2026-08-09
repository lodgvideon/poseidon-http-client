package conn

import (
	"time"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// AdvertisedSettings is what we send to the peer in our SETTINGS frame.
// Zero values are replaced by RFC 7540 defaults; MaxConcurrentStreams
// defaults to 100 (B.2).
type AdvertisedSettings struct {
	HeaderTableSize      uint32
	MaxConcurrentStreams uint32
	InitialWindowSize    uint32
	MaxFrameSize         uint32
	MaxHeaderListSize    uint32
}

// defaultMaxHeaderListSize bounds the decompressed header-list size we will
// accept from a peer (RFC 7540 §6.5.2 / §10.5.1). Unlike the compressed
// CONTINUATION-accumulation ceiling (handler.defaultMaxHeaderBytes), this
// caps the sum of decoded HeaderField.Size() and defends against HPACK
// expansion bombs — a small block of indexed references that decode to a
// very large field list. It is announced to the peer and enforced on decode.
// 8 MiB matches the sibling compressed ceiling: generous for legitimate
// response headers, bounded against an adversarial or buggy server. A caller
// wanting a different bound sets AdvertisedSettings.MaxHeaderListSize
// explicitly; a very large value effectively opts out.
const defaultMaxHeaderListSize = 8 << 20 // 8 MiB

// applyDecoderSettings makes the HPACK decoder enforce what we advertised.
//
// Both limits are promises to the peer about the blocks it sends us, so the
// decoder is the side that has to keep them:
//
//   - MAX_HEADER_LIST_SIZE bounds the decompressed field list (RFC 7540
//     §10.5.1) — the HPACK expansion-bomb defense. The Framer and handler byte
//     caps only bound the compressed block.
//   - HEADER_TABLE_SIZE is how large a dynamic table the peer may use. A peer
//     that takes us at our word announces the new size with a dynamic table
//     size update, and RFC 7541 §6.3 makes an update above the SETTINGS limit a
//     decoding error. Left at hpack's 4096 default, advertising anything larger
//     killed the connection the moment a conformant peer used exactly what we
//     offered.
//
// s must be defaulted; a zero value here would disable the list bound.
func applyDecoderSettings(d *hpack.Decoder, s AdvertisedSettings) {
	d.SetMaxHeaderListSize(s.MaxHeaderListSize)
	d.SetMaxDynamicTableSize(s.HeaderTableSize)
}

func (s AdvertisedSettings) defaulted() AdvertisedSettings {
	if s.HeaderTableSize == 0 {
		s.HeaderTableSize = 4096
	}
	if s.MaxConcurrentStreams == 0 {
		s.MaxConcurrentStreams = 100
	}
	if s.InitialWindowSize == 0 {
		s.InitialWindowSize = 65535
	}
	if s.MaxFrameSize == 0 {
		s.MaxFrameSize = 16384
	}
	// RFC 9113 §6.5.2: "The value advertised by an endpoint MUST be between this
	// initial value and the maximum allowed frame size ... inclusive." Clamp a
	// caller value outside [2^14, 2^24-1] to the nearest bound so our SETTINGS,
	// the Framer read limit and outbound chunking stay conformant (a sub-16384
	// value would otherwise shrink our own framing).
	if s.MaxFrameSize < frameSizeFloor {
		s.MaxFrameSize = frameSizeFloor
	} else if s.MaxFrameSize > frameSizeCeil {
		s.MaxFrameSize = frameSizeCeil
	}
	if s.MaxHeaderListSize == 0 {
		s.MaxHeaderListSize = defaultMaxHeaderListSize
	}
	return s
}

// ConnOptions tunes a Conn. The zero value is sensible.
type ConnOptions struct {
	Dialer            Dialer
	Settings          AdvertisedSettings
	StreamEventBuffer int
	// KeepaliveInterval, when non-zero, enables a background keepalive
	// loop. The loop sends a PING every interval; if no ACK arrives
	// within KeepaliveTimeout (see below) the connection is closed.
	// Zero disables keepalive.
	KeepaliveInterval time.Duration

	// KeepaliveTimeout is the maximum time the keepalive loop waits
	// for a PING ACK before declaring the connection dead and closing
	// it. When zero, defaults to max(KeepaliveInterval*5, 5s) to
	// tolerate write-queue latency under heavy load. Has no effect
	// when KeepaliveInterval is zero.
	KeepaliveTimeout time.Duration

	// Padding controls outbound frame padding (RFC 7540 §4.2).
	// The zero value disables padding. See PaddingStrategy for details.
	Padding PaddingStrategy

	// EnablePush controls whether the server may send PUSH_PROMISE frames
	// (RFC 7540 §8.2). When false (default), the client advertises
	// SETTINGS_ENABLE_PUSH=0 and treats any PUSH_PROMISE as a PROTOCOL_ERROR.
	// When true, pushed streams are created automatically and delivered via
	// EventPushPromise on the parent stream's Recv channel.
	EnablePush bool

	// GroupCommit enables group-commit write batching (opt-in, default off).
	// When a HEADERS writer finds another writer queued on the write lock, it
	// defers its flush so the next holder batches both frames into a single
	// tls.Conn.Write — fewer TLS record encrypts + socket writes under high
	// per-connection stream concurrency. It is a strict no-op when there is no
	// contention (a lone request is never delayed) and preserves per-frame
	// write-error semantics. Trades a bounded amount of client-side batching
	// for throughput; leave off for latency-fidelity-critical measurements.
	GroupCommit bool

	// AutoTuneRecvWindow enables bandwidth-delay-product tuning of the receive
	// windows (opt-in, default off).
	//
	// RFC 9113 §6.5.2 fixes both receive windows at 65535 bytes until an
	// endpoint raises them, which limits a connection to one window per round
	// trip — roughly 6.5 MB/s in total at 10 ms RTT, no matter how fast the link
	// or the CPU is. It is invisible on loopback and binding on any real
	// network. With this on, the connection measures how much a peer delivers in
	// one round trip (a PING, and the DATA that arrives before its ACK) and
	// grows both windows to twice that, up to MaxRecvWindow. Windows only ever
	// grow, and probing backs off once growth stops.
	//
	// It costs one PING per round trip while a transfer is ramping, and nothing
	// once the window is no longer the constraint. Leave it off to hold the
	// protocol default, which is also what a latency-fidelity measurement of a
	// default-configured peer wants.
	AutoTuneRecvWindow bool

	// MaxRecvWindow caps what AutoTuneRecvWindow may grow a window to. It has no
	// effect when auto-tuning is off.
	//
	// Zero derives the cap from the per-stream event budget:
	// StreamEventBuffer x Settings.MaxFrameSize, which is the memory this
	// connection has already committed to buffering for one stream. Growing only
	// that far is what keeps auto-tuning from making the event-channel overflow
	// reset (see Stream.push) any likelier than the configured buffer already
	// does. Raise it only alongside StreamEventBuffer.
	//
	// Values above 64 MiB are clamped, and a value below the window already in
	// effect is ignored.
	MaxRecvWindow uint32
}

func (o ConnOptions) defaulted() ConnOptions {
	if o.Dialer == nil {
		o.Dialer = &TLSDialer{}
	}
	o.Settings = o.Settings.defaulted()
	if o.StreamEventBuffer <= 0 {
		o.StreamEventBuffer = 8
	}
	return o
}
