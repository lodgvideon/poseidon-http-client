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
