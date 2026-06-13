package conn

import "time"

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
