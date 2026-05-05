package conn

import (
	"context"
	"net"
	"time"
)

// AdvertisedSettings is what we send to the peer in our SETTINGS frame.
// Zero values are replaced by RFC 7540 defaults except MaxConcurrentStreams,
// which is always capped to 1 in B.1.
type AdvertisedSettings struct {
	HeaderTableSize      uint32 // default 4096
	MaxConcurrentStreams uint32 // B.1: capped to 1
	InitialWindowSize    uint32 // default 65535
	MaxFrameSize         uint32 // default 16384
	MaxHeaderListSize    uint32 // 0 = unset (peer chooses)
}

func (s AdvertisedSettings) defaulted() AdvertisedSettings {
	if s.HeaderTableSize == 0 {
		s.HeaderTableSize = 4096
	}
	s.MaxConcurrentStreams = 1 // B.1 hard cap
	if s.InitialWindowSize == 0 {
		s.InitialWindowSize = 65535
	}
	if s.MaxFrameSize == 0 {
		s.MaxFrameSize = 16384
	}
	return s
}

// Dialer is forward-declared here so options_test.go can compile in
// isolation. Task 8 lifts the real Dialer interface into dial.go.
type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// ConnOptions tunes a connection. Zero value is sensible.
type ConnOptions struct {
	Dialer            Dialer
	Settings          AdvertisedSettings
	StreamDeadline    time.Duration
	StreamEventBuffer int
}

func (o ConnOptions) defaulted() ConnOptions {
	if o.Dialer == nil {
		o.Dialer = stubDialer{}
	}
	o.Settings = o.Settings.defaulted()
	if o.StreamEventBuffer <= 0 {
		o.StreamEventBuffer = 8
	}
	return o
}

// stubDialer is removed by Task 8 (replaced with &TLSDialer{}). It exists
// only so options.go compiles before dial.go lands.
type stubDialer struct{}

func (stubDialer) Dial(_ context.Context, _ string) (net.Conn, error) {
	return nil, ErrConnClosed
}
