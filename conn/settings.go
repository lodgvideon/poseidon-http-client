package conn

import (
	"context"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// handshakeSettings runs the sequence:
//
//  1. WriteClientPreface
//  2. WriteSettings(advertised)
//  3. ReadFrame loop until first server SETTINGS frame is observed
//     (server may send other control frames first per RFC 7540 §3.5,
//     but in practice never does on the first frame; we handle both).
//  4. WriteSettingsAck
//  5. ReadFrame loop until our own SETTINGS is ACKed.
//
// Returns the peer's SETTINGS as observed in step 3.
func handshakeSettings(ctx context.Context, fr *frame.Framer, advertised AdvertisedSettings) (frame.SettingsParams, error) {
	if err := fr.WriteClientPreface(); err != nil {
		return frame.SettingsParams{}, err
	}
	myParams := encodeAdvertised(advertised)
	if err := fr.WriteSettings(myParams); err != nil {
		return frame.SettingsParams{}, err
	}

	rec := &settingsRecorder{}
	for !rec.peerSeen {
		if err := readOne(ctx, fr, rec); err != nil {
			return frame.SettingsParams{}, err
		}
	}
	if err := fr.WriteSettingsAck(); err != nil {
		return frame.SettingsParams{}, err
	}
	for !rec.ackSeen {
		if err := readOne(ctx, fr, rec); err != nil {
			return frame.SettingsParams{}, err
		}
	}
	return rec.peer, nil
}

func readOne(ctx context.Context, fr *frame.Framer, h frame.Handler) error {
	_, err := fr.ReadFrame(ctx, h)
	return err
}

func encodeAdvertised(a AdvertisedSettings) frame.SettingsParams {
	var p frame.SettingsParams
	add := func(id frame.SettingID, v uint32) {
		p.Pairs[p.N] = frame.SettingPair{ID: id, Value: v}
		p.N++
	}
	add(frame.SettingHeaderTableSize, a.HeaderTableSize)
	add(frame.SettingEnablePush, 0) // hard-coded — B.1 never accepts push
	add(frame.SettingMaxConcurrentStreams, a.MaxConcurrentStreams)
	add(frame.SettingInitialWindowSize, a.InitialWindowSize)
	add(frame.SettingMaxFrameSize, a.MaxFrameSize)
	if a.MaxHeaderListSize != 0 {
		add(frame.SettingMaxHeaderListSize, a.MaxHeaderListSize)
	}
	return p
}

// settingsRecorder records the peer's first SETTINGS and notes when our
// SETTINGS gets ACKed. Other frames during the handshake are ignored
// (B.1 does not expect them; if they appear we proceed regardless).
type settingsRecorder struct {
	peer     frame.SettingsParams
	peerSeen bool
	ackSeen  bool
}

// OnData implements frame.Handler.
func (r *settingsRecorder) OnData(frame.FrameHeader, []byte, uint8) error { return nil }
// OnHeaders implements frame.Handler.
func (r *settingsRecorder) OnHeaders(frame.FrameHeader, frame.HeaderBlock, *frame.Priority, uint8) error {
	return nil
}
// OnPriority implements frame.Handler.
func (r *settingsRecorder) OnPriority(frame.FrameHeader, frame.Priority) error { return nil }
// OnRSTStream implements frame.Handler.
func (r *settingsRecorder) OnRSTStream(frame.FrameHeader, frame.ErrCode) error { return nil }
// OnSettings implements frame.Handler.
func (r *settingsRecorder) OnSettings(fh frame.FrameHeader, s frame.SettingsParams) error {
	if fh.Flags&frame.FlagSettingsAck != 0 {
		r.ackSeen = true
		return nil
	}
	r.peer = s
	r.peerSeen = true
	return nil
}
// OnPushPromise implements frame.Handler.
func (r *settingsRecorder) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "PUSH_PROMISE during handshake"}
}
// OnPing implements frame.Handler.
func (r *settingsRecorder) OnPing(frame.FrameHeader, [8]byte) error                         { return nil }
// OnGoAway implements frame.Handler.
func (r *settingsRecorder) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error { return nil }
// OnWindowUpdate implements frame.Handler.
func (r *settingsRecorder) OnWindowUpdate(frame.FrameHeader, uint32) error                  { return nil }
// OnContinuation implements frame.Handler.
func (r *settingsRecorder) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error       { return nil }

var _ frame.Handler = (*settingsRecorder)(nil)
