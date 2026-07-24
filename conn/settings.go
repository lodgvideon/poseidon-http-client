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
// The flush callback drains the buffered writer wrapping the transport (see
// Conn.flushWrite). It MUST be invoked after the preface+SETTINGS are written
// and before we block reading the server's SETTINGS — otherwise the preface
// sits in the buffer and a peer that waits for the client preface before
// responding would deadlock. It is invoked again after our SETTINGS ACK so
// the peer observes it promptly.
func handshakeSettings(ctx context.Context, fr *frame.Framer, flush func() error, advertised AdvertisedSettings, enablePush bool) (frame.SettingsParams, error) {
	if err := fr.WriteClientPreface(); err != nil {
		return frame.SettingsParams{}, err
	}
	myParams := encodeAdvertised(advertised, enablePush)
	if err := fr.WriteSettings(myParams); err != nil {
		return frame.SettingsParams{}, err
	}
	// Flush the preface + our SETTINGS to the wire before blocking on the
	// server's SETTINGS below.
	if err := flush(); err != nil {
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
	// Flush our SETTINGS ACK so the peer sees it promptly.
	if err := flush(); err != nil {
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

func encodeAdvertised(a AdvertisedSettings, enablePush bool) frame.SettingsParams {
	var p frame.SettingsParams
	add := func(id frame.SettingID, v uint32) {
		p.Pairs[p.N] = frame.SettingPair{ID: id, Value: v}
		p.N++
	}
	add(frame.SettingHeaderTableSize, a.HeaderTableSize)
	if enablePush {
		add(frame.SettingEnablePush, 1)
	} else {
		add(frame.SettingEnablePush, 0)
	}
	add(frame.SettingMaxConcurrentStreams, a.MaxConcurrentStreams)
	add(frame.SettingInitialWindowSize, a.InitialWindowSize)
	add(frame.SettingMaxFrameSize, a.MaxFrameSize)
	if a.MaxHeaderListSize != 0 {
		add(frame.SettingMaxHeaderListSize, a.MaxHeaderListSize)
	}
	return p
}

// settingValue returns the value of `id` from `s` or `def` when the
// peer did not advertise it. The peer SETTINGS array is small (<=16
// pairs in practice) so a linear scan is fine and zero-alloc.
func settingValue(s frame.SettingsParams, id frame.SettingID, def uint32) uint32 {
	for i := 0; i < s.N; i++ {
		if s.Pairs[i].ID == id {
			return s.Pairs[i].Value
		}
	}
	return def
}

// lookupPeerSetting reports whether `id` is present in `s` and, if
// so, returns its value. Distinct from settingValue so callers can
// tell "peer advertised zero" from "peer never advertised this".
func lookupPeerSetting(s frame.SettingsParams, id frame.SettingID) (uint32, bool) {
	for i := 0; i < s.N; i++ {
		if s.Pairs[i].ID == id {
			return s.Pairs[i].Value, true
		}
	}
	return 0, false
}

// settingsRecorder records the peer's first SETTINGS and notes when our
// SETTINGS gets ACKed. Other frames during the handshake are ignored
// (B.1 does not expect them; if they appear we proceed regardless).
type settingsRecorder struct {
	peer     frame.SettingsParams
	peerSeen bool
	ackSeen  bool
}

// prefaceGuard rejects a frame received before the server's connection preface.
// RFC 9113 §3.4 requires the server's SETTINGS to be the first frame it sends and
// makes an invalid connection preface a connection error of type PROTOCOL_ERROR.
// Any non-SETTINGS frame seen before the server's SETTINGS is such a violation;
// once the preface SETTINGS has arrived (peerSeen), later frames during the
// SETTINGS-ACK wait are handled normally.
func (r *settingsRecorder) prefaceGuard() error {
	if r.peerSeen {
		return nil
	}
	return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "server preface: first frame was not a SETTINGS frame"}
}

// OnData implements frame.Handler.
func (r *settingsRecorder) OnData(frame.FrameHeader, []byte, uint8) error { return r.prefaceGuard() }

// OnHeaders implements frame.Handler.
func (r *settingsRecorder) OnHeaders(frame.FrameHeader, frame.HeaderBlock, *frame.Priority, uint8) error {
	return r.prefaceGuard()
}

// OnPriority implements frame.Handler.
func (r *settingsRecorder) OnPriority(frame.FrameHeader, frame.Priority) error {
	return r.prefaceGuard()
}

// OnRSTStream implements frame.Handler.
func (r *settingsRecorder) OnRSTStream(frame.FrameHeader, frame.ErrCode) error {
	return r.prefaceGuard()
}

// OnSettings implements frame.Handler. Non-ACK SETTINGS are the server's
// connection preface (recorded here; side effects applied later by
// applyInitialPeerSettings). A SETTINGS ACK is accepted only after the preface
// has arrived — an ACK as the first frame is itself a preface violation
// (RFC 9113 §3.4), since the server's own SETTINGS must come first.
func (r *settingsRecorder) OnSettings(fh frame.FrameHeader, s frame.SettingsParams) error {
	if fh.Flags&frame.FlagSettingsAck != 0 {
		if !r.peerSeen {
			return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "server preface: SETTINGS ACK before server SETTINGS"}
		}
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
func (r *settingsRecorder) OnPing(frame.FrameHeader, [8]byte) error { return r.prefaceGuard() }

// OnGoAway implements frame.Handler.
func (r *settingsRecorder) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error {
	return r.prefaceGuard()
}

// OnWindowUpdate implements frame.Handler.
func (r *settingsRecorder) OnWindowUpdate(frame.FrameHeader, uint32) error {
	return r.prefaceGuard()
}

// OnContinuation implements frame.Handler.
func (r *settingsRecorder) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error {
	return r.prefaceGuard()
}
func (r *settingsRecorder) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error {
	return r.prefaceGuard()
}
func (r *settingsRecorder) OnOrigin(frame.FrameHeader, []string) error { return r.prefaceGuard() }

var _ frame.Handler = (*settingsRecorder)(nil)
