package conn

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// Batch 2 — SETTINGS value validation (RFC 9113 §6.5.2 / §8.4).
//
// A client receives the server's SETTINGS and must reject values it may not
// accept: SETTINGS_ENABLE_PUSH other than 0 (a server may only send 0), and a
// SETTINGS_MAX_FRAME_SIZE outside [2^14, 2^24-1]. Both are connection errors of
// type PROTOCOL_ERROR. The client's own advertised MAX_FRAME_SIZE is likewise
// clamped into range.

// settingsPair builds a one-entry SETTINGS payload.
func settingsPair(id frame.SettingID, v uint32) frame.SettingsParams {
	s := frame.SettingsParams{N: 1}
	s.Pairs[0] = frame.SettingPair{ID: id, Value: v}
	return s
}

// TestConformance_RFC9113_Sec6_5_2_ServerEnablePushOne_ConnError pins that a
// mid-connection server SETTINGS with SETTINGS_ENABLE_PUSH=1 is a connection
// error of type PROTOCOL_ERROR, torn down with a typed GOAWAY.
func TestConformance_RFC9113_Sec6_5_2_ServerEnablePushOne_ConnError(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		drainFrames(srvFr, probe)
		<-asyncWrite(func() error { return srvFr.WriteSettings(settingsPair(frame.SettingEnablePush, 1)) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	require.NoError(t, err, "NewClientConn")
	defer c.Close()

	code := recvCode(t, "GOAWAY", probe.away)

	assert.Equalf(t, frame.ErrCodeProtocolError, code, "GOAWAY code = %v, want PROTOCOL_ERROR", code)
	assert.False(t, aliveWithin(c, false, 2*time.Second),
		"connection alive after server SETTINGS_ENABLE_PUSH=1")
	release()
}

// TestConformance_RFC9113_Sec6_5_2_MaxFrameSizeOutOfRange_ConnError pins that a
// mid-connection SETTINGS_MAX_FRAME_SIZE below 2^14 or above 2^24-1 is a
// connection error of type PROTOCOL_ERROR.
func TestConformance_RFC9113_Sec6_5_2_MaxFrameSizeOutOfRange_ConnError(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  uint32
	}{
		{"below floor", 100},
		{"above ceil", 1 << 24},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := net.Pipe()
			defer cli.Close()
			probe := newFramingProbe()
			finish, release := newFinish()

			go pipeServer(t, srv, func(srvFr *frame.Framer) {
				drainFrames(srvFr, probe)
				<-asyncWrite(func() error {
					return srvFr.WriteSettings(settingsPair(frame.SettingMaxFrameSize, tc.val))
				})
				<-finish
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
			require.NoError(t, err, "NewClientConn")
			defer c.Close()

			code := recvCode(t, "GOAWAY", probe.away)

			assert.Equalf(t, frame.ErrCodeProtocolError, code, "GOAWAY code = %v, want PROTOCOL_ERROR", code)
			assert.Falsef(t, aliveWithin(c, false, 2*time.Second),
				"connection alive after MAX_FRAME_SIZE=%d", tc.val)
			release()
		})
	}
}

// TestConformance_RFC9113_Sec6_5_2_HandshakeEnablePushOne_Refused pins that the
// same rule binds the server's connection-preface SETTINGS: a preface carrying
// SETTINGS_ENABLE_PUSH=1 makes the handshake fail rather than come up.
func TestConformance_RFC9113_Sec6_5_2_HandshakeEnablePushOne_Refused(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()

	go pipeServerWithSettings(t, srv, settingsPair(frame.SettingEnablePush, 1), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())

	if err == nil {
		_ = c.Close()
	}
	require.Error(t, err, "NewClientConn accepted a server preface with SETTINGS_ENABLE_PUSH=1")
	var ce *ConnError
	require.Truef(t, errors.As(err, &ce), "err = %v, want ConnError PROTOCOL_ERROR", err)
	assert.Equalf(t, frame.ErrCodeProtocolError, ce.Code, "err = %v, want ConnError PROTOCOL_ERROR", err)
}

// TestCheckPeerSettingValues covers the validator directly, including the
// over-rejection guard: the in-range boundary values and unrelated settings must
// be accepted.
func TestCheckPeerSettingValues(t *testing.T) {
	for _, tc := range []struct {
		name    string
		s       frame.SettingsParams
		wantErr bool
	}{
		{"enable_push 0 ok", settingsPair(frame.SettingEnablePush, 0), false},
		{"enable_push 1 bad", settingsPair(frame.SettingEnablePush, 1), true},
		{"enable_push 2 bad", settingsPair(frame.SettingEnablePush, 2), true},
		{"max_frame floor ok", settingsPair(frame.SettingMaxFrameSize, 16384), false},
		{"max_frame ceil ok", settingsPair(frame.SettingMaxFrameSize, 16777215), false},
		{"max_frame below floor bad", settingsPair(frame.SettingMaxFrameSize, 16383), true},
		{"max_frame above ceil bad", settingsPair(frame.SettingMaxFrameSize, 16777216), true},
		{"unrelated setting ignored", settingsPair(frame.SettingInitialWindowSize, 1<<30), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkPeerSettingValues(tc.s)

			if tc.wantErr {
				require.Errorf(t, err, "checkPeerSettingValues = %v, wantErr=%v", err, tc.wantErr)
				var ce *ConnError
				require.Truef(t, errors.As(err, &ce), "err = %v, want ConnError PROTOCOL_ERROR", err)
				assert.Equalf(t, frame.ErrCodeProtocolError, ce.Code,
					"err = %v, want ConnError PROTOCOL_ERROR", err)
				return
			}
			require.NoErrorf(t, err, "checkPeerSettingValues = %v, wantErr=%v", err, tc.wantErr)
		})
	}
}

// TestConformance_RFC9113_Sec6_5_2_AdvertisedMaxFrameSizeClamped pins that our
// own advertised MAX_FRAME_SIZE is clamped into [2^14, 2^24-1].
func TestConformance_RFC9113_Sec6_5_2_AdvertisedMaxFrameSizeClamped(t *testing.T) {
	for _, tc := range []struct{ in, want uint32 }{
		{0, 16384},           // unset → default
		{100, 16384},         // below floor → floor
		{16384, 16384},       // floor
		{50000, 50000},       // in range → unchanged
		{16777215, 16777215}, // ceil
		{16777216, 16777215}, // above ceil → ceil
		{1 << 30, 16777215},  // far above → ceil
	} {
		got := AdvertisedSettings{MaxFrameSize: tc.in}.defaulted().MaxFrameSize

		assert.Equalf(t, tc.want, got,
			"MaxFrameSize %d defaulted to %d, want %d (RFC 9113 §6.5.2 range)", tc.in, got, tc.want)
	}
}
