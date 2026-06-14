package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

func TestConn_ConnectProtocolSupported_False(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	// peerSettings is zero-value — no SETTINGS_ENABLE_CONNECT_PROTOCOL
	if c.ConnectProtocolSupported() {
		t.Error("expected false when peer did not advertise connect protocol")
	}
}

func TestConn_ConnectProtocolSupported_True(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.peerSettings = frame.SettingsParams{
		Pairs: [16]frame.SettingPair{
			{ID: frame.SettingEnableConnectProtocol, Value: 1},
		},
		N: 1,
	}
	if !c.ConnectProtocolSupported() {
		t.Error("expected true when peer advertised SETTINGS_ENABLE_CONNECT_PROTOCOL=1")
	}
}

func TestConn_ConnectProtocolSupported_ZeroValue(t *testing.T) {
	t.Parallel()

	c := &Conn{}
	c.peerSettings = frame.SettingsParams{
		Pairs: [16]frame.SettingPair{
			{ID: frame.SettingEnableConnectProtocol, Value: 0},
		},
		N: 1,
	}
	if c.ConnectProtocolSupported() {
		t.Error("expected false when value is 0 (explicitly disabled)")
	}
}
