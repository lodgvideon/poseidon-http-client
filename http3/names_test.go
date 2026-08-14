package http3

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/trace"
)

func TestFrameTypeName(t *testing.T) {
	for typ, want := range map[uint64]string{
		FrameData: "DATA", FrameHeaders: "HEADERS", FrameCancelPush: "CANCEL_PUSH",
		FrameSettings: "SETTINGS", FramePushPromise: "PUSH_PROMISE",
		FrameGoaway: "GOAWAY", FrameMaxPushID: "MAX_PUSH_ID",
		// The reserved HTTP/2 carryovers §7.2.4.1 makes a connection error: this
		// implementation rejects them, and a frame log still has to say which.
		0x02: "RESERVED_H2_PRIORITY", 0x06: "RESERVED_H2_PING",
		0x08: "RESERVED_H2_WINDOW_UPDATE", 0x09: "RESERVED_H2_CONTINUATION",
		// §9 grease: 0x1f * N + 0x21, ordinary and required to be ignored.
		0x21: trace.UnknownName, 0x40: trace.UnknownName,
	} {
		if got := FrameTypeName(typ); got != want {
			t.Errorf("FrameTypeName(%#x) = %q, want %q", typ, got, want)
		}
	}
}

func TestSettingName(t *testing.T) {
	for id, want := range map[uint64]string{
		SettingQPACKMaxTableCapacity: "QPACK_MAX_TABLE_CAPACITY",
		SettingMaxFieldSectionSize:   "MAX_FIELD_SECTION_SIZE",
		SettingQPACKBlockedStreams:   "QPACK_BLOCKED_STREAMS",
		0x1234:                       trace.UnknownName,
	} {
		if got := SettingName(id); got != want {
			t.Errorf("SettingName(%#x) = %q, want %q", id, got, want)
		}
	}
}
