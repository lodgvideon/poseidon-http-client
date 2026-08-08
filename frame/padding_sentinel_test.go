package frame_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestConformance_RFC9113_Sec61_PaddingErrorIsMatchable pins that a padding
// violation is reportable through the exported sentinel.
//
// RFC 9113 §6.1: "If the length of the padding is the length of the frame
// payload or greater, the recipient MUST treat this as a connection error
// (Section 5.4.1) of type PROTOCOL_ERROR."
//
// Emitting that code is the receiver's job, and to do it the receiver must be
// able to recognise the condition. frame.ErrInvalidPadding is exported and
// documented for exactly that, but the error actually returned came from
// internal/bytesx — a package no consumer outside the module can name, let alone
// match. errors.Is against the exported sentinel was always false, which is why
// conn's own mapFrameError case for oversized padding could never fire.
func TestConformance_RFC9113_Sec61_PaddingErrorIsMatchable(t *testing.T) {
	// PADDED DATA on stream 1: pad length 0xff over a 2-octet payload.
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"DATA", []byte{0x00, 0x00, 0x02, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0xff, 'a'}},
		{"HEADERS", []byte{0x00, 0x00, 0x02, 0x01, 0x08, 0x00, 0x00, 0x00, 0x01, 0xff, 'a'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fr := frame.NewFramer(nil, bytes.NewReader(tc.raw))
			_, err := fr.ReadFrame(context.Background(), discardHandler{})
			if err == nil {
				t.Fatal("pad length past the payload was accepted")
			}
			if !errors.Is(err, frame.ErrInvalidPadding) {
				t.Fatalf("errors.Is(err, frame.ErrInvalidPadding) = false; err = %v\n"+
					"the exported sentinel is what consumers are told to match, so a "+
					"receiver cannot classify this as PROTOCOL_ERROR (§6.1)", err)
			}
		})
	}
}

// discardHandler satisfies the Framer's Handler without recording anything.
type discardHandler struct{}

func (discardHandler) OnData(frame.FrameHeader, []byte, uint8) error { return nil }
func (discardHandler) OnHeaders(frame.FrameHeader, frame.HeaderBlock, *frame.Priority, uint8) error {
	return nil
}
func (discardHandler) OnPriority(frame.FrameHeader, frame.Priority) error       { return nil }
func (discardHandler) OnRSTStream(frame.FrameHeader, frame.ErrCode) error       { return nil }
func (discardHandler) OnSettings(frame.FrameHeader, frame.SettingsParams) error { return nil }
func (discardHandler) OnPushPromise(frame.FrameHeader, uint32, frame.HeaderBlock, uint8) error {
	return nil
}
func (discardHandler) OnPing(frame.FrameHeader, [8]byte) error { return nil }
func (discardHandler) OnGoAway(frame.FrameHeader, uint32, frame.ErrCode, []byte) error {
	return nil
}
func (discardHandler) OnWindowUpdate(frame.FrameHeader, uint32) error            { return nil }
func (discardHandler) OnContinuation(frame.FrameHeader, frame.HeaderBlock) error { return nil }
func (discardHandler) OnOrigin(frame.FrameHeader, []string) error                { return nil }
func (discardHandler) OnAltSvc(frame.FrameHeader, []frame.AltSvcEntry) error     { return nil }
