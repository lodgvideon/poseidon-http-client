package frame

import (
	"bytes"
	"context"
	"testing"
)

type dropHandler struct{}

func (dropHandler) OnData(FrameHeader, []byte, uint8) error                      { return nil }
func (dropHandler) OnHeaders(FrameHeader, HeaderBlock, *Priority, uint8) error   { return nil }
func (dropHandler) OnPriority(FrameHeader, Priority) error                       { return nil }
func (dropHandler) OnRSTStream(FrameHeader, ErrCode) error                       { return nil }
func (dropHandler) OnSettings(FrameHeader, SettingsParams) error                 { return nil }
func (dropHandler) OnPushPromise(FrameHeader, uint32, HeaderBlock, uint8) error  { return nil }
func (dropHandler) OnPing(FrameHeader, [8]byte) error                            { return nil }
func (dropHandler) OnGoAway(FrameHeader, uint32, ErrCode, []byte) error          { return nil }
func (dropHandler) OnWindowUpdate(FrameHeader, uint32) error                     { return nil }
func (dropHandler) OnContinuation(FrameHeader, HeaderBlock) error                { return nil }
func (dropHandler) OnOrigin(FrameHeader, []string) error                          { return nil }

func FuzzFramerReadFrame(f *testing.F) {
	// SETTINGS empty
	f.Add([]byte{0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00})
	// PING with payload
	f.Add([]byte{0x00, 0x00, 0x08, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 1, 2, 3, 4, 5, 6, 7, 8})
	// HEADERS minimal
	f.Add([]byte{0x00, 0x00, 0x02, 0x01, 0x05, 0x00, 0x00, 0x00, 0x01, 0x82, 0x84})
	// DATA 1 byte
	f.Add([]byte{0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xff})

	f.Fuzz(func(_ *testing.T, data []byte) {
		fr := NewFramer(nil, bytes.NewReader(data))
		var h dropHandler
		for i := 0; i < 100; i++ {
			_, err := fr.ReadFrame(context.Background(), h)
			if err != nil {
				return
			}
		}
		// Invariant: no panic.
	})
}
