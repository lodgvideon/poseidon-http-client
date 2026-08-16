package frame

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The limit is checked on read AND at eight write sites, so SetMaxReadFrameSize
// named half of what it did — its own doc comment spent a sentence explaining
// that away. It is SetMaxFrameSize now, with the old name kept as a deprecated
// alias (#516).
//
// An alias is only worth keeping if it still works, and "still works" here means
// it drives the same limit, not merely that it compiles.

// TestSetMaxFrameSize_DeprecatedAliasDrivesTheSameLimit pins the alias against
// the behaviour, on the WRITE path — the half the old name denied existed.
func TestSetMaxFrameSize_DeprecatedAliasDrivesTheSameLimit(t *testing.T) {
	payload := make([]byte, 64)

	t.Run("new name bounds writes", func(t *testing.T) {
		fr, _ := newFramerWithBuffer()
		fr.SetMaxFrameSize(32)

		err := fr.WriteData(1, false, payload)

		assert.ErrorIsf(t, err, ErrFrameTooLarge,
			"WriteData over the limit = %v, want ErrFrameTooLarge — the limit "+
				"is not applied on write", err)
	})

	t.Run("deprecated alias bounds writes identically", func(t *testing.T) {
		fr, _ := newFramerWithBuffer()
		fr.SetMaxReadFrameSize(32)

		err := fr.WriteData(1, false, payload)

		assert.ErrorIsf(t, err, ErrFrameTooLarge,
			"WriteData over the limit set via the alias = %v, want "+
				"ErrFrameTooLarge — the alias no longer drives the same field", err)
	})

	t.Run("either name lets a frame under the limit through", func(t *testing.T) {
		for name, set := range map[string]func(*Framer, uint32){
			"SetMaxFrameSize":     (*Framer).SetMaxFrameSize,
			"SetMaxReadFrameSize": (*Framer).SetMaxReadFrameSize,
		} {
			fr, _ := newFramerWithBuffer()
			set(fr, 128)

			err := fr.WriteData(1, false, payload)

			assert.NoErrorf(t, err, "%s(128) then a 64-byte frame: %v", name, err)
		}
	})
}
