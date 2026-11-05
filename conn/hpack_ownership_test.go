package conn

import (
	"bytes"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_HeaderFieldsSurviveHPACKReuse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		prefix          []hpack.HeaderField
		headersReceived bool
		endStream       bool
		wantType        StreamEventType
	}{
		{
			name:      "response",
			prefix:    []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}},
			endStream: true,
			wantType:  EventHeaders,
		},
		{
			name:            "trailers",
			headersReceived: true,
			endStream:       true,
			wantType:        EventTrailers,
		},
		{
			name: "interim",
			prefix: []hpack.HeaderField{{
				Name: []byte(":status"), Value: []byte("103"), Indexing: hpack.IndexWithout,
			}},
			wantType: EventInterimHeaders,
		},
	} {
		for _, fragmented := range []bool{false, true} {
			name := tc.name + "/HEADERS"
			if fragmented {
				name += "+CONTINUATION"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				m := newFakeStreamMap()
				dec, enc := seededOwnershipCodecs(t, "x-old")
				h := newConnHandler(m, dec)
				s := m.addStream(1)
				s.headersReceived = tc.headersReceived
				want := append(tc.prefix[:len(tc.prefix):len(tc.prefix)], ownershipFields()...)
				block := enc.EncodeBlock(nil, want)
				reuse := enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte("x-end"), Value: []byte("c")}})

				err := deliverOwnershipHeaders(h, block, tc.endStream, fragmented)
				reuseErr := h.OnHeaders(frame.FrameHeader{
					Type: frame.FrameHeaders, StreamID: 3, Flags: frame.FlagHeadersEndHeaders,
				}, reuse, nil, 0)
				clear(block)
				clear(reuse)

				require.NoError(t, err, "a later insertion must not corrupt an earlier decoded field")
				require.NoError(t, reuseErr, "the connection decoder must remain usable")
				require.Len(t, s.events, 1, "the header block must produce one queued event")
				ev := <-s.events
				defer ev.Release()
				assert.Equal(t, tc.wantType, ev.Type)
				assert.Equal(t, tc.endStream, ev.EndStream)
				assertOwnedHeaderFields(t, want, ev.Headers)
			})
		}
	}
}

func TestHandler_PushPromiseFieldsSurviveHPACKReuse(t *testing.T) {
	t.Parallel()
	for _, fragmented := range []bool{false, true} {
		name := "PUSH_PROMISE"
		if fragmented {
			name += "+CONTINUATION"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := newFakeStreamMap()
			m.pushEnabled = true
			dec, enc := seededOwnershipCodecs(t, "x-old")
			h := newConnHandler(m, dec)
			parent := m.addStream(1)
			parent.authorityBuf = []byte("example.com")
			want := append([]hpack.HeaderField{
				{Name: []byte(":method"), Value: []byte("GET")},
				{Name: []byte(":scheme"), Value: []byte("https")},
				{Name: []byte(":authority"), Value: []byte("example.com"), Indexing: hpack.IndexWithout},
				{Name: []byte(":path"), Value: []byte("/")},
			}, ownershipFields()...)
			block := enc.EncodeBlock(nil, want)
			reuse := enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte("x-end"), Value: []byte("c")}})

			err := deliverOwnershipPromise(h, block, fragmented)
			reuseErr := h.OnHeaders(frame.FrameHeader{
				Type: frame.FrameHeaders, StreamID: 3, Flags: frame.FlagHeadersEndHeaders,
			}, reuse, nil, 0)
			clear(block)
			clear(reuse)

			require.NoError(t, err, "the promised request must survive dynamic-table eviction within its block")
			require.NoError(t, reuseErr, "the connection decoder must remain usable")
			require.Len(t, parent.events, 1, "the promise must produce one queued event")
			ev := <-parent.events
			defer ev.Release()
			assert.Equal(t, EventPushPromise, ev.Type)
			assert.Equal(t, uint32(2), ev.PushStreamID)
			assertOwnedHeaderFields(t, want, ev.Headers)
		})
	}
}

func TestHandler_HPACKReuseCannotHideInvalidField(t *testing.T) {
	t.Parallel()
	for _, push := range []bool{false, true} {
		name := "response"
		if push {
			name = "push promise"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := newFakeStreamMap()
			m.pushEnabled = true
			dec, enc := seededOwnershipCodecs(t, "X-old")
			h := newConnHandler(m, dec)
			s := m.addStream(1)
			s.authorityBuf = []byte("example.com")
			fields := make([]hpack.HeaderField, 0, 6)
			fields = append(fields, hpack.HeaderField{Name: []byte(":status"), Value: []byte("200")})
			if push {
				fields = append(fields[:0], []hpack.HeaderField{
					{Name: []byte(":method"), Value: []byte("GET")},
					{Name: []byte(":scheme"), Value: []byte("https")},
					{Name: []byte(":authority"), Value: []byte("example.com"), Indexing: hpack.IndexWithout},
					{Name: []byte(":path"), Value: []byte("/")},
				}...)
			}
			fields = append(fields,
				hpack.HeaderField{Name: []byte("X-old"), Value: []byte("a")},
				hpack.HeaderField{Name: []byte("x-new"), Value: []byte("b")})
			block := enc.EncodeBlock(nil, fields)

			var err error
			if push {
				err = deliverOwnershipPromise(h, block, false)
			} else {
				err = deliverOwnershipHeaders(h, block, true, false)
			}

			if push {
				assert.NoError(t, err, "a malformed promise is refused without closing the connection")
			} else {
				var streamErr *StreamError
				require.ErrorAs(t, err, &streamErr, "validation must see the original uppercase name before it was evicted")
				assert.Equal(t, frame.ErrCodeProtocolError, streamErr.Code)
			}
			assert.Empty(t, s.events, "an overwritten invalid name must not turn a malformed block into an accepted event")
		})
	}
}

func TestHandler_HPACKErrorAfterFieldsDiscardsBlock(t *testing.T) {
	t.Parallel()
	for _, push := range []bool{false, true} {
		name := "response"
		if push {
			name = "push promise"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := newFakeStreamMap()
			m.pushEnabled = true
			h := newConnHandler(m, hpack.NewDecoder())
			s := m.addStream(1)
			s.authorityBuf = []byte("example.com")
			fields := []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}
			if push {
				fields = []hpack.HeaderField{
					{Name: []byte(":method"), Value: []byte("GET")},
					{Name: []byte(":scheme"), Value: []byte("https")},
					{Name: []byte(":authority"), Value: []byte("example.com")},
					{Name: []byte(":path"), Value: []byte("/")},
				}
			}
			block := append(encodeBlock(t, fields), 0x80)

			var err error
			if push {
				err = deliverOwnershipPromise(h, block, false)
			} else {
				err = deliverOwnershipHeaders(h, block, true, false)
			}

			var connErr *ConnError
			require.ErrorAs(t, err, &connErr, "a bad index after decoded fields must still reject the complete block")
			assert.Equal(t, frame.ErrCodeCompressionError, connErr.Code)
			assert.Empty(t, s.events, "fields decoded before an HPACK error must not escape as a partial event")
		})
	}
}

func assertOwnedHeaderFields(t *testing.T, want, got []hpack.HeaderField) {
	t.Helper()
	assert.Equal(t, want, got, "queued fields must retain their order, bytes and indexing modes until Release")
	for i, field := range got {
		assert.Equal(t, len(field.Name), cap(field.Name), "appending to field %d's name must not overwrite its value", i)
		assert.Equal(t, len(field.Value), cap(field.Value), "appending to field %d's value must not overwrite its neighbor", i)
	}
}

func seededOwnershipCodecs(t *testing.T, seedName string) (*hpack.Decoder, *hpack.Encoder) {
	t.Helper()
	enc := hpack.NewEncoder()
	enc.SetMaxDynamicTableSize(64)
	dec := hpack.NewDecoder()
	seed := enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(seedName), Value: []byte("a")}})
	require.NoError(t, dec.DecodeBlock(seed, func(hpack.HeaderField) error { return nil }))
	return dec, enc
}

func ownershipFields() []hpack.HeaderField {
	return []hpack.HeaderField{
		{Name: []byte("x-old"), Value: []byte("a")},
		{Name: []byte("x-new"), Value: []byte("b")},
		{Name: []byte("x-large"), Value: bytes.Repeat([]byte("z"), 600), Indexing: hpack.IndexWithout},
		{Name: []byte("x-repeat"), Value: []byte("first"), Indexing: hpack.IndexWithout},
		{Name: []byte("x-repeat"), Value: []byte("second"), Indexing: hpack.IndexNever},
	}
}

func deliverOwnershipHeaders(h *connHandler, block []byte, endStream, fragmented bool) error {
	fh := frame.FrameHeader{Type: frame.FrameHeaders, StreamID: 1}
	if endStream {
		fh.Flags |= frame.FlagHeadersEndStream
	}
	if !fragmented {
		fh.Flags |= frame.FlagHeadersEndHeaders
		return h.OnHeaders(fh, block, nil, 0)
	}
	mid := len(block) / 2
	if err := h.OnHeaders(fh, block[:mid], nil, 0); err != nil {
		return err
	}
	return h.OnContinuation(frame.FrameHeader{
		Type: frame.FrameContinuation, StreamID: 1, Flags: frame.FlagContinuationEndHeaders,
	}, block[mid:])
}

func deliverOwnershipPromise(h *connHandler, block []byte, fragmented bool) error {
	fh := frame.FrameHeader{Type: frame.FramePushPromise, StreamID: 1}
	if !fragmented {
		fh.Flags = frame.FlagPushPromiseEndHeaders
		return h.OnPushPromise(fh, 2, block, 0)
	}
	mid := len(block) / 2
	if err := h.OnPushPromise(fh, 2, block[:mid], 0); err != nil {
		return err
	}
	return h.OnContinuation(frame.FrameHeader{
		Type: frame.FrameContinuation, StreamID: 1, Flags: frame.FlagContinuationEndHeaders,
	}, block[mid:])
}
