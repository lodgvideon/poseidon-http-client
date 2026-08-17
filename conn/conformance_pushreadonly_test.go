package conn

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// TestConformance_RFC9113_Sec5_1_SendOnPushedStreamRejected pins RFC 9113 §5.1
// (reserved (remote)): "An endpoint MUST NOT send any type of frame other than
// RST_STREAM, WINDOW_UPDATE, or PRIORITY in this state." A server-pushed stream
// is receive-only for the client for its whole life (reserved(remote) then, once
// the pushed response HEADERS arrive, half-closed(local)) — the client answers a
// promised request, it never originates one. A caller that reaches a pushed
// stream via LookupStream and calls SendHeaders/SendData must be refused, not
// allowed to put a client HEADERS/DATA frame on a server-initiated stream.
func TestConformance_RFC9113_Sec5_1_SendOnPushedStreamRejected(t *testing.T) {
	newPushed := func() *Stream {
		s := newStream(2, 8, &fakeStreamWriter{}, 65535) // even id: server-initiated
		s.pushed = true
		return s
	}
	ctx := context.Background()
	fields := []header.Field{{Name: []byte(":status"), Value: []byte("200")}}

	t.Run("SendHeaders", func(t *testing.T) {
		err := newPushed().ref().SendHeaders(ctx, fields, false)

		assert.Truef(t, errors.Is(err, ErrPushedStreamReadOnly),
			"SendHeaders on a pushed stream: err = %v, want ErrPushedStreamReadOnly (§5.1 reserved(remote))", err)
	})
	t.Run("SendData", func(t *testing.T) {
		err := newPushed().ref().SendData(ctx, []byte("x"), false)

		assert.Truef(t, errors.Is(err, ErrPushedStreamReadOnly),
			"SendData on a pushed stream: err = %v, want ErrPushedStreamReadOnly (§5.1 reserved(remote))", err)
	})
}

// TestConformance_RFC9113_Sec5_1_SendOnClientStreamStillWorks is the
// over-rejection guard: a normal client-initiated (odd, not pushed) stream must
// still accept SendHeaders/SendData — the pushed-stream guard must not block the
// request path.
func TestConformance_RFC9113_Sec5_1_SendOnClientStreamStillWorks(t *testing.T) {
	ctx := context.Background()
	fields := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	s := newStream(1, 8, &fakeStreamWriter{}, 65535) // odd id, not pushed

	herr := s.ref().SendHeaders(ctx, fields, false)
	derr := s.ref().SendData(ctx, []byte("body"), true)

	require.NoError(t, herr,
		"SendHeaders on a client stream — the pushed-stream guard must not block requests")
	assert.NoError(t, derr, "SendData on a client stream")
}
