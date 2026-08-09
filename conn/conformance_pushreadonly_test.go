package conn

import (
	"context"
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
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
	fields := []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}

	t.Run("SendHeaders", func(t *testing.T) {
		if err := newPushed().ref().SendHeaders(ctx, fields, false); !errors.Is(err, ErrPushedStreamReadOnly) {
			t.Errorf("SendHeaders on a pushed stream: err = %v, want ErrPushedStreamReadOnly (§5.1 reserved(remote))", err)
		}
	})
	t.Run("SendData", func(t *testing.T) {
		if err := newPushed().ref().SendData(ctx, []byte("x"), false); !errors.Is(err, ErrPushedStreamReadOnly) {
			t.Errorf("SendData on a pushed stream: err = %v, want ErrPushedStreamReadOnly (§5.1 reserved(remote))", err)
		}
	})
}

// TestConformance_RFC9113_Sec5_1_SendOnClientStreamStillWorks is the
// over-rejection guard: a normal client-initiated (odd, not pushed) stream must
// still accept SendHeaders/SendData — the pushed-stream guard must not block the
// request path.
func TestConformance_RFC9113_Sec5_1_SendOnClientStreamStillWorks(t *testing.T) {
	ctx := context.Background()
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	s := newStream(1, 8, &fakeStreamWriter{}, 65535) // odd id, not pushed
	if err := s.ref().SendHeaders(ctx, fields, false); err != nil {
		t.Fatalf("SendHeaders on a client stream: %v — the pushed-stream guard must not block requests", err)
	}
	if err := s.ref().SendData(ctx, []byte("body"), true); err != nil {
		t.Errorf("SendData on a client stream: %v", err)
	}
}
