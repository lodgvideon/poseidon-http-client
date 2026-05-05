package conn

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func TestStreamEventType_String(t *testing.T) {
	cases := []struct {
		t    StreamEventType
		want string
	}{
		{EventHeaders, "headers"},
		{EventData, "data"},
		{EventTrailers, "trailers"},
		{EventReset, "reset"},
	}
	for _, c := range cases {
		if got := c.t.String(); got != c.want {
			t.Fatalf("%v: got %q, want %q", c.t, got, c.want)
		}
	}
}

func TestStreamEvent_TypeDispatch(t *testing.T) {
	headers := []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}
	e := StreamEvent{Type: EventHeaders, Headers: headers, EndStream: false}
	if e.Type != EventHeaders || len(e.Headers) != 1 {
		t.Fatalf("event = %+v", e)
	}
	r := StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeCancel}
	if r.Type != EventReset || r.RSTCode != frame.ErrCodeCancel {
		t.Fatalf("reset event = %+v", r)
	}
}
