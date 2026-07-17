package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// customPromiseBlock encodes a promised request header list with the four
// standard pseudo-headers plus whatever extra fields the test needs. Used to
// build both malformed promises (an injected CR/NUL, a forbidden field) and the
// request-legal "te: trailers" promise the acceptance guard sends.
func customPromiseBlock(enc *hpack.Encoder, extra ...hpack.HeaderField) []byte {
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":path"), Value: []byte("/a.css")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
	}
	fields = append(fields, extra...)
	return enc.EncodeBlock(nil, fields)
}

// TestConformance_RFC7540_Sec1030_PushPromiseMalformedFields_Rejected pins the
// receive-side field validation on the push path. RFC 7540 §10.3: "Requests or
// responses containing invalid header field names MUST be treated as malformed
// (Section 8.1.2.6). ... Any request or response that contains a character not
// permitted in a header field value MUST be treated as malformed (Section
// 8.1.2.6)." A PUSH_PROMISE carries a request header set, so the rule binds it.
// §8.1.2.6 makes a malformed block a stream error of type PROTOCOL_ERROR.
//
// Before the fix, OnPushPromise decoded the promised block and handed it to the
// caller verbatim — the send-validated/receive-trusting asymmetry §10.3 exists
// to close, and the same gap validateResponseFields (#263) closed on the normal
// response path. Each subtest promises one malformed field; the client must
// RST_STREAM(PROTOCOL_ERROR) the promised stream, not register it, and not
// deliver an EventPushPromise.
func TestConformance_RFC7540_Sec1030_PushPromiseMalformedFields_Rejected(t *testing.T) {
	cases := []struct {
		name  string
		extra hpack.HeaderField
	}{
		{"NUL in value", hpack.HeaderField{Name: []byte("x-evil"), Value: []byte("a\x00b")}},
		{"CR in value", hpack.HeaderField{Name: []byte("x-evil"), Value: []byte("a\rb")}},
		{"LF in value", hpack.HeaderField{Name: []byte("x-evil"), Value: []byte("a\nb")}},
		{"uppercase field name", hpack.HeaderField{Name: []byte("X-Bad"), Value: []byte("y")}},
		{"connection-specific field", hpack.HeaderField{Name: []byte("transfer-encoding"), Value: []byte("chunked")}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := net.Pipe()
			defer cli.Close()

			probe := newPushProbe()

			go pipeServer(t, srv, func(srvFr *frame.Framer) {
				if !awaitRequest(t, srvFr) {
					return
				}
				drainFrames(srvFr, probe)

				enc := hpack.NewEncoder()
				<-asyncWrite(func() error {
					return srvFr.WriteHeaders(frame.WriteHeadersParams{
						StreamID:      1,
						BlockFragment: enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}),
						EndHeaders:    true,
					})
				})
				<-asyncWrite(func() error {
					return srvFr.WritePushPromise(1, 2, customPromiseBlock(enc, tc.extra), true, 0)
				})
				// Ping barrier: an observed ACK proves the reader has processed
				// the PUSH_PROMISE that preceded it on the wire.
				<-asyncWrite(func() error { return srvFr.WritePing(false, [8]byte{9}) })
				<-time.After(2 * time.Second)
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			c, err := NewClientConn(ctx, cli, ConnOptions{
				Settings:          AdvertisedSettings{}.defaulted(),
				StreamEventBuffer: 16,
				EnablePush:        true,
			})
			if err != nil {
				t.Fatalf("NewClientConn: %v", err)
			}
			defer c.Close()

			parent := openParentStream(ctx, t, c)

			// Wait for the ping ACK: after it, the malformed promise has been
			// handled, so the registry and the RST record are settled.
			select {
			case <-probe.pingAck:
			case <-ctx.Done():
				t.Fatal("timed out waiting for ping ACK barrier")
			}

			if _, ok := c.LookupStream(2); ok {
				t.Fatal("promised stream 2 was registered; malformed promise must be refused")
			}

			var sawProto bool
			for _, code := range probe.rstCodes {
				if code == frame.ErrCodeProtocolError {
					sawProto = true
				}
			}
			if !sawProto {
				t.Fatalf("no RST_STREAM(PROTOCOL_ERROR); got codes %v", probe.rstCodes)
			}

			// The parent stream survives: a stream-level refusal leaves the
			// connection usable. A pending event would be the malformed promise
			// leaking through, so there must be none buffered.
			if got := len(parent.events); got != 0 {
				t.Fatalf("parent has %d buffered events; malformed promise leaked to caller", got)
			}
		})
	}
}

// TestConformance_RFC7540_Sec8122_PushPromiseTETrailers_Accepted guards the
// half that makes the malformed-field check worth having: a promised request
// carrying "te: trailers" is legal and MUST be delivered. RFC 7540 §8.1.2.2:
// "The only exception to this is the TE header field, which MAY be present in
// an HTTP/2 request; when it is, it MUST NOT contain any value other than
// "trailers"." The push block is a request, so forbiddenRequestField diverges
// from the response-side helper here — a client that rejected te: trailers on a
// promise would refuse a legal push.
func TestConformance_RFC7540_Sec8122_PushPromiseTETrailers_Accepted(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		drainFrames(srvFr, &nilHandler{})

		enc := hpack.NewEncoder()
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}),
				EndHeaders:    true,
			})
		})
		<-asyncWrite(func() error {
			return srvFr.WritePushPromise(1, 2, customPromiseBlock(enc,
				hpack.HeaderField{Name: []byte("te"), Value: []byte("trailers")}), true, 0)
		})
		<-time.After(2 * time.Second)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClientConn(ctx, cli, ConnOptions{
		Settings:          AdvertisedSettings{}.defaulted(),
		StreamEventBuffer: 16,
		EnablePush:        true,
	})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	parent := openParentStream(ctx, t, c)

	ev, err := parent.Recv(ctx)
	if err != nil {
		t.Fatalf("parent Recv: %v", err)
	}
	if ev.Type != EventPushPromise {
		t.Fatalf("event = %s, want EventPushPromise (te: trailers is a legal request field)", ev.Type)
	}
	if ev.Slab != nil {
		GetHeaderSlabPool().Put(ev.Slab)
	}
	if _, ok := c.LookupStream(2); !ok {
		t.Fatal("promised stream 2 not registered; legal push was refused")
	}
}
