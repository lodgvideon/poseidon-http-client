// client/conformance_protocol_method_test.go — RFC 8441 §4
// conformance tests for the request validator.
//
// RFC 8441 §4 (Extended CONNECT) requires that the :protocol
// pseudo-header MAY appear ONLY when Method is "CONNECT" and the
// peer has advertised SETTINGS_ENABLE_CONNECT_PROTOCOL=1. A request
// with Protocol set but Method != CONNECT is malformed.
//
// We also check that an empty Protocol on a CONNECT request is fine
// (RFC 8441 only mandates the header when an extended-CONNECT
// sub-protocol is being negotiated; legacy CONNECT for tunneling
// has no :protocol).
package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// protocolReproServer: handshake, then respond 200 to any incoming
// request, capturing the first HEADERS for inspection.
func protocolReproServer(stopSrv <-chan struct{}) func(*frame.Framer) {
	return func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			select {
			case <-stopSrv:
				return
			default:
			}
			capH.mu.Lock()
			var handled []capturedHeaders
			handled = append(handled, capH.headers...)
			capH.mu.Unlock()
			for _, hd := range handled {
				enc := hpack.NewEncoder()
				block := enc.EncodeBlock(nil, []hpack.HeaderField{
					{Name: []byte(":status"), Value: []byte("200")},
				})
				_ = srvFr.WriteHeaders(frame.WriteHeadersParams{
					StreamID:      hd.streamID,
					BlockFragment: block,
					EndHeaders:    true,
					EndStream:     true,
				})
			}
		}
	}
}

// TestRepro_ProtocolRequiresConnectMethod: setting Protocol on a
// non-CONNECT request must be rejected by validateRequest.
//
// After hardening, this should fail with ErrInvalidRequest. Today
// (pre-fix) it likely passes through.
func TestConformance_RFC8441_Sec4_ProtocolRequiresConnectMethod(t *testing.T) {
	stopSrv := make(chan struct{})
	t.Cleanup(func() { close(stopSrv) })
	d := &fakeDialer{srvAfter: protocolReproServer(stopSrv)}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Method=GET, Protocol="websocket" — must be rejected per RFC 8441.
	var resp Response
	err = c.Do(ctx, &Request{
		Method:   "GET",
		Path:     "/",
		Protocol: "websocket",
	}, &resp)

	require.Errorf(t, err,
		"GET + Protocol:websocket accepted; must be rejected per RFC 8441 §4 (status=%d)", resp.Status)
	require.ErrorIs(t, err, ErrInvalidRequest,
		"a :protocol on a non-CONNECT method is malformed, not a transport failure")
}

// TestRepro_Protocol_EmptyOnConnect_OK: a CONNECT with empty Protocol
// (legacy tunneling) must be allowed. No :protocol pseudo-header is
// emitted.
func TestConformance_RFC8441_Sec4_Protocol_EmptyOnConnect_OK(t *testing.T) {
	stopSrv := make(chan struct{})
	t.Cleanup(func() { close(stopSrv) })
	d := &fakeDialer{srvAfter: protocolReproServer(stopSrv)}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp Response
	err = c.Do(ctx, &Request{
		Method:    "CONNECT",
		Authority: "example.com:443", // authority-form target; :path omitted (RFC 9113 §8.5)
	}, &resp)

	require.NoError(t, err, "CONNECT without Protocol should be allowed")
	assert.Equal(t, 200, resp.Status, "status = %d, want 200", resp.Status)
}

// TestRepro_Protocol_NonEmptyOnConnect_OK: a CONNECT with Protocol set
// must be allowed (the server's responsibility to advertise
// SETTINGS_ENABLE_CONNECT_PROTOCOL=1). Local server does not advertise,
// but client should still emit the request — server is responsible for
// rejecting if it doesn't support.
func TestConformance_RFC8441_Sec4_Protocol_NonEmptyOnConnect_OK(t *testing.T) {
	stopSrv := make(chan struct{})
	t.Cleanup(func() { close(stopSrv) })
	d := &fakeDialer{srvAfter: protocolReproServer(stopSrv)}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp Response
	err = c.Do(ctx, &Request{
		Method:   "CONNECT",
		Path:     "/chat",
		Protocol: "websocket",
	}, &resp)

	require.NoError(t, err, "CONNECT + Protocol should be allowed at client layer")
	assert.Equal(t, 200, resp.Status, "status = %d, want 200", resp.Status)
}
