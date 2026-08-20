// client/conformance_forbidden_headers_test.go — RFC 7540 §8.1.2.3
// conformance tests for the request validator.
//
// RFC 7540 §8.1.2.3 forbids endpoints from generating these connection-
// specific headers in HTTP/2 requests:
//
//   - Connection
//   - Keep-Alive
//   - Proxy-Connection
//   - Transfer-Encoding
//   - Upgrade
//   - TE (except exactly "trailers")
//
// Sending any of them is a request-smuggling vector: an HTTP/1.1
// intermediary that downgrades the connection can misinterpret the
// payload. The safe thing for a client is to reject them at validate
// time, before they ever reach the wire.
//
// These tests pin the request-validator behavior at the boundary:
// any client/Request with a forbidden header MUST be rejected before
// it reaches the wire, otherwise the value can be smuggled through an
// HTTP/1.1 downgrading intermediary (RFC 7230 §6.1).
package client

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// forbiddenHeadersRepro: capture every header the client emits on
// stream #1, then send a 200 so the test can finish cleanly.
func forbiddenHeadersRepro() func(*frame.Framer) {
	return func(srvFr *frame.Framer) {
		capH := newCaptureHandler()
		for {
			if _, err := srvFr.ReadFrame(context.Background(), capH); err != nil {
				return
			}
			capH.mu.Lock()
			var captured []capturedHeaders
			captured = append(captured, capH.headers...)
			capH.mu.Unlock()
			for _, hd := range captured {
				if hd.streamID == 0 {
					continue
				}
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

// forbiddenHeaderClient builds a client against the capture server above.
func forbiddenHeaderClient(t *testing.T) *Client {
	t.Helper()
	d := &fakeDialer{srvAfter: forbiddenHeadersRepro()}
	c, err := NewClient(ClientOptions{
		Addr:     "fake:0",
		ConnOpts: conn.ConnOptions{Dialer: d},
	})
	require.NoError(t, err, "NewClient")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestRepro_ForbiddenHeader_Connection_Behavior is the REPRO for
// RFC 7540 §8.1.2.3 — sending "Connection" in regular headers.
//
// After hardening validateRequest, this test must fail with
// ErrInvalidRequest. Today (pre-fix) it likely passes through.
func TestConformance_RFC7540_Sec8_1_2_3_ForbidsConnection(t *testing.T) {
	c := forbiddenHeaderClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp Response
	err := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("connection"), Value: []byte("keep-alive")},
		},
	}, &resp)

	// Hardened behavior: ErrInvalidRequest up front.
	require.Errorf(t, err,
		"Connection header was accepted; should be rejected per RFC 7540 §8.1.2.3 (status=%d)", resp.Status)
	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for Connection header")
	assert.Containsf(t, strings.ToLower(err.Error()), "connection",
		"error should name the offending header: %v", err)
}

// TestRepro_ForbiddenHeader_TransferEncoding_Behavior is the REPRO for
// RFC 7540 §8.1.2.3 — sending "Transfer-Encoding".
//
// HTTP/2 uses DATA frames, not chunked encoding. Sending
// Transfer-Encoding: chunked is a classic HTTP/1.1 request smuggling
// payload: an HTTP/1.1→2 downgrade proxy can misinterpret it.
func TestConformance_RFC7540_Sec8_1_2_3_ForbidsTransferEncoding(t *testing.T) {
	c := forbiddenHeaderClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp Response
	err := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("transfer-encoding"), Value: []byte("chunked")},
		},
	}, &resp)

	require.Errorf(t, err,
		"Transfer-Encoding header was accepted; should be rejected per RFC 7540 §8.1.2.3 (status=%d)", resp.Status)
	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for Transfer-Encoding header")
}

// TestRepro_ForbiddenHeader_Upgrade_Behavior is the REPRO for
// RFC 7540 §8.1.2.3 — sending "Upgrade" header.
//
// HTTP/2 has its own upgrade mechanism (:protocol pseudo-header,
// RFC 8441). Plain Upgrade header is meaningless on H2 and is a
// downgrade-misread vector.
func TestConformance_RFC7540_Sec8_1_2_3_ForbidsUpgrade(t *testing.T) {
	c := forbiddenHeaderClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp Response
	err := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("upgrade"), Value: []byte("h2c")},
		},
	}, &resp)

	require.Errorf(t, err,
		"Upgrade header was accepted; should be rejected per RFC 7540 §8.1.2.3 (status=%d)", resp.Status)
	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for Upgrade header")
}

// TestRepro_ForbiddenHeader_KeepAlive_Behavior is the REPRO for
// RFC 7540 §8.1.2.3 — sending "Keep-Alive" (HTTP/1.1 persistent-
// connection signal; meaningless on H2).
func TestConformance_RFC7540_Sec8_1_2_3_ForbidsKeepAlive(t *testing.T) {
	c := forbiddenHeaderClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp Response
	err := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("keep-alive"), Value: []byte("timeout=5")},
		},
	}, &resp)

	require.Errorf(t, err,
		"Keep-Alive header was accepted; should be rejected per RFC 7540 §8.1.2.3 (status=%d)", resp.Status)
	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for Keep-Alive header")
}

// TestRepro_ForbiddenHeader_ProxyConnection_Behavior is the REPRO for
// RFC 7540 §8.1.2.3 — sending "Proxy-Connection" (legacy proxy
// keep-alive signal).
func TestConformance_RFC7540_Sec8_1_2_3_ForbidsProxyConnection(t *testing.T) {
	c := forbiddenHeaderClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp Response
	err := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte("proxy-connection"), Value: []byte("keep-alive")},
		},
	}, &resp)

	require.Errorf(t, err,
		"Proxy-Connection header was accepted; should be rejected per RFC 7540 §8.1.2.3 (status=%d)", resp.Status)
	require.ErrorIs(t, err, ErrInvalidRequest, "expected ErrInvalidRequest for Proxy-Connection header")
}

// TestRepro_TE_Header_Only_Trailers is the REPRO for RFC 7540
// §8.1.2.3 — "TE" is allowed ONLY if its value is exactly "trailers".
// Any other value (including "trailers, gzip") is forbidden.
func TestConformance_RFC7540_Sec8_1_2_3_TEOnlyTrailers(t *testing.T) {
	c := forbiddenHeaderClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// TE: gzip — must be rejected. TE: trailers — MUST be allowed (used to
	// signal trailer support). Both directions of the same decision.
	var resp1, resp2 Response
	err1 := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{{Name: []byte("te"), Value: []byte("gzip")}},
	}, &resp1)
	err2 := c.Do(ctx, &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{{Name: []byte("te"), Value: []byte("trailers")}},
	}, &resp2)

	assert.Errorf(t, err1,
		"TE: gzip was accepted; should be rejected per RFC 7540 §8.1.2.3 (status=%d)", resp1.Status)
	assert.ErrorIs(t, err1, ErrInvalidRequest, "TE: gzip must be refused as an invalid request")
	assert.NoErrorf(t, err2, "TE: trailers must be allowed (status=%d)", resp2.Status)
}
