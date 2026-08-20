package client

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http3"
	"github.com/lodgvideon/poseidon-http-client/trace"
)

// TestRequestComplete_H3ReportsProtocolAndConnect is the third protocol's copy
// of what request_stats_test.go checks for HTTP/2 and HTTP/1.1. It lives here
// rather than beside them because reaching the HTTP/3 transport without a live
// QUIC peer means replacing singleH3Conn.dialFn, which is unexported.
//
// The dial is padded with a sleep so Connect is a duration a test can tell from
// zero: on a fake client the handshake is a function return, and "positive" and
// "the clock did not tick" are otherwise the same observation.
func TestRequestComplete_H3ReportsProtocolAndConnect(t *testing.T) {
	fake := &fakeH3Client{
		resp: &http3.Response{
			Status:  200,
			Headers: []conn.HeaderField{{Name: []byte("content-type"), Value: []byte("text/plain")}},
		},
		body: []byte("hello"),
	}
	var events []RequestCompleteEvent
	c, err := NewClient(ClientOptions{
		Addr:      "h3.example:443",
		Transport: TransportH3,
		TLSConfig: &tls.Config{ServerName: "h3.example"},
		Hooks: &Hooks{OnRequestComplete: func(e RequestCompleteEvent) {
			events = append(events, e)
		}},
	})
	require.NoError(t, err, "NewClient(TransportH3)")
	defer c.Close()
	sc, ok := c.tr.(*singleH3Conn)
	require.Truef(t, ok, "transport is %T, want *singleH3Conn", c.tr)
	sc.dialFn = func(context.Context, string, *tls.Config) (h3Client, error) {
		time.Sleep(2 * time.Millisecond)
		return fake, nil
	}

	for i := 0; i < 2; i++ {
		var resp Response
		resp.Reset()
		require.NoErrorf(t, c.Do(context.Background(),
			&Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp), "Do #%d", i)
	}

	require.Len(t, events, 2, "two requests, two completion events")
	assert.Equalf(t, trace.ProtoH3, events[0].Proto, "Proto = %v, want h3", events[0].Proto)
	assert.Equal(t, "h3.example:443", events[0].RemoteAddr, "RemoteAddr must name the QUIC peer this attempt went to")
	assert.GreaterOrEqualf(t, events[0].Connect, 2*time.Millisecond,
		"Connect = %v on the attempt that performed the QUIC handshake, want at least the %v the dial slept — over HTTP/3 the handshake IS the dial, so a zero here means it was never charged",
		events[0].Connect, 2*time.Millisecond)
	assert.Zerof(t, events[1].Connect,
		"Connect = %v on an attempt that reused the QUIC connection", events[1].Connect)
}

// TestRequestComplete_StreamingReportsFirstByte covers the other entry point.
// DoStream returns AT the response head, so its Latency and TTFB happen to
// measure nearly the same instant — but they are computed on different paths,
// and the streaming one was the path with no first-byte measurement at all
// until this event grew one.
func TestRequestComplete_StreamingReportsFirstByte(t *testing.T) {
	fake := &fakeH3Client{
		resp: &http3.Response{Status: 200},
		body: []byte("chunk"),
	}
	var events []RequestCompleteEvent
	c, err := NewClient(ClientOptions{
		Addr:      "h3.example:443",
		Transport: TransportH3,
		TLSConfig: &tls.Config{ServerName: "h3.example"},
		Hooks: &Hooks{OnRequestComplete: func(e RequestCompleteEvent) {
			events = append(events, e)
		}},
	})
	require.NoError(t, err, "NewClient(TransportH3)")
	defer c.Close()
	sc, ok := c.tr.(*singleH3Conn)
	require.Truef(t, ok, "transport is %T, want *singleH3Conn", c.tr)
	sc.dialFn = func(context.Context, string, *tls.Config) (h3Client, error) {
		time.Sleep(2 * time.Millisecond)
		return fake, nil
	}
	var sr StreamResponse

	err = c.DoStream(context.Background(), &Request{Method: "GET", Path: "/"}, &sr)

	require.NoError(t, err, "DoStream over the fake HTTP/3 client")
	defer func() { _ = sr.Close() }()
	require.Len(t, events, 1, "one attempt must produce exactly one completion event")
	e := events[0]
	assert.Positivef(t, e.TTFB,
		"TTFB = %v on a stream whose head arrived; the streaming path fills this in from beginStreaming, not from drainResponse, so it is a separate measurement that can be lost on its own",
		e.TTFB)
	assert.LessOrEqualf(t, e.Acquire, e.TTFB,
		"Acquire %v > TTFB %v; the connection is acquired before the head arrives", e.Acquire, e.TTFB)
}
