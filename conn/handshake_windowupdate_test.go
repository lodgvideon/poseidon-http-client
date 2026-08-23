package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
)

// nginxLikePeer answers the client preface the way openresty/nginx does, and the
// only thing that matters about it is the ORDER: the connection-level
// WINDOW_UPDATE goes out after the peer's SETTINGS and before the peer's SETTINGS
// ACK. That is the window in which handshakeSettings is still reading frames
// through settingsRecorder, and it is where #701's credit was lost.
//
// The peer then drains whatever the client writes. net.Pipe is unbuffered, so a
// peer that stops reading stalls the client's writes for reasons that have
// nothing to do with flow control, and the test would pass or fail on its
// fixture instead of on the property.
func nginxLikePeer(t *testing.T, srv net.Conn, connIncr uint32) {
	t.Helper()
	defer srv.Close()

	preface := make([]byte, 24)
	if _, err := readN(srv, preface); err != nil {
		t.Logf("peer read preface: %v", err)
		return
	}
	srvFr := frame.NewFramer(srv, srv)

	// A large per-stream initial window, so the CONNECTION window is the only
	// thing this fixture can stall on. With the default 65535 on both, a body
	// big enough to prove the connection credit survived is also big enough to
	// exhaust the stream window, and the test would report a stall it cannot
	// attribute.
	var sp frame.SettingsParams
	sp.N = 1
	sp.Pairs[0] = frame.SettingPair{ID: frame.SettingInitialWindowSize, Value: 1 << 24}

	writeDone := make(chan error, 1)
	go func() { writeDone <- srvFr.WriteSettings(sp) }()
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("peer read client settings: %v", err)
		return
	}
	if err := <-writeDone; err != nil {
		t.Logf("peer write settings: %v", err)
		return
	}

	go func() {
		writeDone <- func() error {
			if err := srvFr.WriteWindowUpdate(0, connIncr); err != nil {
				return err
			}
			return srvFr.WriteSettingsAck()
		}()
	}()
	if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
		t.Logf("peer read client ack: %v", err)
		return
	}
	if err := <-writeDone; err != nil {
		t.Logf("peer write window update + ack: %v", err)
		return
	}

	for {
		if _, err := srvFr.ReadFrame(context.Background(), &nilHandler{}); err != nil {
			return
		}
	}
}

// TestConformance_RFC9113_Sec69_HandshakeWindowUpdateIsCredit pins that a
// connection-level WINDOW_UPDATE arriving during the SETTINGS-ACK wait is applied
// rather than discarded.
//
// RFC 9113 §6.9 defines WINDOW_UPDATE for the connection as well as for a
// stream and carves out no interval in which one may be ignored — the SETTINGS
// exchange included. A client that reads the frame and drops its increment has
// silently refused credit the peer already granted, and the peer has no way to
// learn that it must send it again.
//
// It is not a theoretical ordering. nginx sends its connection-window raise
// immediately after its SETTINGS, which lands in exactly that interval, and it
// grants the whole 2^31-1 at once — so there is no later WINDOW_UPDATE to
// recover from a dropped one. Against the real fixture this cost 9 of 30
// concurrent 3006-byte POSTs, which hung until their context expired after the
// client had written exactly 65535 bytes (#701).
//
// The body is deliberately larger than connInitialRecvWindow: at or below it the
// send succeeds whether or not the increment survived, so the test would pass on
// the broken code.
func TestConformance_RFC9113_Sec69_HandshakeWindowUpdateIsCredit(t *testing.T) {
	const incr = 1 << 20
	cli, srv := net.Pipe()
	go nginxLikePeer(t, srv, incr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	require.NoError(t, err, "NewClientConn against a peer that credits the connection during the handshake")
	defer c.Close()
	body := make([]byte, connInitialRecvWindow+4096)

	s, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")
	require.NoError(t, s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("POST")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/upload")},
	}, false), "SendHeaders")
	sendErr := s.SendData(ctx, body, true)

	assert.NoErrorf(t, sendErr,
		"sending %d bytes stalled after the peer had already granted %d more than the "+
			"initial %d. The credit arrived during the SETTINGS-ACK wait, so a writer parked "+
			"in acquireSendCredits is waiting for a frame the peer will never send again",
		len(body), incr, connInitialRecvWindow)
}

// TestConformance_RFC9113_Sec69_HandshakeWindowUpdateAtTheLimit walks the boundary
// the handshake credit is checked against. RFC 9113 §6.9.1 caps a flow-control
// window at 2^31-1 and makes exceeding it a connection error of type
// FLOW_CONTROL_ERROR, so the last legal byte must be accepted and the first
// illegal one refused.
//
// The accepted row is not a hypothetical: 65535 + 2147418112 = 2^31-1 exactly is
// what nginx sends. A guard written with >= instead of > refuses it and breaks
// every connection to that peer at the handshake — which is why both rows are
// here rather than only the overflow one.
func TestConformance_RFC9113_Sec69_HandshakeWindowUpdateAtTheLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		incr    uint32
		wantErr bool
	}{
		{"exactly the maximum, the value nginx sends", uint32(maxFlowWindow) - connInitialRecvWindow, false},
		{"one past the maximum", uint32(maxFlowWindow) - connInitialRecvWindow + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := net.Pipe()
			go nginxLikePeer(t, srv, tc.incr)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())

			if !tc.wantErr {
				require.NoErrorf(t, err,
					"a connection window of exactly 2^31-1 is the largest RFC 9113 §6.9.1 allows, "+
						"and it is the one nginx grants; refusing it breaks every connection to that peer")
				assert.EqualValuesf(t, maxFlowWindow, c.peerConnSendWindow,
					"send window = %d, want the full %d the peer granted", c.peerConnSendWindow, maxFlowWindow)
				_ = c.Close()
				return
			}
			require.Errorf(t, err, "a WINDOW_UPDATE past 2^31-1 must fail the handshake, not be truncated into it")
			var ce *ConnError
			require.ErrorAsf(t, err, &ce, "want a typed ConnError, got %T: a caller cannot classify this otherwise", err)
			assert.Equalf(t, frame.ErrCodeFlowControlError, ce.Code,
				"code = %v; RFC 9113 §6.9.1 names FLOW_CONTROL_ERROR for a window pushed past 2^31-1", ce.Code)
			_ = cli.Close()
		})
	}
}

// TestHandshakeSettings_ReportsConnectionWindowUpdate is the white-box half of the
// test above: it names the mechanism rather than the symptom, so a future change
// that keeps uploads working by some other route does not quietly stop carrying
// the increment out of the handshake.
//
// Only stream 0 is reported. The client opens no streams until the handshake has
// returned, so a stream-level WINDOW_UPDATE here names an idle stream and is the
// peer's problem, not credit this connection can hold.
func TestHandshakeSettings_ReportsConnectionWindowUpdate(t *testing.T) {
	const incr = 4096
	cli, srv := net.Pipe()
	go nginxLikePeer(t, srv, incr)
	defer cli.Close()
	fr := frame.NewFramer(cli, cli)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, gotIncr, err := handshakeSettings(ctx, fr, func() error { return nil }, AdvertisedSettings{}.defaulted(), false)

	require.NoError(t, err, "handshake")
	assert.EqualValuesf(t, incr, gotIncr,
		"handshakeSettings reported %d of the %d bytes of connection credit the peer sent "+
			"before its SETTINGS ACK; the rest is spent, and no peer resends it", gotIncr, incr)
}
