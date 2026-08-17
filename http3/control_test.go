package http3

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serverControl builds the bytes of a server control stream: the control
// stream-type (0x00) then a SETTINGS frame, optionally followed by extra frames.
func serverControl(settings []Setting, extra ...byte) []byte {
	b := AppendClientControlStream(nil, settings) // type 0x00 + SETTINGS
	return append(b, extra...)
}

// TestConformance_RFC9114_Sec621_ReadsServerSettings checks that the client
// accepts the server control stream and reads its mandatory first SETTINGS frame.
func TestConformance_RFC9114_Sec621_ReadsServerSettings(t *testing.T) {
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl([]Setting{{SettingMaxFieldSectionSize, 16384}})}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	require.NoErrorf(t, serr, "serviceControl: %v", serr)
	assert.Truef(t, client.control != nil, "server control stream should be identified")
	assert.True(t, client.settingsRead, "SETTINGS should have been read")
	// The parsed SETTINGS_MAX_FIELD_SECTION_SIZE must reach the field the request
	// path reads (RFC 9114 §4.2.2); this pins the whole parse→store wiring.
	assert.Equalf(t, uint64(16384), client.maxFieldSection.Load(),
		"maxFieldSection = %d, want 16384 (read from the server SETTINGS)", client.maxFieldSection.Load())
}

// TestConformance_RFC9114_Sec621_MissingSettings checks that a control stream
// whose first frame is not SETTINGS is a H3_MISSING_SETTINGS connection error.
func TestConformance_RFC9114_Sec621_MissingSettings(t *testing.T) {
	bad := append([]byte{0x00}, AppendGoaway(nil, 0)...) // control type, then GOAWAY first
	server := &fakeStream{id: 3, recvChunks: [][]byte{bad}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.True(t, conn.closed, "connection not closed when SETTINGS was not the first control frame")
	assert.Equalf(t, H3MissingSettings, conn.closeCode,
		"close code = %#x (closed=%v), want H3_MISSING_SETTINGS", conn.closeCode, conn.closed)
}

// TestConformance_RFC9114_Sec52_GoAwayGatesRequests checks that after GOAWAY the
// client refuses a request on a stream the server will not process.
func TestConformance_RFC9114_Sec52_GoAwayGatesRequests(t *testing.T) {
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil, AppendGoaway(nil, 0)...)}}
	conn := &fakeConn{req: &fakeStream{id: 0}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	// The reader owns control servicing, but with an empty request stream its Poll
	// parks before reaching serviceControl; drive it here so the GOAWAY is processed
	// before Do checks the gate (RFC 9114 §5.2).
	require.NoError(t, client.serviceControl(), "serviceControl over a control stream carrying GOAWAY(0)")
	// Bounded: an ungated Do parks on the fake stream waiting for a response that
	// never comes, so without a deadline a regression here hangs the package to its
	// timeout instead of failing.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, doErr := client.Do(ctx, &Request{Method: "GET", Scheme: "https", Authority: "e", Path: "/"})

	assert.Equalf(t, ErrGoAway, doErr,
		"Do after GOAWAY(0) on stream 0 = %v, want ErrGoAway", doErr)
	assert.Equalf(t, uint64(0), client.goaway.Load(),
		"goaway state: id=%d, want 0", client.goaway.Load())
}

// TestConformance_RFC9114_Sec724_SettingsSendSideValidated pins the two send-side
// SETTINGS MUST NOTs on the exported NewClient path, whose caller supplies the
// slice: the same identifier twice (§7.2.4) and a reserved HTTP/2-carryover
// identifier 0x02–0x05 (§7.2.4.1). The MUST NOT names the HTTP/2 carryover range,
// which starts at 0x01, so 0x00 falls outside it and stays the caller's call.
func TestConformance_RFC9114_Sec724_SettingsSendSideValidated(t *testing.T) {
	cases := []struct {
		name string
		in   []Setting
		want error
	}{
		{"duplicate", []Setting{{SettingQPACKBlockedStreams, 16}, {SettingQPACKBlockedStreams, 8}}, ErrH3Settings},
		{"reserved_h2", []Setting{{0x03, 1}}, ErrH3Settings},
		{"zero_outside_h2_carryover_range", []Setting{{0x00, 1}}, nil},
		{"defaults_ok", defaultSettings, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn := &fakeConn{req: &fakeStream{}}

			_, err := NewClientFake(conn, c.in)

			assert.ErrorIsf(t, err, c.want,
				"NewClient(%v) err = %v, want %v — a settings slice a conformant peer "+
					"would answer with H3_SETTINGS_ERROR must be refused before it reaches the wire",
				c.in, err, c.want)
		})
	}
}

// TestConformance_RFC9114_Sec7241_GreaseSettingSent pins the §7.2.4.1 SHOULD that
// an endpoint include at least one reserved identifier of the form 0x1f*N + 0x21 in
// its own SETTINGS, and the matching receive-side rule that one arriving from the
// server is ignored rather than rejected.
func TestConformance_RFC9114_Sec7241_GreaseSettingSent(t *testing.T) {
	raw := AppendSettings(nil, []Setting{{greaseSettingID, 7}, {SettingQPACKBlockedStreams, 4}})

	var grease int
	for _, s := range defaultSettings {
		if s.ID >= 0x21 && (s.ID-0x21)%0x1f == 0 {
			grease++
		}
	}
	_, _, hn, herr := ParseFrameHeader(raw)
	require.NoErrorf(t, herr, "ParseFrameHeader: %v", herr)
	got, perr := ParseSettings(raw[hn:])

	assert.NotZerof(t, grease,
		"defaultSettings = %v, want at least one reserved 0x1f*N+0x21 identifier", defaultSettings)
	// Receive side: the server's own grease identifier must parse and be ignored.
	require.NoErrorf(t, perr, "ParseSettings with a grease id = %v, %v; want both settings, nil", got, perr)
	assert.Lenf(t, got, 2,
		"ParseSettings with a grease id = %v; a reserved identifier must be carried "+
			"through, not rejected (§9)", got)
}

// TestConformance_RFC9114_Sec52_GoAwayHighIDGatesRequests pins the §5.2 MUST NOT
// against the graceful shutdown the RFC itself describes: a server opens with
// GOAWAY carrying the maximum request id (2^62-4) and only later lowers it. Every
// real stream id is below that, so gating on "id >= goaway" alone lets the client
// keep opening requests indefinitely. "Endpoints MUST NOT initiate new requests
// ... after receipt of a GOAWAY frame from the peer" is unconditional.
func TestConformance_RFC9114_Sec52_GoAwayHighIDGatesRequests(t *testing.T) {
	const maxRequestID = uint64(1)<<62 - 4
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil, AppendGoaway(nil, maxRequestID)...)}}
	conn := &fakeConn{req: &fakeStream{id: 0}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")
	require.NoError(t, client.serviceControl(), "serviceControl over a control stream carrying GOAWAY(2^62-4)")
	// Bounded: an ungated Do parks on the fake stream waiting for a response.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, doErr := client.Do(ctx, &Request{Method: "GET", Scheme: "https", Authority: "e", Path: "/"})

	assert.Equalf(t, ErrGoAway, doErr,
		"Do after GOAWAY(2^62-4) = %v, want ErrGoAway — the gate is unconditional, "+
			"not a comparison against the published id", doErr)
}

// TestConformance_RFC9114_Sec728_ForbiddenControlFrame checks that a frame that
// may not appear on the control stream (here HEADERS) is H3_FRAME_UNEXPECTED.
func TestConformance_RFC9114_Sec728_ForbiddenControlFrame(t *testing.T) {
	bad := serverControl(nil, AppendHeaders(nil, []byte{0x00, 0x00})...) // HEADERS after SETTINGS
	server := &fakeStream{id: 3, recvChunks: [][]byte{bad}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.Equalf(t, H3FrameUnexpected, conn.closeCode,
		"close code = %#x, want H3_FRAME_UNEXPECTED — a request-stream frame on the "+
			"control stream must not be ignored the way an unknown type is", conn.closeCode)
}

// TestConformance_RFC9114_Sec52_GoAwayMustNotIncrease checks that a GOAWAY with a
// larger identifier than a previous one is a H3_ID_ERROR connection error.
func TestConformance_RFC9114_Sec52_GoAwayMustNotIncrease(t *testing.T) {
	ctrl := serverControl(nil, AppendGoaway(nil, 4)...)
	ctrl = append(ctrl, AppendGoaway(nil, 8)...) // increasing → forbidden
	server := &fakeStream{id: 3, recvChunks: [][]byte{ctrl}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.Equalf(t, H3IDError, conn.closeCode,
		"close code = %#x, want H3_ID_ERROR — a rising drain boundary would let the "+
			"server take back streams it already disowned", conn.closeCode)
}

// TestConformance_RFC9114_Sec726_GoAwayNonRequestStreamID checks that a GOAWAY
// carrying a stream id that is not a client-initiated bidirectional (request)
// stream is a H3_ID_ERROR connection error (RFC 9114 §7.2.6).
func TestConformance_RFC9114_Sec726_GoAwayNonRequestStreamID(t *testing.T) {
	for _, badID := range []uint64{1, 2, 3, 5, 7} { // server-bidi, client-uni, server-uni, …
		ctrl := serverControl(nil, AppendGoaway(nil, badID)...)
		server := &fakeStream{id: 3, recvChunks: [][]byte{ctrl}}
		conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
		client, err := NewClientFake(conn, nil)
		require.NoErrorf(t, err, "GOAWAY(%d): NewClientFake over the fake transport", badID)

		serr := client.serviceControl()

		assert.ErrorIsf(t, serr, ErrH3Control,
			"GOAWAY(%d): serviceControl = %v, want ErrH3Control", badID, serr)
		assert.Equalf(t, H3IDError, conn.closeCode,
			"GOAWAY(%d): close code = %#x, want H3_ID_ERROR", badID, conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec723_CancelPushRejected checks that a CANCEL_PUSH on
// the control stream — the client never sent MAX_PUSH_ID, so any push ID is out
// of range — is a H3_ID_ERROR connection error (RFC 9114 §7.2.3).
func TestConformance_RFC9114_Sec723_CancelPushRejected(t *testing.T) {
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil, AppendCancelPush(nil, 0)...)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.Equalf(t, H3IDError, conn.closeCode,
		"close code = %#x, want H3_ID_ERROR — with no MAX_PUSH_ID sent, every push "+
			"id is out of range", conn.closeCode)
}

// TestConformance_RFC9114_Sec71_CancelPushMalformed checks that a CANCEL_PUSH with
// no push-ID varint (empty payload) is a H3_FRAME_ERROR connection error (§7.1).
func TestConformance_RFC9114_Sec71_CancelPushMalformed(t *testing.T) {
	bad := AppendFrameHeader(nil, FrameCancelPush, 0) // empty payload — no push ID
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil, bad...)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.Equalf(t, H3FrameError, conn.closeCode,
		"close code = %#x, want H3_FRAME_ERROR — a malformed payload is a framing "+
			"fault, distinct from the out-of-range push id H3_ID_ERROR reports", conn.closeCode)
}

// TestConformance_RFC9114_Sec625_PushStreamRejected checks that a server push
// stream (we sent no MAX_PUSH_ID) is a H3_ID_ERROR connection error.
func TestConformance_RFC9114_Sec625_PushStreamRejected(t *testing.T) {
	push := &fakeStream{id: 3, recvChunks: [][]byte{{0x01}}} // stream type 0x01 = push
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{push}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.Equalf(t, H3IDError, conn.closeCode,
		"close code = %#x, want H3_ID_ERROR", conn.closeCode)
}

// TestClient_ControlFrameTooLarge checks that an oversized control frame is a
// H3_EXCESSIVE_LOAD connection error rather than buffered unboundedly.
func TestClient_ControlFrameTooLarge(t *testing.T) {
	huge := AppendFrameHeader(nil, 0x21, maxControlFrameLen+1) // GREASE type, oversized length
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil, huge...)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.Equalf(t, H3ExcessiveLoad, conn.closeCode,
		"close code = %#x, want H3_EXCESSIVE_LOAD — a declared length the client "+
			"refuses to buffer must be refused, not treated as need-more", conn.closeCode)
}

// TestConformance_RFC9114_Sec621_ControlStreamClosed checks that the server
// closing its control stream (FIN after a valid SETTINGS) is a
// H3_CLOSED_CRITICAL_STREAM connection error.
func TestConformance_RFC9114_Sec621_ControlStreamClosed(t *testing.T) {
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl([]Setting{{SettingMaxFieldSectionSize, 16384}})}, fin: true}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.Equalf(t, H3ClosedCriticalStream, conn.closeCode,
		"close code = %#x, want H3_CLOSED_CRITICAL_STREAM", conn.closeCode)
}

// TestConformance_RFC9204_Sec42_QPACKStreamClosed checks that the server closing
// its QPACK encoder stream is a H3_CLOSED_CRITICAL_STREAM connection error, even
// though the client processes no instructions on it.
func TestConformance_RFC9204_Sec42_QPACKStreamClosed(t *testing.T) {
	enc := &fakeStream{id: 7, recvChunks: [][]byte{appendV(nil, StreamTypeQPACKEncoder)}, fin: true}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{enc}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.Equalf(t, H3ClosedCriticalStream, conn.closeCode,
		"close code = %#x, want H3_CLOSED_CRITICAL_STREAM — the QPACK streams are "+
			"critical even when this client reads nothing off them", conn.closeCode)
}

// TestConformance_RFC9204_Sec42_DuplicateQPACKStream checks that a second QPACK
// encoder stream is a H3_STREAM_CREATION_ERROR connection error.
func TestConformance_RFC9204_Sec42_DuplicateQPACKStream(t *testing.T) {
	s1 := &fakeStream{id: 7, recvChunks: [][]byte{appendV(nil, StreamTypeQPACKEncoder)}}
	s2 := &fakeStream{id: 11, recvChunks: [][]byte{appendV(nil, StreamTypeQPACKEncoder)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{s1, s2}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.Equalf(t, H3StreamCreationError, conn.closeCode,
		"close code = %#x, want H3_STREAM_CREATION_ERROR", conn.closeCode)
}

// TestClient_UniStream_PartialType checks that a stream whose leading type varint
// is not yet buffered stays pending and is typed on a later service call.
func TestClient_UniStream_PartialType(t *testing.T) {
	server := &fakeStream{id: 3, recvChunks: [][]byte{nil, serverControl(nil)}} // nothing, then the stream
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	firstErr := client.serviceControl()
	controlAfterFirst, pendingAfterFirst := client.control, len(client.pendingUni)
	secondErr := client.serviceControl()

	require.NoError(t, firstErr, "servicing a uni stream whose type varint has not arrived")
	assert.Truef(t, controlAfterFirst == nil,
		"stream should stay pending until its type arrives (control=%v pending=%d)",
		controlAfterFirst, pendingAfterFirst)
	assert.Equalf(t, 1, pendingAfterFirst,
		"stream should stay pending until its type arrives (control=%v pending=%d)",
		controlAfterFirst, pendingAfterFirst)
	require.NoError(t, secondErr, "servicing the same uni stream once its type has arrived")
	assert.True(t, client.settingsRead, "SETTINGS should be read once the type + frame arrive")
}
