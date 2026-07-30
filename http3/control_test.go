package http3

import (
	"context"
	"errors"
	"testing"
	"time"
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
	if err != nil {
		t.Fatal(err)
	}
	if err := client.serviceControl(); err != nil {
		t.Fatalf("serviceControl: %v", err)
	}
	if client.control == nil {
		t.Fatal("server control stream should be identified")
	}
	if !client.settingsRead {
		t.Fatal("SETTINGS should have been read")
	}
	// The parsed SETTINGS_MAX_FIELD_SECTION_SIZE must reach the field the request
	// path reads (RFC 9114 §4.2.2); this pins the whole parse→store wiring.
	if client.maxFieldSection.Load() != 16384 {
		t.Fatalf("maxFieldSection = %d, want 16384 (read from the server SETTINGS)", client.maxFieldSection.Load())
	}
}

// TestConformance_RFC9114_Sec621_MissingSettings checks that a control stream
// whose first frame is not SETTINGS is a H3_MISSING_SETTINGS connection error.
func TestConformance_RFC9114_Sec621_MissingSettings(t *testing.T) {
	bad := append([]byte{0x00}, AppendGoaway(nil, 0)...) // control type, then GOAWAY first
	server := &fakeStream{id: 3, recvChunks: [][]byte{bad}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if !conn.closed || conn.closeCode != H3MissingSettings {
		t.Fatalf("close code = %#x (closed=%v), want H3_MISSING_SETTINGS", conn.closeCode, conn.closed)
	}
}

// TestConformance_RFC9114_Sec52_GoAwayGatesRequests checks that after GOAWAY the
// client refuses a request on a stream the server will not process.
func TestConformance_RFC9114_Sec52_GoAwayGatesRequests(t *testing.T) {
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil, AppendGoaway(nil, 0)...)}}
	conn := &fakeConn{req: &fakeStream{id: 0}, acceptQ: []quicStream{server}}
	client, _ := NewClientFake(conn, nil)
	// The reader owns control servicing, but with an empty request stream its Poll
	// parks before reaching serviceControl; drive it here so the GOAWAY is processed
	// before Do checks the gate (RFC 9114 §5.2).
	if err := client.serviceControl(); err != nil {
		t.Fatalf("serviceControl: %v", err)
	}
	_, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "e", Path: "/"})
	if err != ErrGoAway {
		t.Fatalf("Do after GOAWAY(0) on stream 0 = %v, want ErrGoAway", err)
	}
	if client.goaway.Load() != 0 {
		t.Fatalf("goaway state: id=%d, want 0", client.goaway.Load())
	}
}

// TestConformance_RFC9114_Sec724_SettingsSendSideValidated pins the two send-side
// SETTINGS MUST NOTs on the exported NewClient path, whose caller supplies the
// slice: the same identifier twice (§7.2.4) and a reserved HTTP/2-carryover
// identifier 0x02–0x05 (§7.2.4.1). Identifier 0x00 is unassigned, not reserved, so
// it must still be accepted.
func TestConformance_RFC9114_Sec724_SettingsSendSideValidated(t *testing.T) {
	cases := []struct {
		name string
		in   []Setting
		want error
	}{
		{"duplicate", []Setting{{SettingQPACKBlockedStreams, 16}, {SettingQPACKBlockedStreams, 8}}, ErrH3Settings},
		{"reserved_h2", []Setting{{0x03, 1}}, ErrH3Settings},
		{"unassigned_zero_ok", []Setting{{0x00, 1}}, nil},
		{"defaults_ok", defaultSettings, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn := &fakeConn{req: &fakeStream{}}
			_, err := NewClientFake(conn, c.in)
			if !errors.Is(err, c.want) {
				t.Fatalf("NewClient(%v) err = %v, want %v", c.in, err, c.want)
			}
		})
	}
}

// TestConformance_RFC9114_Sec7241_GreaseSettingSent pins the §7.2.4.1 SHOULD that
// an endpoint include at least one reserved identifier of the form 0x1f*N + 0x21 in
// its own SETTINGS, and the matching receive-side rule that one arriving from the
// server is ignored rather than rejected.
func TestConformance_RFC9114_Sec7241_GreaseSettingSent(t *testing.T) {
	var grease int
	for _, s := range defaultSettings {
		if s.ID >= 0x21 && (s.ID-0x21)%0x1f == 0 {
			grease++
		}
	}
	if grease == 0 {
		t.Fatalf("defaultSettings = %v, want at least one reserved 0x1f*N+0x21 identifier", defaultSettings)
	}
	// Receive side: the server's own grease identifier must parse and be ignored.
	raw := AppendSettings(nil, []Setting{{greaseSettingID, 7}, {SettingQPACKBlockedStreams, 4}})
	_, _, hn, err := ParseFrameHeader(raw)
	if err != nil {
		t.Fatalf("ParseFrameHeader: %v", err)
	}
	got, err := ParseSettings(raw[hn:])
	if err != nil || len(got) != 2 {
		t.Fatalf("ParseSettings with a grease id = %v, %v; want both settings, nil", got, err)
	}
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
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != nil {
		t.Fatalf("serviceControl: %v", err)
	}
	// Bounded: an ungated Do parks on the fake stream waiting for a response.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := client.Do(ctx, &Request{Method: "GET", Scheme: "https", Authority: "e", Path: "/"})
	if err != ErrGoAway {
		t.Fatalf("Do after GOAWAY(2^62-4) = %v, want ErrGoAway", err)
	}
}

// TestConformance_RFC9114_Sec728_ForbiddenControlFrame checks that a frame that
// may not appear on the control stream (here HEADERS) is H3_FRAME_UNEXPECTED.
func TestConformance_RFC9114_Sec728_ForbiddenControlFrame(t *testing.T) {
	bad := serverControl(nil, AppendHeaders(nil, []byte{0x00, 0x00})...) // HEADERS after SETTINGS
	server := &fakeStream{id: 3, recvChunks: [][]byte{bad}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3FrameUnexpected {
		t.Fatalf("close code = %#x, want H3_FRAME_UNEXPECTED", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec52_GoAwayMustNotIncrease checks that a GOAWAY with a
// larger identifier than a previous one is a H3_ID_ERROR connection error.
func TestConformance_RFC9114_Sec52_GoAwayMustNotIncrease(t *testing.T) {
	ctrl := serverControl(nil, AppendGoaway(nil, 4)...)
	ctrl = append(ctrl, AppendGoaway(nil, 8)...) // increasing → forbidden
	server := &fakeStream{id: 3, recvChunks: [][]byte{ctrl}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3IDError {
		t.Fatalf("close code = %#x, want H3_ID_ERROR", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec726_GoAwayNonRequestStreamID checks that a GOAWAY
// carrying a stream id that is not a client-initiated bidirectional (request)
// stream is a H3_ID_ERROR connection error (RFC 9114 §7.2.6).
func TestConformance_RFC9114_Sec726_GoAwayNonRequestStreamID(t *testing.T) {
	for _, badID := range []uint64{1, 2, 3, 5, 7} { // server-bidi, client-uni, server-uni, …
		ctrl := serverControl(nil, AppendGoaway(nil, badID)...)
		server := &fakeStream{id: 3, recvChunks: [][]byte{ctrl}}
		conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
		client, _ := NewClientFake(conn, nil)
		if err := client.serviceControl(); err != ErrH3Control {
			t.Fatalf("GOAWAY(%d): serviceControl = %v, want ErrH3Control", badID, err)
		}
		if conn.closeCode != H3IDError {
			t.Fatalf("GOAWAY(%d): close code = %#x, want H3_ID_ERROR", badID, conn.closeCode)
		}
	}
}

// TestConformance_RFC9114_Sec723_CancelPushRejected checks that a CANCEL_PUSH on
// the control stream — the client never sent MAX_PUSH_ID, so any push ID is out
// of range — is a H3_ID_ERROR connection error (RFC 9114 §7.2.3).
func TestConformance_RFC9114_Sec723_CancelPushRejected(t *testing.T) {
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil, AppendCancelPush(nil, 0)...)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3IDError {
		t.Fatalf("close code = %#x, want H3_ID_ERROR", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec71_CancelPushMalformed checks that a CANCEL_PUSH with
// no push-ID varint (empty payload) is a H3_FRAME_ERROR connection error (§7.1).
func TestConformance_RFC9114_Sec71_CancelPushMalformed(t *testing.T) {
	bad := AppendFrameHeader(nil, FrameCancelPush, 0) // empty payload — no push ID
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil, bad...)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3FrameError {
		t.Fatalf("close code = %#x, want H3_FRAME_ERROR", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec625_PushStreamRejected checks that a server push
// stream (we sent no MAX_PUSH_ID) is a H3_ID_ERROR connection error.
func TestConformance_RFC9114_Sec625_PushStreamRejected(t *testing.T) {
	push := &fakeStream{id: 3, recvChunks: [][]byte{{0x01}}} // stream type 0x01 = push
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{push}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3IDError {
		t.Fatalf("close code = %#x, want H3_ID_ERROR", conn.closeCode)
	}
}

// TestClient_ControlFrameTooLarge checks that an oversized control frame is a
// H3_EXCESSIVE_LOAD connection error rather than buffered unboundedly.
func TestClient_ControlFrameTooLarge(t *testing.T) {
	huge := AppendFrameHeader(nil, 0x21, maxControlFrameLen+1) // GREASE type, oversized length
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil, huge...)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3ExcessiveLoad {
		t.Fatalf("close code = %#x, want H3_EXCESSIVE_LOAD", conn.closeCode)
	}
}

// TestConformance_RFC9114_Sec621_ControlStreamClosed checks that the server
// closing its control stream (FIN after a valid SETTINGS) is a
// H3_CLOSED_CRITICAL_STREAM connection error.
func TestConformance_RFC9114_Sec621_ControlStreamClosed(t *testing.T) {
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl([]Setting{{SettingMaxFieldSectionSize, 16384}})}, fin: true}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3ClosedCriticalStream {
		t.Fatalf("close code = %#x, want H3_CLOSED_CRITICAL_STREAM", conn.closeCode)
	}
}

// TestConformance_RFC9204_Sec42_QPACKStreamClosed checks that the server closing
// its QPACK encoder stream is a H3_CLOSED_CRITICAL_STREAM connection error, even
// though the client processes no instructions on it.
func TestConformance_RFC9204_Sec42_QPACKStreamClosed(t *testing.T) {
	enc := &fakeStream{id: 7, recvChunks: [][]byte{appendV(nil, StreamTypeQPACKEncoder)}, fin: true}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{enc}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3ClosedCriticalStream {
		t.Fatalf("close code = %#x, want H3_CLOSED_CRITICAL_STREAM", conn.closeCode)
	}
}

// TestConformance_RFC9204_Sec42_DuplicateQPACKStream checks that a second QPACK
// encoder stream is a H3_STREAM_CREATION_ERROR connection error.
func TestConformance_RFC9204_Sec42_DuplicateQPACKStream(t *testing.T) {
	s1 := &fakeStream{id: 7, recvChunks: [][]byte{appendV(nil, StreamTypeQPACKEncoder)}}
	s2 := &fakeStream{id: 11, recvChunks: [][]byte{appendV(nil, StreamTypeQPACKEncoder)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{s1, s2}}
	client, _ := NewClientFake(conn, nil)
	if err := client.serviceControl(); err != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control", err)
	}
	if conn.closeCode != H3StreamCreationError {
		t.Fatalf("close code = %#x, want H3_STREAM_CREATION_ERROR", conn.closeCode)
	}
}

// TestClient_UniStream_PartialType checks that a stream whose leading type varint
// is not yet buffered stays pending and is typed on a later service call.
func TestClient_UniStream_PartialType(t *testing.T) {
	server := &fakeStream{id: 3, recvChunks: [][]byte{nil, serverControl(nil)}} // nothing, then the stream
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, _ := NewClientFake(conn, nil)

	if err := client.serviceControl(); err != nil {
		t.Fatal(err)
	}
	if client.control != nil || len(client.pendingUni) != 1 {
		t.Fatalf("stream should stay pending until its type arrives (control=%v pending=%d)", client.control, len(client.pendingUni))
	}
	if err := client.serviceControl(); err != nil {
		t.Fatal(err)
	}
	if !client.settingsRead {
		t.Fatal("SETTINGS should be read once the type + frame arrive")
	}
}
