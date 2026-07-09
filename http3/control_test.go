package http3

import (
	"context"
	"testing"
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
	_, _, err := client.Do(context.Background(), &Request{Method: "GET", Scheme: "https", Authority: "e", Path: "/"})
	if err != ErrGoAway {
		t.Fatalf("Do after GOAWAY(0) on stream 0 = %v, want ErrGoAway", err)
	}
	if !client.haveGoaway || client.goawayID != 0 {
		t.Fatalf("goaway state: have=%v id=%d", client.haveGoaway, client.goawayID)
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
