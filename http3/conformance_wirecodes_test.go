package http3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The two RFC-named connection error codes whose WIRE value no test observed.
//
// http3.connError puts the code on the wire via CloseWithError but returns the
// generic ErrH3Control for every one of them, so a test that only checks the
// returned error proves nothing about which code the peer received. Eleven of
// the thirteen codes are checked against conn.closeCode somewhere. These two
// were not: H3_SETTINGS_ERROR on the receive path, and
// QPACK_ENCODER_STREAM_ERROR. Both were reachable only through their generic
// error, so an implementation closing with any other code passed the suite.

// TestConformance_RFC9114_Sec724_DuplicateServerSettingIsSettingsError pins that
// a server SETTINGS frame carrying the same identifier twice closes the
// connection with H3_SETTINGS_ERROR.
//
// §7.2.4: "The same setting identifier MUST NOT occur more than once in the
// SETTINGS frame. A receiver MAY treat the presence of duplicate setting
// identifiers as a connection error of type H3_SETTINGS_ERROR."
//
// ParseSettings was tested directly for this, but the parser error is not the
// requirement — the code the peer reads is. The mapping between them
// (ErrH3Frame to H3_FRAME_ERROR, everything else to H3_SETTINGS_ERROR) had no
// test at all, so returning H3_FRAME_ERROR here, or closing with nothing, was
// invisible.
func TestConformance_RFC9114_Sec724_DuplicateServerSettingIsSettingsError(t *testing.T) {
	dup := AppendClientControlStream(nil, []Setting{
		{SettingMaxFieldSectionSize, 4096},
		{SettingMaxFieldSectionSize, 8192}, // same identifier again
	})
	server := &fakeStream{id: 3, recvChunks: [][]byte{dup}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.True(t, conn.closed, "connection not closed after a duplicate setting identifier")
	assert.Equalf(t, H3SettingsErrorCode, conn.closeCode,
		"close code = %#x, want H3_SETTINGS_ERROR (%#x);\n"+
			"the parser error is not the requirement — the code the peer reads is",
		conn.closeCode, H3SettingsErrorCode)
}

// TestConformance_RFC9204_Sec43_MalformedEncoderInstructionIsEncoderStreamError
// pins that a malformed QPACK encoder instruction closes the connection with
// QPACK_ENCODER_STREAM_ERROR.
//
// RFC 9204 §4.3 names that code for this stream. The instruction below is an
// Insert With Name Reference against the STATIC table at index 200, which does
// not exist — an error whatever the client's table capacity happens to be.
//
// The only previous coverage was a fuzz target asserting ErrH3Control, which
// every code path returns. The literally adjacent branch in the same function
// closes with H3_EXCESSIVE_LOAD, so confusing the two was a one-line slip that
// nothing would have caught.
func TestConformance_RFC9204_Sec43_MalformedEncoderInstructionIsEncoderStreamError(t *testing.T) {
	// 0xff: Insert With Name Reference (1...), T=1 static, 6-bit prefix all ones
	// so the index continues; 0x89 0x01 encodes the remainder, giving index 200.
	encoderStream := []byte{byte(StreamTypeQPACKEncoder), 0xff, 0x89, 0x01}
	server := &fakeStream{id: 3, recvChunks: [][]byte{serverControl(nil)}}
	encoder := &fakeStream{id: 7, recvChunks: [][]byte{encoderStream}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server, encoder}}
	client, err := NewClientFake(conn, nil)
	require.NoError(t, err, "NewClientFake over the fake transport")

	serr := client.serviceControl()

	assert.ErrorIsf(t, serr, ErrH3Control, "serviceControl = %v, want ErrH3Control", serr)
	assert.True(t, conn.closed, "connection not closed after a malformed encoder instruction")
	assert.Equalf(t, H3QpackEncoderStreamError, conn.closeCode,
		"close code = %#x, want QPACK_ENCODER_STREAM_ERROR (%#x);\n"+
			"H3_EXCESSIVE_LOAD is the branch next to this one — returning it here would "+
			"tell the peer its stream was too large rather than malformed",
		conn.closeCode, H3QpackEncoderStreamError)
}
