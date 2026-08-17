package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pathRespConn builds a 1-RTT-ready connection whose writes are captured.
func pathRespConn() (*Conn, *capturePC) {
	dcid := []byte("pathtst0")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	pc := &capturePC{}
	c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer, handshakeComplete: true}
	c.keys.OneRTT, _ = NewOpener(keys)
	return c, pc
}

// TestConformance_RFC9000_Sec822_PathResponsePaddedTo1200 checks that the datagram
// carrying the client's PATH_RESPONSE (queued in answer to a received
// PATH_CHALLENGE) is expanded to at least 1200 bytes (RFC 9000 §8.2.2).
func TestConformance_RFC9000_Sec822_PathResponsePaddedTo1200(t *testing.T) {
	c, pc := pathRespConn()
	var data [8]byte
	copy(data[:], "challeng")
	require.NoError(t, (&connFrameHandler{c: c, space: spaceApp}).OnPathChallenge(&data),
		"queue a PATH_RESPONSE for the received challenge")

	require.NoError(t, c.flush(), "flush the queued PATH_RESPONSE")

	require.Lenf(t, pc.pkts, 1, "wrote %d datagrams, want 1", len(pc.pkts))
	assert.GreaterOrEqualf(t, len(pc.pkts[0]), InitialDatagramMinSize,
		"PATH_RESPONSE datagram = %d bytes, want >= %d (§8.2.2)", len(pc.pkts[0]), InitialDatagramMinSize)
	assert.False(t, c.pathRespPending,
		"pathRespPending should be cleared once the PATH_RESPONSE is sent")
}

// TestConformance_RFC9000_Sec822_PlainControlNotPadded is the negative arm: an
// ordinary control-frame datagram (a MAX_DATA grant, no PATH_RESPONSE) is not
// expanded, so the padding is specific to path validation.
func TestConformance_RFC9000_Sec822_PlainControlNotPadded(t *testing.T) {
	c, pc := pathRespConn()
	c.pendingCtrl = AppendMaxData(nil, 1000)

	require.NoError(t, c.flush(), "flush a plain MAX_DATA grant")

	require.Lenf(t, pc.pkts, 1, "wrote %d datagrams, want 1", len(pc.pkts))
	assert.Lessf(t, len(pc.pkts[0]), InitialDatagramMinSize,
		"a plain MAX_DATA datagram = %d bytes, should not be padded to 1200", len(pc.pkts[0]))
}
