package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9000_Sec511_ActiveCIDLimit checks that the server may not
// keep more than active_connection_id_limit (2) destination connection IDs active
// (RFC 9000 §5.1.1): the excess is a CONNECTION_ID_LIMIT_ERROR.
func TestConformance_RFC9000_Sec511_ActiveCIDLimit(t *testing.T) {
	c := &Conn{dcid: []byte("origcid0")}
	h := &connFrameHandler{c: c}

	first := h.OnNewConnectionID(1, 0, []byte("newcid01"), &[16]byte{}) // seq0+seq1 = 2, ok
	third := h.OnNewConnectionID(2, 0, []byte("newcid02"), &[16]byte{})

	require.NoErrorf(t, first, "first NEW_CONNECTION_ID: %v", first)
	assert.ErrorIsf(t, third, ErrConnectionIDLimit,
		"a third active CID = %v, want ErrConnectionIDLimit", third)
}

// TestConformance_RFC9000_Sec512_RetirePriorToSwitchesCID checks that an increased
// Retire Prior To retires lower-sequence connection IDs, switches the one in use
// if it was retired, and queues a RETIRE_CONNECTION_ID (RFC 9000 §5.1.2).
func TestConformance_RFC9000_Sec512_RetirePriorToSwitchesCID(t *testing.T) {
	c := &Conn{dcid: []byte("origcid0")}
	h := &connFrameHandler{c: c}

	err := h.OnNewConnectionID(1, 1, []byte("newcid01"), &[16]byte{}) // retire_prior_to 1 retires seq 0

	require.NoErrorf(t, err, "NEW_CONNECTION_ID: %v", err)
	assert.EqualValuesf(t, 1, c.curCIDSeq,
		"dcid = %q seq %d, want newcid01 seq 1 (switched after the current CID was retired)",
		c.dcid, c.curCIDSeq)
	assert.Equalf(t, "newcid01", string(c.dcid),
		"dcid = %q seq %d, want newcid01 seq 1 (switched after the current CID was retired)",
		c.dcid, c.curCIDSeq)
	assert.NotEmpty(t, c.retransQueue[spaceApp],
		"a RETIRE_CONNECTION_ID should be queued for the retired seq 0")
	assert.Lenf(t, c.serverCIDs, 1, "active CIDs = %d, want 1 (only seq 1 remains)", len(c.serverCIDs))

	// A NEW_CONNECTION_ID below the retire boundary is retired (a RETIRE_CONNECTION_ID
	// is sent, RFC 9000 §5.1.2) and not stored. A conformant server can deliver such a
	// frame by reordering, so it must be retired even below the boundary; a peer that
	// floods them is bounded by maxPendingRetires (see TestConn_NewConnectionID_FloodBounded).
	before := len(c.retransQueue[spaceApp])

	err = h.OnNewConnectionID(0, 0, []byte("origcid0"), &[16]byte{})

	require.NoErrorf(t, err, "NEW_CONNECTION_ID for a below-boundary seq: %v", err)
	_, stored := c.serverCIDs[0]
	assert.False(t, stored, "a NEW_CONNECTION_ID below the retire boundary must not be stored")
	assert.Equal(t, before+1, len(c.retransQueue[spaceApp]),
		"a below-boundary NEW_CONNECTION_ID must queue a RETIRE_CONNECTION_ID (§5.1.2)")
}

// TestConn_NewConnectionID_DuplicateSeqConflict checks that reusing a sequence
// number for a different connection ID is a PROTOCOL_VIOLATION, while an
// identical retransmission is accepted (RFC 9000 §19.15).
func TestConn_NewConnectionID_DuplicateSeqConflict(t *testing.T) {
	c := &Conn{dcid: []byte("origcid0")}
	h := &connFrameHandler{c: c}
	require.NoError(t, h.OnNewConnectionID(1, 0, []byte("newcid01"), &[16]byte{}))

	conflict := h.OnNewConnectionID(1, 0, []byte("different"), &[16]byte{})
	retransmit := h.OnNewConnectionID(1, 0, []byte("newcid01"), &[16]byte{})

	assert.ErrorIsf(t, conflict, ErrProtocolViolation,
		"sequence conflict = %v, want ErrProtocolViolation", conflict)
	assert.NoErrorf(t, retransmit, "identical retransmit = %v, want nil", retransmit)
}

// TestConn_NewConnectionID_FloodBounded checks that a peer advancing Retire Prior
// To in lockstep with the sequence number — keeping the active set within the
// limit while forcing one retirement per frame — cannot grow the retransmit queue
// without bound: the connection is closed with CONNECTION_ID_LIMIT_ERROR once the
// queued RETIRE frames exceed the cap (RFC 9000 §5.1.2).
func TestConn_NewConnectionID_FloodBounded(t *testing.T) {
	c := &Conn{dcid: []byte("origcid0")}
	h := &connFrameHandler{c: c}
	closed := false

	for i := uint64(1); i < 100000; i++ {
		cid := []byte{byte(i), byte(i >> 8), byte(i >> 16), 0xcc}
		err := h.OnNewConnectionID(i, i-1, cid, &[16]byte{}) // rpt = i-1 retires the prior CID
		if err == ErrConnectionIDLimit {
			closed = true
			break
		}
		require.NoErrorf(t, err, "frame %d: unexpected %v", i, err)
	}

	require.True(t, closed, "a NEW_CONNECTION_ID flood must be refused with ErrConnectionIDLimit")
	assert.LessOrEqualf(t, len(c.retransQueue[spaceApp]), maxPendingRetires+1,
		"retransQueue = %d, want bounded near %d", len(c.retransQueue[spaceApp]), maxPendingRetires)
	assert.LessOrEqualf(t, len(c.serverCIDs), int(activeCIDLimit),
		"active CIDs = %d, want <= %d (retirement kept the set small)", len(c.serverCIDs), activeCIDLimit)
	code, ok := closeCodeFor(ErrConnectionIDLimit)
	require.Truef(t, ok, "closeCodeFor = %#x,%v, want CONNECTION_ID_LIMIT_ERROR", code, ok)
	assert.Equalf(t, ErrCodeConnectionIDLimitError, code,
		"closeCodeFor = %#x,%v, want CONNECTION_ID_LIMIT_ERROR", code, ok)
}

// TestConn_NewConnectionID_ReorderedBelowBoundaryRetired: a conformant server
// issues sequence numbers in order, but reordering can deliver a lower sequence
// after a higher one already advanced Retire Prior To past it. That connection ID
// was never stored, so the boundary sweep never retired it; it must still get its
// own RETIRE_CONNECTION_ID (RFC 9000 §5.1.2).
func TestConn_NewConnectionID_ReorderedBelowBoundaryRetired(t *testing.T) {
	c := &Conn{dcid: []byte("origcid0")}
	h := &connFrameHandler{c: c}
	// seq 5 with retire_prior_to 5 arrives first (reordered ahead of seq 3).
	require.NoError(t, h.OnNewConnectionID(5, 5, []byte("cid00005"), &[16]byte{}))
	before := len(c.retransQueue[spaceApp])

	// seq 3 arrives late, below the boundary, and was never stored.
	err := h.OnNewConnectionID(3, 0, []byte("cid00003"), &[16]byte{})

	require.NoError(t, err)
	_, stored := c.serverCIDs[3]
	assert.False(t, stored, "a below-boundary seq must not be stored")
	assert.Equal(t, before+1, len(c.retransQueue[spaceApp]),
		"a genuinely-new below-boundary connection ID must be retired (§5.1.2)")
}

// TestConformance_RFC9000_Sec1916_RetireConnectionID checks that any
// RETIRE_CONNECTION_ID is a PROTOCOL_VIOLATION: this client provides a
// zero-length source connection ID, so it has issued nothing to retire
// (RFC 9000 §19.16).
func TestConformance_RFC9000_Sec1916_RetireConnectionID(t *testing.T) {
	h := &connFrameHandler{c: &Conn{}}

	for _, seq := range []uint64{0, 1, 5} {
		err := h.OnRetireConnectionID(seq)

		assert.ErrorIsf(t, err, ErrProtocolViolation,
			"RETIRE_CONNECTION_ID(%d) = %v, want ErrProtocolViolation", seq, err)
	}
}
