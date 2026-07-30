package quic

import "crypto/rand"

// activeCIDLimit is the active_connection_id_limit the client honors: it never
// advertises the parameter, so the RFC 9000 §18.2 default of 2 applies — the
// server may keep at most this many of the client's destination connection IDs
// active at once.
const activeCIDLimit uint64 = 2

// OnNewConnectionID records a destination connection ID the server issued
// (RFC 9000 §5.1.1, §19.15). It retires connection IDs below the frame's Retire
// Prior To (switching the one in use if it is retired), rejects a reused
// sequence number with a different ID (PROTOCOL_VIOLATION), and closes with
// CONNECTION_ID_LIMIT_ERROR if more than active_connection_id_limit IDs would be
// active. The stateless reset token is retained so a stateless reset ending in it
// can be recognized (RFC 9000 §10.3).
func (h *connFrameHandler) OnNewConnectionID(seq, retirePriorTo uint64, cid []byte, token *[16]byte) error {
	h.ackEliciting = true
	c := h.c
	if c.serverCIDs == nil {
		// Seed sequence 0 with the server SCID already in use (adopted at handshake).
		c.serverCIDs = map[uint64][]byte{0: append([]byte(nil), c.dcid...)}
	}
	// A connection ID at or below the retire boundary is retired at once and not
	// stored (RFC 9000 §5.1.2): send a RETIRE_CONNECTION_ID for it. This is not
	// always a duplicate — a conformant server issuing sequence numbers in order
	// can still deliver one below the boundary by reordering (a higher sequence
	// with a higher Retire Prior To overtakes it), and that connection ID was never
	// stored and so never retired by the loop below. A peer that instead floods
	// below-boundary frames only queues redundant RETIREs, which maxPendingRetires
	// bounds — the old permanent per-sequence dedup map grew without bound.
	if seq < c.retirePriorTo {
		c.queueRetire(seq)
		if c.pendingRetires > maxPendingRetires {
			return ErrConnectionIDLimit // a below-boundary flood, bounded (§5.1.2)
		}
		return nil
	}
	if existing, ok := c.serverCIDs[seq]; ok {
		if !bytesEqual(existing, cid) {
			return ErrProtocolViolation // same sequence, different connection ID (§19.15)
		}
		return nil // a duplicate retransmission
	}
	c.serverCIDs[seq] = append([]byte(nil), cid...)
	if token != nil {
		// Arm the token only for a CID actually stored — never one retired on arrival
		// (the seq < retirePriorTo path above returns before here) (§10.3.1).
		c.registerResetToken(seq, *token)
	}

	// An increased Retire Prior To retires every lower-sequence connection ID
	// (§5.1.2). The frame guarantees retirePriorTo <= seq, so the ID just stored is
	// never itself retired here.
	if retirePriorTo > c.retirePriorTo {
		c.retirePriorTo = retirePriorTo
		for s := range c.serverCIDs {
			if s < retirePriorTo {
				c.queueRetire(s)
				delete(c.serverCIDs, s)
				delete(c.resetTokens, s) // a retired CID's token MUST NOT be checked (§10.3.1)
			}
		}
		if c.curCIDSeq < retirePriorTo {
			c.switchToActiveCID() // the ID in use was retired; adopt a remaining one
		}
	}

	// Enforce the active_connection_id_limit after retirement (§5.1.1).
	if uint64(len(c.serverCIDs)) > activeCIDLimit {
		return ErrConnectionIDLimit
	}
	// A peer can keep the active set at or below the limit while still forcing a
	// retirement per frame by advancing Retire Prior To in lockstep with the
	// sequence number. Bound the RETIRE_CONNECTION_ID frames that queues but the
	// connection has not yet sent: past the cap the peer is issuing connection IDs
	// faster than they can be retired, which §5.1.2 permits closing with
	// CONNECTION_ID_LIMIT_ERROR.
	if c.pendingRetires > maxPendingRetires {
		return ErrConnectionIDLimit
	}
	return nil
}

// OnRetireConnectionID handles a RETIRE_CONNECTION_ID from the server. This
// client provides a zero-length source connection ID, so it has issued nothing
// the server could retire: any RETIRE_CONNECTION_ID is a PROTOCOL_VIOLATION
// (RFC 9000 §19.16).
func (h *connFrameHandler) OnRetireConnectionID(uint64) error {
	return ErrProtocolViolation
}

// maxPendingRetires bounds RETIRE_CONNECTION_ID frames queued but not yet sent.
// A conformant server keeps at most active_connection_id_limit connection IDs
// active and retires them slowly, so pending retirements never approach this; a
// peer flooding NEW_CONNECTION_ID hits it and the connection is closed (§5.1.2).
const maxPendingRetires = 64

// queueRetire schedules a RETIRE_CONNECTION_ID for a connection ID the client no
// longer uses (RFC 9000 §19.16). It goes on the retransmit queue so a lost frame
// is resent (§13.3). Each sequence is retired exactly once — the caller retires a
// stored connection ID only as the boundary passes it, then deletes it — so no
// deduplication is needed here; pendingRetires tracks the queue depth so a flood
// is bounded (see maxPendingRetires).
func (c *Conn) queueRetire(seq uint64) {
	c.retransQueue[spaceApp] = append(c.retransQueue[spaceApp], retransFrame{kind: retransRetire, offset: seq})
	c.pendingRetires++
}

// switchToActiveCID moves the connection to a remaining active connection ID
// after the one in use has been retired, choosing the lowest remaining sequence
// deterministically. A NEW_CONNECTION_ID's sequence is at least its Retire Prior
// To, so at least the just-stored ID always remains.
// redrawSpin draws a fresh latency-spin bit for the current connection ID (RFC 9000
// §17.4). crypto/rand.Read cannot fail on the supported Go versions; on the
// impossible error the bit simply keeps its previous value, which is no worse than
// the constant this replaced. Assumes c.mu is held (or that the conn is not yet
// published, as in NewConn).
func (c *Conn) redrawSpin() {
	var b [1]byte
	if _, err := rand.Read(b[:]); err != nil {
		return
	}
	c.spin = b[0]&1 != 0
}

func (c *Conn) switchToActiveCID() {
	best, found := uint64(0), false
	for s := range c.serverCIDs {
		if !found || s < best {
			best, found = s, true
		}
	}
	if found {
		c.dcid = append(c.dcid[:0], c.serverCIDs[best]...)
		c.curCIDSeq = best
		// RFC 9000 §17.4 asks for a spin bit "chosen independently for each packet or
		// chosen independently for each connection ID". Keeping the NewConn draw across
		// a rotation would hand a passive observer one bit linking the old connection
		// ID to the new one — exactly the boundary the per-CID wording exists to break.
		c.redrawSpin()
	}
}
