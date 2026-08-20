package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConformance_RFC9002_Sec7_RetransmitRespectsCwnd checks that a retransmission
// counts against the congestion window: flushRetransmits sends nothing while the
// window is full and drains the queue once there is room (RFC 9002 §7).
func TestConformance_RFC9002_Sec7_RetransmitRespectsCwnd(t *testing.T) {
	dcid := []byte("retrcwnd")
	keys, _ := InitialKeys(dcid)
	sealer, _ := NewSealer(keys)
	newConn := func() (*Conn, *capturePC) {
		pc := &capturePC{}
		c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer, cwnd: 12000, ssthresh: ^uint64(0)}
		c.keys.OneRTT, _ = NewOpener(keys)
		c.retransQueue[spaceApp] = []retransFrame{{kind: retransStream, streamID: 0, offset: 0, data: []byte("xxxx")}}
		return c, pc
	}

	t.Run("window_full_sends_nothing", func(t *testing.T) {
		c, pc := newConn()
		c.bytesInFlight = 12000

		err := c.flushRetransmits(spaceApp)

		require.NoError(t, err, "flushRetransmits with a full window")
		assert.Emptyf(t, pc.pkts, "wrote %d datagrams with a full window, want 0", len(pc.pkts))
		assert.Lenf(t, c.retransQueue[spaceApp], 1,
			"retransmit queue = %d, want it left intact", len(c.retransQueue[spaceApp]))
	})

	t.Run("room_under_window_sends", func(t *testing.T) {
		c, pc := newConn()
		c.bytesInFlight = 0

		err := c.flushRetransmits(spaceApp)

		require.NoError(t, err, "flushRetransmits with room under the window")
		assert.Lenf(t, pc.pkts, 1,
			"wrote %d datagrams with room under the window, want 1", len(pc.pkts))
	})

	// A PTO exemption lets exactly one packet past a full window (RFC 9002 §6.2.4),
	// then the gate resumes and the remainder stays queued.
	t.Run("pto_exemption_lets_exactly_one_past", func(t *testing.T) {
		c, pc := newConn()
		c.bytesInFlight = 12000
		c.retransQueue[spaceApp] = append(c.retransQueue[spaceApp],
			retransFrame{kind: retransStream, streamID: 0, offset: 4, data: []byte("yyyy")})
		c.ptoExempt = true

		err := c.flushRetransmits(spaceApp)

		require.NoError(t, err, "flushRetransmits with a PTO exemption")
		assert.Lenf(t, pc.pkts, 1,
			"with a PTO exemption on a full window, sent %d datagrams, want exactly 1", len(pc.pkts))
		assert.False(t, c.ptoExempt, "the PTO exemption should be consumed after one packet")
		assert.Lenf(t, c.retransQueue[spaceApp], 1,
			"remaining queue = %d, want 1 (only the probe went past the window)",
			len(c.retransQueue[spaceApp]))
	})
}
