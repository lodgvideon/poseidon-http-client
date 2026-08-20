package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The two gates in this file cover the send path's outputs rather than its
// inputs: the timestamp sealPacket hands to loss recovery, and the fate of a
// failed socket write. Both were mutation-survivors (#653) — the whole quic and
// http3 suites stayed green with the send timestamp zeroed and with flush's
// write error discarded — because every other test in the package either builds
// the sent-packet record by hand (bypassing sealPacket) or uses a PacketConn
// whose Write cannot fail.

// errSendFailed is the sentinel a failWritePC returns instead of writing. A
// distinct value, not a generic error, because the gate below asserts identity:
// "Establish returned some error" is satisfied by the mutant too, since a
// connection that silently failed to send its ClientHello then fails at the
// read instead.
var errSendFailed = errors.New("quic_test: datagram write failed")

// errNoResponse is what the fixture's Read returns. It stands in for the real
// consequence of swallowing a write error — waiting for a reply to a datagram
// that never left the host — and makes the mutant's failure message say so.
var errNoResponse = errors.New("quic_test: no response (nothing was ever sent)")

// failWritePC is a PacketConn whose n-th datagram write (1-based) fails with
// errSendFailed instead of being delivered. Every other test transport in this
// package reports success unconditionally — faultPC included, which models loss
// by discarding the datagram and returning (len(b), nil), because that is what
// the network losing it looks like to the sender. A local send error is the
// other failure: the datagram never reached the network at all, and the caller
// must hear about it.
type failWritePC struct {
	failOn  int // 1-based index of the write that fails; 0 = never fail
	writes  int
	written [][]byte
}

func (p *failWritePC) Write(b []byte) (int, error) {
	p.writes++
	if p.writes == p.failOn {
		return 0, errSendFailed
	}
	p.written = append(p.written, append([]byte(nil), b...))
	return len(b), nil
}

func (p *failWritePC) Read([]byte) (int, error) { return 0, errNoResponse }
func (p *failWritePC) Close() error             { return nil }

// TestConn_SendPath_WriteErrorReachesCaller pins that a transport-level send
// failure surfaces to the caller as that failure, rather than being absorbed
// into a connection that goes on waiting for a reply to a packet which never
// left the host.
//
// Establish is the observable: it is the public entry point that owns the
// initial flight, so no test needs to know that flush is what performs the
// write. The assertion is errors.Is against the sentinel, not "err != nil" —
// with the propagation removed Establish still returns an error, just the wrong
// one (the read that follows fails because the peer was never contacted), and a
// non-nil check would pass on the mutant.
func TestConn_SendPath_WriteErrorReachesCaller(t *testing.T) {
	_, pool := genServerCert(t)
	pc := &failWritePC{failOn: 1} // the ClientHello datagram
	c, err := NewConn(pc, &tls.Config{ServerName: "example.com", RootCAs: pool}, []byte{0x01, 0x02, 0x03})
	require.NoError(t, err, "NewConn against the fail-on-first-write transport")

	err = c.Establish(context.Background())

	assert.Truef(t, errors.Is(err, errSendFailed),
		"Establish = %v, want the transport's send error %v: the socket refused "+
			"the ClientHello and the caller was told something else, so a host that "+
			"cannot send looks to the caller like a peer that will not answer",
		err, errSendFailed)
	assert.Equalf(t, 1, pc.writes,
		"transport saw %d writes, want 1 — the failing write is the one being "+
			"gated, so a different count means this test proved something else", pc.writes)
	assert.Emptyf(t, pc.written,
		"%d datagrams were delivered; the only write was supposed to fail", len(pc.written))
}

// TestConn_SendPath_WriteErrorFixtureIsTheControl pins that failWritePC is
// transparent when it is not injecting: without this, a green gate above would
// be indistinguishable from one whose fixture fails every write for an unrelated
// reason. The Initial flight goes out and Establish gets as far as the read.
func TestConn_SendPath_WriteErrorFixtureIsTheControl(t *testing.T) {
	_, pool := genServerCert(t)
	pc := &failWritePC{} // failOn 0: never fail
	c, err := NewConn(pc, &tls.Config{ServerName: "example.com", RootCAs: pool}, []byte{0x01, 0x02, 0x03})
	require.NoError(t, err, "NewConn against the never-failing control transport")

	err = c.Establish(context.Background())

	assert.False(t, errors.Is(err, errSendFailed),
		"the fixture failed a write with no injection configured")
	assert.Truef(t, errors.Is(err, errNoResponse),
		"Establish = %v, want the read to be what fails once the flight is sent", err)
	assert.Lenf(t, pc.written, 1,
		"delivered %d datagrams, want the 1 Initial flight — the fixture is not "+
			"on the send path, so the gate above could not have observed anything", len(pc.written))
}

// TestConformance_RFC9002_Sec51_RTTSampleUsesRecordedSendTime pins the send
// timestamp sealPacket records into the in-flight set. RFC 9002 §5.1 defines the
// sample as "latest_rtt = ack_time - send_time_of_largest_acked", so that
// timestamp is one of the two terms of every RTT measurement the connection ever
// makes — and the only one the send path supplies.
//
// It is asserted through what the connection does with the measurement rather
// than by reading the record back. lossDetectionDeadline is the instant the
// engine arms as its retransmission timer (and hands to the transport's read
// deadline), so "the packet's send time was recorded correctly" is observable as
// "the connection will re-probe one PTO from now" — the property §5.1 exists to
// support. Reading the stored timestamp instead would restate the assignment
// under test and would not survive a change in how the send time is kept.
//
// The suite's existing RTT and PTO tests all seed rttStats or call onSent
// directly, so none of them reach this line; with the recorded time zeroed the
// whole quic and http3 suites stay green while the first sample saturates to the
// maximum representable Duration and the probe timer effectively stops (#653).
func TestConformance_RFC9002_Sec51_RTTSampleUsesRecordedSendTime(t *testing.T) {
	const sampleRTT = 50 * time.Millisecond
	base := time.Unix(9000, 0)
	now := base

	_, pool := genServerCert(t)
	pc := &capturePacketConn{}
	c, err := NewConn(pc, &tls.Config{ServerName: "example.com", RootCAs: pool}, []byte{0x01, 0x02, 0x03})
	require.NoError(t, err, "NewConn for the RTT-sample fixture")
	c.now = func() time.Time { return now }

	// The real send path: sendInitialFlight is an ordinary flush of the Initial
	// space, so the ClientHello is sealed and recorded by sealPacket, at c.clock().
	require.NoError(t, c.sendInitialFlight(context.Background()), "sendInitialFlight")
	require.Lenf(t, pc.written, 1,
		"wrote %d datagrams, want 1 — no packet was sealed, so nothing recorded "+
			"a send time and this test cannot observe one", len(pc.written))

	// One RTT later the peer acknowledges Initial packet 0, through the real frame
	// parser and ACK handler.
	now = base.Add(sampleRTT)
	ack := AppendAck(nil, 0, 0, 0, nil)
	require.NoError(t, ParseFrames(ack, &connFrameHandler{c: c, space: spaceInitial}), "ParseFrames(ACK)")

	// §5.3: the first sample initialises smoothed_rtt = latest_rtt and
	// rttvar = latest_rtt/2. §6.2.1: PTO = smoothed_rtt + max(4*rttvar,
	// kGranularity) + max_ack_delay, with max_ack_delay set to 0 for the Initial
	// and Handshake spaces. So a 50 ms round trip must arm the probe exactly
	// 3*50 ms out, not at some multiple of the epoch.
	wantPTO := sampleRTT + 4*(sampleRTT/2)
	dl, isLoss := c.lossDetectionDeadline()

	require.False(t, isLoss,
		"a fully acknowledged flight must arm the probe timeout, not a loss timer")
	want := now.Add(wantPTO)
	assert.Truef(t, dl.Equal(want),
		"probe armed for %v, want %v (%v after the ACK): a %v round trip must "+
			"produce a %v RTT sample, so the send time sealPacket recorded is not the "+
			"instant the packet was sealed",
		dl, want, wantPTO, sampleRTT, sampleRTT)
}

// TestConformance_RFC9002_Sec51_RTTSampleScalesWithSendTime is the discriminator
// for the test above: one fixed expectation could be met by a formula that
// ignores the send time and happens to land on it. Doubling the round trip must
// double the sample, and therefore the armed probe.
func TestConformance_RFC9002_Sec51_RTTSampleScalesWithSendTime(t *testing.T) {
	armedPTO := func(t *testing.T, rtt time.Duration) time.Duration {
		t.Helper()
		base := time.Unix(9000, 0)
		now := base
		_, pool := genServerCert(t)
		pc := &capturePacketConn{}
		c, err := NewConn(pc, &tls.Config{ServerName: "example.com", RootCAs: pool}, []byte{0x01, 0x02, 0x03})
		require.NoErrorf(t, err, "NewConn for the %v arm", rtt)
		c.now = func() time.Time { return now }
		require.NoError(t, c.sendInitialFlight(context.Background()), "sendInitialFlight")
		now = base.Add(rtt)
		require.NoError(t, ParseFrames(AppendAck(nil, 0, 0, 0, nil), &connFrameHandler{c: c, space: spaceInitial}),
			"ParseFrames(ACK)")
		dl, _ := c.lossDetectionDeadline()
		return dl.Sub(now)
	}

	slow, fast := armedPTO(t, 80*time.Millisecond), armedPTO(t, 40*time.Millisecond)

	assert.Equalf(t, 2*fast, slow,
		"probe timeout on an 80 ms path = %v, on a 40 ms path = %v; the slower "+
			"path must arm exactly twice the timeout, or the estimate is not derived "+
			"from when the packet was actually sent", slow, fast)
}

// Compile-time proof the gate fixture really is a PacketConn: a Write signature
// that drifted out of the interface would make NewConn reject it at run time
// with a message about the transport rather than about the property under test.
var _ PacketConn = (*failWritePC)(nil)
