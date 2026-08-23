package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// groCapablePC implements both PacketConn and groReader.
type groCapablePC struct{}

func (groCapablePC) Write(p []byte) (int, error)        { return len(p), nil }
func (groCapablePC) Read(p []byte) (int, error)         { return 0, nil }
func (groCapablePC) Close() error                       { return nil }
func (groCapablePC) ReadGRO(b []byte) (int, int, error) { return 0, 0, nil }

// plainPC is a PacketConn with no batched receive.
type plainPC struct{}

func (plainPC) Write(p []byte) (int, error) { return len(p), nil }
func (plainPC) Read(p []byte) (int, error)  { return 0, nil }
func (plainPC) Close() error                { return nil }

// TestPollBufLenFor_PlatformGate pins the whole decision table, including the row
// this test run's own platform cannot reach.
//
// The defect being guarded is sizing the receive buffer on the groReader
// interface assertion alone. http3's udpConn implements ReadGRO unconditionally
// so the tree builds everywhere, but off Linux RecvGRO is a plain
// single-datagram Read, so that handed every connection a 64 KiB buffer instead
// of 2 KiB on Windows, macOS and the BSDs — 32x the memory for a coalescing that
// cannot happen, on a client whose whole point is thousands of pooled
// connections.
//
// The row that carries that property is {canCoalesce: false, isGROReader: true},
// and it is why pollBufLenFor takes the capability as an argument instead of
// reading the build-tagged constant. groCanCoalesce is true on Linux and every
// `runs-on:` in .github/workflows/ is ubuntu-24.04, so a test that derived its
// expectation from that constant agreed with the gate being deleted just as
// happily as with it being there — the regression could not turn CI red (#839).
func TestPollBufLenFor_PlatformGate(t *testing.T) {
	tests := []struct {
		name        string
		canCoalesce bool
		isGROReader bool
		want        int
		why         string
	}{
		{
			name: "coalescing platform, GRO transport", canCoalesce: true, isGROReader: true,
			want: groReadBuffer,
			why:  "one recvmsg can return a whole coalesced burst, so the buffer must hold it",
		},
		{
			name: "non-coalescing platform, GRO transport", canCoalesce: false, isGROReader: true,
			want: pollBufSize,
			why: "RecvGRO is a plain single-datagram Read off Linux; a 64 KiB buffer here is " +
				"32x the memory for a coalescing that cannot happen",
		},
		{
			name: "coalescing platform, plain transport", canCoalesce: true, isGROReader: false,
			want: pollBufSize,
			why:  "without ReadGRO the receive path never reads more than one datagram",
		},
		{
			name: "non-coalescing platform, plain transport", canCoalesce: false, isGROReader: false,
			want: pollBufSize,
			why:  "neither half of the condition holds",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pollBufLenFor(tc.canCoalesce, tc.isGROReader)

			assert.Equalf(t, tc.want, got,
				"pollBufLenFor(canCoalesce=%v, isGROReader=%v) = %d, want %d — %s",
				tc.canCoalesce, tc.isGROReader, got, tc.want, tc.why)
		})
	}
}

// TestPollBufLen_MatchesPlatformCoalescing binds the method on Conn to that
// decision table: it checks that pollBufLen really does feed the platform
// constant and the interface assertion into pollBufLenFor, rather than deciding
// for itself. Without this, the table above could stay green while the method
// drifted away from it.
func TestPollBufLen_MatchesPlatformCoalescing(t *testing.T) {
	c := &Conn{pc: groCapablePC{}}
	plain := &Conn{pc: plainPC{}} // no ReadGRO: always the single-datagram size

	got, gotPlain := c.pollBufLen(), plain.pollBufLen()

	assert.Equalf(t, pollBufLenFor(groCanCoalesce, true), got,
		"pollBufLen with a groReader transport = %d; the method and pollBufLenFor must not disagree "+
			"(groCanCoalesce=%v)", got, groCanCoalesce)
	assert.Equalf(t, pollBufSize, gotPlain,
		"pollBufLen with a plain transport = %d, want %d — a transport without ReadGRO never "+
			"receives a coalesced burst, on any platform", gotPlain, pollBufSize)
}
