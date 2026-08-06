package quic

import "testing"

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

// TestPollBufLen_MatchesPlatformCoalescing pins that the receive buffer is sized
// on whether this platform can actually coalesce, not merely on whether the
// transport implements ReadGRO.
//
// http3's udpConn implements ReadGRO unconditionally so the tree builds
// everywhere, but off Linux RecvGRO is a plain single-datagram Read. Sizing on
// the interface assertion alone handed every connection a 64 KiB buffer instead
// of 2 KiB on Windows, macOS and the BSDs — 32x the memory for a coalescing that
// cannot happen, on a client whose whole point is running thousands of pooled
// connections.
func TestPollBufLen_MatchesPlatformCoalescing(t *testing.T) {
	c := &Conn{pc: groCapablePC{}}
	got := c.pollBufLen()
	want := pollBufSize
	if groCanCoalesce {
		want = groReadBuffer
	}
	if got != want {
		t.Fatalf("pollBufLen with a groReader transport = %d, want %d (groCanCoalesce=%v)",
			got, want, groCanCoalesce)
	}

	// A transport without ReadGRO always gets the single-datagram size.
	plain := &Conn{pc: plainPC{}}
	if got := plain.pollBufLen(); got != pollBufSize {
		t.Fatalf("pollBufLen with a plain transport = %d, want %d", got, pollBufSize)
	}
}
