package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// goAwayCapture records the last-stream-id and error code of a GOAWAY the peer
// reads off the wire.
type goAwayCapture struct {
	nilHandler
	ch chan goAwayInfo
}

type goAwayInfo struct {
	last uint32
	code frame.ErrCode
}

func (g *goAwayCapture) OnGoAway(_ frame.FrameHeader, last uint32, code frame.ErrCode, _ []byte) error {
	g.ch <- goAwayInfo{last, code}
	return nil
}

// TestConformance_RFC9113_Sec6_8_ClientEmitsGoAwayBeforeClose pins RFC 9113 §6.8:
// "Endpoints SHOULD always send a GOAWAY frame before closing a connection so
// that the remote peer can know whether a stream has been partially processed or
// not." A real peer reads the client's Close-time GOAWAY off the wire and
// confirms it is NO_ERROR with last-stream-id 0 (no server push, so the
// peer-initiated scope is empty — the B10 monotonic/peer-scoped value). The
// existing Close/Shutdown unit tests write to a reader-less pipe and never assert
// the emitted frame.
func TestConformance_RFC9113_Sec6_8_ClientEmitsGoAwayBeforeClose(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()

	got := make(chan goAwayInfo, 1)
	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		// The next frame the client sends after the handshake is the Close GOAWAY.
		_, _ = srvFr.ReadFrame(context.Background(), &goAwayCapture{ch: got})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	require.NoError(t, err, "NewClientConn")

	// WriteGoAway blocks on the synchronous pipe until the server reads it, so run
	// Close concurrently with the server's ReadFrame.
	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close() }()

	select {
	case g := <-got:
		assert.Equalf(t, frame.ErrCodeNoError, g.code,
			"Close GOAWAY code = %v, want NO_ERROR (graceful close, §6.8)", g.code)
		assert.EqualValuesf(t, 0, g.last,
			"Close GOAWAY last-stream-id = %d, want 0 (no server push; §6.8 peer-initiated scope)", g.last)
	case <-time.After(3 * time.Second):
		require.FailNow(t, "client did not emit a GOAWAY before closing (§6.8: Endpoints SHOULD always send a GOAWAY frame before closing a connection)")
	}
	assert.NoError(t, <-closeDone, "Close")
}
