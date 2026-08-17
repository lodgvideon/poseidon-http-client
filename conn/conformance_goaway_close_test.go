package conn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"

	"net"
)

// Batch 4 — RFC 9113 §5.4.1: after sending a GOAWAY for an error condition the
// endpoint MUST close the TCP connection. The reader loop emitted the GOAWAY but
// left the socket open, so a peer that provoked a connection error was left with
// a half-alive connection until something else happened to close it.

// TestConformance_RFC9113_Sec5_4_1_ErrorGoAwayClosesTransport pins that a
// connection-error GOAWAY is followed by an actual transport close: the peer's
// read side unblocks (returns an error) once the client has torn the socket down.
func TestConformance_RFC9113_Sec5_4_1_ErrorGoAwayClosesTransport(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	drainDone := make(chan struct{}, 1)
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		d := drainFrames(srvFr, probe)
		// DATA on stream 0 is a connection error (PROTOCOL_ERROR): the client emits
		// a GOAWAY and, per §5.4.1, must then close the socket.
		<-asyncWrite(func() error { return writeRawFrame(srv, frame.FrameData, 0, 0, []byte("x")) })
		select {
		case <-d: // our read side returned an error => the client closed the transport
			drainDone <- struct{}{}
		case <-finish:
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	require.NoError(t, err, "NewClientConn")

	code := recvCode(t, "GOAWAY", probe.away)

	assert.Equalf(t, frame.ErrCodeProtocolError, code, "GOAWAY code = %v, want PROTOCOL_ERROR", code)
	select {
	case <-drainDone:
	case <-time.After(3 * time.Second):
		assert.Fail(t, "client did not close the transport after an error-condition GOAWAY (RFC 9113 §5.4.1)")
	}
	release()
	_ = c.Close()
}
