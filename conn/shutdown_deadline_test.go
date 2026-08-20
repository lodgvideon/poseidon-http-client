package conn

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShutdown_DoesNotStrandAWriteDeadline is the regression for a GOAWAY whose
// write deadline was armed and never disarmed.
//
// writeGoAwayBestEffort bounds its own write with closeGoAwayDeadline so an
// unresponsive peer cannot block shutdown. Its sibling writeRSTStreamBestEffort
// arms the same kind of deadline and clears it again; this one did not. Under
// Close that is invisible, because the transport dies immediately afterwards.
// Under Shutdown it is not: Shutdown's whole contract is that the connection
// stays alive for gracefulTimeout so in-flight streams can finish, and every one
// of those writes inherited a deadline 200ms after the GOAWAY. A drain longer
// than 200ms — which is every drain worth asking for — failed the very sends it
// existed to permit, with the socket's own i/o timeout.
//
// The test waits well past closeGoAwayDeadline before writing, so it fails on
// the pre-fix code and cannot pass by being fast.
func TestShutdown_DoesNotStrandAWriteDeadline(t *testing.T) {
	p := newLoadGenPeer(t, 64)
	c := dialLoadGenPeer(t, p, nil)
	ctx := context.Background()

	// One in-flight stream, so Shutdown drains rather than closing at once.
	held, err := c.NewStream(ctx)
	require.NoError(t, err, "NewStream")
	require.NoError(t, held.SendHeaders(ctx, lgRequestFields("drain.local"), false), "SendHeaders")
	drained := make(chan struct{})
	go func() {
		_ = c.Shutdown(30 * time.Second)
		close(drained)
	}()
	// Past the GOAWAY's own deadline by a wide margin.
	time.Sleep(4 * closeGoAwayDeadline)

	sendErr := held.SendData(ctx, make([]byte, 64), true)

	assert.Falsef(t, errors.Is(sendErr, os.ErrDeadlineExceeded),
		"a send %v into a 30s graceful drain failed with the GOAWAY's own "+
			"write deadline: %v", 4*closeGoAwayDeadline, sendErr)
	require.NoError(t, sendErr, "SendData during the drain")
	for {
		ev, rerr := held.Recv(ctx)
		require.NoError(t, rerr, "Recv during the drain")
		ev.Release()
		if ev.DataSlab != nil {
			dataBufPool.Put(ev.DataSlab)
		}
		if ev.EndStream {
			break
		}
	}
	_ = held.Close()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		assert.Fail(t, "Shutdown did not return after its last stream completed")
	}
}
