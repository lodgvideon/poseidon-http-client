package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestConn_Shutdown_InflightReadIsSynchronized is a -race regression guard for
// Shutdown's in-flight decision. Every access to c.inflight is guarded by
// c.smu — NewStream, markStreamDone, releaseSlotLocked — except that Shutdown
// read it lock-free to choose between an immediate Close and the graceful wait.
// Shutdown is called precisely while streams are in flight, so that read races
// the reader goroutine decrementing c.inflight (markStreamDone ->
// releaseSlotLocked) as responses complete. `go test -race` flags it.
//
// The goroutine below mutates c.inflight under c.smu for the whole duration of
// the Shutdown call, standing in for the reader goroutine. With the lock-free
// read the detector reports a data race here; with the read taken under c.smu
// it is clean.
func TestConn_Shutdown_InflightReadIsSynchronized(t *testing.T) {
	cli, srv := net.Pipe()
	go pipeServer(t, srv, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
	require.NoError(t, err, "NewClientConn")

	// One stream in flight, so Shutdown reaches the c.inflight decision instead
	// of the no-inflight fast path.
	c.smu.Lock()
	c.inflight = 1
	c.smu.Unlock()

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Touch c.inflight under c.smu, as releaseSlotLocked does.
			c.smu.Lock()
			c.inflight++
			c.inflight--
			c.smu.Unlock()
		}
	}()

	_ = c.Shutdown(50 * time.Millisecond)
	close(stop)
	<-done
}
