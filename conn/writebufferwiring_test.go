package conn

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// settingsConn answers the HTTP/2 handshake with one empty SETTINGS frame, then
// blocks, and records the largest slice its Write was ever handed. A
// bufio.Writer hands the transport its whole buffer when the buffer fills, so
// that largest slice IS the buffer size the constructor chose.
type settingsConn struct {
	net.Conn
	mu       sync.Mutex
	max      int
	script   []byte
	block    chan struct{}
	unblocks sync.Once
}

func newSettingsConn(t *testing.T) *settingsConn {
	t.Helper()
	c := &settingsConn{block: make(chan struct{})}
	// handshakeSettings reads two frames: the peer's SETTINGS, then the ACK of
	// ours. Both are 9-byte headers with an empty payload — length 0, type
	// SETTINGS, stream 0 — differing only in the ACK flag.
	c.script = append(c.script,
		0, 0, 0, byte(frame.FrameSettings), 0, 0, 0, 0, 0,
		0, 0, 0, byte(frame.FrameSettings), byte(frame.FlagSettingsAck), 0, 0, 0, 0,
	)
	return c
}

// unblock releases the reader goroutine parked in Read. It has to run BEFORE
// Conn.Close, which waits for that goroutine, so the caller registers it as the
// last cleanup rather than this constructor registering it as the first.
func (c *settingsConn) unblock() {
	c.unblocks.Do(func() { close(c.block) })
}

func (c *settingsConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.script) > 0 {
		n := copy(p, c.script)
		c.script = c.script[n:]
		c.mu.Unlock()
		return n, nil
	}
	c.mu.Unlock()
	<-c.block
	return 0, io.EOF
}

func (c *settingsConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if len(p) > c.max {
		c.max = len(p)
	}
	c.mu.Unlock()
	return len(p), nil
}

func (c *settingsConn) forget() {
	c.mu.Lock()
	c.max = 0
	c.mu.Unlock()
}

func (c *settingsConn) largestWrite() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.max
}

func (c *settingsConn) Close() error                     { return nil }
func (c *settingsConn) SetDeadline(time.Time) error      { return nil }
func (c *settingsConn) SetReadDeadline(time.Time) error  { return nil }
func (c *settingsConn) SetWriteDeadline(time.Time) error { return nil }

// TestWriteBufferSize_ReachesTheWriter is the write-side mirror of
// TestReadBufferSize_ReachesTheReader, and it exists for the identical reason
// that one does.
//
// Four tests already cover ConnOptions.WriteBufferSize — the defaulting table,
// the floor's rationale, "the writer holds N bytes", the convoy threshold and an
// end-to-end SendBatch write count — and not one of them goes through
// NewClientConn. TestWriteBufferSize_SizesTheWriter calls bufio.NewWriterSize
// itself, so it asserts that bufio honours its own size argument; the other two
// hand-build a Conn with their own wb. Replacing opts.WriteBufferSize with the
// old constant inside the constructor left the whole package green (#852).
func TestWriteBufferSize_ReachesTheWriter(t *testing.T) {
	largestWriteFor := func(t *testing.T, size int) int {
		t.Helper()
		tr := newSettingsConn(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c, err := NewClientConn(ctx, tr, ConnOptions{WriteBufferSize: size})
		require.NoErrorf(t, err, "NewClientConn at WriteBufferSize %d", size)
		t.Cleanup(func() { _ = c.Close() })
		t.Cleanup(tr.unblock) // LIFO: this runs first, so Close is not left waiting
		// The handshake's own writes say nothing about the buffer's capacity.
		tr.forget()

		// One PING is 9 header bytes plus an 8-byte payload. Pushing more than a
		// buffer's worth through without flushing makes bufio hand the transport
		// a full buffer, which is the only externally visible measure of it.
		var werr error
		c.wmu.Lock()
		for n := 0; n*17 < size+2*17 && werr == nil; n++ {
			werr = c.fr.WritePing(false, [8]byte{byte(n), byte(n >> 8)})
		}
		c.wmu.Unlock()
		require.NoErrorf(t, werr, "WritePing at WriteBufferSize %d", size)
		return tr.largestWrite()
	}

	for _, size := range []int{minWriteBufferSize, 64 * 1024} {
		t.Run(strconv.Itoa(size)+" bytes", func(t *testing.T) {
			got := largestWriteFor(t, size)

			assert.Equalf(t, size, got,
				"the transport's largest write was %d bytes for a %d-byte WriteBufferSize — "+
					"the option is not sizing the writer the Framer writes through, so a "+
					"caller who raised it to cut syscalls is still paying the old rate",
				got, size)
		})
	}
}
