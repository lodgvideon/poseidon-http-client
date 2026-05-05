package conn

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Conn is one HTTP/2 connection.
type Conn struct {
	transport net.Conn
	fr        *frame.Framer
	enc       *hpack.Encoder
	dec       *hpack.Decoder
	opts      ConnOptions

	// peerSettings is the most recently observed server SETTINGS.
	peerSettings frame.SettingsParams

	wmu sync.Mutex // serializes all writes to fr

	smu      sync.Mutex // guards next stream id and streams map
	nextID   uint32
	streams  map[uint32]*Stream
	inflight uint32

	closed     atomic.Bool
	readerDone chan struct{}

	statsMu sync.Mutex
	stats   ConnStats
}

// ConnStats is a point-in-time counter snapshot.
type ConnStats struct {
	BytesSent      int64
	BytesReceived  int64
	FramesSent     int64
	FramesReceived int64
	StreamsOpened  uint32
}

// NewClientConn wraps an already-handshaken transport.
func NewClientConn(ctx context.Context, transport net.Conn, opts ConnOptions) (*Conn, error) {
	opts = opts.defaulted()
	c := &Conn{
		transport:  transport,
		fr:         frame.NewFramer(transport, transport),
		enc:        hpack.NewEncoder(),
		dec:        hpack.NewDecoder(),
		opts:       opts,
		nextID:     1,
		streams:    map[uint32]*Stream{},
		readerDone: make(chan struct{}),
	}
	peer, err := handshakeSettings(ctx, c.fr, opts.Settings)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	c.peerSettings = peer
	go c.readerLoop()
	return c, nil
}

func (c *Conn) lookupStream(id uint32) *Stream {
	c.smu.Lock()
	defer c.smu.Unlock()
	return c.streams[id]
}

// NewStream allocates a new stream. B.1 enforces at most one in-flight
// stream per Conn; subsequent calls return ErrTooManyStreams until the
// active stream completes.
func (c *Conn) NewStream(ctx context.Context) (*Stream, error) {
	if c.closed.Load() {
		return nil, ErrConnClosed
	}
	c.smu.Lock()
	defer c.smu.Unlock()
	if c.inflight >= 1 { // B.1 cap
		return nil, ErrTooManyStreams
	}
	id := c.nextID
	c.nextID += 2 // odd-only client stream IDs
	s := newStream(id, c.opts.StreamEventBuffer, c)
	c.streams[id] = s
	c.inflight++
	c.statsMu.Lock()
	c.stats.StreamsOpened++
	c.statsMu.Unlock()
	return s, nil
}

// Close sends a best-effort GOAWAY(NO_ERROR), closes the transport, and
// waits for the reader goroutine to drain. Idempotent under concurrent
// callers.
func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Best-effort GOAWAY (NO_ERROR). Bound the write so an unresponsive
	// peer cannot wedge Close indefinitely (e.g. net.Pipe with no
	// active reader, or a real TCP peer that has stopped reading).
	if dl, ok := c.transport.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = dl.SetWriteDeadline(time.Now().Add(closeGoAwayDeadline))
	}
	c.wmu.Lock()
	_ = c.fr.WriteGoAway(c.lastClientStreamID(), frame.ErrCodeNoError, nil)
	c.wmu.Unlock()
	_ = c.transport.Close()
	<-c.readerDone
	return nil
}

// closeGoAwayDeadline bounds the GOAWAY write during Close so an
// unresponsive peer cannot block shutdown.
const closeGoAwayDeadline = 200 * time.Millisecond

func (c *Conn) lastClientStreamID() uint32 {
	c.smu.Lock()
	defer c.smu.Unlock()
	if c.nextID < 3 {
		return 0
	}
	return c.nextID - 2
}

// Stats returns a point-in-time snapshot of connection counters.
func (c *Conn) Stats() ConnStats {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	return c.stats
}

// --- streamWriter implementation (called from *Stream).

func (c *Conn) writeHeaders(streamID uint32, fields []hpack.HeaderField, endStream bool) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	block := c.enc.EncodeBlock(nil, fields)
	err := c.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      streamID,
		BlockFragment: block,
		EndHeaders:    true,
		EndStream:     endStream,
	})
	if err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}

func (c *Conn) writeData(streamID uint32, p []byte, endStream bool) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WriteData(streamID, endStream, p); err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}

func (c *Conn) writeRSTStream(streamID uint32, code frame.ErrCode) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WriteRSTStream(streamID, code); err != nil {
		return err
	}
	c.bumpFramesSent()
	c.releaseInflight(streamID)
	return nil
}

func (c *Conn) bumpFramesSent() {
	c.statsMu.Lock()
	c.stats.FramesSent++
	c.statsMu.Unlock()
}

// readerLoop owns frame.ReadFrame for the lifetime of the connection.
func (c *Conn) readerLoop() {
	defer close(c.readerDone)
	h := newConnHandler(c, c.dec)
	for {
		_, err := c.fr.ReadFrame(context.Background(), h)
		if err != nil {
			c.shutdownStreams(err)
			return
		}
		c.statsMu.Lock()
		c.stats.FramesReceived++
		c.statsMu.Unlock()
	}
}

func (c *Conn) shutdownStreams(reason error) {
	c.smu.Lock()
	defer c.smu.Unlock()
	for _, s := range c.streams {
		select {
		case s.events <- StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeInternalError, EndStream: true}:
		default:
		}
		close(s.events)
	}
	if errors.Is(reason, io.EOF) {
		return
	}
}

// markStreamDone is called by the connHandler when a stream's response
// side closes (END_STREAM observed or RST received), and from local
// SendHeaders/SendData when END_STREAM goes out. It releases the
// stream's slot in the inflight pool exactly once.
func (c *Conn) markStreamDone(id uint32) {
	c.smu.Lock()
	defer c.smu.Unlock()
	s, ok := c.streams[id]
	if !ok {
		return
	}
	s.mu.Lock()
	ended := s.localEnded && s.remoteEnded
	released := s.inflightDone
	if ended && !released {
		s.inflightDone = true
	}
	s.mu.Unlock()
	if ended && !released && c.inflight > 0 {
		c.inflight--
	}
}

// releaseInflight is called when an RST_STREAM is sent to the peer. RST
// closes the stream regardless of whether either end observed END_STREAM,
// so the inflight slot must be returned. Idempotent via Stream.inflightDone.
func (c *Conn) releaseInflight(id uint32) {
	c.smu.Lock()
	defer c.smu.Unlock()
	s, ok := c.streams[id]
	if !ok {
		return
	}
	s.mu.Lock()
	released := s.inflightDone
	if !released {
		s.inflightDone = true
	}
	s.mu.Unlock()
	if !released && c.inflight > 0 {
		c.inflight--
	}
}
