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

	// fcMu guards the connection-level recv window. The corresponding
	// per-stream window lives on Stream and is guarded by Stream.mu.
	fcMu              sync.Mutex
	connRecvWindow    int32  // bytes the peer can still send to us at the conn level (RFC 7540 §6.9.1)
	connRefundPending uint32 // bytes consumed but not yet returned via WINDOW_UPDATE(stream=0)

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
		transport:      transport,
		fr:             frame.NewFramer(transport, transport),
		enc:            hpack.NewEncoder(),
		dec:            hpack.NewDecoder(),
		opts:           opts,
		nextID:         1,
		streams:        map[uint32]*Stream{},
		readerDone:     make(chan struct{}),
		connRecvWindow: int32(connInitialRecvWindow),
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
// NewStream allocates a new outbound stream. Returns ErrTooManyStreams
// when the in-flight count has reached the locally advertised
// MaxConcurrentStreams (peer-advertised enforcement is B.2.5).
// NewStream allocates an in-flight slot for a new outbound stream. The
// stream's HTTP/2 ID is assigned later, when SendHeaders writes the
// first HEADERS frame under the writer mutex; this preserves the
// monotonic-id ordering required by RFC 7540 §5.1.1 even with many
// concurrent NewStream callers. Returns ErrTooManyStreams when the
// in-flight count has reached the locally advertised
// MaxConcurrentStreams (peer-advertised enforcement is B.2.5).
// NewStream allocates an in-flight slot for a new outbound stream. The
// stream's HTTP/2 ID is assigned later, when SendHeaders writes the
// first HEADERS frame under the writer mutex; this preserves the
// monotonic-id ordering required by RFC 7540 §5.1.1 even with many
// concurrent NewStream callers. Returns ErrTooManyStreams when the
// in-flight count has reached the locally advertised
// MaxConcurrentStreams (peer-advertised enforcement is B.2.5).
// NewStream allocates an in-flight slot for a new outbound stream. The
// stream's HTTP/2 ID is assigned later, when SendHeaders writes the
// first HEADERS frame under the writer mutex; this preserves the
// monotonic-id ordering required by RFC 7540 §5.1.1 even with many
// concurrent NewStream callers. Returns ErrTooManyStreams when the
// in-flight count has reached the locally advertised
// MaxConcurrentStreams (peer-advertised enforcement is B.2.5).
func (c *Conn) NewStream(ctx context.Context) (*Stream, error) {
	if c.closed.Load() {
		return nil, ErrConnClosed
	}
	c.smu.Lock()
	defer c.smu.Unlock()
	if c.inflight >= c.opts.Settings.MaxConcurrentStreams {
		return nil, ErrTooManyStreams
	}
	s := newStream(0, c.opts.StreamEventBuffer, c, int32(c.opts.Settings.InitialWindowSize))
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

// connInitialRecvWindow is the connection-level recv window size. RFC
// 7540 §6.9.2 fixes this at 65535 octets at handshake; the
// SETTINGS_INITIAL_WINDOW_SIZE we advertise affects per-stream windows
// only, never the connection window.
const connInitialRecvWindow = 65535

// recvWindowRefundThreshold is the minimum number of bytes accumulated
// before we batch a WINDOW_UPDATE refund. Half of the default window
// keeps refund frames at one per ~32 KiB of data and bounds peer-side
// stalls to at most that much in-flight without a window credit.
const recvWindowRefundThreshold = 32768

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

func (c *Conn) writeHeaders(s *Stream, fields []hpack.HeaderField, endStream bool) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if s.id == 0 {
		c.smu.Lock()
		s.id = c.nextID
		c.nextID += 2
		c.streams[s.id] = s
		c.smu.Unlock()
	}
	block := c.enc.EncodeBlock(nil, fields)
	err := c.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      s.id,
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

func (c *Conn) writeData(s *Stream, p []byte, endStream bool) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	if s.id == 0 {
		// SendHeaders has not run; the stream has no on-wire identity.
		return ErrStreamClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WriteData(s.id, endStream, p); err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}

func (c *Conn) writeRSTStream(s *Stream, code frame.ErrCode) error {
	if s.id == 0 {
		// Stream never reached the wire; no peer state to reset.
		c.releaseUnassignedInflight(s)
		return nil
	}
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WriteRSTStream(s.id, code); err != nil {
		return err
	}
	c.bumpFramesSent()
	c.releaseInflight(s.id)
	return nil
}

// onDataReceived debits both the stream-level and connection-level
// recv windows for a DATA frame whose total payload is `length` bytes
// (RFC 7540 §6.9.1: includes the data, the pad-length octet, and the
// padding). Returns an error to abort the connection (peer
// FLOW_CONTROL_ERROR) or the stream when its window is exceeded. On
// success it eagerly accumulates a refund counter and, once the
// per-stream or connection counter crosses recvWindowRefundThreshold,
// emits a WINDOW_UPDATE for that scope.
// onDataReceived debits both the stream-level and connection-level
// recv windows for a DATA frame whose total payload is `length` bytes
// (RFC 7540 §6.9.1: includes the data, the pad-length octet, and the
// padding). Returns an error to abort the connection (peer
// FLOW_CONTROL_ERROR) or the stream when its window is exceeded. On
// success it eagerly accumulates a refund counter and, once the
// per-stream or connection counter crosses recvWindowRefundThreshold,
// emits a WINDOW_UPDATE for that scope.
func (c *Conn) onDataReceived(s *Stream, length uint32) error {
	debit := int32(length)

	s.mu.Lock()
	s.recvWindow -= debit
	if s.recvWindow < 0 {
		s.mu.Unlock()
		return &StreamError{StreamID: s.id, Code: frame.ErrCodeFlowControlError}
	}
	s.recvRefundPending += length
	streamRefund := uint32(0)
	if s.recvRefundPending >= recvWindowRefundThreshold {
		streamRefund = s.recvRefundPending
		s.recvRefundPending = 0
		s.recvWindow += int32(streamRefund)
	}
	s.mu.Unlock()

	c.fcMu.Lock()
	c.connRecvWindow -= debit
	if c.connRecvWindow < 0 {
		c.fcMu.Unlock()
		return &ConnError{Code: frame.ErrCodeFlowControlError, Reason: "peer overflowed connection recv window"}
	}
	c.connRefundPending += length
	connRefund := uint32(0)
	if c.connRefundPending >= recvWindowRefundThreshold {
		connRefund = c.connRefundPending
		c.connRefundPending = 0
		c.connRecvWindow += int32(connRefund)
	}
	c.fcMu.Unlock()

	if streamRefund > 0 {
		if err := c.writeWindowUpdate(s.id, streamRefund); err != nil {
			return err
		}
	}
	if connRefund > 0 {
		if err := c.writeWindowUpdate(0, connRefund); err != nil {
			return err
		}
	}
	return nil
}

// writeWindowUpdate emits a WINDOW_UPDATE frame for the given scope
// (streamID==0 means connection-level). Called from the reader loop
// after a refund threshold trip; takes wmu briefly.
func (c *Conn) writeWindowUpdate(streamID uint32, increment uint32) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WriteWindowUpdate(streamID, increment); err != nil {
		return err
	}
	c.bumpFramesSent()
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
// markStreamDone is called by the connHandler when a stream's response
// side closes (END_STREAM observed or RST received), and from local
// SendHeaders/SendData when END_STREAM goes out. It releases the
// stream's slot in the inflight pool exactly once and evicts the
// stream from the registry once both ends have closed.
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
	if ended && !released {
		if c.inflight > 0 {
			c.inflight--
		}
		delete(c.streams, id)
	}
}

// releaseInflight is called when an RST_STREAM is sent to the peer. RST
// closes the stream regardless of whether either end observed END_STREAM,
// so the inflight slot must be returned. Idempotent via Stream.inflightDone.
// releaseInflight is called when an RST_STREAM is sent to the peer. RST
// closes the stream regardless of whether either end observed END_STREAM,
// so the inflight slot must be returned and the stream evicted from the
// registry. Idempotent via Stream.inflightDone.
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
	if !released {
		if c.inflight > 0 {
			c.inflight--
		}
		delete(c.streams, id)
	}
}

// releaseUnassignedInflight returns the slot for a Stream that was
// allocated via NewStream but never wrote a HEADERS frame, so it is not
// in c.streams and has no on-wire ID. Idempotent via inflightDone.
func (c *Conn) releaseUnassignedInflight(s *Stream) {
	c.smu.Lock()
	defer c.smu.Unlock()
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
