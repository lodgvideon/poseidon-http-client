package conn

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// encBufPool recycles the HPACK block-fragment buffer used by writeHeaders.
// The buffer is returned immediately after Framer.WriteHeaders — the call
// is synchronous under wmu, so no concurrent access is possible.
var encBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

// Conn is one HTTP/2 connection.
type Conn struct {
	transport net.Conn
	fr        *frame.Framer
	enc       *hpack.Encoder
	dec       *hpack.Decoder
	opts      ConnOptions

	// peerSettings is the most recently observed server SETTINGS.
	// Guarded by psMu: written by handshake / connHandler.OnSettings,
	// read by writeData (chunking decision) and writeHeaders (initial
	// per-stream send-window seed).
	psMu         sync.RWMutex
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

	// fcOutMu guards the outbound (peer-advertised) connection-level
	// send window and is the locker for fcOutCond. peerConnSendWindow
	// starts at 65535 (RFC §6.9.2 fixes this at handshake regardless
	// of SETTINGS_INITIAL_WINDOW_SIZE) and is replenished by inbound
	// WINDOW_UPDATE(stream=0). fcOutCond.Broadcast wakes writers
	// blocked in acquireSendCredits.
	fcOutMu            sync.Mutex
	fcOutCond          *sync.Cond
	peerConnSendWindow int32

	// goAwayReceived flags that the peer has sent GOAWAY (RFC 7540
	// §6.8). New NewStream calls return ErrGoAway; existing streams
	// whose id is ≤ goAwayLastStreamID continue.
	goAwayReceived     atomic.Bool
	goAwayLastStreamID atomic.Uint32

	closed     atomic.Bool
	readerDone chan struct{}

	// draining is set by Shutdown to mark the conn as draining. New
	// NewStream calls return ErrConnDraining (RFC 7540 §6.8 graceful
	// shutdown pattern). drainDone is closed when the inflight count
	// reaches zero, allowing Shutdown to wake up.
	draining  atomic.Bool
	drainDone chan struct{}

	// pingMu guards pingWaiters. pingCounter produces unique payloads.
	pingMu      sync.Mutex
	pingWaiters map[[8]byte]chan struct{}
	pingCounter atomic.Uint64

	// originsMu guards origins, populated from an ORIGIN frame
	// (RFC 8336 §3). Used for connection coalescing decisions.
	originsMu sync.RWMutex
	origins   []string

	// altSvcMu guards altSvcEntries, populated from an ALTSVC frame
	// (RFC 7838 §4). Used for alternative-service routing.
	altSvcMu      sync.RWMutex
	altSvcEntries []frame.AltSvcEntry

	// streamPool recycles *Stream structs (struct + channel) to eliminate
	// 2 allocs per request after warmup. Only streams whose channel cap
	// equals opts.StreamEventBuffer are recycled; mis-sized ones are discarded.
	streamPool sync.Pool

	// Stats counters: atomics for lock-free updates on the hot write
	// and read paths. Snapshot via Stats() which loads each.
	atomicBytesSent      atomic.Int64
	atomicBytesReceived  atomic.Int64
	atomicFramesSent     atomic.Int64
	atomicFramesReceived atomic.Int64
	atomicStreamsOpened  atomic.Uint32
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
		transport:          transport,
		fr:                 frame.NewFramer(transport, transport),
		enc:                hpack.NewEncoder(),
		dec:                hpack.NewDecoder(),
		opts:               opts,
		nextID:             1,
		streams:            map[uint32]*Stream{},
		readerDone:         make(chan struct{}),
		drainDone:          make(chan struct{}),
		pingWaiters:        make(map[[8]byte]chan struct{}),
		connRecvWindow:     int32(connInitialRecvWindow),
		peerConnSendWindow: int32(connInitialRecvWindow),
	}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	// Sync Framer read limit to our advertised MaxFrameSize. Default Framer
	// cap is 16384; peers honouring our SETTINGS may send frames up to the
	// advertised value, which would be rejected as ErrFrameTooLarge otherwise.
	c.fr.SetMaxReadFrameSize(opts.Settings.MaxFrameSize)
	peer, err := handshakeSettings(ctx, c.fr, opts.Settings, opts.EnablePush)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	c.psMu.Lock()
	c.peerSettings = peer
	c.psMu.Unlock()
	// Apply the initial peer SETTINGS to encoder / streams (no streams
	// exist yet, so this just propagates HEADER_TABLE_SIZE to the
	// HPACK encoder when the peer advertised one).
	c.applyInitialPeerSettings(peer)
	go c.readerLoop()
	if opts.KeepaliveInterval > 0 {
		go c.keepaliveLoop(opts.KeepaliveInterval)
	}
	return c, nil
}

func (c *Conn) lookupStream(id uint32) *Stream {
	c.smu.Lock()
	defer c.smu.Unlock()
	return c.streams[id]
}

// pushSupport reports whether server push is enabled and returns the
// stream-event buffer size for new pushed streams.
func (c *Conn) pushSupport() (enabled bool, eventBuf int) {
	return c.opts.EnablePush, c.opts.StreamEventBuffer
}

// registerPushedStream creates and registers a server-initiated stream
// with the given even ID.
func (c *Conn) registerPushedStream(id uint32) *Stream {
	s := c.allocStream(c.opts.StreamEventBuffer, c.connRecvWindow)
	s.id = id
	c.smu.Lock()
	c.streams[id] = s
	c.smu.Unlock()
	return s
}

// rstStream sends a RST_STREAM frame for the given stream ID.
func (c *Conn) rstStream(id uint32, code frame.ErrCode) error {
	return c.writeRSTStream(&Stream{id: id}, code)
}

// LookupStream returns the stream with the given ID, or (nil, false) if
// no such stream exists. This is primarily used to access server-pushed
// streams after receiving an EventPushPromise on the parent stream.
func (c *Conn) LookupStream(id uint32) (*Stream, bool) {
	c.smu.Lock()
	defer c.smu.Unlock()
	s, ok := c.streams[id]
	return s, ok
}

// NewStream allocates an in-flight slot for a new outbound stream. The
// stream's HTTP/2 ID is assigned later, when SendHeaders writes the
// first HEADERS frame under the writer mutex; this preserves the
// monotonic-id ordering required by RFC 7540 §5.1.1 even with many
// concurrent NewStream callers. Returns ErrTooManyStreams when the
// in-flight count has reached min(local MaxConcurrentStreams,
// peer-advertised SETTINGS_MAX_CONCURRENT_STREAMS).
func (c *Conn) NewStream(_ context.Context) (*Stream, error) {
	if c.closed.Load() {
		return nil, ErrConnClosed
	}
	if c.draining.Load() {
		return nil, ErrConnDraining
	}
	if c.goAwayReceived.Load() {
		return nil, ErrGoAway
	}
	// Read peer setting OUTSIDE smu (lock order: psMu before smu in
	// applyPeerSettings, so we must not invert here).
	c.psMu.RLock()
	peerLimit, peerHas := lookupPeerSetting(c.peerSettings, frame.SettingMaxConcurrentStreams)
	c.psMu.RUnlock()
	limit := c.opts.Settings.MaxConcurrentStreams
	if peerHas && peerLimit < limit {
		limit = peerLimit
	}
	c.smu.Lock()
	if c.inflight >= limit {
		c.smu.Unlock()
		return nil, ErrTooManyStreams
	}
	s := c.allocStream(c.opts.StreamEventBuffer, int32(c.opts.Settings.InitialWindowSize))
	c.inflight++
	c.smu.Unlock()
	c.atomicStreamsOpened.Add(1)
	return s, nil
}

// allocStream returns a recycled *Stream if one with matching channel
// capacity is available, otherwise allocates fresh.
func (c *Conn) allocStream(eventBuf int, recvWindow int32) *Stream {
	if v := c.streamPool.Get(); v != nil {
		s := v.(*Stream)
		if cap(s.events) == eventBuf {
			s.w = c
			s.recvWindow = recvWindow
			s.released.Store(false) // new lifetime: re-arm Close idempotency guard
			return s
		}
		// Wrong capacity — discard; fall through to fresh allocation.
	}
	return newStream(0, eventBuf, c, recvWindow)
}

// Close sends a best-effort GOAWAY(NO_ERROR), closes the transport, and
// waits for the reader goroutine to drain. Idempotent under concurrent
// callers.
func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Wake any writers blocked in acquireSendCredits so they observe
	// the closed flag and bail out.
	c.fcOutMu.Lock()
	if c.fcOutCond != nil {
		c.fcOutCond.Broadcast()
	}
	c.fcOutMu.Unlock()
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
	c.fr.Close()
	return nil
}

// closeGoAwayDeadline bounds the GOAWAY write during Close so an
// unresponsive peer cannot block shutdown.
const closeGoAwayDeadline = 200 * time.Millisecond

// Shutdown performs a graceful connection close (RFC 7540 §6.8).
// It sends GOAWAY(lastClientStreamID, NO_ERROR) to inform the peer
// that no new streams will be opened, marks the conn as draining
// (so NewStream returns ErrConnDraining), and waits up to gracefulTimeout
// for all in-flight streams to complete naturally. After the timeout
// (or immediately if there are no in-flight streams), it falls through
// to the same logic as Close. Idempotent — calling Shutdown on an
// already-closed conn returns nil without side effects.
func (c *Conn) Shutdown(gracefulTimeout time.Duration) error {
	if c.closed.Load() {
		return nil
	}
	if !c.draining.CompareAndSwap(false, true) {
		// Already draining. Wait for the existing drain to finish,
		// then fall through to Close.
		select {
		case <-c.drainDone:
		case <-time.After(gracefulTimeout):
		}
		return c.Close()
	}
	// Send GOAWAY with our last issued client stream ID. The peer
	// will see this and stop opening new streams; existing streams
	// keep flowing.
	if dl, ok := c.transport.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = dl.SetWriteDeadline(time.Now().Add(closeGoAwayDeadline))
	}
	c.wmu.Lock()
	_ = c.fr.WriteGoAway(c.lastClientStreamID(), frame.ErrCodeNoError, nil)
	c.wmu.Unlock()
	// Wake any writers blocked in acquireSendCredits so they observe
	// the draining flag and surface ErrConnDraining to their callers.
	c.fcOutMu.Lock()
	if c.fcOutCond != nil {
		c.fcOutCond.Broadcast()
	}
	c.fcOutMu.Unlock()
	// If no in-flight streams, close immediately. Otherwise wait.
	if c.inflight == 0 {
		return c.Close()
	}
	timer := time.NewTimer(gracefulTimeout)
	defer timer.Stop()
	select {
	case <-c.drainDone:
	case <-timer.C:
	}
	return c.Close()
}

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
// Each field is loaded atomically; the snapshot is consistent
// per-field but not across fields (a high-throughput conn may produce
// counters that don't sum cleanly across the snapshot boundary).
func (c *Conn) Stats() ConnStats {
	return ConnStats{
		BytesSent:      c.atomicBytesSent.Load(),
		BytesReceived:  c.atomicBytesReceived.Load(),
		FramesSent:     c.atomicFramesSent.Load(),
		FramesReceived: c.atomicFramesReceived.Load(),
		StreamsOpened:  c.atomicStreamsOpened.Load(),
	}
}

// IsAlive reports whether the connection has neither been Closed nor
// received a GOAWAY frame from the peer. It is a cheap atomic check
// suitable for transport pools that need to decide whether to reuse
// or redial.
func (c *Conn) IsAlive() bool {
	return !c.closed.Load() && !c.goAwayReceived.Load()
}

// GoAwayReceived reports whether the peer has sent a GOAWAY frame.
// Used by upstream pools to distinguish CloseGoAway from CloseDead.
func (c *Conn) GoAwayReceived() bool {
	return c.goAwayReceived.Load()
}

// PeerMaxConcurrentStreams returns the peer-advertised
// SETTINGS_MAX_CONCURRENT_STREAMS, or 0 if the peer has not
// advertised a value. Callers that intend to gate stream
// allocation should treat 0 as "no peer cap" and fall back to
// their own local limit.
func (c *Conn) PeerMaxConcurrentStreams() int {
	c.psMu.RLock()
	defer c.psMu.RUnlock()
	v, ok := lookupPeerSetting(c.peerSettings, frame.SettingMaxConcurrentStreams)
	if !ok {
		return 0
	}
	return int(v)
}

// --- streamWriter implementation (called from *Stream).

func (c *Conn) writeHeadersWithPriority(_ context.Context, s *Stream, fields []hpack.HeaderField, endStream bool, prio *frame.Priority) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if s.id == 0 {
		// Seed the per-stream outbound flow-control window from the peer's
		// most recently observed SETTINGS_INITIAL_WINDOW_SIZE and register
		// the stream atomically under psMu (lock order psMu->smu->s.mu, per
		// NewStream's documented convention). Holding psMu across the seed +
		// insert makes it mutually exclusive with applyPeerSettings' merge +
		// retroactive delta, so this stream is never BOTH seeded at the new
		// value AND credited the delta — the previous split-lock window
		// over-credited the send window (RFC 7540 §6.9.2).
		c.psMu.RLock()
		initial := settingValue(c.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
		c.smu.Lock()
		s.id = c.nextID
		c.nextID += 2
		c.streams[s.id] = s
		s.mu.Lock()
		s.sendWindow = int32(initial)
		s.mu.Unlock()
		c.smu.Unlock()
		c.psMu.RUnlock()
	}
	buf := encBufPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	block := c.enc.EncodeBlock(*buf, fields)

	err := c.writeHeaderBlock(s.id, block, endStream, prio)

	*buf = block[:0]
	encBufPool.Put(buf)
	if err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}

// maxOutFrameSize returns the largest frame payload we may emit: the
// minimum of the peer's advertised SETTINGS_MAX_FRAME_SIZE and our own,
// floored at the RFC default (16384). Mirrors the bound used by writeData.
// Caller may hold c.wmu; this takes c.psMu (the established wmu→psMu order).
func (c *Conn) maxOutFrameSize() int {
	c.psMu.RLock()
	peerMax := settingValue(c.peerSettings, frame.SettingMaxFrameSize, 16384)
	c.psMu.RUnlock()
	maxFrame := int(peerMax)
	if ourMax := int(c.opts.Settings.MaxFrameSize); ourMax < maxFrame {
		maxFrame = ourMax
	}
	if maxFrame <= 0 {
		maxFrame = 16384
	}
	return maxFrame
}

// writeHeaderBlock emits the encoded HPACK block as a HEADERS frame
// followed by zero or more CONTINUATION frames when it exceeds one frame's
// payload budget (RFC 7540 §6.2 / §6.10). The caller MUST hold c.wmu so the
// HEADERS+CONTINUATION run is contiguous (RFC §6.10: no interleaving).
// END_STREAM and padding/priority ride the HEADERS frame only; END_HEADERS
// rides the final frame.
func (c *Conn) writeHeaderBlock(streamID uint32, block []byte, endStream bool, prio *frame.Priority) error {
	maxFrame := c.maxOutFrameSize()
	padLen := c.opts.Padding.ForHeaders()

	budget0 := maxFrame
	if padLen > 0 {
		budget0 -= 1 + int(padLen)
	}
	if prio != nil {
		budget0 -= 5
	}
	if budget0 <= 0 {
		budget0 = 1
	}

	if len(block) <= budget0 {
		return c.fr.WriteHeaders(frame.WriteHeadersParams{
			StreamID:      streamID,
			BlockFragment: block,
			EndHeaders:    true,
			EndStream:     endStream,
			PadLength:     padLen,
			Priority:      prio,
		})
	}

	if err := c.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      streamID,
		BlockFragment: block[:budget0],
		EndHeaders:    false,
		EndStream:     endStream,
		PadLength:     padLen,
		Priority:      prio,
	}); err != nil {
		return err
	}

	rest := block[budget0:]
	for len(rest) > 0 {
		n := len(rest)
		if n > maxFrame {
			n = maxFrame
		}
		last := n == len(rest)
		if err := c.fr.WriteContinuation(streamID, last, rest[:n]); err != nil {
			return err
		}
		rest = rest[n:]
	}
	return nil
}

func (c *Conn) writeData(ctx context.Context, s *Stream, p []byte, endStream bool) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	if s.id == 0 {
		// SendHeaders has not run; the stream has no on-wire identity.
		return ErrStreamClosed
	}
	// Chunk at the minimum of peer's advertised MAX_FRAME_SIZE and our
	// own (the framer's outgoing cap matches our advertised value).
	c.psMu.RLock()
	peerMax := settingValue(c.peerSettings, frame.SettingMaxFrameSize, 16384)
	c.psMu.RUnlock()
	ourMax := c.opts.Settings.MaxFrameSize
	maxFrame := int(peerMax)
	if int(ourMax) < maxFrame {
		maxFrame = int(ourMax)
	}
	if maxFrame <= 0 {
		maxFrame = 16384
	}
	// Pre-compute per-frame padding. When padding is disabled (padLen=0),
	// use WriteData (no padding overhead). When enabled, use WriteDataPadded
	// and account for the pad-length byte + padding bytes in frame size.
	padLen := c.opts.Padding.ForData()
	padOverhead := 0
	if padLen > 0 {
		padOverhead = 1 + int(padLen) // pad-length byte + padding bytes
	}
	effectiveMaxFrame := maxFrame
	if padOverhead > 0 && padOverhead < effectiveMaxFrame {
		effectiveMaxFrame -= padOverhead
	}
	// Empty DATA with END_STREAM is allowed and consumes no credit.
	if len(p) == 0 {
		if !endStream {
			return nil
		}
		c.wmu.Lock()
		defer c.wmu.Unlock()
		if padLen > 0 {
			if err := c.fr.WriteDataPadded(s.id, true, nil, padLen); err != nil {
				return err
			}
		} else {
			if err := c.fr.WriteData(s.id, true, nil); err != nil {
				return err
			}
		}
		c.bumpFramesSent()
		return nil
	}
	for len(p) > 0 {
		want := len(p)
		if want > effectiveMaxFrame {
			want = effectiveMaxFrame
		}
		n, err := c.acquireSendCredits(ctx, s, want)
		if err != nil {
			return err
		}
		last := endStream && n == len(p)
		c.wmu.Lock()
		if c.closed.Load() {
			c.wmu.Unlock()
			return ErrConnClosed
		}
		if padLen > 0 {
			if werr := c.fr.WriteDataPadded(s.id, last, p[:n], padLen); werr != nil {
				c.wmu.Unlock()
				return werr
			}
		} else {
			if werr := c.fr.WriteData(s.id, last, p[:n]); werr != nil {
				c.wmu.Unlock()
				return werr
			}
		}
		c.bumpFramesSent()
		c.wmu.Unlock()
		p = p[n:]
	}
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

// writeRSTStreamBestEffort sends RST_STREAM under a short write deadline so
// the fire-and-forget goroutine spawned by Stream.push on event-channel
// overflow cannot block indefinitely on a stuck transport (F-P0-04). Write
// errors are silently ignored: RST_STREAM is best-effort per RFC 7540 §8.1.
// The write deadline is cleared after the write so subsequent normal writes
// on this connection are not affected.
func (c *Conn) writeRSTStreamBestEffort(s *Stream, code frame.ErrCode) {
	const rstTimeout = 5 * time.Second
	if s.id == 0 {
		c.releaseUnassignedInflight(s)
		return
	}
	if c.closed.Load() {
		return
	}
	type deadliner interface{ SetWriteDeadline(time.Time) error }
	c.wmu.Lock()
	if dl, ok := c.transport.(deadliner); ok {
		_ = dl.SetWriteDeadline(time.Now().Add(rstTimeout))
	}
	if err := c.fr.WriteRSTStream(s.id, code); err == nil {
		c.bumpFramesSent()
	}
	if dl, ok := c.transport.(deadliner); ok {
		_ = dl.SetWriteDeadline(time.Time{})
	}
	c.wmu.Unlock()
	c.releaseInflight(s.id)
}

// onDataReceived debits both the connection-level and stream-level recv
// windows for a DATA frame whose total payload is `length` bytes (RFC 7540
// §6.9.1: includes the data, the pad-length octet, and the padding). The
// connection window is accounted FIRST and unconditionally — every received
// DATA frame counts against it regardless of the per-stream outcome, so a
// stream reset does not leak the peer's connection send window. Returns a
// *ConnError to abort the connection (peer overflowed the connection window)
// or a non-fatal *StreamError to reset just the offending stream. On success
// it accumulates a refund counter per scope and emits a WINDOW_UPDATE once a
// counter crosses recvWindowRefundThreshold.
func (c *Conn) onDataReceived(s *Stream, length uint32) error {
	debit := int32(length)

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

	s.mu.Lock()
	s.recvWindow -= debit
	streamOverrun := s.recvWindow < 0
	streamRefund := uint32(0)
	if !streamOverrun {
		s.recvRefundPending += length
		if s.recvRefundPending >= recvWindowRefundThreshold {
			streamRefund = s.recvRefundPending
			s.recvRefundPending = 0
			s.recvWindow += int32(streamRefund)
		}
	}
	s.mu.Unlock()

	if streamOverrun {
		// Reset only this stream; the connection — already accounted above —
		// survives (readerLoop turns this *StreamError into an RST_STREAM).
		// Still flush any pending connection refund so the peer's connection
		// window is replenished.
		if connRefund > 0 {
			if err := c.writeWindowUpdate(0, connRefund); err != nil {
				return err
			}
		}
		return &StreamError{StreamID: s.id, Code: frame.ErrCodeFlowControlError}
	}

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

// acquireSendCredits blocks until both the per-stream and the
// connection-level outbound send windows have at least one byte of
// credit, then atomically deducts up to `want` bytes from each and
// returns the number actually granted. Returns ctx.Err() if cancelled
// or ErrConnClosed if the connection drops while waiting.
//
// A context-cancellation watcher (context.AfterFunc) is registered only
// when we actually need to block in cond.Wait, not on every call. This
// avoids a goroutine + channel allocation per write chunk (F-P1-04).
func (c *Conn) acquireSendCredits(ctx context.Context, s *Stream, want int) (int, error) {
	if want <= 0 {
		return 0, nil
	}
	c.fcOutMu.Lock()
	defer c.fcOutMu.Unlock()
	var stopWatcher func() bool
	defer func() {
		if stopWatcher != nil {
			stopWatcher()
		}
	}()
	for {
		if c.closed.Load() {
			return 0, ErrConnClosed
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		s.mu.Lock()
		streamWin := s.sendWindow
		s.mu.Unlock()
		connWin := c.peerConnSendWindow
		avail := streamWin
		if connWin < avail {
			avail = connWin
		}
		if avail > 0 {
			n := int32(want)
			if n > avail {
				n = avail
			}
			c.peerConnSendWindow -= n
			s.mu.Lock()
			s.sendWindow -= n
			s.mu.Unlock()
			return int(n), nil
		}
		// Register the context watcher only on the first actual block so
		// the common fast-path (window has credit) pays no allocation cost.
		if stopWatcher == nil {
			stopWatcher = context.AfterFunc(ctx, func() {
				c.fcOutMu.Lock()
				c.fcOutCond.Broadcast()
				c.fcOutMu.Unlock()
			})
		}
		c.fcOutCond.Wait()
	}
}

// onWindowUpdate replenishes the appropriate outbound send window and
// wakes any writers blocked in acquireSendCredits. RFC 7540 §6.9.1
// says a flow-control window must not exceed 2^31-1; if the increment
// would push us past that, the stream is RST'd or the connection is
// closed with FLOW_CONTROL_ERROR depending on scope.
func (c *Conn) onWindowUpdate(streamID uint32, increment uint32) error {
	const maxWindow = int32(1<<31 - 1)
	if streamID == 0 {
		c.fcOutMu.Lock()
		newVal := int64(c.peerConnSendWindow) + int64(increment)
		if newVal > int64(maxWindow) {
			c.fcOutMu.Unlock()
			return &ConnError{Code: frame.ErrCodeFlowControlError, Reason: "WINDOW_UPDATE overflowed connection send window"}
		}
		c.peerConnSendWindow = int32(newVal)
		c.fcOutCond.Broadcast()
		c.fcOutMu.Unlock()
		return nil
	}
	s := c.lookupStream(streamID)
	if s == nil {
		return nil // unknown / closed stream — peer chatter
	}
	s.mu.Lock()
	newVal := int64(s.sendWindow) + int64(increment)
	if newVal > int64(maxWindow) {
		s.mu.Unlock()
		return &StreamError{StreamID: streamID, Code: frame.ErrCodeFlowControlError}
	}
	s.sendWindow = int32(newVal)
	s.mu.Unlock()
	c.fcOutMu.Lock()
	c.fcOutCond.Broadcast()
	c.fcOutMu.Unlock()
	return nil
}

// applyInitialPeerSettings is called once after the handshake returns
// the peer's first SETTINGS frame. There are no open streams yet, so
// only the connection-scoped knobs (HPACK table size) need to be
// propagated; the per-stream INITIAL_WINDOW_SIZE will be picked up
// when the first stream calls writeHeaders.
func (c *Conn) applyInitialPeerSettings(peer frame.SettingsParams) {
	for i := 0; i < peer.N; i++ {
		p := peer.Pairs[i]
		if p.ID == frame.SettingHeaderTableSize {
			c.enc.SetMaxDynamicTableSize(p.Value)
		}
	}
}

// applyPeerSettings handles a non-ACK SETTINGS frame received after
// the handshake. It merges each pair into c.peerSettings, applies the
// side effects (HPACK encoder resize, retroactive INITIAL_WINDOW_SIZE
// delta on every open stream, updated MAX_FRAME_SIZE picked up by the
// next writeData call), and returns a typed ConnError if the
// INITIAL_WINDOW_SIZE delta would push any stream's send window past
// 2^31-1 (RFC 7540 §6.9.2).
func (c *Conn) applyPeerSettings(s frame.SettingsParams) error {
	const maxWindow = int64(1<<31 - 1)

	// Merge the settings AND apply the retroactive INITIAL_WINDOW_SIZE delta to
	// existing streams atomically under psMu (lock order psMu->smu->s.mu),
	// making the seed/delta mutually exclusive with writeHeadersWithPriority so
	// a freshly opened stream is seeded EITHER old+delta OR new-and-skipped,
	// never both (RFC 7540 §6.9.2). In the same pass reject an out-of-range
	// INITIAL_WINDOW_SIZE as FLOW_CONTROL_ERROR (§6.5.2) before it is stored (it
	// would later seed a negative int32 send window). The HPACK encoder resize
	// is captured here but applied below under wmu (not psMu) — the same mutex
	// EncodeBlock takes — so it cannot race an in-flight header encode.
	var newTableSize uint32
	var haveTableSize bool
	c.psMu.Lock()
	oldInitial := settingValue(c.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
	for i := 0; i < s.N; i++ {
		p := s.Pairs[i]
		switch p.ID {
		case frame.SettingInitialWindowSize:
			if int64(p.Value) > maxWindow {
				c.psMu.Unlock()
				return &ConnError{Code: frame.ErrCodeFlowControlError, Reason: "SETTINGS_INITIAL_WINDOW_SIZE exceeds 2^31-1"}
			}
		case frame.SettingHeaderTableSize:
			newTableSize, haveTableSize = p.Value, true
		}
		setPeerSetting(&c.peerSettings, p.ID, p.Value)
	}
	newInitial := settingValue(c.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
	changed := newInitial != oldInitial

	var overflow bool
	if changed {
		delta := int64(newInitial) - int64(oldInitial)
		c.smu.Lock()
		for _, st := range c.streams {
			st.mu.Lock()
			newWin := int64(st.sendWindow) + delta
			if newWin > maxWindow {
				st.mu.Unlock()
				overflow = true
				break
			}
			st.sendWindow = int32(newWin)
			st.mu.Unlock()
		}
		c.smu.Unlock()
	}
	c.psMu.Unlock()

	if overflow {
		return &ConnError{Code: frame.ErrCodeFlowControlError, Reason: "SETTINGS_INITIAL_WINDOW_SIZE delta overflowed a stream send window"}
	}
	// Apply the HPACK encoder dynamic-table resize under wmu (shared with
	// EncodeBlock) so it cannot race an in-flight header encode and emit a torn
	// dynamic-table-size update or desync from the peer decoder (which the peer
	// would reject as a fatal COMPRESSION_ERROR).
	if haveTableSize {
		c.wmu.Lock()
		c.enc.SetMaxDynamicTableSize(newTableSize)
		c.wmu.Unlock()
	}
	if changed {
		// Wake any writers blocked on send credit — the delta may have just
		// unblocked them.
		c.fcOutMu.Lock()
		c.fcOutCond.Broadcast()
		c.fcOutMu.Unlock()
	}
	return nil
}

// setPeerSetting merges a single SETTINGS pair into params, replacing
// any prior value for the same ID. The 16-pair array is large enough
// for every defined setting in RFC 7540 §6.5.2 (IDs 0x1..0x6).
func setPeerSetting(params *frame.SettingsParams, id frame.SettingID, val uint32) {
	for i := 0; i < params.N; i++ {
		if params.Pairs[i].ID == id {
			params.Pairs[i].Value = val
			return
		}
	}
	if params.N < len(params.Pairs) {
		params.Pairs[params.N] = frame.SettingPair{ID: id, Value: val}
		params.N++
	}
}

// writeSettingsAck emits a SETTINGS frame with ACK=1 in response to a
// peer SETTINGS frame (RFC 7540 §6.5.3). Called from the reader loop;
// takes wmu briefly.
func (c *Conn) writeSettingsAck() error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WriteSettingsAck(); err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}

// onGoAwayReceived stores the peer's GOAWAY state and resets every
// stream whose id is strictly greater than lastStreamID — those
// streams the peer never accepted (RFC 7540 §6.8). Streams with id
// ≤ lastStreamID continue. Wakes writers blocked on send credit so
// they observe the GOAWAY-induced flow termination via subsequent
// SendData calls (which still go through, until the peer closes the
// transport).
func (c *Conn) onGoAwayReceived(lastStreamID uint32, _ frame.ErrCode) {
	c.goAwayLastStreamID.Store(lastStreamID)
	c.goAwayReceived.Store(true)

	c.smu.Lock()
	victims := make([]*Stream, 0, len(c.streams))
	for id, s := range c.streams {
		if id > lastStreamID {
			victims = append(victims, s)
		}
	}
	c.smu.Unlock()

	for _, s := range victims {
		// Surface the cancellation as REFUSED_STREAM — the peer never
		// processed our HEADERS, so it is safe for the caller to retry
		// on a fresh connection.
		select {
		case s.events <- StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeRefusedStream, EndStream: true}:
		default:
			s.signalReset(frame.ErrCodeRefusedStream)
		}
		s.markRemoteEnd()
		s.mu.Lock()
		s.localEnded = true
		s.mu.Unlock()
		c.markStreamDone(s.id)
	}

	c.fcOutMu.Lock()
	c.fcOutCond.Broadcast()
	c.fcOutMu.Unlock()
}

// storeOrigins saves origins received via an ORIGIN frame (RFC 8336 §3).
func (c *Conn) storeOrigins(origins []string) {
	c.originsMu.Lock()
	c.origins = origins
	c.originsMu.Unlock()
}

// storeAltSvc saves ALTSVC entries received via an ALTSVC frame (RFC 7838 §4).
func (c *Conn) storeAltSvc(entries []frame.AltSvcEntry) {
	c.altSvcMu.Lock()
	c.altSvcEntries = entries
	c.altSvcMu.Unlock()
}

// AltSvcEntries returns the server's advertised alternative-service
// entries from the most recent ALTSVC frame (RFC 7838 §4). Returns
// nil if no ALTSVC frame was received. The returned slice is a copy.
func (c *Conn) AltSvcEntries() []frame.AltSvcEntry {
	c.altSvcMu.RLock()
	defer c.altSvcMu.RUnlock()
	if len(c.altSvcEntries) == 0 {
		return nil
	}
	dup := make([]frame.AltSvcEntry, len(c.altSvcEntries))
	copy(dup, c.altSvcEntries)
	return dup
}

// Origins returns the server's advertised origin list from the ORIGIN
// frame (RFC 8336 §3). Returns nil if no ORIGIN frame was received.
// The returned slice is a copy; callers may modify it freely.
func (c *Conn) Origins() []string {
	c.originsMu.RLock()
	defer c.originsMu.RUnlock()
	if len(c.origins) == 0 {
		return nil
	}
	dup := make([]string, len(c.origins))
	copy(dup, c.origins)
	return dup
}

// CanCoalesce reports whether the server has advertised (via ORIGIN
// frame, RFC 8336) that it is authoritative for the given origin.
// The origin must be in "scheme://host[:port]" form (e.g.
// "https://example.com" or "https://example.com:8443").
// If no ORIGIN frame was received, CanCoalesce returns false.
func (c *Conn) CanCoalesce(origin string) bool {
	c.originsMu.RLock()
	defer c.originsMu.RUnlock()
	for _, o := range c.origins {
		if o == origin {
			return true
		}
	}
	return false
}

// ConnectProtocolSupported reports whether the server advertised
// SETTINGS_ENABLE_CONNECT_PROTOCOL=1 (RFC 8441 §3), allowing the
// client to send extended-CONNECT requests with a :protocol
// pseudo-header (e.g. for WebSockets over HTTP/2).
func (c *Conn) ConnectProtocolSupported() bool {
	c.psMu.RLock()
	defer c.psMu.RUnlock()
	return settingValue(c.peerSettings, frame.SettingEnableConnectProtocol, 0) == 1
}

// writePingAck emits a PING frame with ACK=1 and the peer's payload
// echoed back (RFC 7540 §6.7).
func (c *Conn) writePingAck(payload [8]byte) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WritePing(true, payload); err != nil {
		return err
	}
	c.bumpFramesSent()
	return nil
}

// deliverPingAck signals any Ping call waiting for payload.
// Unsolicited ACKs (no matching waiter) are silently ignored.
func (c *Conn) deliverPingAck(payload [8]byte) {
	c.pingMu.Lock()
	ch, ok := c.pingWaiters[payload]
	if ok {
		delete(c.pingWaiters, payload)
	}
	c.pingMu.Unlock()
	if ok {
		close(ch)
	}
}

// Ping sends a PING frame and blocks until the peer's ACK arrives,
// returning the round-trip time. Returns ErrConnClosed if the
// connection is already closed or closes before the ACK arrives.
// Returns ctx.Err() if the context expires or is cancelled first.
// Any other error indicates a write failure on the underlying transport.
func (c *Conn) Ping(ctx context.Context) (time.Duration, error) {
	if c.closed.Load() {
		return 0, ErrConnClosed
	}

	n := c.pingCounter.Add(1)
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], n)

	ch := make(chan struct{})
	c.pingMu.Lock()
	c.pingWaiters[payload] = ch
	c.pingMu.Unlock()

	c.wmu.Lock()
	if c.closed.Load() {
		c.wmu.Unlock()
		c.pingMu.Lock()
		delete(c.pingWaiters, payload)
		c.pingMu.Unlock()
		return 0, ErrConnClosed
	}
	start := time.Now()
	err := c.fr.WritePing(false, payload)
	if err == nil {
		c.bumpFramesSent()
	}
	c.wmu.Unlock()

	if err != nil {
		c.pingMu.Lock()
		delete(c.pingWaiters, payload)
		c.pingMu.Unlock()
		return 0, err
	}

	select {
	case <-ch:
		return time.Since(start), nil
	case <-ctx.Done():
		c.pingMu.Lock()
		delete(c.pingWaiters, payload)
		c.pingMu.Unlock()
		return 0, ctx.Err()
	case <-c.readerDone:
		c.pingMu.Lock()
		delete(c.pingWaiters, payload)
		c.pingMu.Unlock()
		return 0, ErrConnClosed
	}
}

// keepaliveLoop sends a PING every interval. If the ACK does not
// arrive within the same interval the connection is closed.
// The loop exits when the connection closes (readerDone is closed).
func (c *Conn) keepaliveLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if c.goAwayReceived.Load() {
				return
			}
			pingTimeout := c.opts.KeepaliveTimeout
			if pingTimeout == 0 {
				pingTimeout = interval * 5
				if pingTimeout < 5*time.Second {
					pingTimeout = 5 * time.Second
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
			_, err := c.Ping(ctx)
			cancel()
			if err != nil {
				_ = c.Close()
				return
			}
		case <-c.readerDone:
			// Reader exited due to transport error or remote close; mark the
			// connection closed so IsAlive() returns false.
			_ = c.Close()
			return
		}
	}
}

func (c *Conn) bumpFramesSent()     { c.atomicFramesSent.Add(1) }
func (c *Conn) bumpFramesReceived() { c.atomicFramesReceived.Add(1) }

// readerLoop owns frame.ReadFrame for the lifetime of the connection.
// On a typed *ConnError, emits GOAWAY with the error code before
// shutting down streams (RFC 7540 §5.4.1). I/O errors and EOF skip
// GOAWAY (transport already gone).
func (c *Conn) readerLoop() {
	defer close(c.readerDone)
	h := newConnHandler(c, c.dec)
	h.raiseMaxHeaderBytes(c.opts.Settings.MaxHeaderListSize)
	for {
		_, err := c.fr.ReadFrame(context.Background(), h)
		if err != nil {
			// A *StreamError is non-fatal (RFC 7540 §5.4.2): reset only the
			// offending stream and keep the connection — and every other
			// in-flight stream — alive. onDataReceived / onWindowUpdate
			// return this on a single stream's flow-control overrun. Only
			// *ConnError and I/O errors tear the whole connection down.
			var se *StreamError
			if errors.As(err, &se) {
				// push() delivers EventReset to the caller; on events-channel
				// overflow it already fires a best-effort RST_STREAM and releases
				// the slot (returns false), so send our own RST only when push
				// enqueued cleanly, avoiding a duplicate frame. rstStream
				// releases the inflight slot via writeRSTStream.
				if s := c.lookupStream(se.StreamID); s == nil || s.push(StreamEvent{Type: EventReset, RSTCode: se.Code, EndStream: true}) {
					_ = c.rstStream(se.StreamID, se.Code)
				}
				continue
			}
			c.emitConnGoAwayIfTyped(err)
			c.shutdownStreams(err)
			return
		}
	}
}

// emitConnGoAwayIfTyped writes a best-effort GOAWAY when the reader
// loop terminates via a *ConnError so the peer learns the diagnosis
// (RFC 7540 §5.4.1). Bounded write deadline avoids wedging on an
// unresponsive transport.
func (c *Conn) emitConnGoAwayIfTyped(err error) {
	var ce *ConnError
	if !errors.As(err, &ce) {
		return
	}
	if dl, ok := c.transport.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = dl.SetWriteDeadline(time.Now().Add(closeGoAwayDeadline))
	}
	c.wmu.Lock()
	_ = c.fr.WriteGoAway(c.lastClientStreamID(), ce.Code, nil)
	c.wmu.Unlock()
}

func (c *Conn) shutdownStreams(reason error) {
	c.smu.Lock()
	defer c.smu.Unlock()
	for _, s := range c.streams {
		select {
		case s.events <- StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeInternalError, EndStream: true}:
		default:
			s.signalReset(frame.ErrCodeInternalError)
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
	// Wake Shutdown when the conn is fully drained.
	if c.draining.Load() && c.inflight == 0 {
		select {
		case <-c.drainDone:
			// already closed
		default:
			close(c.drainDone)
		}
	}
}

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
