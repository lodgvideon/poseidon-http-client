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

	// pingMu guards pingWaiters. pingCounter produces unique payloads.
	pingMu      sync.Mutex
	pingWaiters map[[8]byte]chan struct{}
	pingCounter atomic.Uint64

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
		pingWaiters:        make(map[[8]byte]chan struct{}),
		connRecvWindow:     int32(connInitialRecvWindow),
		peerConnSendWindow: int32(connInitialRecvWindow),
	}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
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
	s := c.allocStream(c.opts.StreamEventBuffer, int32(c.connRecvWindow))
	s.id = id
	c.smu.Lock()
	c.streams[id] = s
	c.smu.Unlock()
	return s
}

// initialRecvWindow returns the peer's INITIAL_WINDOW_SIZE setting.
func (c *Conn) initialRecvWindow() int32 {
	c.psMu.RLock()
	initial := settingValue(c.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
	c.psMu.RUnlock()
	return int32(initial)
}

// peerSettingsRLocked calls f with the peer settings under RLock.
func (c *Conn) peerSettingsRLocked(f func(s frame.SettingsParams)) {
	c.psMu.RLock()
	defer c.psMu.RUnlock()
	f(c.peerSettings)
}

// rstStream sends a RST_STREAM frame for the given stream ID.
func (c *Conn) rstStream(id uint32, code frame.ErrCode) error {
	return c.writeRSTStream(&Stream{id: id}, code)
}

// NewStream allocates a new stream. B.1 enforces at most one in-flight
// stream per Conn; subsequent calls return ErrTooManyStreams until the
// active stream completes.
// NewStream allocates an in-flight slot for a new outbound stream. The
// stream's HTTP/2 ID is assigned later, when SendHeaders writes the
// first HEADERS frame under the writer mutex; this preserves the
// monotonic-id ordering required by RFC 7540 §5.1.1 even with many
// concurrent NewStream callers. Returns ErrTooManyStreams when the
// in-flight count has reached min(local MaxConcurrentStreams,
// peer-advertised SETTINGS_MAX_CONCURRENT_STREAMS).
// LookupStream returns the stream with the given ID, or (nil, false) if
// no such stream exists. This is primarily used to access server-pushed
// streams after receiving an EventPushPromise on the parent stream.
func (c *Conn) LookupStream(id uint32) (*Stream, bool) {
	c.smu.Lock()
	defer c.smu.Unlock()
	s, ok := c.streams[id]
	return s, ok
}

func (c *Conn) NewStream(_ context.Context) (*Stream, error) {
	if c.closed.Load() {
		return nil, ErrConnClosed
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

func (c *Conn) writeHeaders(_ context.Context, s *Stream, fields []hpack.HeaderField, endStream bool) error {
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
		// Seed the per-stream outbound flow-control window from the
		// peer's most recently observed SETTINGS_INITIAL_WINDOW_SIZE
		// (RFC 7540 §6.9.2; default 65535).
		c.psMu.RLock()
		initial := settingValue(c.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
		c.psMu.RUnlock()
		s.mu.Lock()
		s.sendWindow = int32(initial)
		s.mu.Unlock()
	}
	buf := encBufPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	block := c.enc.EncodeBlock(*buf, fields)
	err := c.fr.WriteHeaders(frame.WriteHeadersParams{
		StreamID:      s.id,
		BlockFragment: block,
		EndHeaders:    true,
		EndStream:     endStream,
		PadLength:     c.opts.Padding.ForHeaders(),
	})
	*buf = block[:0]
	encBufPool.Put(buf)
	if err != nil {
		return err
	}
	c.bumpFramesSent()
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

// acquireSendCredits blocks until both the per-stream and the
// connection-level outbound send windows have at least one byte of
// credit, then atomically deducts up to `want` bytes from each and
// returns the number actually granted. Returns ctx.Err() if cancelled
// or ErrConnClosed if the connection drops while waiting.
func (c *Conn) acquireSendCredits(ctx context.Context, s *Stream, want int) (int, error) {
	if want <= 0 {
		return 0, nil
	}
	// Spawn a watchdog that broadcasts when ctx is cancelled, so the
	// cond.Wait below wakes up even though no WINDOW_UPDATE arrived.
	watchdog := make(chan struct{})
	defer close(watchdog)
	go func() {
		select {
		case <-ctx.Done():
			c.fcOutMu.Lock()
			c.fcOutCond.Broadcast()
			c.fcOutMu.Unlock()
		case <-watchdog:
		}
	}()

	c.fcOutMu.Lock()
	defer c.fcOutMu.Unlock()
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

	c.psMu.Lock()
	oldInitial := settingValue(c.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
	for i := 0; i < s.N; i++ {
		p := s.Pairs[i]
		setPeerSetting(&c.peerSettings, p.ID, p.Value)
	}
	newInitial := settingValue(c.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
	c.psMu.Unlock()

	for i := 0; i < s.N; i++ {
		p := s.Pairs[i]
		switch p.ID {
		case frame.SettingHeaderTableSize:
			c.enc.SetMaxDynamicTableSize(p.Value)
		case frame.SettingInitialWindowSize:
			// retroactively re-apply to all streams below
		}
	}

	if newInitial != oldInitial {
		delta := int64(newInitial) - int64(oldInitial)
		c.smu.Lock()
		victims := make([]*Stream, 0, len(c.streams))
		for _, st := range c.streams {
			victims = append(victims, st)
		}
		c.smu.Unlock()

		for _, st := range victims {
			st.mu.Lock()
			newWin := int64(st.sendWindow) + delta
			if newWin > maxWindow {
				st.mu.Unlock()
				return &ConnError{Code: frame.ErrCodeFlowControlError, Reason: "SETTINGS_INITIAL_WINDOW_SIZE delta overflowed a stream send window"}
			}
			st.sendWindow = int32(newWin)
			st.mu.Unlock()
		}

		// Wake any writers blocked on send credit — the delta may
		// have just unblocked them.
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

func (c *Conn) bumpFramesSent() { c.atomicFramesSent.Add(1) }

// readerLoop owns frame.ReadFrame for the lifetime of the connection.
// On a typed *ConnError, emits GOAWAY with the error code before
// shutting down streams (RFC 7540 §5.4.1). I/O errors and EOF skip
// GOAWAY (transport already gone).
func (c *Conn) readerLoop() {
	defer close(c.readerDone)
	h := newConnHandler(c, c.dec)
	for {
		_, err := c.fr.ReadFrame(context.Background(), h)
		if err != nil {
			c.emitConnGoAwayIfTyped(err)
			c.shutdownStreams(err)
			return
		}
		c.atomicFramesReceived.Add(1)
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
