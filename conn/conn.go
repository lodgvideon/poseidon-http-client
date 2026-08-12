package conn

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// readBufferSize is the size of the buffered reader wrapping the transport on
// the receive path. The Framer reads each frame as a 9-byte header read plus a
// payload read; over an unbuffered transport those are two read(2) syscalls per
// frame. A buffered reader lets one socket read drain many frames, which is the
// dominant syscall on the response-receive hot path for h2c/plaintext links.
// (Over TLS, crypto/tls already buffers a whole record per syscall, so the win
// there is smaller.) 16 KiB matches the default max frame size: it batches the
// per-frame header reads and small control/response frames, while payloads at
// or above this size pass through bufio's direct-read fast path without an
// extra copy.
const readBufferSize = 16 * 1024

// writeBufferSize is the size of the buffered writer wrapping the transport on
// the send path. The Framer emits each DATA/HEADERS frame as a 9-byte header
// Write followed by a separate payload Write; over an unbuffered transport (and
// especially over TLS, where each Write becomes its own record + syscall) that
// is two syscalls per frame. Wrapping the transport writer in a bufio.Writer
// lets the header and payload coalesce into one flush — one syscall per frame.
// The buffer is flushed under wmu before releasing the write lock in every
// frame-writing method, so a buffered frame is always on the wire before the
// writer blocks (avoiding a deadlock where the peer never sees the frame). The
// buffer is not goroutine-safe; wmu already serializes all writers to it.
// 16 KiB matches the default max frame size.
const writeBufferSize = 16 * 1024

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
//
// # Lock ordering
//
// Conn has eight mutexes, plus each Stream's own mu. To avoid deadlock a
// goroutine that needs more than one at a time MUST acquire them left-to-right
// in the order below, and MUST NOT acquire a lock that sits to the left of one
// it already holds:
//
//	wmu → psMu → smu → fcOutMu → Stream.mu
//
// That total order is a linearization of the nestings that actually occur in
// this package (traced across every Lock/RLock site):
//
//   - wmu → psMu → smu → Stream.mu: writeHeadersWithPriority, holding wmu,
//     assigns a first-time stream's id and seeds its send window from the peer's
//     SETTINGS_INITIAL_WINDOW_SIZE (deferred id allocation preserves the RFC 7540
//     §5.1.1 monotonic-id order across concurrent openers).
//   - psMu → smu → Stream.mu: applyPeerSettings applies a retroactive
//     SETTINGS_INITIAL_WINDOW_SIZE delta to every open stream (§6.9.2). It takes
//     wmu — for the HPACK-encoder resize — only AFTER releasing psMu, so wmu
//     stays strictly above psMu rather than nesting under it.
//   - wmu → smu → Stream.mu: writeRSTStream, holding wmu, returns the stream's
//     inflight slot via releaseInflight; releaseInflight and markStreamDone are
//     the smu → Stream.mu nesting.
//   - fcOutMu → Stream.mu: acquireSendCredits debits the per-stream and the
//     connection outbound send windows together (§6.9). fcOutMu never nests with
//     wmu, psMu, or smu; it shares only Stream.mu, the innermost lock.
//
// Stream.mu is always innermost: Stream methods (SendHeaders, SendData, Close,
// push) release it before calling any Conn write or registry method, so it never
// reaches back up the chain.
//
// fcMu, pingMu, originsMu, and altSvcMu are leaf locks — each is only ever held
// on its own, never together with another of these mutexes or with each other,
// so they are unordered. (onDataReceived takes fcMu and Stream.mu sequentially,
// not nested; the outbound Ping registers its waiter under pingMu and releases
// it before taking wmu, while the inbound writePingAck takes only wmu and
// deliverPingAck only pingMu — so pingMu and wmu are never held together.)
type Conn struct {
	transport net.Conn
	fr        *frame.Framer
	// wb is the buffered writer wrapping the transport that fr writes into.
	// Flushed under wmu before every wmu release in a frame-writing method.
	// Not goroutine-safe; wmu serializes all access.
	wb   *bufio.Writer
	enc  *hpack.Encoder
	dec  *hpack.Decoder
	opts ConnOptions

	// peerSettings is the most recently observed server SETTINGS.
	// Guarded by psMu: written by handshake / connHandler.OnSettings,
	// read by writeData (chunking decision) and writeHeaders (initial
	// per-stream send-window seed).
	psMu         sync.RWMutex
	peerSettings frame.SettingsParams

	wmu sync.Mutex // serializes all writes to fr

	// wbatch implements the opt-in group-commit write optimization and owns all
	// group-commit state (see writeBatcher). Its cond is locked by wmu. Left nil
	// on hand-constructed Conns; every method is nil-safe, so a nil batcher
	// behaves as if group-commit were off.
	wbatch *writeBatcher

	// dvSegs is the reusable segment scratch for vectored DATA writes (datav.go).
	// Guarded by wmu, which is exactly the section it is used in, so its capacity
	// survives between frames without a per-frame allocation. emitDataV nils the
	// elements before releasing wmu: reslicing alone would leave this Conn holding
	// the last message's backing array for as long as it sits idle in a pool.
	dvSegs [][]byte

	smu     sync.Mutex // guards next stream id and streams map
	nextID  uint32
	streams map[uint32]*Stream
	// inflight counts streams *we* initiated (NewStream) that have not
	// been released. SETTINGS_MAX_CONCURRENT_STREAMS is directional (RFC
	// 7540 §5.1.2, §6.5.2): the peer's advertised value limits the streams
	// we open, so a server-pushed stream — which the server initiated —
	// must never be counted here. pushInflight is its counterpart.
	inflight uint32
	// pushInflight counts registered server-initiated (pushed) streams.
	// Bounded by the value *we* advertise in SETTINGS_MAX_CONCURRENT_STREAMS,
	// which per §6.5.2 is precisely "the number of streams that the sender
	// permits the receiver to create". Kept separate from inflight so a
	// pushing server can neither starve nor inflate our own stream gate.
	pushInflight uint32
	// lastPromisedID is the highest promised stream id accepted from a
	// PUSH_PROMISE. §5.1.1 requires a new id to exceed every id the sender
	// has opened or reserved, so promised ids must strictly increase; §6.6
	// makes a violation a connection error of type PROTOCOL_ERROR.
	lastPromisedID uint32

	// fcMu guards the connection-level recv window. The corresponding
	// per-stream window lives on Stream and is guarded by Stream.mu.
	fcMu              sync.Mutex
	connRecvWindow    int32  // bytes the peer can still send to us at the conn level (RFC 7540 §6.9.1)
	connRefundPending uint32 // bytes consumed but not yet returned via WINDOW_UPDATE(stream=0)

	// connRecvTarget and streamRecvTarget are the window sizes each refund
	// restores its scope to. They start at the windows already in effect, so
	// with auto-tuning off a refund returns exactly what was spent and nothing
	// about the flow-control behaviour changes. recvWindowTuner raises them, and
	// nothing ever lowers them — WINDOW_UPDATE can only add.
	//
	// Atomics rather than fields under fcMu because the per-stream target is
	// read while holding Stream.mu and the connection one while holding fcMu; a
	// scalar atomic composes with both without adding a lock-order edge.
	connRecvTarget   atomic.Uint32
	streamRecvTarget atomic.Uint32
	// tuner samples the bandwidth-delay product and raises the two targets. nil
	// when ConnOptions.AutoTuneRecvWindow is off. Touched only by the reader
	// goroutine — see the concurrency note in windowtuner.go.
	tuner *recvWindowTuner

	// fcOutMu guards the outbound (peer-advertised) connection-level
	// send window and is the locker for fcOutCond. peerConnSendWindow
	// starts at 65535 (RFC §6.9.2 fixes this at handshake regardless
	// of SETTINGS_INITIAL_WINDOW_SIZE) and is replenished by inbound
	// WINDOW_UPDATE(stream=0). fcOutCond.Broadcast wakes writers
	// blocked in acquireSendCredits.
	fcOutMu            sync.Mutex
	fcOutCond          *sync.Cond
	peerConnSendWindow int32
	// readerGone is set (under fcOutMu) when the reader goroutine exits on a
	// transport error, and fcOutCond is broadcast, so a writer parked in
	// acquireSendCredits wakes and bails instead of waiting for a WINDOW_UPDATE
	// that can never arrive on a dead connection.
	readerGone bool

	// goAwayReceived flags that the peer has sent GOAWAY (RFC 7540
	// §6.8). New NewStream calls return ErrGoAway; existing streams
	// whose id is ≤ goAwayLastStreamID continue.
	goAwayReceived     atomic.Bool
	goAwayLastStreamID atomic.Uint32
	// goAwaySentLast is the last-stream-id advertised in the last GOAWAY we
	// SENT (goAwayNoneSent until the first). RFC 9113 §6.8 forbids a later
	// GOAWAY from advertising a larger id, so each send clamps to this.
	goAwaySentLast atomic.Uint32

	closed     atomic.Bool
	readerDone chan struct{}

	// readerExited mirrors "readerDone is closed" as an atomic. IsAlive runs once
	// per pooled connection per request, and a channel select is far too
	// expensive at that rate — see http3.Client.dead, where a profile at 4k
	// connections put the equivalent select at 19.6% of all CPU. Nothing waits on
	// this; readerDone remains the thing to block on.
	readerExited atomic.Bool

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
	wb := bufio.NewWriterSize(transport, writeBufferSize)
	c := &Conn{
		transport:          transport,
		wb:                 wb,
		fr:                 frame.NewFramer(wb, bufio.NewReaderSize(transport, readBufferSize)),
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
	// The targets start at the windows already in effect, so with auto-tuning
	// off the refund arithmetic reduces exactly to "give back what was spent".
	c.connRecvTarget.Store(connInitialRecvWindow)
	c.streamRecvTarget.Store(opts.Settings.InitialWindowSize)
	c.tuner = newRecvWindowTuner(opts, opts.Settings.InitialWindowSize)
	c.goAwaySentLast.Store(goAwayNoneSent)
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.wbatch = newWriteBatcher(opts.GroupCommit, &c.wmu, wb)
	// Sync Framer read limit to our advertised MaxFrameSize. Default Framer
	// cap is 16384; peers honouring our SETTINGS may send frames up to the
	// advertised value, which would be rejected as ErrFrameTooLarge otherwise.
	c.fr.SetMaxReadFrameSize(opts.Settings.MaxFrameSize)
	// Enforce our advertised SETTINGS_MAX_HEADER_LIST_SIZE on the decode path.
	// This bounds the decompressed field list (HPACK expansion bomb defense,
	// RFC 7540 §10.5.1); the Framer/handler byte caps only bound the compressed
	// block. opts is defaulted, so this is non-zero unless the caller opted out.
	applyDecoderSettings(c.dec, opts.Settings)
	peer, err := handshakeSettings(ctx, c.fr, c.flushWrite, opts.Settings, opts.EnablePush)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	c.psMu.Lock()
	c.peerSettings = peer
	c.psMu.Unlock()
	// Apply the initial peer SETTINGS to encoder / streams (no streams
	// exist yet, so this just propagates HEADER_TABLE_SIZE to the
	// HPACK encoder when the peer advertised one) and reject an out-of-range
	// SETTINGS_INITIAL_WINDOW_SIZE before any stream is opened against it.
	if err := c.applyInitialPeerSettings(peer); err != nil {
		// The reader loop never started, so nothing else will close the
		// transport; a rejected server preface must not leak the socket.
		_ = transport.Close()
		return nil, err
	}
	go c.readerLoop()
	if opts.KeepaliveInterval > 0 {
		go c.keepaliveLoop(opts.KeepaliveInterval)
	}
	return c, nil
}

// flushWrite flushes the buffered writer to the transport. MUST be called
// while holding c.wmu (the buffered writer is not goroutine-safe; wmu
// serializes every writer that touches it). Callers flush before releasing
// wmu in every frame-writing method so a buffered frame is guaranteed on the
// wire before the writer blocks — a missed flush would deadlock (the peer
// never sees the frame, so never replies with the WINDOW_UPDATE / response
// the writer is waiting for).
func (c *Conn) flushWrite() error {
	if c.wb == nil {
		// Hand-constructed Conns (unit tests wiring fr straight to a buffer)
		// have no buffered writer; there is nothing to flush.
		return nil
	}
	return c.wb.Flush()
}

func (c *Conn) lookupStream(id uint32) *Stream {
	c.smu.Lock()
	defer c.smu.Unlock()
	return c.streams[id]
}

// isIdleStream reports whether id names a stream that has never been opened
// (RFC 9113 §5.1 "idle"): a client-initiated (odd) id the client has not yet
// allocated, or a server-initiated (even) id no PUSH_PROMISE has reserved. The
// reader uses it to distinguish an idle stream — where a frame other than
// HEADERS/PRIORITY is a connection error PROTOCOL_ERROR — from a closed stream,
// where a late frame is minimally processed and discarded (§5.1 leniency for a
// stream we reset). Client ids are allocated below nextID (§5.1.1 monotonic), and
// pushed ids up to lastPromisedID, so anything at or beyond those is idle.
func (c *Conn) isIdleStream(id uint32) bool {
	c.smu.Lock()
	defer c.smu.Unlock()
	if id%2 == 1 {
		return id >= c.nextID
	}
	return id > c.lastPromisedID
}

// isKnownOrigin reports whether authority appears in the origin set the peer
// advertised via ORIGIN frames (RFC 8336). The push accept path treats such an
// authority as one the server is authoritative for (RFC 9113 §8.4 / §10.1).
func (c *Conn) isKnownOrigin(authority []byte) bool {
	a := string(authority)
	c.originsMu.RLock()
	defer c.originsMu.RUnlock()
	for _, o := range c.origins {
		if originAuthority(o) == a {
			return true
		}
	}
	return false
}

// originAuthority returns the host[:port] authority of an ORIGIN entry. An ORIGIN
// frame carries an ASCII-serialized origin ("scheme://host[:port]", RFC 6454
// §6.2), whereas a promised :authority carries no scheme, so the scheme must be
// stripped before the two are compared.
func originAuthority(origin string) string {
	if i := strings.Index(origin, "://"); i >= 0 {
		return origin[i+3:]
	}
	return origin
}

// authorityOf returns the :authority pseudo-header value from a request field
// list, or "" when it is absent.
func authorityOf(fields []hpack.HeaderField) string {
	for i := range fields {
		if string(fields[i].Name) == ":authority" {
			return string(fields[i].Value)
		}
	}
	return ""
}

// pushSupport reports whether server push is enabled and returns the
// stream-event buffer size for new pushed streams.
func (c *Conn) pushSupport() (enabled bool, eventBuf int) {
	return c.opts.EnablePush, c.opts.StreamEventBuffer
}

// notePromisedID validates and records a promised stream id (RFC 9113 §6.6 /
// §5.1.1): it must be even, non-zero, and greater than every id already
// promised; an illegal id is a connection error PROTOCOL_ERROR (ErrIllegalPromisedID).
//
// Recording it — advancing lastPromisedID — is what keeps a REFUSED promise safe.
// The id is spent on the wire whether or not we accept the promise, so a later
// promise must still exceed it; and, critically, the server may race the pushed
// response onto the promised stream before our RST_STREAM lands. Once
// lastPromisedID covers the id, those in-flight frames resolve to a *closed*
// stream (leniently discarded) rather than an *idle* one, whose frames would be a
// connection error (§5.1) that tears the whole multiplexed connection down.
// Called before any stream-level refusal in handlePushPromiseBlock, so every
// refusal path marks the id spent.
func (c *Conn) notePromisedID(id uint32) error {
	c.smu.Lock()
	defer c.smu.Unlock()
	if id == 0 || id%2 != 0 || id <= c.lastPromisedID {
		return ErrIllegalPromisedID
	}
	c.lastPromisedID = id
	return nil
}

// reservePushedStream applies the concurrent server-initiated stream cap and, if
// there is room, allocates and registers the pushed stream. The id must already
// have been accepted by notePromisedID (which validated its legality and advanced
// lastPromisedID); reservePushedStream returns only ErrPushRefused — a §6.6
// stream-level rejection the caller resets rather than escalating.
func (c *Conn) reservePushedStream(id uint32) (*Stream, error) {
	c.smu.Lock()
	defer c.smu.Unlock()
	// id legality and the lastPromisedID advance are handled by notePromisedID,
	// called earlier on every path; here only the concurrency cap and the
	// allocation remain.
	if c.pushInflight >= c.opts.Settings.MaxConcurrentStreams {
		return nil, ErrPushRefused
	}
	// Seed the pushed stream's per-stream recv window from the value we
	// advertised as SETTINGS_INITIAL_WINDOW_SIZE — the same seed NewStream uses —
	// NOT from c.connRecvWindow. connRecvWindow is the CONNECTION window, a
	// different quantity that fluctuates as inbound DATA is debited; seeding a
	// per-stream window from it under-credited the push (badly once other streams
	// have debited the connection window, or when InitialWindowSize is configured
	// above the connection default), so the peer's legal push overran a window
	// smaller than the one we told it about and got RST(FLOW_CONTROL_ERROR).
	s := c.allocStream(c.opts.StreamEventBuffer, int32(c.opts.Settings.InitialWindowSize))
	s.id = id
	s.pushed = true
	c.streams[id] = s
	c.pushInflight++
	return s, nil
}

// rstStream sends a RST_STREAM frame for the given stream ID.
func (c *Conn) rstStream(id uint32, code frame.ErrCode) error {
	return c.writeRSTStreamID(id, code)
}

// resetStreamOnError tears down the one stream a non-fatal *StreamError names
// and tells the peer, keeping the connection and every other in-flight stream
// alive (RFC 9113 §5.4.2).
//
// Delivery goes through endWithReset, not push, because a *Stream is pooled:
// between the lookup here and the delivery the application can finish this
// request, Close it, and a fresh NewStream can claim the same struct. push has
// no id gate, so the dead lifetime's EventReset landed in the NEXT request's
// channel — a reset that request was never sent. endWithReset re-checks s.id
// under s.mu and refuses.
//
// It also sets the terminal flags, which push did not. That removes a duplicate
// RST the old arm produced on the ordinary path: a cleanly enqueued push left
// the stream looking open, so the application's later Close sent a second RST —
// carrying CANCEL, misreporting the error that actually killed the stream.
//
// Split out of readerLoop so the wake below can be tested; inline it was a
// branch no test could reach without a live flow-control overrun.
func (c *Conn) resetStreamOnError(se *StreamError) {
	ended := false
	if s := c.lookupStream(se.StreamID); s != nil {
		ended = s.endWithReset(se.StreamID, se.Code)
	}
	// Unconditional: nothing else emits an RST on this path (endWithReset
	// signals in-process only), so the peer gets exactly one, carrying se.Code.
	// rstStream releases the inflight slot via writeRSTStream; releaseInflight
	// is id-keyed and idempotent, so it no-ops for an already-evicted id.
	_ = c.rstStream(se.StreamID, se.Code)
	if ended {
		// Wake a writer blocked in acquireSendCredits so it observes s.closed
		// and bails instead of sending DATA once credit frees up (RFC 9113
		// §6.4). That loop only re-checks the flag on a cond wake, and neither
		// endWithReset nor rstStream broadcasts. OnRSTStream does the same after
		// its endWithReset. Done last, with no lock held.
		c.wakeSendWaiters()
	}
}

// LookupStream returns the stream with the given ID, or (nil, false) if
// no such stream exists. This is primarily used to access server-pushed
// streams after receiving an EventPushPromise on the parent stream.
func (c *Conn) LookupStream(id uint32) (StreamRef, bool) {
	c.smu.Lock()
	defer c.smu.Unlock()
	s, ok := c.streams[id]
	if !ok {
		return StreamRef{}, false
	}
	return s.ref(), true
}

// NewStream allocates an in-flight slot for a new outbound stream. The
// stream's HTTP/2 ID is assigned later, when SendHeaders writes the
// first HEADERS frame under the writer mutex; this preserves the
// monotonic-id ordering required by RFC 7540 §5.1.1 even with many
// concurrent NewStream callers. Returns ErrTooManyStreams when the
// in-flight count has reached min(local MaxConcurrentStreams,
// peer-advertised SETTINGS_MAX_CONCURRENT_STREAMS).
func (c *Conn) NewStream(_ context.Context) (StreamRef, error) {
	if c.closed.Load() {
		return StreamRef{}, ErrConnClosed
	}
	if c.draining.Load() {
		return StreamRef{}, ErrConnDraining
	}
	if c.goAwayReceived.Load() {
		return StreamRef{}, ErrGoAway
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
		return StreamRef{}, ErrTooManyStreams
	}
	s := c.allocStream(c.opts.StreamEventBuffer, int32(c.opts.Settings.InitialWindowSize))
	c.inflight++
	c.smu.Unlock()
	c.atomicStreamsOpened.Add(1)
	return s.ref(), nil
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
	_ = c.fr.WriteGoAway(c.goAwayLastStreamIDToSend(), frame.ErrCodeNoError, nil)
	_ = c.flushWrite()
	// Wake any group-commit writer deferring on a flush; it re-checks and
	// flushes itself against the now-closing transport rather than hanging.
	c.wbatch.wakeAllLocked()
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
// It sends GOAWAY(last peer-initiated stream id, NO_ERROR) to inform the peer
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
	_ = c.fr.WriteGoAway(c.goAwayLastStreamIDToSend(), frame.ErrCodeNoError, nil)
	_ = c.flushWrite()
	c.wmu.Unlock()
	// Wake any writers blocked in acquireSendCredits so they observe
	// the draining flag and surface ErrConnDraining to their callers.
	c.fcOutMu.Lock()
	if c.fcOutCond != nil {
		c.fcOutCond.Broadcast()
	}
	c.fcOutMu.Unlock()
	// If no in-flight streams, close immediately. Otherwise wait. Read
	// c.inflight under c.smu — the reader goroutine decrements it under the
	// same lock as responses complete (markStreamDone -> releaseSlotLocked), so
	// a lock-free read here races that write. draining is already set (above),
	// so a stream that completes after this read still closes drainDone and the
	// select below observes it; no wakeup is lost.
	c.smu.Lock()
	noInflight := c.inflight == 0
	c.smu.Unlock()
	if noInflight {
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

// streamRefundThreshold is the refund threshold for a stream whose receive
// window is `window` bytes.
//
// recvWindowRefundThreshold is half of the *default* window, which is only a
// batching choice while the window is the default. Advertise a smaller one and
// the constant becomes a deadlock: the peer spends its whole window, we
// accumulate fewer bytes than the threshold, and neither side ever moves —
// a counter cannot reach a threshold larger than the window that feeds it.
// AdvertisedSettings.InitialWindowSize is public and RFC 7540 §6.5.2 allows
// 0..2^31-1, so every value below 32768 (including 16384, the default
// MAX_FRAME_SIZE) hung every download.
//
// Keeping it at half the window preserves the original intent — one refund per
// half window of data — at any size. The floor of 1 keeps a pathological
// window of 1 refunding per byte rather than never.
func streamRefundThreshold(window uint32) uint32 {
	if half := window / 2; half < recvWindowRefundThreshold {
		if half == 0 {
			return 1
		}
		return half
	}
	return recvWindowRefundThreshold
}

// goAwayNoneSent is the goAwaySentLast sentinel meaning no GOAWAY has been sent.
const goAwayNoneSent = ^uint32(0)

// goAwayLastStreamIDToSend returns the last-stream-id to advertise in a GOAWAY we
// send. RFC 9113 §6.8: the value is the highest-numbered stream the sender may
// have acted on. A client acts on peer-initiated (even, server-pushed) streams,
// so it is lastPromisedID — 0 when server push is unused, which is the common
// case. The value is clamped so a later GOAWAY never advertises a larger id than
// an earlier one ("Endpoints MUST NOT increase the value they send in the last
// stream identifier"). Called under wmu, where all GOAWAY writes are serialized.
func (c *Conn) goAwayLastStreamIDToSend() uint32 {
	c.smu.Lock()
	id := c.lastPromisedID
	c.smu.Unlock()
	if prev := c.goAwaySentLast.Load(); prev != goAwayNoneSent && id > prev {
		id = prev
	}
	c.goAwaySentLast.Store(id)
	return id
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
	// The reader owns the transport, so its exit is the connection's death —
	// whether or not anyone called Close. readerLoop returns on a transport
	// error without closing the Conn, and the only other listener on readerDone
	// is keepaliveLoop, which does not run unless opts.KeepaliveInterval > 0
	// (zero by default, and client/ never sets it). Without this check a peer
	// that vanished — crash, restart, RST — left IsAlive answering true forever,
	// and a pool kept handing the corpse out. http3.Client.Alive is this.
	//
	// A hand-built Conn that never started a reader has readerExited false, so it
	// falls through to the flags below rather than reading as dead — the same
	// outcome the old nil-channel receive produced, without the select.
	return !c.readerExited.Load() && !c.closed.Load() && !c.goAwayReceived.Load()
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

// writeHeadersAndData emits HEADERS and the whole of p as one flush, so a unary
// request costs ONE transport write instead of two (#451).
//
// It only does that when the send windows can cover p in full WITHOUT waiting.
// Blocking for credit under wmu would stall every other stream on the
// connection, and leaving a frame buffered while blocking is the deadlock
// writeData's pre-block flush exists to avoid — the peer never sees the frame,
// so it never sends the credit being waited for. When the credit is not there,
// HEADERS goes out on its own and the body falls back to writeData: exactly
// today's two writes, so this is never worse than not calling it.
//
// This is NOT the batching #360 ruled out. Group-commit makes a writer wait for
// OTHER streams' frames to form a convoy, and that wait was measured as the
// cost. This waits for nothing: it writes frames one request already holds, on
// one stream.
func (c *Conn) writeHeadersAndData(ctx context.Context, s *Stream, wantGen uint64, fields []hpack.HeaderField, p []byte, endStream bool) error {
	var v dataVec
	v.seatSingle(p)
	return c.writeHeadersAndDataVec(ctx, s, wantGen, fields, &v, endStream)
}

func (c *Conn) writeHeadersWithPriority(_ context.Context, s *Stream, fields []hpack.HeaderField, endStream bool, prio *frame.Priority) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wbatch.enter()
	c.wmu.Lock()
	c.wbatch.leave()
	defer c.wmu.Unlock()
	c.assignStreamIDLocked(s, fields)
	buf := encBufPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	block := c.enc.EncodeBlock(*buf, fields)

	err := c.writeHeaderBlock(s.id, block, endStream, prio)

	*buf = block[:0]
	encBufPool.Put(buf)
	if err != nil {
		// We buffered nothing to flush; wake any writer deferring on our flush
		// so it does not wait for a convoy flush that will never come (it will
		// re-check and flush itself). Guarded so an uncontended write does
		// nothing.
		c.wbatch.wakeDeferredLocked()
		return err
	}
	c.bumpFramesSent()
	// Flush the buffered HEADERS (+ any CONTINUATION) to the wire before
	// releasing wmu; the deferred Unlock runs after this returns. With
	// group-commit on this may defer to (and be flushed by) the next queued
	// writer, batching both frames into one tls.Conn.Write.
	return c.commitFrame()
}

// assignStreamIDLocked gives a not-yet-sent stream its on-wire identity and
// seeds its outbound window. No-op once the id is set. MUST hold c.wmu, so the
// id order on the wire matches the allocation order (RFC 7540 §5.1.1).
//
// Seeding the per-stream outbound flow-control window from the peer's most
// recently observed SETTINGS_INITIAL_WINDOW_SIZE and registering the stream
// happen together under psMu (lock order psMu->smu->s.mu, per NewStream's
// documented convention). Holding psMu across the seed + insert makes it
// mutually exclusive with applyPeerSettings' merge + retroactive delta, so this
// stream is never BOTH seeded at the new value AND credited the delta — the
// previous split-lock window over-credited the send window (RFC 7540 §6.9.2).
func (c *Conn) assignStreamIDLocked(s *Stream, fields []hpack.HeaderField) {
	if s.id != 0 {
		return
	}
	c.psMu.RLock()
	initial := settingValue(c.peerSettings, frame.SettingInitialWindowSize, connInitialRecvWindow)
	c.smu.Lock()
	s.id = c.nextID
	c.nextID += 2
	c.streams[s.id] = s
	s.mu.Lock()
	s.sendWindow = int32(initial)
	s.reqAuthority = authorityOf(fields)
	s.mu.Unlock()
	c.smu.Unlock()
	c.psMu.RUnlock()
}

// commitFrame flushes the buffered writer under wmu via the group-commit batcher
// (a plain flush when group-commit is off; see writeBatcher.commit). A
// hand-constructed Conn with no batcher flushes directly. MUST hold c.wmu.
func (c *Conn) commitFrame() error {
	if c.wbatch == nil {
		return c.flushWrite()
	}
	return c.wbatch.commit()
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

func (c *Conn) writeRSTStream(s *Stream, code frame.ErrCode) error {
	if s.id == 0 {
		// Stream never reached the wire; no peer state to reset.
		c.releaseUnassignedInflight(s)
		return nil
	}
	return c.writeRSTStreamID(s.id, code)
}

// writeRSTStreamID resets a stream by id. It is the primitive; the *Stream form
// above is the wrapper that additionally handles a stream with no on-wire
// identity yet.
//
// This way round because the id is all the frame needs. The other way round,
// rstStream had to fabricate a &Stream{id: id} to call it — a struct that looks
// like a live registered stream, is not one, allocates on the push-refusal path,
// and would have been a real hazard the moment writeRSTStream read a second
// field off it.
func (c *Conn) writeRSTStreamID(id uint32, code frame.ErrCode) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WriteRSTStream(id, code); err != nil {
		return err
	}
	c.bumpFramesSent()
	c.releaseInflight(id)
	// Flush the RST_STREAM before the deferred Unlock releases wmu.
	return c.flushWrite()
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
		// Best-effort: flush under the same deadline; ignore the error.
		_ = c.flushWrite()
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

	connRefund, err := c.debitConnRecv(length)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.recvWindow -= debit
	streamOverrun := s.recvWindow < 0
	streamRefund := uint32(0)
	if !streamOverrun {
		s.recvRefundPending += length
		if s.recvRefundPending >= streamRefundThreshold(c.opts.Settings.InitialWindowSize) {
			spent := s.recvRefundPending
			s.recvRefundPending = 0
			// Restore to the target, which equals what was spent until the tuner
			// raises it — see refundIncrement. A stream opened before a raise
			// catches up here, at its first refund, rather than at creation: the
			// id is not assigned until SendHeaders, so there is no earlier
			// moment a WINDOW_UPDATE could name it.
			streamRefund = refundIncrement(c.streamRecvTarget.Load(), s.recvWindow, spent)
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
	// Feed the frame to the window tuner and open a new sample when it asks. A
	// failed probe is not escalated: it is an optimization, the transport error
	// that caused it will surface on the next frame this connection has to
	// write, and killing a healthy-looking connection over a PING it did not
	// need would be a worse outcome than measuring nothing.
	//
	// accountConnRecvOnly does not feed the tuner. Its bytes arrived on a stream
	// we had already reset, which is rare, and leaving them out only makes the
	// sample smaller and the estimate more conservative.
	if c.tuner != nil && c.tuner.onData(length) {
		if err := c.writeBDPPing(); err != nil {
			c.tuner.probeFailed()
		}
	}
	return nil
}

// debitConnRecv accounts one flow-controlled frame of `length` bytes against the
// connection-level recv window and returns the bytes that crossed the refund
// threshold (0 if none) so the caller can emit WINDOW_UPDATE(stream=0). It is the
// single place the connection window is debited: onDataReceived charges it
// alongside the per-stream window, and accountConnRecvOnly charges it for a frame
// whose stream is gone. Both MUST agree, and one copy of the arithmetic is how
// they stay agreed.
func (c *Conn) debitConnRecv(length uint32) (uint32, error) {
	c.fcMu.Lock()
	defer c.fcMu.Unlock()
	c.connRecvWindow -= int32(length)
	if c.connRecvWindow < 0 {
		return 0, &ConnError{Code: frame.ErrCodeFlowControlError, Reason: "peer overflowed connection recv window"}
	}
	c.connRefundPending += length
	if c.connRefundPending >= recvWindowRefundThreshold {
		spent := c.connRefundPending
		c.connRefundPending = 0
		refund := refundIncrement(c.connRecvTarget.Load(), c.connRecvWindow, spent)
		if refund == 0 {
			return 0, nil
		}
		c.connRecvWindow += int32(refund)
		return refund, nil
	}
	return 0, nil
}

// refundIncrement is the WINDOW_UPDATE increment that restores a receive window
// to its target: what was spent, plus whatever the tuner has since added.
//
// While the target sits at the window the connection started with — which is
// every connection with ConnOptions.AutoTuneRecvWindow off — target minus window
// IS spent, so this reduces exactly to the classic "give back what was
// consumed" and the flow-control behaviour is unchanged.
//
// A zero target means none was ever published, which is the hand-constructed
// Conn the unit tests drive directly. Those get the classic answer too, for the
// same reason writeBatcher's methods are nil-receiver-safe: a Conn assembled
// without its constructor must not silently lose a protocol obligation.
func refundIncrement(target uint32, window int32, spent uint32) uint32 {
	if target == 0 {
		return spent
	}
	if inc := int32(target) - window; inc > 0 {
		return uint32(inc)
	}
	return 0
}

// accountConnRecvOnly charges the connection-level recv window for a DATA frame
// whose stream is not in the registry — one we reset, or one already fully closed
// and evicted. RFC 7540 §6.9: "A receiver that receives a flow-controlled frame
// MUST always account for its contribution against the connection flow-control
// window ... This is necessary even if the frame is in error." §5.1 names this
// exact case: "Flow-controlled frames (i.e., DATA) received after sending
// RST_STREAM are counted toward the connection flow-control window." OnData used
// to drop such a frame outright, so its bytes were never charged nor refunded and
// the peer's connection send window shrank permanently on every cancelled stream;
// on a long-lived pooled connection that cancels streams it reaches zero and
// stalls every stream. The payload is still dropped (§5.1: an endpoint "MUST
// ignore frames" on a stream it has reset) — only the window is settled.
func (c *Conn) accountConnRecvOnly(length uint32) error {
	connRefund, err := c.debitConnRecv(length)
	if err != nil {
		return err
	}
	if connRefund > 0 {
		return c.writeWindowUpdate(0, connRefund)
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
	// Flush the WINDOW_UPDATE before the deferred Unlock so the peer's send
	// window is replenished promptly.
	return c.flushWrite()
}

// tryAcquireSendCreditsAll is acquireSendCredits without the wait, and
// all-or-nothing: it debits `want` plus padOverhead from both windows and
// reports true, or touches nothing and reports false when either window cannot
// cover the whole amount.
//
// It exists so the one-shot send path can decide, while already holding wmu,
// whether it may emit HEADERS and the whole body in one flush. Blocking for
// credit under wmu would stall every other stream on the connection, and
// leaving a frame buffered while blocking is the deadlock writeData's own
// pre-block flush exists to avoid — the peer never sees the frame, so it never
// sends the credit being waited for. Never blocking makes both impossible.
//
// Partial grants are deliberately refused: a partial write needs a second flush,
// which is the thing the caller is trying not to do.
func (c *Conn) tryAcquireSendCreditsAll(s *Stream, wantGen uint64, want, padOverhead int) (bool, error) {
	if want <= 0 {
		return true, nil
	}
	c.fcOutMu.Lock()
	defer c.fcOutMu.Unlock()
	if c.closed.Load() || c.readerGone {
		return false, ErrConnClosed
	}
	s.mu.Lock()
	streamWin, streamClosed, stale := s.sendWindow, s.closed, s.gen.Load() != wantGen
	s.mu.Unlock()
	if stale {
		return false, ErrStaleStream
	}
	if streamClosed {
		return false, ErrStreamClosed
	}
	need := int64(want) + int64(padOverhead)
	if int64(streamWin) < need || int64(c.peerConnSendWindow) < need {
		return false, nil
	}
	c.peerConnSendWindow -= int32(need)
	s.mu.Lock()
	s.sendWindow -= int32(need)
	s.mu.Unlock()
	return true, nil
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
func (c *Conn) acquireSendCredits(ctx context.Context, s *Stream, wantGen uint64, want, padOverhead int) (int, error) {
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
		if c.closed.Load() || c.readerGone {
			return 0, ErrConnClosed
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		s.mu.Lock()
		streamWin := s.sendWindow
		streamClosed := s.closed
		stale := s.gen.Load() != wantGen
		s.mu.Unlock()
		if stale {
			// The struct was recycled while this writer was parked. s.closed is
			// no help here: markStreamDone pools a stream only when it is false,
			// so the flag this loop already re-read is guaranteed false exactly
			// when the stream we were writing to is gone. Without this the woken
			// writer debits the live connection window and emits DATA carrying
			// the next request's stream id.
			return 0, ErrStaleStream
		}
		if streamClosed {
			// RFC 9113 §6.4: after a stream is reset (peer RST_STREAM) or otherwise
			// closed, we must not send further DATA on it. A writer already blocked
			// here when the reset arrives re-checks on wake and bails, rather than
			// sending DATA once credit finally frees up. Covers a peer RST and a
			// GOAWAY that made this stream a refused victim.
			return 0, ErrStreamClosed
		}
		connWin := c.peerConnSendWindow
		avail := streamWin
		if connWin < avail {
			avail = connWin
		}
		// A padded DATA frame costs its data bytes PLUS the pad-length octet and
		// the padding against both windows (RFC 7540 §6.9.1). Reserve room for the
		// padding overhead and at least one data byte before committing; debit the
		// whole frame cost so our windows track what the peer debits. padOverhead
		// is 0 on the unpadded path, where this reduces to the original arithmetic.
		if avail > int32(padOverhead) {
			maxData := avail - int32(padOverhead)
			n := int32(want)
			if n > maxData {
				n = maxData
			}
			debit := n + int32(padOverhead)
			c.peerConnSendWindow -= debit
			s.mu.Lock()
			s.sendWindow -= debit
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
		// RFC 9113 §5.1: a WINDOW_UPDATE on an idle stream is a connection error
		// PROTOCOL_ERROR — any frame other than HEADERS/PRIORITY on an idle stream
		// is. On a closed stream a late WINDOW_UPDATE is peer chatter and ignored.
		if c.isIdleStream(streamID) {
			return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "WINDOW_UPDATE on idle stream"}
		}
		return nil
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
// maxInitialWindowSize is the RFC 7540 §6.9.1 ceiling on a flow-control window,
// 2^31-1. §6.5.2 makes a SETTINGS_INITIAL_WINDOW_SIZE above it a connection error
// of type FLOW_CONTROL_ERROR.
const maxInitialWindowSize = int64(1)<<31 - 1

// DefaultMaxFrameSize is SETTINGS_MAX_FRAME_SIZE's initial value (RFC 9113
// §6.5.2), which is what a connection advertises and assumes of a peer until a
// SETTINGS frame says otherwise.
//
// Exported because a caller that sizes buffers per frame has to know it, and the
// alternative is what grpc did: re-hardcode 16384 with a comment saying "conn's
// advertised default", so a change here would have silently mis-sized another
// package's per-stream memory budget.
const DefaultMaxFrameSize uint32 = 1 << 14

// frameSizeFloor and frameSizeCeil bound SETTINGS_MAX_FRAME_SIZE (RFC 9113
// §6.5.2): the initial/minimum value 2^14 and the maximum 2^24-1. The floor is
// the same number as the initial value, which is why DefaultMaxFrameSize is
// defined separately rather than aliased — they are different rules that agree.
const (
	frameSizeFloor uint32 = 1 << 14   // 16384
	frameSizeCeil  uint32 = 1<<24 - 1 // 16777215
)

// checkPeerSettingValues rejects a peer SETTINGS value a client must not accept,
// each a connection error of type PROTOCOL_ERROR (RFC 9113 §5.4.1):
//
//   - SETTINGS_ENABLE_PUSH: §6.5.2 "Any value other than 0 or 1 MUST be treated
//     as a connection error … of type PROTOCOL_ERROR", and §8.4 "A client MUST
//     treat receipt of a SETTINGS frame with SETTINGS_ENABLE_PUSH set to 1 as a
//     connection error … of type PROTOCOL_ERROR" — so a server may only send 0.
//   - SETTINGS_MAX_FRAME_SIZE: §6.5.2 "Values outside this range [2^14, 2^24-1]
//     MUST be treated as a connection error … of type PROTOCOL_ERROR".
//
// SETTINGS_INITIAL_WINDOW_SIZE range is a FLOW_CONTROL_ERROR and is checked at
// the apply sites, not here.
func checkPeerSettingValues(s frame.SettingsParams) error {
	for i := 0; i < s.N; i++ {
		p := s.Pairs[i]
		//exhaustive:ignore // Only the two settings whose *range* RFC 7540 §6.5.2
		// makes a connection error are checked here. The rest have no illegal
		// value, and INITIAL_WINDOW_SIZE is a FLOW_CONTROL_ERROR checked at the
		// apply sites (see the doc comment).
		switch p.ID {
		case frame.SettingEnablePush:
			if p.Value != 0 {
				return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "peer SETTINGS_ENABLE_PUSH must be 0"}
			}
		case frame.SettingMaxFrameSize:
			if p.Value < frameSizeFloor || p.Value > frameSizeCeil {
				return &ConnError{Code: frame.ErrCodeProtocolError, Reason: "SETTINGS_MAX_FRAME_SIZE out of range"}
			}
		}
	}
	return nil
}

func (c *Conn) applyInitialPeerSettings(peer frame.SettingsParams) error {
	if err := checkPeerSettingValues(peer); err != nil {
		return err
	}
	for i := 0; i < peer.N; i++ {
		p := peer.Pairs[i]
		//exhaustive:ignore // Only settings with a handshake-time side effect.
		// The others are stored by the caller and read through lookupPeerSetting
		// when they are needed, so there is nothing to do at apply time.
		switch p.ID {
		case frame.SettingHeaderTableSize:
			c.enc.SetMaxDynamicTableSize(p.Value)
		case frame.SettingInitialWindowSize:
			// RFC 7540 §6.5.2: a value above 2^31-1 MUST be a FLOW_CONTROL_ERROR.
			// The mid-connection applyPeerSettings enforced this; the handshake path
			// did not, so int32(0x80000000) seeded every new stream's send window
			// deeply negative. acquireSendCredits' `avail > padOverhead` is then
			// never true, and a body-bearing request blocks in fcOutCond.Wait
			// forever (with a non-cancellable context) instead of failing fast.
			if int64(p.Value) > maxInitialWindowSize {
				return &ConnError{Code: frame.ErrCodeFlowControlError, Reason: "SETTINGS_INITIAL_WINDOW_SIZE exceeds 2^31-1"}
			}
		}
	}
	return nil
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

	// Reject ENABLE_PUSH != 0 and an out-of-range MAX_FRAME_SIZE (both connection
	// errors of type PROTOCOL_ERROR) before any value is merged or applied.
	if err := checkPeerSettingValues(s); err != nil {
		return err
	}

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
		//exhaustive:ignore // Only the two settings needing work beyond the
		// unconditional setPeerSetting below: a range check that is a
		// FLOW_CONTROL_ERROR, and the HPACK table resize deferred past psMu.
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
	// Flush the SETTINGS ACK before the deferred Unlock releases wmu.
	return c.flushWrite()
}

// onGoAwayReceived stores the peer's GOAWAY state and resets every
// stream whose id is strictly greater than lastStreamID — those
// streams the peer never accepted (RFC 7540 §6.8). Streams with id
// ≤ lastStreamID continue. Wakes writers blocked on send credit so
// they observe the GOAWAY-induced flow termination via subsequent
// SendData calls (which still go through, until the peer closes the
// transport).
func (c *Conn) onGoAwayReceived(lastStreamID uint32, _ frame.ErrCode) {
	// RFC 9113 §6.8: a peer MUST NOT raise the last-stream-id across successive
	// GOAWAYs. Defensively clamp a second GOAWAY that tries to — never widen the
	// set of streams we treat as accepted by the peer. Single-goroutine (reader)
	// access, so the load-then-store needs no CAS.
	if c.goAwayReceived.Load() {
		if prev := c.goAwayLastStreamID.Load(); lastStreamID > prev {
			lastStreamID = prev
		}
	}
	c.goAwayLastStreamID.Store(lastStreamID)
	c.goAwayReceived.Store(true)

	// The id travels with the stream rather than being read back from s.id in
	// the loop: the struct can be recycled between the snapshot and the reset,
	// and s.id is one of the fields recycling zeroes.
	type goAwayVictim struct {
		id uint32
		s  *Stream
	}
	c.smu.Lock()
	victims := make([]goAwayVictim, 0, len(c.streams))
	for id, s := range c.streams {
		if id > lastStreamID {
			victims = append(victims, goAwayVictim{id: id, s: s})
		}
	}
	c.smu.Unlock()

	for _, v := range victims {
		// Surface the cancellation as REFUSED_STREAM — the peer never
		// processed our HEADERS, so it is safe for the caller to retry
		// on a fresh connection. endWithReset does the delivery and the
		// end-of-stream flags in one s.mu section, and declines both when the
		// struct has already been recycled out from under this loop — which the
		// snapshot cannot prevent, since smu is released before it runs.
		if !v.s.endWithReset(v.id, frame.ErrCodeRefusedStream) {
			continue
		}
		c.markStreamDone(v.id)
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
	// Flush the PING ACK before the deferred Unlock releases wmu.
	return c.flushWrite()
}

// writeBDPPing opens a bandwidth-delay-product sample by writing the tuner's
// PING and flushing it: the sample is the DATA that arrives before its ACK, so
// a PING left in the write buffer would measure the buffer rather than the
// link. Called from the reader goroutine, which already takes wmu on this path
// for WINDOW_UPDATE.
func (c *Conn) writeBDPPing() error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.fr.WritePing(false, bdpPingPayload); err != nil {
		return err
	}
	c.bumpFramesSent()
	return c.flushWrite()
}

// deliverPingAck signals any Ping call waiting for payload.
// Unsolicited ACKs (no matching waiter) are silently ignored.
func (c *Conn) deliverPingAck(payload [8]byte) {
	// The tuner's own PING is answered here rather than by a waiter: it has no
	// caller to wake, only a sample to close. Conn.Ping numbers its payloads
	// from a counter, so it can never register a waiter under this key and no
	// application ping is being stolen.
	if payload == bdpPingPayload {
		if c.tuner != nil {
			c.tuner.onAck(c)
		}
		return
	}
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
		// Flush the PING to the wire before releasing wmu — we are about to
		// block waiting for its ACK, which never arrives if the frame is left
		// buffered.
		if ferr := c.flushWrite(); ferr != nil {
			err = ferr
		} else {
			c.bumpFramesSent()
		}
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
	// Publish the flag BEFORE closing the channel: a goroutine woken by the close
	// must never then observe the connection as alive. Early is safe, late is the
	// bug. Deferred in one statement so no return path can set one without the
	// other.
	defer func() {
		c.readerExited.Store(true)
		close(c.readerDone)
	}()
	h := newConnHandler(c, c.dec)
	h.raiseMaxHeaderBytes(c.opts.Settings.MaxHeaderListSize)
	for {
		fh, err := c.fr.ReadFrame(context.Background(), h)
		if err != nil {
			// The Framer surfaces a malformed frame as a plain sentinel; map it
			// to the RFC 9113 scope and typed error code before routing.
			err = c.mapFrameError(fh, err)
			// A *StreamError is non-fatal (RFC 7540 §5.4.2): reset only the
			// offending stream and keep the connection — and every other
			// in-flight stream — alive. onDataReceived / onWindowUpdate
			// return this on a single stream's flow-control overrun. Only
			// *ConnError and I/O errors tear the whole connection down.
			var se *StreamError
			if errors.As(err, &se) {
				// Delivery goes through endWithReset, not push, because a
				// *Stream is pooled: between the lookup above and the delivery
				// the application can finish this request, Close it, and a fresh
				// NewStream can claim the same struct. push has no id gate, so
				// the dead lifetime's EventReset landed in the NEXT request's
				// channel. endWithReset re-checks s.id under s.mu and refuses.
				//
				// It also sets the terminal flags, which push did not. That
				// removes a duplicate RST the old arm produced on the ordinary
				// path: a cleanly enqueued push left the stream looking open, so
				// the application's later Close sent a second RST — with CANCEL,
				// misreporting the flow-control error that actually killed it.
				//
				// The RST is now unconditional. Nothing else emits one on this
				// path (endWithReset signals in-process only), so the peer gets
				// exactly one, carrying se.Code.
				c.resetStreamOnError(se)
				continue
			}
			c.emitConnGoAwayIfTyped(err)
			c.shutdownStreams(err)
			// RFC 9113 §5.4.1: after sending a GOAWAY for an error condition the
			// endpoint must close the TCP connection, so the peer is not left with a
			// half-alive socket. emitConnGoAwayIfTyped writes a GOAWAY only for a
			// *ConnError, so gate the close on the same type; an io.EOF / transport
			// error means the socket is already gone.
			var ce *ConnError
			if errors.As(err, &ce) {
				_ = c.transport.Close()
			}
			return
		}
	}
}

// mapFrameError converts a Framer sentinel into a typed *StreamError or
// *ConnError carrying the RFC 9113 scope and error code, so the reader loop
// resets a single stream where the spec says stream error (a wrong-length
// PRIORITY, a 0-increment stream WINDOW_UPDATE) and tears the whole connection
// down with a typed GOAWAY where it says connection error. Errors the handler
// already typed (flow-control overruns, malformed responses) and transport / EOF
// errors pass through unchanged.
func (c *Conn) mapFrameError(fh frame.FrameHeader, err error) error {
	var se *StreamError
	var ce *ConnError
	if errors.As(err, &se) || errors.As(err, &ce) {
		return err
	}
	connErr := func(code frame.ErrCode) error {
		return &ConnError{Code: code, Reason: err.Error()}
	}
	switch {
	case errors.Is(err, frame.ErrPriorityWrongLength):
		// §6.3: a PRIORITY frame of length != 5 is a STREAM error of type
		// FRAME_SIZE_ERROR — reset the one stream, keep the connection.
		return &StreamError{StreamID: fh.StreamID, Code: frame.ErrCodeFrameSizeError}
	case errors.Is(err, frame.ErrZeroIncrement):
		// §6.9: a WINDOW_UPDATE with a 0 increment is a stream error of type
		// PROTOCOL_ERROR on a stream, and a connection error on the connection
		// flow-control window (stream 0).
		if fh.StreamID == 0 {
			return connErr(frame.ErrCodeProtocolError)
		}
		return &StreamError{StreamID: fh.StreamID, Code: frame.ErrCodeProtocolError}
	case errors.Is(err, frame.ErrInvalidStreamID),
		errors.Is(err, frame.ErrInvalidPadding),
		errors.Is(err, frame.ErrProtocolError),
		errors.Is(err, frame.ErrContinuationExpected),
		errors.Is(err, frame.ErrUnexpectedContinuation):
		// Stream-id rule violations (§5.1.1), oversized padding (§6.1), the
		// ORIGIN/ALTSVC framing faults, and field-block continuity violations
		// (§6.10) are connection errors of type PROTOCOL_ERROR.
		return connErr(frame.ErrCodeProtocolError)
	case errors.Is(err, frame.ErrFrameTooLarge),
		errors.Is(err, frame.ErrRSTWrongLength),
		errors.Is(err, frame.ErrSettingsLength),
		errors.Is(err, frame.ErrSettingsAck),
		errors.Is(err, frame.ErrPingWrongLength),
		errors.Is(err, frame.ErrWindowWrongLength),
		errors.Is(err, frame.ErrShortRead):
		// A frame that exceeds SETTINGS_MAX_FRAME_SIZE, a fixed-size frame of the
		// wrong length, or one too small for its mandatory fields is a connection
		// error of type FRAME_SIZE_ERROR (§4.2, §6.4, §6.5, §6.7, §6.9).
		return connErr(frame.ErrCodeFrameSizeError)
	default:
		// io.EOF, context cancellation, transport errors: connection teardown
		// with no typed GOAWAY (the peer is already gone or not speaking HTTP/2).
		return err
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
	_ = c.fr.WriteGoAway(c.goAwayLastStreamIDToSend(), ce.Code, nil)
	_ = c.flushWrite()
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
		// Recorded before the close so the recycle path knows the channel needs
		// replacing. This is the only site that closes s.events, and a closed
		// channel survives a drain — len() and receive both keep working on it —
		// so a struct pooled without repair would hand the next request a channel
		// that reports ErrStreamClosed before anything is sent, and panic the
		// reader goroutine on the first delivery.
		s.eventsClosed.Store(true)
		close(s.events)
	}
	// Wake any writer parked in acquireSendCredits. The connection is dead, so the
	// WINDOW_UPDATE it waits for can never arrive; without this a SendData blocked
	// on an exhausted send window hangs forever on a non-cancellable context.
	// readerGone and the broadcast are set under fcOutMu — the cond's locker — so a
	// writer that has just released it in Wait re-checks readerGone and bails with
	// ErrConnClosed. c.closed stays false on a reader-death teardown (see IsAlive),
	// which is why the send window is the only other exit and this is needed.
	c.fcOutMu.Lock()
	c.readerGone = true
	if c.fcOutCond != nil {
		c.fcOutCond.Broadcast()
	}
	c.fcOutMu.Unlock()
	if errors.Is(reason, io.EOF) {
		return
	}
}

// wakeSendWaiters broadcasts the outbound flow-control condition so every writer
// blocked in acquireSendCredits re-checks its stream and connection state. Holds
// only fcOutMu, so it is safe to call with no other lock held.
func (c *Conn) wakeSendWaiters() {
	c.fcOutMu.Lock()
	c.fcOutCond.Broadcast()
	c.fcOutMu.Unlock()
}

// markStreamDone is called by the connHandler when a stream's response
// side closes (END_STREAM observed or RST received), and from local
// SendHeaders/SendData when END_STREAM goes out. It releases the
// stream's slot in the inflight pool exactly once and evicts the
// stream from the registry once both ends have closed.
//
// It holds c.smu across its ENTIRE body on purpose, and that span is
// load-bearing for the recycle rendezvous, not incidental. This method runs
// on two goroutines for one stream — the reader (terminal frame) and the app
// (SendData/SendHeaders with END_STREAM) — and smu serializes them so that the
// inflightDone false->true guard admits exactly one into the eviction block.
// A future "release smu earlier to cut contention" refactor would let both in,
// re-open the double-recycle this fix closes, and reintroduce the data race
// against recycleStream. Do not narrow the hold without moving the guard.
func (c *Conn) markStreamDone(id uint32) {
	c.smu.Lock()
	s, ok := c.streams[id]
	if !ok {
		c.smu.Unlock()
		return
	}
	s.mu.Lock()
	ended := s.localEnded && s.remoteEnded
	released := s.inflightDone
	pushed := s.pushed
	if ended && !released {
		s.inflightDone = true
	}
	s.mu.Unlock()

	recycle := false
	if ended && !released {
		c.releaseSlotLocked(pushed)
		delete(c.streams, id)
		// Registry entry gone: the reader can no longer look this struct up,
		// so this is the connection side's final contact. Rendezvous with
		// Close via appClosed/connDone: whichever of the two runs second
		// recycles. Gated on !closed so only a cleanly-completed stream is
		// pooled — a reset/overflow stream may still have a background RST
		// goroutine referencing s.w (see Stream.push's overflow path).
		s.mu.Lock()
		if !s.closed {
			if s.appClosed {
				recycle = true
			} else {
				s.connDone = true
			}
		}
		s.mu.Unlock()
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
	c.smu.Unlock()
	// Recycle outside smu — recycleStream never needs it, and holding smu
	// across it would serialize unrelated streams' registry lookups behind a
	// pool drain/refill.
	if recycle {
		recycleStream(s)
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
	pushed := s.pushed
	if !released {
		s.inflightDone = true
	}
	s.mu.Unlock()
	if !released {
		c.releaseSlotLocked(pushed)
		delete(c.streams, id)
	}
}

// releaseSlotLocked returns one slot to the counter that issued it: the
// pushed stream's own bound, or our NewStream gate. Routing by the stream's
// origin is what keeps each count exact — the two limits are directional and
// independent (RFC 7540 §5.1.2). Callers hold smu.
func (c *Conn) releaseSlotLocked(pushed bool) {
	if pushed {
		if c.pushInflight > 0 {
			c.pushInflight--
		}
		return
	}
	if c.inflight > 0 {
		c.inflight--
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
