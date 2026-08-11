package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/qpack"
	"github.com/lodgvideon/poseidon-http-client/quic"
)

// quicStream is the QUIC stream surface the client uses. *quic.Stream satisfies
// it; tests supply a fake. RecvState / WaitReadable / WaitSendable are the
// reader-goroutine wake vocabulary (docs/HTTP3_DESIGN.md §3.3): a Do reads its
// response via Recv, waits for more with WaitReadable, and parks on WaitSendable
// when a Send is flow-control blocked; the reader signals progress.
type quicStream interface {
	ID() uint64
	Send(data []byte, fin bool) (int, error)
	Recv() []byte
	RecvState() (finished, reset bool, code uint64)
	WaitReadable(ctx context.Context) error
	WaitSendable(ctx context.Context) error
	Finished() bool
	Reset(errCode uint64) error
	StopSending(errCode uint64) error
}

// quicConn is the QUIC connection surface the client uses. A connAdapter over
// *quic.Conn satisfies it; tests supply a fake.
type quicConn interface {
	OpenStream(ctx context.Context) (quicStream, error)
	OpenUniStream() (quicStream, error)
	AcceptUniStream() quicStream // next accepted server-initiated uni stream, or nil
	Poll(ctx context.Context) error
	CloseWithError(app bool, code uint64, reason string) error
}

// ErrGoAway is returned by Do when the server has sent GOAWAY and the new request
// would use a stream the server will not process (RFC 9114 §5.2).
var ErrGoAway = errors.New("http3: server is going away")

// ErrH3Control reports a fatal HTTP/3 connection error — a control-stream
// violation (a missing or duplicate SETTINGS, a forbidden push, an oversized
// frame, a closed critical stream) or a frame that may not appear on a request
// stream (§7.2); a CONNECTION_CLOSE with the specific HTTP/3 error code has
// already been sent.
var ErrH3Control = errors.New("http3: connection error")

// H3ConnError is a connection-level HTTP/3 error carrying the code that was put
// on the wire (RFC 9114 §8.1) — H3_FRAME_ERROR, H3_SETTINGS_ERROR,
// QPACK_DECOMPRESSION_FAILED and the rest.
//
// Every one of those used to collapse into the bare ErrH3Control sentinel, so a
// peer's protocol violation and a local QPACK failure were indistinguishable to
// a pool, a retry policy, a metric or a test — the code existed only on the wire
// and nowhere in the returned error. The HTTP/2 engine types the same thing
// (conn.ConnError), and this package already types the stream-level case
// (StreamResetError).
//
// It matches errors.Is(err, ErrH3Control), so code written against the sentinel
// keeps working; only a direct == comparison has to change.
type H3ConnError struct{ Code uint64 }

// Error implements error.
func (e *H3ConnError) Error() string {
	if name := h3ErrorCodeName(e.Code); name != "" {
		return fmt.Sprintf("http3: connection error %s (%#x)", name, e.Code)
	}
	return fmt.Sprintf("http3: connection error %#x", e.Code)
}

// Is reports whether target is ErrH3Control, so errors.Is against the sentinel
// still matches a typed connection error.
func (e *H3ConnError) Is(target error) bool { return target == ErrH3Control }

// h3ErrorCodeName returns the RFC 9114 §8.1 name for a known code, or "".
func h3ErrorCodeName(code uint64) string {
	switch code {
	case H3NoError:
		return "H3_NO_ERROR"
	case H3InternalError:
		return "H3_INTERNAL_ERROR"
	case H3StreamCreationError:
		return "H3_STREAM_CREATION_ERROR"
	case H3ClosedCriticalStream:
		return "H3_CLOSED_CRITICAL_STREAM"
	case H3FrameUnexpected:
		return "H3_FRAME_UNEXPECTED"
	case H3FrameError:
		return "H3_FRAME_ERROR"
	case H3ExcessiveLoad:
		return "H3_EXCESSIVE_LOAD"
	case H3IDError:
		return "H3_ID_ERROR"
	case H3SettingsErrorCode:
		return "H3_SETTINGS_ERROR"
	case H3MissingSettings:
		return "H3_MISSING_SETTINGS"
	case H3RequestRejected:
		return "H3_REQUEST_REJECTED"
	case H3RequestCancelled:
		return "H3_REQUEST_CANCELLED"
	case H3MessageError:
		return "H3_MESSAGE_ERROR"
	}
	return ""
}

// HTTP/3 error codes (RFC 9114 §8.1), carried in the QUIC application
// CONNECTION_CLOSE frame.
const (
	H3NoError              uint64 = 0x0100 // H3_NO_ERROR
	H3InternalError        uint64 = 0x0102 // H3_INTERNAL_ERROR
	H3StreamCreationError  uint64 = 0x0103 // H3_STREAM_CREATION_ERROR
	H3ClosedCriticalStream uint64 = 0x0104 // H3_CLOSED_CRITICAL_STREAM
	H3FrameUnexpected      uint64 = 0x0105 // H3_FRAME_UNEXPECTED
	H3FrameError           uint64 = 0x0106 // H3_FRAME_ERROR
	H3ExcessiveLoad        uint64 = 0x0107 // H3_EXCESSIVE_LOAD
	H3IDError              uint64 = 0x0108 // H3_ID_ERROR
	H3SettingsErrorCode    uint64 = 0x0109 // H3_SETTINGS_ERROR
	H3MissingSettings      uint64 = 0x010a // H3_MISSING_SETTINGS
	H3RequestRejected      uint64 = 0x010b // H3_REQUEST_REJECTED
	H3RequestCancelled     uint64 = 0x010c // H3_REQUEST_CANCELLED
	H3MessageError         uint64 = 0x010e // H3_MESSAGE_ERROR

	// QPACK error codes (RFC 9204 §6), carried in the same HTTP/3 CONNECTION_CLOSE.
	H3QpackDecompressionFailed uint64 = 0x0200 // QPACK_DECOMPRESSION_FAILED
	H3QpackEncoderStreamError  uint64 = 0x0201 // QPACK_ENCODER_STREAM_ERROR
	H3QpackDecoderStreamError  uint64 = 0x0202 // QPACK_DECODER_STREAM_ERROR
)

// maxInterimResponses bounds the 1xx informational responses buffered before the
// final response (RFC 9114 §4.1), so a server streaming them endlessly — including
// as empty frames that add no bytes — cannot exhaust memory.
const maxInterimResponses = 100

// maxResponseBytes bounds a whole response the client buffers in memory: it is
// both the per-frame declared-length cap (a single HEADERS, trailer, or DATA frame
// past it is refused before its payload is buffered — RFC 9114 places no per-frame
// size limit, so the request stream otherwise had none) and the cumulative cap on
// the header, body, trailer, and 1xx payloads retained together. One limit keeps
// the two consistent: a single DATA frame up to the whole budget is accepted, but
// the retained total cannot exceed it. A var, not a const, so a test can exercise
// the limit without buffering hundreds of megabytes.
var maxResponseBytes uint64 = 1 << 27 // 128 MiB

// ErrResponseTooLarge is returned by Do when a response exceeds a client buffering
// limit — a single frame or the retained total past maxResponseBytes, or the
// interim (1xx) responses past maxInterimResponses. The request stream is aborted;
// it is not a connection error.
var ErrResponseTooLarge = errors.New("http3: response exceeds client buffering limit")

// StreamResetError reports that the server abruptly aborted the request stream
// with RESET_STREAM (RFC 9000 §3.5) before the response finished. Code is the
// HTTP/3 application error code the server signalled (RFC 9114 §8.1).
type StreamResetError struct{ Code uint64 }

// Error implements error.
func (e *StreamResetError) Error() string {
	return fmt.Sprintf("http3: server reset the request stream (error %#x)", e.Code)
}

// Retryable reports whether the reset means the request received no application
// processing and is safe to retry on a new connection — the server signalled
// H3_REQUEST_REJECTED (RFC 9114 §4.1.1).
func (e *StreamResetError) Retryable() bool { return e.Code == H3RequestRejected }

// connAdapter lets a concrete *quic.Conn satisfy quicConn — the interface methods
// return quicStream where *quic.Conn returns the concrete *quic.Stream.
type connAdapter struct{ *quic.Conn }

func (a connAdapter) OpenStream(ctx context.Context) (quicStream, error) {
	s, err := a.OpenStreamContext(ctx) // waits on stream credit; threads the request ctx (2d)
	if err != nil {
		return nil, err // avoid returning a non-nil interface wrapping a nil *Stream
	}
	return s, nil
}

func (a connAdapter) OpenUniStream() (quicStream, error) {
	s, err := a.Conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (a connAdapter) AcceptUniStream() quicStream {
	if s := a.Conn.AcceptUniStream(); s != nil {
		return s // avoid a non-nil interface wrapping a nil *Stream
	}
	return nil
}

// ConnectionState returns the TLS connection state of the underlying QUIC
// connection (ALPN, negotiated cipher suite, peer certificates). It returns the
// zero value when the connection does not expose one (e.g. a test fake).
func (c *Client) ConnectionState() tls.ConnectionState {
	if cs, ok := c.conn.(interface {
		ConnectionState() tls.ConnectionState
	}); ok {
		return cs.ConnectionState()
	}
	return tls.ConnectionState{}
}

// Client is a minimal HTTP/3 client over an established QUIC connection. It owns
// the connection's control stream and its own QPACK instruction streams; each
// request carries its own stack-local QPACK decoder scratch (docs/HTTP3_DESIGN.md
// §3.5, PR 2d), while TWO pieces of QPACK state are connection-scoped and shared:
// the decode-side dynamic table under qpackMu (the reader applies the server
// encoder stream under Lock, a Do resolves references under RLock; RFC 9204 Q2),
// and the encode-side dynamic table under encMu (a Do inserts + references while
// encoding one request, the reader applies the server decoder stream; RFC 9204 Q5).
// A dedicated reader goroutine drives the QUIC engine and services the server
// control + both QPACK instruction streams for the connection's lifetime (§3.1). Do
// sends its own request frames and blocks on per-stream wakeups, so N goroutines may
// call Do on one Client concurrently — OpenStream's id/map mutation and every seal
// are under the QUIC c.mu, streams wake independently, control state is reader-owned,
// the shared decode table is serialized by qpackMu and the shared encode table by
// encMu (both leaf locks never nested with c.mu). Client is safe for concurrent use.
type Client struct {
	conn quicConn

	// Connection lifecycle. The reader goroutine runs Poll + serviceControl on
	// connCtx until the connection terminates; Close cancels connCtx and waits on
	// readerDone (docs/HTTP3_DESIGN.md §3.1, F6).
	connCtx    context.Context
	connCancel context.CancelFunc
	readerDone chan struct{}

	// dead mirrors "readerDone is closed" as an atomic, because Alive is called
	// once per pooled connection per request and a channel select is far too
	// expensive for that: at 4k connections a profile put runtime.chanrecv at
	// 19.6% and this method at 11.5% of all CPU, entirely from the pool's
	// selection scan. Nothing waits on it — readerDone remains the thing to
	// block on; this is only the predicate.
	dead atomic.Bool

	// Server control-stream state (RFC 9114 §6.2.1, §5.2), reader-owned: touched
	// only on the reader goroutine (serviceControl after each Poll), so no lock.
	pendingUni    []*uniStream // accepted server uni streams whose type isn't peeled yet
	control       quicStream   // the server control stream, once identified
	controlReader FrameReader
	qpackEnc      quicStream // the server QPACK encoder stream (RFC 9204 §4.2)
	qpackDec      quicStream // the server QPACK decoder stream (RFC 9204 §4.2)
	qpackEncBuf   []byte     // reader-owned: server encoder-stream bytes not yet a whole instruction
	qpackDecBuf   []byte     // reader-owned: server decoder-stream bytes not yet a whole instruction (Q5)
	settingsRead  bool       // the mandatory first SETTINGS frame has been read
	// qpackEncBufMax caps qpackEncBuf, the retained partial encoder instruction; it
	// is derived in newClient from the table capacity we advertise, so it can never
	// be tighter than the largest insert our own SETTINGS invite (qpackEncoderInstrCap).
	qpackEncBufMax uint64

	// The client's own QPACK instruction streams (RFC 9204 §4.2), opened at
	// newClient. clientQPACKEnc carries our encoder instructions — none in the
	// static-only profile, so it stays at just its type byte — and clientQPACKDec
	// carries our decoder instructions (Insert Count Increment, Section
	// Acknowledgment, Stream Cancellation). Writes to the decoder stream funnel
	// through qpackDecWrite under decWMu so the reader's ICI and each Do's Section
	// Ack never interleave a partial instruction.
	clientQPACKEnc quicStream
	clientQPACKDec quicStream
	decWMu         sync.Mutex  // serializes decoder-stream writes; a leaf lock (never held across a wait)
	decWriteBuf    []byte      // decoder-stream bytes accepted for send but not yet flushed (flow-control residue)
	decHasResidue  atomic.Bool // true when decWriteBuf holds unflushed bytes — lets the reader skip the lock otherwise

	// knownReceivedCount mirrors the encoder's Known Received Count (RFC 9204
	// §2.1.4): the number of dynamic-table insertions the server's encoder knows we
	// have received. BOTH decoder instructions we send advance it — an Insert Count
	// Increment (§4.4.3) reports inserts past it, and a Section Acknowledgment
	// (§4.4.1) implicitly advances it to the acknowledged section's Required Insert
	// Count. Guarded by decWMu together with the decoder-stream writes, so the value
	// we compute matches the order the encoder applies our instructions and the two
	// mechanisms never double-count the same insert (which would push the encoder's
	// Known Received Count past the inserts it made — a QPACK_DECODER_STREAM_ERROR).
	knownReceivedCount uint64

	// Encode-side dynamic QPACK (RFC 9204 §2.1, Q5). The client inserts repeated
	// request-header entries into its OWN encoder dynamic table — the table the
	// SERVER maintains as decoder — sends Insert instructions on our encoder stream
	// (clientQPACKEnc), and references acknowledged entries in request field
	// sections. qpackEncoder is created only once the server's SETTINGS reveal a
	// non-zero SETTINGS_QPACK_MAX_TABLE_CAPACITY (the encoder MUST NOT insert before
	// it knows the decoder's capacity); until then, and against a server that
	// advertises 0, it stays nil and every request encodes static-only. All encoder
	// state — the table, its Known Received Count, and the encoder-stream write
	// buffer — is guarded by encMu, a leaf lock (never nested with the QUIC c.mu,
	// which the best-effort encoder-stream Send takes beneath it, and never held
	// across a wait): a Do holds it to encode one request, the reader holds it to
	// apply the server's decoder-stream acknowledgments (advancing the Known
	// Received Count) and to install the encoder when SETTINGS arrive.
	encMu         sync.Mutex
	qpackEncoder  *qpack.Encoder // nil ⇒ static-only encoding (pre-SETTINGS or server capacity 0)
	encWriteBuf   []byte         // encoder-stream bytes accepted for send but not yet flushed (flow-control residue)
	encHasResidue atomic.Bool    // true when encWriteBuf holds unflushed bytes — lets the reader skip the lock otherwise

	// Shared connection-scoped QPACK dynamic table (RFC 9204 §3.2). This is the
	// ONE piece of QPACK state shared across goroutines: the reader WRITES it
	// (applying the server encoder stream's inserts/evictions under qpackMu.Lock),
	// every Do READS it (resolving dynamic references during one DecodeFieldSection
	// under qpackMu.RLock, copying every kept field before releasing — eviction and
	// arena compaction rewrite the shared bytes). qpackMu is a NEW, SEPARATE leaf
	// lock: it is never nested with the quic connection's c.mu and never held
	// across a wait (docs/HTTP3_DESIGN.md concurrency rules R2). insertCount is
	// published as an atomic after each apply so it can be observed without the
	// lock. Live in this build: dial.go advertises a non-zero capacity, so a server
	// inserts entries here and references them from response field sections.
	qpackMu     sync.RWMutex
	qpackDyn    *qpack.DynamicTable
	insertCount atomic.Uint64

	// Blocked-stream wake (RFC 9204 §2.1.3, Q4). SETTINGS_QPACK_BLOCKED_STREAMS is
	// non-zero, so a response field section may reference dynamic-table entries the
	// server's encoder stream has not delivered yet — its Required Insert Count is
	// ahead of insertCount. A Do decoding such a section parks (off qpackMu — never
	// hold a lock across a wait, R2) until the reader advances insertCount to cover
	// it. Unlike the per-stream signalReady and the cap-1 c.streamCredit, which each
	// hand a token to ONE waiter, a single insert-count advance can unblock ANY
	// number of parked Dos, each waiting on a DIFFERENT Required Insert Count — so
	// the wake is a broadcast: signalQPACKReady closes qpackReady (waking every
	// current waiter) and installs a fresh channel for the next generation, guarded
	// by qpackReadyMu. Level-triggered: a woken Do re-reads insertCount (the atomic)
	// and re-parks on the new channel if still short, so close-then-replace closes
	// the check-then-block lost-wakeup window exactly as the cap-1 signals do. The
	// wait is always bounded by the request ctx and by connCtx (connection
	// teardown), so a promised insert that never arrives fails the request rather
	// than hanging.
	qpackReadyMu sync.Mutex
	qpackReady   chan struct{} // broadcast channel: closed+replaced to wake blocked decodes (§2.1.3)

	// qpackBlocked is the number of request streams currently parked on the insert
	// count, and qpackMaxBlocked is the SETTINGS_QPACK_BLOCKED_STREAMS we advertise —
	// the ceiling the encoder must respect (RFC 9204 §2.1.2). A new blocked section
	// that would push qpackBlocked past qpackMaxBlocked is an encoder violation, a
	// QPACK_DECOMPRESSION_FAILED. registerBlocked reserves a slot with an atomic
	// compare-and-increment against the ceiling so concurrent Dos cannot overshoot it.
	qpackBlocked    atomic.Uint64
	qpackMaxBlocked uint64

	// maxFieldSection and goaway are published as atomics (docs/HTTP3_DESIGN.md
	// §3.5): the reader writes them from serviceControl, a Do reads them. maxFieldSection
	// is the peer SETTINGS_MAX_FIELD_SECTION_SIZE (init ^uint64(0) = no limit, §4.2.2).
	// goaway is the largest request stream id the server will process (init ^uint64(0)
	// = "none", so any other value means a GOAWAY has landed and no new request may
	// be initiated, §5.2 — one atomic, no separate haveGoaway bool).
	maxFieldSection atomic.Uint64
	goaway          atomic.Uint64
}

// uniStream is an accepted server unidirectional stream whose leading stream-type
// varint has not yet been fully received (it may span datagrams).
type uniStream struct {
	stream quicStream
	buf    []byte
}

// NewClient wraps an established QUIC connection: it spawns the connection's
// reader goroutine, opens the client's control stream, and sends the mandatory
// first SETTINGS frame (RFC 9114 §6.2.1). The connection's handshake must already
// have completed.
func NewClient(conn *quic.Conn, settings []Setting) (*Client, error) {
	return newClient(connAdapter{conn}, settings)
}

func newClient(conn quicConn, settings []Setting) (*Client, error) {
	if err := validateSettings(settings); err != nil {
		return nil, err
	}
	c := &Client{conn: conn, readerDone: make(chan struct{})}
	c.maxFieldSection.Store(^uint64(0)) // no limit until the peer's SETTINGS arrive
	c.goaway.Store(^uint64(0))          // "none" until a real GOAWAY lands (§5.2)
	// The shared dynamic table's maximum capacity is the SETTINGS_QPACK_MAX_TABLE_
	// CAPACITY we advertise — the decoder controls its own table size (RFC 9204
	// §3.2.3). dial.go advertises a non-zero capacity, so the server may insert up
	// to this many bytes into the table we maintain and reference them.
	c.qpackDyn = qpack.NewDynamicTable(qpackMaxTableCapacity(settings))
	// The server's encoder stream is unframed, so nothing but this cap bounds the
	// partial instruction we hold while waiting for the rest of it (RFC 9204 §4.3).
	// It tracks the capacity we advertise above, so it admits every insert those
	// SETTINGS invite.
	c.qpackEncBufMax = qpackEncoderInstrCap(qpackMaxTableCapacity(settings))
	// SETTINGS_QPACK_BLOCKED_STREAMS (Q4): the ceiling on simultaneously-blocked
	// decodes we advertise, and a fresh broadcast channel a blocked Do parks on until
	// the reader advances the insert count (RFC 9204 §2.1.2, §2.1.3).
	c.qpackMaxBlocked = qpackBlockedStreamsSetting(settings)
	c.qpackReady = make(chan struct{})
	c.connCtx, c.connCancel = context.WithCancel(context.Background())

	// Open the control stream plus the client's own QPACK encoder + decoder streams
	// (RFC 9204 §4.2). OpenUniStream does not block — it fails immediately if the
	// peer granted too few unidirectional streams — so a server that grants fewer
	// than three is a startup error rather than a hang. All three are opened before
	// the reader starts so a stream-type varint is never sent out of order.
	control, err := conn.OpenUniStream()
	if err != nil {
		c.connCancel()
		c.markDead() // no reader was started
		return nil, err
	}
	qenc, err := conn.OpenUniStream()
	if err != nil {
		c.connCancel()
		c.markDead()
		return nil, err
	}
	qdec, err := conn.OpenUniStream()
	if err != nil {
		c.connCancel()
		c.markDead()
		return nil, err
	}
	c.clientQPACKEnc = qenc
	c.clientQPACKDec = qdec
	// Seed BOTH QPACK instruction streams' type bytes through the same serialized
	// write buffers the reader later appends to, so each type byte is guaranteed to
	// be the first byte on its stream even though the reader goroutine starts below.
	// The decoder stream's buffer carries the reader's Insert Count Increment; the
	// encoder stream's buffer carries the reader's Set Dynamic Table Capacity +
	// inserts once the server's SETTINGS enable the encode side (Q5) — routing the
	// encoder type byte here (rather than a separate startup send) closes the race
	// where the reader could flush Set Capacity ahead of the type byte. Best-effort
	// (non-blocking): any flow-control residue is flushed on a later reader pass.
	c.qpackDecWrite(AppendClientQPACKStream(nil, StreamTypeQPACKDecoder))
	c.encMu.Lock()
	c.encWriteBuf = append(c.encWriteBuf, AppendClientQPACKStream(nil, StreamTypeQPACKEncoder)...)
	c.flushEncWriteLocked()
	c.encMu.Unlock()

	// Start the reader BEFORE sending SETTINGS (docs/HTTP3_DESIGN.md §5, ordering
	// fix): a flow-control-blocked SETTINGS send is unblocked by the peer's MAX_DATA
	// that the reader processes — otherwise a startup deadlock.
	go c.readLoop()
	if err := c.sendAll(c.connCtx, control, AppendClientControlStream(nil, settings), false); err != nil {
		// The reader is running; tear it down so it is not leaked.
		c.connCancel()
		_ = c.conn.CloseWithError(true, H3NoError, "")
		<-c.readerDone
		return nil, err
	}
	return c, nil
}

// qpackMaxTableCapacity returns the SETTINGS_QPACK_MAX_TABLE_CAPACITY the client
// advertises (RFC 9204 §5), or 0 if the setting is absent — the value that sizes
// the shared dynamic table's maximum capacity. dial.go advertises a non-zero
// value, so the table is live.
func qpackMaxTableCapacity(settings []Setting) uint64 {
	for _, s := range settings {
		if s.ID == SettingQPACKMaxTableCapacity {
			return s.Value
		}
	}
	return 0
}

// qpackBlockedStreamsSetting returns the SETTINGS_QPACK_BLOCKED_STREAMS the client
// advertises (RFC 9204 §5, §2.1.2), or 0 if the setting is absent — the ceiling on
// how many request streams may be simultaneously blocked waiting for the encoder
// stream to reach a section's Required Insert Count. At 0 the decoder never parks:
// a Required Insert Count past the insert count fails immediately as
// QPACK_DECOMPRESSION_FAILED (the static-only / Q3 profile). dial.go advertises a
// non-zero value (Q4), enabling the blocked-decode wait.
func qpackBlockedStreamsSetting(settings []Setting) uint64 {
	for _, s := range settings {
		if s.ID == SettingQPACKBlockedStreams {
			return s.Value
		}
	}
	return 0
}

// readLoop is the connection's reader goroutine (docs/HTTP3_DESIGN.md §3.1): it
// owns Poll (QUIC receive, loss/PTO, idle, key-update, ACK) and the H3 control
// stream servicing for the connection's lifetime. serviceControl runs OUTSIDE
// c.mu — safe because Poll never returns holding it (the §3.2 postcondition). Any
// error is fatal to the connection.
func (c *Client) readLoop() {
	defer c.markDead()
	for {
		if err := c.conn.Poll(c.connCtx); err != nil {
			c.fatal(err)
			return
		}
		if err := c.serviceControl(); err != nil {
			c.fatal(err)
			return
		}
	}
}

// fatal tears the connection down on a reader-goroutine error. The QUIC layer has
// already latched its terminal error (terminateLocked) for its own teardown paths,
// so a blocked Do has usually already woken with the meaningful error; this
// cancels connCtx (stopping the read watchdog) and closes the transport as a
// catch-all. Idempotent: CloseWithError is a no-op once the connection is closed,
// so it never clobbers a close code an earlier teardown already set.
func (c *Client) fatal(_ error) {
	c.connCancel()
	_ = c.conn.CloseWithError(true, H3InternalError, "")
}

// Close terminates the HTTP/3 connection, sending a CONNECTION_CLOSE with
// H3_NO_ERROR (RFC 9114 §8.1) so the server can release the connection
// immediately rather than waiting for its idle timeout, then stopping the reader
// goroutine. It is idempotent across sequential calls.
//
// Close may be called while a Do is in flight: CloseWithError latches the graceful
// terminal error BEFORE connCancel (docs/HTTP3_DESIGN.md §3.1, F6), so the reader's
// fatal(connCtx.Err()) finds the latch taken and the in-flight Do wakes with the
// graceful ErrConnClosed rather than context.Canceled. The reader releases c.mu
// before returning, so waiting on readerDone here never holds a lock (F6).
func (c *Client) Close() error {
	err := c.conn.CloseWithError(true, H3NoError, "") // latch + CONNECTION_CLOSE + pc.Close
	c.connCancel()                                    // wake the parked reader
	<-c.readerDone                                    // never held under a lock (F6)
	return err
}

// Alive reports, without blocking, whether the connection's reader goroutine is
// still running — i.e. the QUIC connection has neither hit a fatal error nor been
// closed. It is a liveness probe for connection pools: once the reader exits (a
// transport error, an idle timeout, a peer CONNECTION_CLOSE, or a local Close),
// readerDone is closed and Alive returns false, so the pool can evict the client.
//
// There is a brief window between a fatal error latching in the QUIC layer and the
// reader closing readerDone during which Alive still reports true; a Do issued in
// that window returns the terminal error, so a pool that re-checks Alive on release
// still evicts the dead connection promptly (matching the HTTP/2 pool's
// eject-on-release contract).
func (c *Client) Alive() bool { return !c.dead.Load() }

// markDead retires the reader: it publishes the flag Alive reads and then closes
// readerDone. Every path that ends the reader goes through here, so the two can
// never disagree — a raw close would leave Alive reporting true forever and the
// pool handing out a corpse.
//
// The order is load-bearing. Storing BEFORE the close means a goroutine woken by
// readerDone always observes dead == true; closing first would let it see the
// connection as alive. Being early is safe (Alive is already documented as
// allowed to report death slightly ahead of the reader's exit); being late is
// the bug.
func (c *Client) markDead() {
	c.dead.Store(true)
	close(c.readerDone)
}

// GoingAway reports whether the peer has sent GOAWAY. After that this connection
// refuses every new request (RFC 9114 §5.2, enforced in openRequestStream) while
// still finishing the exchanges already in flight — that is the point of a
// graceful shutdown. A pool must therefore stop handing the connection out but
// MUST NOT close it until those exchanges drain; Alive stays true meanwhile.
func (c *Client) GoingAway() bool { return c.goaway.Load() != ^uint64(0) }

// sendAll writes the whole of data on stream. Stream.Send consumes only a prefix
// when flow-control / congestion / pacing blocked, so this advances past each
// partial send until every byte — and the FIN, which rides the final byte — is on
// the wire. When a Send makes no progress (returns zero), it parks on WaitSendable
// until the stream may admit more: a MAX_STREAM_DATA / MAX_DATA frame, the cwnd
// broadcast, or the pacing timer, all delivered by the reader (docs/HTTP3_DESIGN.md
// §3.3). A partial (non-zero) send is retried immediately.
func (c *Client) sendAll(ctx context.Context, stream quicStream, data []byte, fin bool) error {
	sent := 0
	for {
		n, err := stream.Send(data[sent:], fin)
		if err != nil {
			return err
		}
		sent += n
		if sent >= len(data) {
			return nil
		}
		if n == 0 {
			if err := stream.WaitSendable(ctx); err != nil {
				return err
			}
		}
	}
}

// qpackDecWrite appends a complete QPACK decoder-stream instruction (RFC 9204
// §4.4) and flushes as much as the decoder stream's flow-control window admits
// without blocking. Any tail left by back-pressure is retained and flushed by the
// next call or the reader's next service pass, so the byte stream stays whole even
// when split across sends — every caller appends only complete instructions.
//
// All decoder-stream writers funnel through here: the reader's Insert Count
// Increment, each Do's Section Acknowledgment, and a Stream Cancellation on abort.
// decWMu serializes them, so instructions never interleave. The Send is
// best-effort (it never parks on WaitSendable), so this is safe to call from the
// reader goroutine — it never waits for itself — and decWMu is a leaf lock never
// held across a wait.
func (c *Client) qpackDecWrite(instr []byte) {
	c.decWMu.Lock()
	defer c.decWMu.Unlock()
	if len(instr) > 0 {
		c.decWriteBuf = append(c.decWriteBuf, instr...)
	}
	c.flushDecWriteLocked()
}

// flushDecWriteLocked sends as much of decWriteBuf as the decoder stream will
// accept without blocking, retaining any unsent tail. Assumes decWMu is held.
func (c *Client) flushDecWriteLocked() {
	if c.clientQPACKDec == nil || len(c.decWriteBuf) == 0 {
		return
	}
	n, err := c.clientQPACKDec.Send(c.decWriteBuf, false)
	if err != nil {
		return // stream/connection error; the reader teardown handles it
	}
	if n >= len(c.decWriteBuf) {
		c.decWriteBuf = c.decWriteBuf[:0]
	} else {
		c.decWriteBuf = append(c.decWriteBuf[:0], c.decWriteBuf[n:]...) // left-shift the residue
	}
	c.decHasResidue.Store(len(c.decWriteBuf) > 0)
}

// flushQPACKDecoder flushes any flow-control residue on the decoder stream. The
// reader calls it once per service pass, gated by decHasResidue so the common
// no-residue path skips the lock entirely.
func (c *Client) flushQPACKDecoder() {
	c.decWMu.Lock()
	c.flushDecWriteLocked()
	c.decWMu.Unlock()
}

// enableEncoderDynamic installs the connection's encode-side dynamic table once
// the server's SETTINGS reveal its SETTINGS_QPACK_MAX_TABLE_CAPACITY (RFC 9204
// §3.2.3, Q5). It is a no-op when the server advertises a capacity too small to
// hold any entry (below 32, so MaxEntries would be 0), leaving the encoder static-
// only. Our table capacity is min(server max, our own qpackDynamicTableCapacity),
// and a Set Dynamic Table Capacity instruction (§4.3.1) is queued as the first
// bytes on our encoder stream. Idempotent: the server sends SETTINGS once, but a
// second call is guarded. Runs on the reader goroutine (applyServerSettings),
// outside c.mu; encMu is a leaf lock.
func (c *Client) enableEncoderDynamic(serverMaxCapacity uint64) {
	if serverMaxCapacity < 32 {
		return // static-only: no whole entry fits, dynamic referencing impossible
	}
	chosen := serverMaxCapacity
	if chosen > qpackDynamicTableCapacity {
		chosen = qpackDynamicTableCapacity // our table need not be as large as the server permits
	}
	c.encMu.Lock()
	defer c.encMu.Unlock()
	if c.qpackEncoder != nil {
		return // already installed (SETTINGS is processed once)
	}
	enc, err := qpack.NewDynamicEncoder(serverMaxCapacity, chosen)
	if err != nil {
		return // stay static-only rather than risk a malformed encoder table
	}
	c.qpackEncoder = enc
	c.encWriteBuf = enc.DrainEncoderInstructions(c.encWriteBuf) // the Set Dynamic Table Capacity instruction
	c.flushEncWriteLocked()
}

// flushEncWriteLocked sends as much of encWriteBuf as the encoder stream will
// accept without blocking, retaining any unsent tail (RFC 9204 §4.2). Assumes
// encMu is held. Best-effort like the decoder-stream flush: the Send never parks,
// so encMu is never held across a wait, and the retained residue preserves the
// insertion order the byte stream requires.
func (c *Client) flushEncWriteLocked() {
	if c.clientQPACKEnc == nil || len(c.encWriteBuf) == 0 {
		return
	}
	n, err := c.clientQPACKEnc.Send(c.encWriteBuf, false)
	if err != nil {
		return // stream/connection error; the reader teardown handles it
	}
	if n >= len(c.encWriteBuf) {
		c.encWriteBuf = c.encWriteBuf[:0]
	} else {
		c.encWriteBuf = append(c.encWriteBuf[:0], c.encWriteBuf[n:]...) // left-shift the residue
	}
	c.encHasResidue.Store(len(c.encWriteBuf) > 0)
}

// flushQPACKEncoderStream flushes any flow-control residue on our encoder stream.
// The reader calls it once per service pass, gated by encHasResidue so the common
// no-residue path skips the lock entirely.
func (c *Client) flushQPACKEncoderStream() {
	c.encMu.Lock()
	c.flushEncWriteLocked()
	c.encMu.Unlock()
}

// encodeRequestHeaders builds req's HEADERS frame. When the server enabled dynamic
// QPACK it uses the connection's shared encoder dynamic table (Q5): repeated header
// entries are inserted (with the resulting Insert instructions queued on our
// encoder stream, in insertion order) and acknowledged entries are referenced. It
// holds encMu for the whole encode so concurrent Do calls serialize their table
// inserts and encoder-stream writes; the encode is CPU-only and the encoder-stream
// flush is best-effort, so encMu is never held across a wait. When the encoder is
// static-only (pre-SETTINGS or server capacity 0) it produces exactly the
// static-table output — byte-for-byte today's encoding — via a throwaway encoder.
func (c *Client) encodeRequestHeaders(req *Request) ([]byte, error) {
	c.encMu.Lock()
	defer c.encMu.Unlock()
	if c.qpackEncoder == nil {
		var enc qpack.Encoder // static-only profile (nil dynamic table)
		return req.EncodeHeaders(&enc, nil, c.maxFieldSection.Load())
	}
	frame, err := req.EncodeHeaders(c.qpackEncoder, nil, c.maxFieldSection.Load())
	if err != nil {
		return nil, err
	}
	before := len(c.encWriteBuf)
	c.encWriteBuf = c.qpackEncoder.DrainEncoderInstructions(c.encWriteBuf)
	if len(c.encWriteBuf) > before {
		c.flushEncWriteLocked()
	}
	return frame, nil
}

// qpackAckInserts emits an Insert Count Increment (RFC 9204 §4.4.3) covering the
// dynamic-table insertions the encoder does not yet know we hold — those past the
// Known Received Count — and advances the Known Received Count to insertCount. The
// reader calls it after applying encoder-stream inserts. It is a no-op when the
// Known Received Count already covers insertCount, which happens when a Section
// Acknowledgment implicitly acknowledged the same inserts first (§2.1.4): counting
// them again would push the encoder's Known Received Count past the inserts it has
// made, a QPACK_DECODER_STREAM_ERROR. The Known Received Count update and the
// decoder-stream append are both under decWMu, so the increment matches the order
// the encoder applies our instructions. The increment is what lets a conformant
// server learn we hold an insert before it may reference it, so this fires promptly
// as inserts arrive; the same reader pass also broadcasts (signalQPACKReady) to wake
// any decode blocked waiting for that insert count (§2.1.3, Q4).
func (c *Client) qpackAckInserts(insertCount uint64) {
	c.decWMu.Lock()
	defer c.decWMu.Unlock()
	if insertCount > c.knownReceivedCount {
		c.decWriteBuf = append(c.decWriteBuf, qpack.AppendInsertCountIncrement(nil, insertCount-c.knownReceivedCount)...)
		c.knownReceivedCount = insertCount
	}
	c.flushDecWriteLocked()
}

// qpackSectionAck emits a Section Acknowledgment (RFC 9204 §4.4.1) for streamID
// after a field section with Required Insert Count ric > 0 (one that referenced
// the dynamic table) was fully decoded. A Section Acknowledgment also implicitly
// advances the encoder's Known Received Count to ric (§2.1.4), so we mirror that:
// inserts up to ric are now known-received and MUST NOT be re-acknowledged by a
// later Insert Count Increment. The acknowledgment is always sent (the encoder
// needs it to release the stream's outstanding references); only the Known
// Received Count update is conditional. Both are under decWMu so they stay
// consistent with the wire order.
func (c *Client) qpackSectionAck(streamID, ric uint64) {
	c.decWMu.Lock()
	defer c.decWMu.Unlock()
	if ric > c.knownReceivedCount {
		c.knownReceivedCount = ric
	}
	c.decWriteBuf = append(c.decWriteBuf, qpack.AppendSectionAcknowledgment(nil, streamID)...)
	c.flushDecWriteLocked()
}

// qpackCancelStream sends a Stream Cancellation (RFC 9204 §4.4.2) for streamID on
// our decoder stream, telling the server's encoder that an aborted stream will not
// acknowledge the dynamic-table references its field sections made. It does not
// touch the Known Received Count — a cancelled section was never acknowledged, so
// its inserts are only known-received if a prior Insert Count Increment already
// reported them. It is reached only when an aborted stream referenced the table.
func (c *Client) qpackCancelStream(streamID uint64) {
	c.qpackDecWrite(qpack.AppendStreamCancellation(nil, streamID))
}

// signalQPACKReady wakes every Do parked in waitQPACKInsert so each re-checks
// whether the shared dynamic table's insert count has reached its section's
// Required Insert Count (RFC 9204 §2.1.3). The reader calls it after
// readQPACKEncoder advances the insert count. Unlike the per-stream signalReady and
// the single-slot c.streamCredit — which each hand a token to ONE waiter — a single
// advance of the insert count can unblock ANY number of parked Dos, each waiting on
// a different Required Insert Count, so this is a broadcast: it closes the current
// readiness channel (waking all current waiters at once) and installs a fresh one
// for the next generation. It never holds qpackReadyMu across a wait (it only
// closes and re-makes a channel), so it is safe to call from the reader goroutine.
func (c *Client) signalQPACKReady() {
	c.qpackReadyMu.Lock()
	close(c.qpackReady)
	c.qpackReady = make(chan struct{})
	c.qpackReadyMu.Unlock()
}

// waitQPACKInsert parks until the shared dynamic table's insert count reaches need
// — a section's Required Insert Count that is ahead of what the encoder stream has
// delivered (RFC 9204 §2.1.3) — or the request/connection ends. It is
// level-triggered and never holds a lock across the wait (R2): it captures the
// current broadcast channel under qpackReadyMu, re-reads the insert count (the
// atomic the reader publishes), and returns as soon as insertCount >= need; a
// signalQPACKReady between the read and the select closes the captured channel, so
// the recheck-then-park pair closes the lost-wakeup window exactly like the cap-1
// WaitReadable/streamCredit waits. The wait is bounded three ways so it can never
// hang: the request ctx (a promised insert that never arrives fails on the caller's
// deadline/cancel), and connCtx (the reader cancels it on connection teardown).
func (c *Client) waitQPACKInsert(ctx context.Context, need uint64) error {
	for {
		c.qpackReadyMu.Lock()
		ready := c.qpackReady
		c.qpackReadyMu.Unlock()
		if c.insertCount.Load() >= need {
			return nil // the encoder stream has caught up; retry the decode
		}
		select {
		case <-ready:
			// The reader advanced the insert count; loop to re-check the level.
		case <-ctx.Done():
			return ctx.Err() // per-request cancel/deadline — the no-hang guarantee
		case <-c.connCtx.Done():
			return c.connCtx.Err() // the connection was torn down (fatal / Close)
		}
	}
}

// registerBlocked reserves one blocked-stream slot, enforcing the advertised
// SETTINGS_QPACK_BLOCKED_STREAMS ceiling (RFC 9204 §2.1.2) with an atomic
// compare-and-increment so concurrent Dos cannot overshoot it. It returns false
// when all qpackMaxBlocked slots are taken — the encoder referenced entries in a
// way that would block more streams than we permit, a decoder-detectable violation
// the caller surfaces as QPACK_DECOMPRESSION_FAILED. At qpackMaxBlocked == 0 (the
// static-only / Q3 profile) it always returns false, so a blocked section fails
// immediately rather than parking.
func (c *Client) registerBlocked() bool {
	for {
		cur := c.qpackBlocked.Load()
		if cur >= c.qpackMaxBlocked {
			return false
		}
		if c.qpackBlocked.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// unregisterBlocked releases a slot reserved by registerBlocked once the stream is
// no longer blocked (its decode resolved, or the request/connection ended).
func (c *Client) unregisterBlocked() {
	c.qpackBlocked.Add(^uint64(0)) // -1
}

// decodeBlocking decodes a response field section against the shared dynamic table,
// waiting for the encoder stream to reach the section's Required Insert Count when
// it references entries not yet delivered (RFC 9204 §2.1.3, Q4). decodeUnderLock is
// invoked with qpackMu held for reading and the section guaranteed decodable
// (insertCount >= its Required Insert Count), so its own Required-Insert-Count gate
// never trips and the fields it copies cannot be rewritten by a concurrent
// insert/eviction; it must complete every copy before returning. The
// Required-Insert-Count check and the decode run under the SAME RLock, so the
// insert count cannot advance between them (a consistent snapshot).
//
// When the section is blocked (its Required Insert Count is ahead of the insert
// count) this reserves a blocked-stream slot (enforcing the M-blocked-stream limit,
// §2.1.2) and parks off qpackMu on waitQPACKInsert until the reader delivers the
// promised inserts, then retries — level-triggered. It returns:
//   - nil once decodeUnderLock succeeds;
//   - qpack.ErrDecompressionFailed for a malformed / never-satisfiable prefix or an
//     M-blocked-limit violation (the caller maps it to QPACK_DECOMPRESSION_FAILED);
//   - decodeUnderLock's own error (a message-rule violation or a decode failure); or
//   - the request/connection error from waitQPACKInsert (ctx cancel/deadline or
//     teardown) — so a promised insert that never arrives fails, never hangs.
func (c *Client) decodeBlocking(rb *respBuilder, payload []byte, decodeUnderLock func() error) error {
	registered := false
	defer func() {
		if registered {
			c.unregisterBlocked()
		}
	}()
	for {
		c.qpackMu.RLock()
		ric, rerr := qpack.RequiredInsertCount(payload, c.qpackDyn)
		if rerr != nil {
			c.qpackMu.RUnlock()
			return rerr // a malformed or never-satisfiable prefix — a decompression failure
		}
		if ric <= c.qpackDyn.InsertCount() {
			err := decodeUnderLock() // decodable now — decode under the same consistent RLock
			c.qpackMu.RUnlock()
			return err
		}
		c.qpackMu.RUnlock()
		// Blocked: the section references entries the encoder stream has not delivered.
		if !registered {
			if !c.registerBlocked() {
				return qpack.ErrDecompressionFailed // exceeded SETTINGS_QPACK_BLOCKED_STREAMS (§2.1.2)
			}
			registered = true
		}
		if werr := c.waitQPACKInsert(rb.ctx, ric); werr != nil {
			return werr // ctx cancel/deadline or connection teardown — bounded, never a hang
		}
	}
}

// Do sends req on a new request stream and reads the response, driving the QUIC
// connection's receive loop until the response stream finishes. It returns the
// response head and the fully-buffered body. The request carries no body: the
// HEADERS frame is sent with FIN.
//
// ctx bounds the whole exchange: a cancel or deadline unblocks the receive loop
// and Do returns ctx.Err(). On any error (a context cancel, or a malformed
// response) the request stream is aborted with STOP_SENDING and RESET_STREAM so
// the server frees it and stops sending (RFC 9000 §3.5, RFC 9114 §4.1).
func (c *Client) Do(ctx context.Context, req *Request) (*Response, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	stream, err := c.openRequestStream(ctx)
	if err != nil {
		return nil, nil, err
	}
	resp, body, err := c.roundTrip(ctx, stream, req)
	if err != nil {
		abortStream(stream, err)
	}
	return resp, body, err
}

// DoStream sends req on a new request stream and returns after the final response
// HEADERS frame, before the body is buffered. It yields the response head plus a
// *BodyReader that pulls DATA frames incrementally (and the trailer section, if
// any) over the same reader-goroutine wake machinery Do uses, so peak retained
// memory is one frame rather than the whole body. The caller MUST call
// BodyReader.Close when it stops reading early, so the request stream is aborted
// and its resources released.
//
// ctx bounds the whole exchange, including every BodyReader.Next call. On an error
// before the head is returned the stream is aborted exactly like Do; once the head
// is returned, BodyReader owns aborting the stream on its own errors and on Close.
func (c *Client) DoStream(ctx context.Context, req *Request) (*Response, ResponseBody, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	stream, err := c.openRequestStream(ctx)
	if err != nil {
		return nil, nil, err
	}
	resp, br, err := c.roundTripStream(ctx, stream, req)
	if err != nil {
		abortStream(stream, err)
		return nil, nil, err
	}
	return resp, br, nil
}

// openRequestStream opens a new bidirectional request stream and applies the
// GOAWAY gate shared by Do and DoStream.
//
// OpenStream blocks on stream credit (2d): when at the peer's cumulative
// initial_max_streams_bidi limit it parks until a MAX_STREAMS grant, ctx fires, or
// the connection terminates — so a wave of concurrent requests past the limit
// waits for the peer to raise it instead of racing an immediate error.
//
// GOAWAY gate (RFC 9114 §5.2): "Endpoints MUST NOT initiate new requests ... after
// receipt of a GOAWAY frame from the peer" — unconditionally, not only for stream
// ids at or above the published one. §5.2 blesses a two-stage shutdown in which the
// server first sends GOAWAY with the maximum request id (2^62-4) and lowers it
// later, so gating on the id alone would keep admitting requests forever. goaway is
// ^uint64(0) until a real GOAWAY lands, so this never trips on a healthy connection.
// The reader publishes goaway from serviceControl; the request reads it (§3.5).
// Reclaim the just-opened stream with STOP_SENDING + RESET_STREAM so maybeRetire
// drops its c.streams entry — the post-open abandon path that closes the 2d TOCTOU
// leak (F4), and which also covers a GOAWAY landing during OpenStream.
func (c *Client) openRequestStream(ctx context.Context) (quicStream, error) {
	stream, err := c.conn.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	if c.goaway.Load() != ^uint64(0) {
		_ = stream.StopSending(H3RequestCancelled)
		_ = stream.Reset(H3RequestCancelled)
		return nil, ErrGoAway // the caller should retry on a new connection
	}
	return stream, nil
}

// abortStream best-effort aborts an abandoned request exchange with STOP_SENDING +
// RESET_STREAM so the server frees it and stops sending (RFC 9000 §3.5, RFC 9114
// §4.1). A malformed response is a stream error of type H3_MESSAGE_ERROR (§4.1.2);
// a response that exceeds a buffering cap is signalled H3_EXCESSIVE_LOAD (§8.1);
// anything else (a context cancel, a decode abort, an early Close) is signalled
// H3_REQUEST_CANCELLED.
func abortStream(stream quicStream, err error) {
	code := H3RequestCancelled
	switch {
	case errors.Is(err, ErrH3Message):
		code = H3MessageError
	case errors.Is(err, ErrResponseTooLarge):
		code = H3ExcessiveLoad
	}
	_ = stream.StopSending(code)
	_ = stream.Reset(code)
}

// roundTrip sends the request on stream and reads the response. Its caller aborts
// the stream on a non-nil error.
// sendRequest writes the request's HEADERS — ending the stream immediately when
// there is no body — and then the body in a DATA frame carrying the FIN (RFC 9114
// §4.1). If the server aborts reading the request with STOP_SENDING (surfaced as
// ErrStreamReset), the send stops but is not fatal: the caller still reads the
// response on the stream's independent receive side (§4.1). Any other send error is
// returned.
func (c *Client) sendRequest(ctx context.Context, stream quicStream, req *Request, frame []byte) error {
	hasBody := len(req.Body) > 0
	if hasBody {
		// Append the DATA frame header (a <=9-byte Type+Length varint pair) to
		// the request-owned HEADERS buffer, then stream req.Body directly, rather
		// than materialising the whole body into a fresh DATA buffer via
		// AppendData. The old copy bought nothing: writeStreamFrame takes its own
		// retransmit copy of every chunk regardless, so the body was copied twice
		// before the wire — measured at ~len(body) B/op per request.
		//
		// The header rides the HEADERS datagram rather than being sent on its own:
		// a lone sendAll of 9 bytes would flush a GSO batch by itself, one extra
		// datagram and syscall per request, which on a stack whose ceiling is the
		// syscall rate is the wrong trade. frame comes from EncodeHeaders(enc, nil,
		// ...) fresh per request, so appending to it aliases nothing.
		frame = AppendFrameHeader(frame, FrameData, uint64(len(req.Body)))
	}
	if err := c.sendAll(ctx, stream, frame, !hasBody); err != nil {
		if !errors.Is(err, quic.ErrStreamReset) {
			return err
		}
		return nil // send aborted by STOP_SENDING; still read the response
	}
	if hasBody {
		if err := c.sendAll(ctx, stream, req.Body, true); err != nil && !errors.Is(err, quic.ErrStreamReset) {
			return err
		}
	}
	return nil
}

func (c *Client) roundTrip(ctx context.Context, stream quicStream, req *Request) (resp *Response, body []byte, err error) {
	// Per-request QPACK decoder (docs/HTTP3_DESIGN.md §5, PR 2d): its Huffman scratch
	// is stack-local, so N concurrent Do never share it (a shared decoder would let
	// one Do's decoded headers, which alias that scratch, be overwritten by another —
	// per-request is mandatory). The encoder is the OTHER shared piece (Q5): the
	// connection-scoped dynamic table, serialized inside encodeRequestHeaders under
	// encMu, so repeated headers across concurrent requests build one shared table.
	var dec qpack.Decoder
	frame, eerr := c.encodeRequestHeaders(req)
	if eerr != nil {
		return nil, nil, eerr
	}
	if serr := c.sendRequest(ctx, stream, req, frame); serr != nil {
		return nil, nil, serr
	}

	var fr FrameReader
	fr.SetMaxFrameLen(maxResponseBytes) // refuse a frame larger than the whole budget before buffering it
	rb := respBuilder{dec: &dec, streamID: stream.ID(), ctx: ctx}
	// On any abort of a stream that referenced the dynamic table, notify the encoder
	// (RFC 9204 §4.4.2). refDynamic is set when a decoded section resolved a dynamic
	// reference (Required Insert Count > 0).
	defer func() {
		if err != nil && rb.refDynamic {
			c.qpackCancelStream(rb.streamID)
		}
	}()
	for {
		if data := stream.Recv(); len(data) > 0 {
			fr.Feed(data)
		}
		if err := c.consumeFrames(&fr, &rb); err != nil {
			return nil, nil, err
		}
		// One locked snapshot of the receive side (docs/HTTP3_DESIGN.md §3.4, F5).
		finished, reset, code := stream.RecvState()
		if !finished {
			// Park until the reader signals this stream has more response data, the
			// per-request context is cancelled, or the connection terminates (§3.3).
			// Level-triggered: the next iteration re-reads the predicate under c.mu.
			if err := stream.WaitReadable(ctx); err != nil {
				return nil, nil, err
			}
			continue
		}
		// The stream is finished. The reader is asynchronous, so finished can flip
		// between the Recv at the top of this iteration and now; drain once more to
		// feed any bytes delivered in that window before concluding — since finished
		// means the FIN is in, no further bytes can arrive after this drain.
		if data := stream.Recv(); len(data) > 0 {
			fr.Feed(data)
			continue // re-parse the newly drained bytes before concluding
		}
		if reset {
			// The server aborted with RESET_STREAM (RFC 9000 §3.5); surface it so the
			// caller can tell a rejected (retryable) request from a completed one (§4.1.1).
			return nil, nil, &StreamResetError{Code: code}
		}
		if fr.Buffered() > 0 {
			// The stream ended cleanly mid-frame, truncating the last frame (§7.1).
			return nil, nil, c.connError(H3FrameError)
		}
		break
	}
	return finalizeResponse(rb.resp, rb.body, req, rb.interim)
}

// respBuilder accumulates a decoded HTTP/3 response as request-stream frames are
// parsed across successive FrameReader feeds (see roundTrip / consumeFrames).
type respBuilder struct {
	dec          *qpack.Decoder // per-request QPACK decoder (2d): its Huffman scratch is aliased by decoded slices, so it MUST NOT be shared across concurrent Do
	resp         *Response
	interim      []*Response // informational 1xx responses, in receive order
	body         []byte
	total        uint64 // header, body, trailer, and 1xx payload bytes retained so far
	trailersSeen bool

	// ctx bounds a blocked field-section decode (RFC 9204 §2.1.3, Q4): when a section
	// references dynamic entries the encoder stream has not delivered yet, dispatchFrame
	// parks on it via decodeBlocking until the insert count catches up, and this ctx —
	// the request's own — is what makes that wait finite (a promised insert that never
	// arrives fails on cancel/deadline). Carried on rb so dispatchFrame, called from
	// both the buffered and streaming receive loops, need not change signature; the
	// streaming BodyReader refreshes it per Next call. It is nil only in unit tests that
	// drive a non-blocking dispatchFrame directly, where the wait is never reached.
	ctx context.Context

	// streamID is the request stream's id, used to key the QPACK decoder-stream
	// instructions (Section Acknowledgment / Stream Cancellation) this exchange
	// emits. refDynamic records that at least one decoded field section referenced
	// the shared dynamic table, so an abort of this stream must send a Stream
	// Cancellation (RFC 9204 §4.4.2).
	streamID   uint64
	refDynamic bool

	// onData, when non-nil, receives each DATA frame's payload instead of it
	// being appended to body — the streaming path (DoStream). The payload aliases
	// the FrameReader buffer and is valid only until the next ReadFrame/Feed, so
	// the callback must consume or hand it off before the reader advances. Buffered
	// Do leaves onData nil.
	onData func(payload []byte) error
}

// consumeFrames reads and dispatches every complete frame currently buffered in
// fr, accumulating into rb. It returns nil when the reader needs more stream
// bytes (ErrNeedMore) or after the buffer drains, and a non-nil error — already
// scoped to the right connection/stream level — on any protocol violation or an
// oversized frame (mapped to ErrResponseTooLarge).
func (c *Client) consumeFrames(fr *FrameReader, rb *respBuilder) error {
	for {
		typ, payload, rerr := fr.ReadFrame()
		if errors.Is(rerr, ErrNeedMore) {
			return nil // wait for more stream bytes
		}
		if rerr != nil {
			// An oversized frame (ErrH3FrameTooLarge) — abort rather than buffer it.
			return ErrResponseTooLarge
		}
		if err := c.dispatchFrame(rb, typ, payload); err != nil {
			return err
		}
	}
}

// ackDynamicSection, when the just-decoded field section referenced the shared
// dynamic table (Required Insert Count ric > 0), records it on rb and emits a
// Section Acknowledgment for the stream on our decoder stream (RFC 9204 §2.1.4,
// §4.4.1), advancing the Known Received Count to ric. It is called AFTER
// qpackMu.RUnlock, so the decoder-stream Send never runs under the table lock — the
// two are independent leaf locks (decWMu, qpackMu). ric is captured under the same
// RLock as the decode, so it reflects the table state the section resolved against.
func (c *Client) ackDynamicSection(rb *respBuilder, ric uint64) {
	if ric == 0 {
		return
	}
	rb.refDynamic = true
	c.qpackSectionAck(rb.streamID, ric)
}

// dispatchFrame folds one request-stream frame into rb, enforcing HTTP/3 message
// order and the response-size caps (RFC 9114 §4.1). It returns a scoped error for
// an invalid frame sequence (connection error), an oversized response
// (ErrResponseTooLarge), or a header decode failure; nil otherwise.
func (c *Client) dispatchFrame(rb *respBuilder, typ uint64, payload []byte) error {
	switch typ {
	case FrameHeaders:
		// Message order (RFC 9114 §4.1): (1xx HEADERS)* final HEADERS DATA*
		// trailer-HEADERS?. A HEADERS after the trailers is an invalid frame
		// sequence — a connection error, not a stream error.
		if rb.trailersSeen {
			return c.connError(H3FrameUnexpected)
		}
		rb.total += uint64(len(payload))
		if rb.total > maxResponseBytes {
			return ErrResponseTooLarge // retained header/body/trailer bytes over the cap
		}
		if rb.resp == nil {
			// Decode under qpackMu.RLock so the reader's concurrent inserts / arena
			// compaction cannot rewrite the shared bytes the emit callback copies; the
			// copy completes before RUnlock (DecodeResponseHeaders copies each kept
			// field inside the callback). When the section's Required Insert Count is
			// ahead of the insert count (Q4), decodeBlocking parks off qpackMu until the
			// encoder stream catches up (§2.1.3), bounded by rb.ctx. Section Ack is
			// emitted after the decode resolves, off the table lock.
			var r *Response
			var ric uint64
			derr := c.decodeBlocking(rb, payload, func() error {
				var e error
				r, ric, e = DecodeResponseHeaders(rb.dec, c.qpackDyn, payload)
				return e
			})
			if derr != nil {
				_, _, e := c.decodeError(derr)
				return e
			}
			c.ackDynamicSection(rb, ric)
			if r.Status < 200 {
				if len(rb.interim) >= maxInterimResponses {
					return ErrResponseTooLarge // a 1xx flood (RFC 9114 §4.1)
				}
				rb.interim = append(rb.interim, r) // informational 1xx; keep reading
			} else {
				rb.resp = r
			}
		} else {
			var ric uint64
			derr := c.decodeBlocking(rb, payload, func() error {
				tr, r, e := DecodeTrailers(rb.dec, c.qpackDyn, payload)
				if e != nil {
					return e
				}
				rb.resp.Trailers = tr // fields are copied inside the decode, safe to hold past RUnlock
				ric = r
				return nil
			})
			if derr != nil {
				_, _, e := c.decodeError(derr)
				return e
			}
			c.ackDynamicSection(rb, ric)
			rb.trailersSeen = true
		}
	case FrameData:
		if rb.resp == nil || rb.trailersSeen {
			// DATA before the final response, after a 1xx (which has no body), or
			// after the trailers is an invalid frame sequence (RFC 9114 §4.1).
			return c.connError(H3FrameUnexpected)
		}
		if rb.onData != nil {
			// Streaming path (DoStream): the chunk is handed to the BodyReader and
			// not retained — peak memory is one frame, not the whole body — so it
			// does NOT count against maxResponseBytes, the cap on bytes held
			// together in memory. Counting it aborted a legitimate streamed download
			// larger than the cap even though nothing over one frame was ever kept.
			return rb.onData(payload)
		}
		// Buffered path (Do): the body accumulates in rb.body, so it is capped
		// alongside the retained header and trailer bytes.
		rb.total += uint64(len(payload))
		if rb.total > maxResponseBytes {
			return ErrResponseTooLarge
		}
		if rb.body == nil {
			// The overwhelmingly common shape is one DATA frame, and copying it
			// here doubles what the response costs: the reader has already
			// assembled the whole payload contiguously, so the append below only
			// grows a second buffer to the same size. Adopt the reader's bytes
			// instead. Two properties of FrameReader make this safe, and both are
			// pinned by TestRespBuilder_AdoptedBodyIsSafeToAlias:
			//
			//   1. ReadFrame caps the payload at its own length (stream.go's
			//      three-index slice), so the append below — reached as soon as a
			//      second DATA frame arrives — must allocate rather than write
			//      into the reader's buffer.
			//   2. Feed only ever appends at the tail and ReadFrame only ever
			//      slides the window forward, so no consumed region is written
			//      again. A compacting Feed would break this silently.
			//
			// fr is a per-request local (see roundTrip), so the array outlives the
			// exchange and belongs to the caller afterwards.
			rb.body = payload
			return nil
		}
		rb.body = append(rb.body, payload...)
	case FrameSettings, FrameCancelPush, FrameGoaway, FrameMaxPushID, 0x02, 0x06, 0x08, 0x09:
		// Control-stream-only and reserved HTTP/2-carryover frame types MUST NOT
		// appear on a request stream (SETTINGS §7.2.4, CANCEL_PUSH §7.2.3, GOAWAY
		// §7.2.6, MAX_PUSH_ID §7.2.7, reserved §7.2.8): a connection error.
		return c.connError(H3FrameUnexpected)
	case FramePushPromise:
		// Server push is disabled — the client never sends MAX_PUSH_ID — so a
		// PUSH_PROMISE is invalid (RFC 9114 §4.6, §7.2.5): a connection error.
		return c.connError(H3IDError)
	default:
		// GREASE (0x1f·N+0x21) and other genuinely-unknown frame types on a request
		// stream MUST be ignored (§9, §7.2.8).
	}
	return nil
}

// finalizeResponse validates a fully received response and returns it, or
// ErrH3Message if it is malformed: there was no final (non-1xx) response, or a
// response that can carry content declared a Content-Length that does not equal
// the sum of the DATA frame payloads received (RFC 9114 §4.1.2). Responses that
// never have content (204, 304, a HEAD response) may carry an anticipatory
// Content-Length that does not match the absent body.
func finalizeResponse(resp *Response, body []byte, req *Request, interim []*Response) (*Response, []byte, error) {
	if resp == nil {
		return nil, nil, ErrH3Message
	}
	if !contentLengthMatches(resp, req.Method, int64(len(body))) {
		return nil, nil, ErrH3Message
	}
	resp.Interim = interim
	return resp, body, nil
}

// contentLengthMatches reports whether resp's declared Content-Length is
// consistent with a body of bodyLen bytes for a request using method (RFC 9114
// §4.1.2). A response that can carry content and declares a Content-Length must
// have it equal the received DATA; a response that never has content (204, 304, a
// HEAD response) or declares no Content-Length is always consistent. It is shared
// by the buffered finalizeResponse and the streaming BodyReader, which validates
// once the whole body has been observed.
func contentLengthMatches(resp *Response, method string, bodyLen int64) bool {
	if !canHaveContent(resp.Status, method) {
		return true
	}
	n, present, valid := responseContentLength(resp.Headers)
	if !present {
		return true
	}
	return valid && n == bodyLen
}

// decodeError maps a header field-section decode failure to the right scope: a
// QPACK decompression failure is a connection error (RFC 9204 §2.2, §6 —
// QPACK_DECOMPRESSION_FAILED), so it closes the connection, while a message-rule
// violation (ErrH3Message) stays a stream error the caller aborts the request
// with.
func (c *Client) decodeError(err error) (*Response, []byte, error) {
	if errors.Is(err, qpack.ErrDecompressionFailed) {
		return nil, nil, c.connError(H3QpackDecompressionFailed)
	}
	return nil, nil, err
}
