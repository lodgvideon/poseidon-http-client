package conn

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// StreamEventType discriminates the StreamEvent variants.
type StreamEventType uint8

// StreamEventType values. Use Type to dispatch which fields of
// StreamEvent are populated.
const (
	EventHeaders     StreamEventType = iota + 1 // Headers populated
	EventData                                   // Data populated
	EventTrailers                               // Headers populated, trailers
	EventReset                                  // RSTCode populated
	EventPushPromise                            // Headers populated (promised), PushStreamID set
	// EventInterimHeaders carries an informational (1xx) response header
	// block; Headers is populated and EndStream is always false. A 1xx is
	// not the final response: the stream continues and a later EventHeaders
	// delivers the final status (RFC 7540 §8.1). Callers that do not care
	// about informational responses may ignore this event — dropping it
	// yields the correct final status either way.
	EventInterimHeaders
)

// String returns the lowercase name of t.
func (t StreamEventType) String() string {
	switch t {
	case EventHeaders:
		return "headers"
	case EventData:
		return "data"
	case EventTrailers:
		return "trailers"
	case EventReset:
		return "reset"
	case EventPushPromise:
		return "push_promise"
	case EventInterimHeaders:
		return "interim_headers"
	default:
		return "unknown"
	}
}

// StreamEvent is one observation about an in-flight stream. The Type
// field tells the caller which other fields are populated.
//
// When Slab is non-nil, all Headers[i].Name and .Value byte slices are
// sub-slices of *Slab. Ownership transfers to the client layer, which
// returns the pointer to conn.GetHeaderSlabPool() in Response.Reset / sr.Close.
type StreamEvent struct {
	Type      StreamEventType
	Headers   []hpack.HeaderField // EventHeaders / EventTrailers / EventInterimHeaders
	Data      []byte              // EventData
	EndStream bool                // any event closing the response side
	RSTCode   frame.ErrCode       // EventReset

	// PushStreamID is the promised (even) stream ID for EventPushPromise.
	PushStreamID uint32 // EventPushPromise

	// Slab is the pooled backing buffer pointer for all Headers[i].Name
	// and .Value slices. nil for non-headers events and when the pool is
	// cold (first request). The client layer must return this pointer to
	// conn.GetHeaderSlabPool(), not the slice value, to avoid heap escape.
	Slab *[]byte

	// DataSlab is the pooled buffer backing Data (EventData). nil for
	// non-data events and when the pool is cold. The client returns it to
	// conn.GetDataBufPool() once Data is consumed — incrementally on the
	// streaming paths (Data is valid until the next Recv), immediately on
	// the buffered path. Return the pointer, not the slice, to avoid escape.
	// Buffers of events still queued at stream/connection teardown are
	// dropped to GC rather than pooled (see recycleStream).
	DataSlab *[]byte
}

// streamWriter is the narrow surface a *Stream needs from its owner Conn.
// Tests fake this out; production code wires it to *Conn.
type streamWriter interface {
	writeHeadersWithPriority(ctx context.Context, s *Stream, fields []hpack.HeaderField, endStream bool, prio *frame.Priority) error
	writeData(ctx context.Context, s *Stream, p []byte, endStream bool) error
	writeRSTStream(s *Stream, code frame.ErrCode) error
}

// Stream is one in-flight HTTP/2 stream.
type Stream struct {
	id     uint32
	w      streamWriter
	events chan StreamEvent

	mu           sync.Mutex
	localEnded   bool // we sent END_STREAM
	remoteEnded  bool // peer sent END_STREAM (or RST)
	closed       bool // RST or graceful close
	inflightDone bool // inflight slot already returned to the pool
	// pushed marks a server-initiated stream created from a PUSH_PROMISE.
	// It selects which of the two directional concurrency counters this
	// stream's slot came from, so the release path returns it to the same
	// one (RFC 7540 §5.1.2). Set once under smu at reservation; never true
	// for a stream from NewStream. Read under Stream.mu alongside
	// inflightDone in markStreamDone / releaseInflight.
	pushed bool

	// appClosed records that Close() ran. Half of the recycle rendezvous (other
	// half: connDone): a cleanly-completed stream returns to streamPool only once
	// BOTH markStreamDone (which evicts it) and Close have finished with the
	// struct — whichever is second recycles. Guarded by mu; reset by
	// recycleStream. Distinct from `released`, the atomic Close-idempotency latch
	// that must survive recycle.
	appClosed bool
	// connDone records that markStreamDone retired a cleanly-completed stream:
	// both ends ended, slot released, and the registry entry deleted — so the
	// reader can never look the struct up again. Close observing connDone is the
	// rendezvous's second party. Set only AFTER delete(c.streams,id). Guarded by
	// mu; reset by recycleStream.
	connDone bool

	// reqAuthority is the :authority pseudo-header of the request this stream
	// carries, captured when the request HEADERS are written. The push accept
	// path compares a PUSH_PROMISE's :authority against it — a server is
	// authoritative for what it already answered over the cert-validated
	// connection (RFC 9113 §8.4 / §10.1). Guarded by mu.
	reqAuthority string

	// headersReceived is set once a *final* (non-informational) response
	// HEADERS block for this stream is delivered. The reader goroutine
	// consults it to classify subsequent HEADERS frames as trailers
	// (RFC 7540 §8.1 keys trailers on a final status, so a 1xx block must
	// not latch this). Single-goroutine access — only the reader goroutine
	// reads and writes this field — so no synchronization is required.
	headersReceived bool

	// interimCount is the number of informational (1xx) header blocks
	// received on this stream, bounded by maxInterimResponses so a peer
	// cannot stream 1xx blocks forever. Same single-goroutine access
	// discipline as headersReceived.
	interimCount int

	// bodyLen, respStatus and the cl* fields validate the final response's
	// declared Content-Length against the DATA actually received, at END_STREAM
	// (RFC 7540 §8.1.2.6: "A request or response is also malformed if the value
	// of a content-length header field does not equal the sum of the DATA frame
	// payload lengths that form the body"). clValid is false when the value is
	// not 1*DIGIT or repeated values disagree; clPresent records that a
	// Content-Length was on the final response at all. All reader-goroutine-only,
	// like headersReceived.
	bodyLen    int64
	respStatus int
	clDeclared int64
	clPresent  bool
	clValid    bool

	// reqIsHead records that this stream's request method is HEAD, whose response
	// is defined to carry no content whatever its Content-Length says (RFC 9110
	// §9.3.2). Written under mu by the caller's SendHeaders goroutine before any
	// response frame can arrive; read under mu at the Content-Length check.
	reqIsHead bool

	// recvWindow is the number of payload bytes the peer can still
	// send to *this* stream before we must refill it via WINDOW_UPDATE
	// (RFC 7540 §6.9.1). Initialized from our advertised
	// SETTINGS_INITIAL_WINDOW_SIZE; debited by every received DATA
	// frame's full payload length, including padding.
	recvWindow int32
	// recvRefundPending is the number of bytes we have already debited
	// but not yet returned to the peer via a WINDOW_UPDATE. Reset when
	// the connection emits a WINDOW_UPDATE for this stream.
	recvRefundPending uint32

	// sendWindow is the number of payload bytes we may still send on
	// *this* stream without WINDOW_UPDATE credit from the peer (RFC
	// 7540 §6.9.1, peer's per-stream view). Initialized from the
	// peer's SETTINGS_INITIAL_WINDOW_SIZE at first HEADERS write;
	// debited by writeData and replenished by OnWindowUpdate. Guarded
	// by Stream.mu.
	sendWindow int32

	// resetSignal is closed when the stream is forcibly reset (event
	// channel overflow, GOAWAY drain, or connection shutdown). Recv()
	// selects on it so a blocked consumer unblocks immediately even
	// when the events channel is full — no silent hang. Replaced with
	// a fresh channel in recycleStream.
	resetSignal chan struct{}

	// resetCode stores the ErrCode delivered with the forced reset.
	// 0 means no reset has been signalled. Written via CAS in
	// signalReset to guarantee exactly one close(resetSignal).
	resetCode atomic.Uint32

	// released guards Close() idempotency independently of the operational
	// `closed` flag. recycleStream resets `closed` (for pool reuse) but must
	// NOT reset `released`, so a repeat Close() while the struct is still
	// pooled is a no-op instead of dereferencing the nil-ed w. allocStream
	// re-arms it for the next lifetime (so a stale reference to a re-allocated
	// struct is not protected — callers must not retain across Close).
	released atomic.Bool

	// recvActive counts goroutines currently inside Recv, and recycleWanted
	// records a recycle that had to be deferred because one was. Both guarded
	// by mu.
	//
	// The appClosed/connDone handshake below settles which of Close and
	// markStreamDone returns the struct to the pool, on the premise that once
	// both have finished nobody holds a reference. That misses a third party:
	// the application's own reader. Recv necessarily reads s.events and
	// s.resetSignal OUTSIDE mu — it blocks on them — so a recycle running
	// while a reader sits in that select rewrites the very channels it is
	// selecting on, and hands the struct to the pool for another request to
	// claim while the reader still points at it. Callers are told not to keep
	// a *Stream past Close, but Close racing an in-flight Recv is not "past":
	// it is the ordinary shape of one goroutine cancelling another's read.
	//
	// So the reader is a participant too: Recv registers here, and whichever
	// of the three finishes last does the recycling. A reader that blocks
	// forever merely postpones pooling, which costs a pool hit, not
	// correctness.
	recvActive    int
	recycleWanted bool
}

func newStream(id uint32, eventBuf int, w streamWriter, recvWindow int32) *Stream {
	return &Stream{
		id:          id,
		w:           w,
		events:      make(chan StreamEvent, eventBuf),
		recvWindow:  recvWindow,
		resetSignal: make(chan struct{}),
	}
}

// endWithReset delivers a terminal reset event and ends the stream in ONE mu
// section, and reports whether it did. It is how the connection reader tears a
// stream down — from a received RST_STREAM, or from a GOAWAY that stranded it.
//
// wantID is the stream id the caller looked the struct up under. A *Stream is
// pooled, so by the time this runs the struct may already have been recycled
// and handed to another request; recycleStream zeroes id, and ids are assigned
// monotonically under wmu so one never recurs on a connection. A mismatch
// therefore means "this is no longer the stream you meant" and the caller must
// leave it alone — including not calling markStreamDone for it.
//
// Delivering INSIDE the section is the point. Both callers used to deliver
// first and take mu afterwards, on the reasoning that a reset ahead of the
// stream becoming recycle-eligible could not race the recycle. That reasoning
// only covers eligibility this loop creates itself. The application creates it
// independently: finishing its upload sets localEnded and drives markStreamDone
// to set connDone, and its Close then recycles — rewriting s.events and
// s.resetSignal under mu while this delivery read them without it. Both were
// reproducible under -race in about a tenth of a second.
func (s *Stream) endWithReset(wantID uint32, code frame.ErrCode) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id != wantID {
		return false
	}
	// Non-blocking, so holding mu across it cannot stall the reader: the send
	// has a default, and signalReset is a CAS plus a close.
	select {
	case s.events <- StreamEvent{Type: EventReset, RSTCode: code, EndStream: true}:
	default:
		s.signalReset(code)
	}
	// remoteEnded, localEnded and closed are set together so a concurrent
	// Close() can never observe bothEnded && !closed and recycle the struct in
	// between. A stream whose upload already completed has localEnded true
	// beforehand, so setting remoteEnded in a separate section would open that
	// window regardless of this one.
	s.remoteEnded = true
	s.localEnded = true
	s.closed = true
	return true
}

// signalReset marks the stream as forcibly reset and closes resetSignal
// so any Recv() blocked on a full events channel unblocks immediately.
// The CAS ensures only the first caller closes resetSignal (idempotent).
// Callers hold s.mu.
func (s *Stream) signalReset(code frame.ErrCode) {
	if s.resetCode.CompareAndSwap(0, uint32(code)) {
		close(s.resetSignal)
	}
}

// recycleStream drains any buffered events, zeroes all fields, and returns s
// to pool. Only call when the stream is fully done (both sides ended or RST
// sent/received) and the connection and application sides have both let go.
//
// A reader inside Recv is the one holder this cannot be told about in advance,
// so it is checked for here: with one present the recycle is recorded and
// handed to that reader's exit path instead (see Stream.recvActive). The reset
// itself runs under mu — the same mutex Recv takes to snapshot the channels it
// selects on — and pool.Put happens only after unlocking, so the next owner of
// the struct never finds its mutex held by us.
// The destination pool is taken from s.w rather than passed in. It used to be
// a parameter, but the deferred path cannot honour one — recvLeave runs long
// after the caller that wanted the recycle is gone — so a parameter would be
// obeyed on one path and silently ignored on the other. Both call sites pass
// the pool of the very Conn that is already s.w, so nothing is lost by reading
// it from there, and the mismatch cannot be reintroduced.
func recycleStream(s *Stream) {
	s.mu.Lock()
	if s.recvActive > 0 {
		s.recycleWanted = true
		s.mu.Unlock()
		return
	}
	s.putLocked()
}

// recvLeave is Recv's exit path. It drops the reader's registration and, when
// it was the last one out and a recycle was deferred while it read, performs
// that recycle.
func (s *Stream) recvLeave() {
	s.mu.Lock()
	s.recvActive--
	if s.recvActive > 0 || !s.recycleWanted {
		s.mu.Unlock()
		return
	}
	s.recycleWanted = false
	s.putLocked()
}

// putLocked resets s for reuse and returns it to its connection's pool. The
// caller holds s.mu; putLocked releases it before the Put, so the struct's next
// owner never finds its mutex held by the previous one.
func (s *Stream) putLocked() {
	// Captured before resetForPoolLocked nils it out.
	w := s.w
	s.resetForPoolLocked()
	s.mu.Unlock()
	if c, ok := w.(*Conn); ok {
		c.streamPool.Put(s)
	}
}

// resetForPoolLocked drains buffered events and returns every field to its
// zero-lifetime value. Caller holds s.mu and is responsible for the pool.Put.
func (s *Stream) resetForPoolLocked() {
	// Drop any events still buffered on the old channel. Their pooled DATA
	// buffers (DataSlab) are abandoned to GC here, matching shutdownStreams
	// and the header-slab teardown path: sync.Pool tolerates buffers that are
	// never Put back. These events were never delivered to a consumer, so
	// dropping them keeps exactly one return site per buffer — the consumer on
	// the next Recv/Close for delivered frames, or OnData itself for frames
	// dropped at push() under backpressure — and rules out a double-Put.
	for len(s.events) > 0 {
		<-s.events
	}
	// Recreate the events channel with the same capacity. Any stale
	// reference held by a goroutine from the previous stream lifetime
	// (e.g. a deferred push/RST send) now writes to the orphaned old
	// channel, preventing cross-stream event contamination.
	s.events = make(chan StreamEvent, cap(s.events))
	s.id = 0
	s.w = nil
	s.localEnded = false
	s.remoteEnded = false
	s.closed = false
	s.inflightDone = false
	s.pushed = false
	s.appClosed = false
	s.connDone = false
	s.reqAuthority = ""
	s.headersReceived = false
	s.interimCount = 0
	// The Content-Length check's per-response state, reset so a pooled Stream
	// starts each request clean. Missing these leaked a previous response's
	// bodyLen into the next (caught by TestDoStream_WaitTrailers_Reuse: declared
	// 4, bodyLen 8 on the second iteration of a reused connection).
	s.bodyLen = 0
	s.respStatus = 0
	s.clDeclared = 0
	s.clPresent = false
	s.clValid = false
	s.reqIsHead = false
	s.recvRefundPending = 0
	s.sendWindow = 0
	s.resetSignal = make(chan struct{})
	s.resetCode.Store(0)
	// recvActive is zero by construction — this only runs with no reader
	// registered — but recycleWanted must not survive into the next lifetime,
	// or the first Recv of the reused stream would recycle it on exit.
	s.recycleWanted = false
}

// ID returns the HTTP/2 stream identifier.
func (s *Stream) ID() uint32 { return s.id }

// markRemoteEnd is called by the connection-level frame.Handler when
// END_STREAM is observed for this stream.
func (s *Stream) markRemoteEnd() {
	s.mu.Lock()
	s.remoteEnded = true
	s.mu.Unlock()
}

// deliverEnd delivers e and, when end is true, marks the remote side ended —
// both under one s.mu hold.
//
// The ordering is load-bearing against a concurrent Stream.Close(), the same
// way OnRSTStream's is. Close computes bothEnded (localEnded && remoteEnded)
// under s.mu and recycles the stream when it holds. Setting remoteEnded in a
// section separate from the delivery leaves a window where Close observes
// bothEnded — localEnded is already true for any request whose upload finished
// — and recycles the struct while this handler is still pushing into it. Under
// one hold there is no such window: Close either runs first and sees
// remoteEnded false (so it sends its own CANCEL and recycles nothing), or runs
// second and finds the event already delivered.
//
// It also keeps remoteEnded visible before the consumer can observe the event,
// so a Close() immediately after reading END_STREAM still recycles instead of
// emitting a pointless RST_STREAM(CANCEL) on a stream that ended cleanly.
func (s *Stream) deliverEnd(e StreamEvent, end bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if end {
		s.remoteEnded = true
	}
	return s.pushLocked(e)
}

// hasRemoteEnded reports whether the peer has ended its side of the stream
// (END_STREAM observed, or the stream reset). The reader uses it to detect a
// frame arriving after END_STREAM on a half-closed(remote) stream (RFC 9113
// §5.1), which is a stream error STREAM_CLOSED.
func (s *Stream) hasRemoteEnded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteEnded
}

// requestAuthority returns the :authority the request on this stream carried.
func (s *Stream) requestAuthority() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reqAuthority
}

// push delivers an event from the reader goroutine. Non-blocking under
// the channel's capacity; documented as part of the public contract.
// On overflow: marks stream closed, dispatches the RST send to a
// background goroutine (so the reader is never blocked on wmu), and
// signals via resetSignal so a blocked Recv unblocks immediately.
func (s *Stream) push(e StreamEvent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pushLocked(e)
}

// pushLocked is push with s.mu already held by the caller. It exists so the
// reader can deliver a terminal event and mark the stream ended in ONE critical
// section, which is what stops a concurrent Close() from recycling the struct
// in between — recycleStream rewrites s.events and zeroes every field, so a
// push landing after it writes to an orphaned channel on a struct another
// request may already own.
//
// Every step here is non-blocking: the two channel sends use select/default,
// and the RST write is handed to a background goroutine. Holding s.mu across
// them therefore cannot stall the reader.
func (s *Stream) pushLocked(e StreamEvent) bool {
	select {
	case s.events <- e:
		return true
	default:
	}
	already := s.closed
	s.closed = true
	if already {
		return false
	}
	go func() {
		// Use best-effort write with a 5-second deadline so the goroutine
		// cannot hang indefinitely on a stuck transport (F-P0-04).
		if c, ok := s.w.(*Conn); ok {
			c.writeRSTStreamBestEffort(s, frame.ErrCodeRefusedStream)
		} else {
			_ = s.w.writeRSTStream(s, frame.ErrCodeRefusedStream)
		}
	}()
	// Try to deliver EventReset via channel; if full, signal via resetSignal.
	select {
	case s.events <- StreamEvent{
		Type:      EventReset,
		RSTCode:   frame.ErrCodeRefusedStream,
		EndStream: true,
	}:
	default:
		s.signalReset(frame.ErrCodeRefusedStream)
	}
	return false
}

// SendHeaders sends a HEADERS frame with the given fields. Always emits
// END_HEADERS=true (B.1 does not split into CONTINUATION). When endStream
// is true the request side is half-closed.
// SendHeaders sends a HEADERS frame with the given fields. When called
// for the first time on a Stream, the connection assigns the stream ID
// under the writer mutex, ensuring the on-wire ID order matches RFC
// 7540 §5.1.1's monotonic-id rule. Always emits END_HEADERS=true (B.1
// does not split into CONTINUATION). When endStream is true the request
// side is half-closed.
// SendHeaders sends a HEADERS frame with the given fields. When called
// for the first time on a Stream, the connection assigns the stream ID
// under the writer mutex, ensuring the on-wire ID order matches RFC
// 7540 §5.1.1's monotonic-id rule. Always emits END_HEADERS=true (B.1
// does not split into CONTINUATION). When endStream is true the request
// side is half-closed.
func (s *Stream) SendHeaders(ctx context.Context, fields []hpack.HeaderField, endStream bool) error {
	return s.SendHeadersWithPriority(ctx, fields, endStream, nil)
}

// SendHeadersWithPriority sends a HEADERS frame with optional
// PRIORITY fields embedded (RFC 7540 §6.3). When prio is non-nil
// the HEADERS frame carries the PRIORITY flag plus a 5-byte
// priority payload. StreamID 0 in prio means root stream (no
// parent). When prio is nil the frame is emitted identically to
// SendHeaders.
func (s *Stream) SendHeadersWithPriority(ctx context.Context, fields []hpack.HeaderField, endStream bool, prio *frame.Priority) error {
	s.mu.Lock()
	if s.pushed {
		s.mu.Unlock()
		// RFC 9113 §5.1: a server-pushed (reserved(remote)) stream is receive-only
		// for the client; it may only carry RST_STREAM/WINDOW_UPDATE/PRIORITY from us.
		return ErrPushedStreamReadOnly
	}
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	// Record whether this request is a HEAD, for the response's Content-Length
	// check: a HEAD response is defined to carry no content whatever it declares
	// (RFC 9110 §9.3.2). Written here under mu, before any response can arrive, so
	// the reader goroutine reads a stable value at END_STREAM.
	for i := range fields {
		if string(fields[i].Name) == ":method" {
			s.reqIsHead = string(fields[i].Value) == "HEAD"
			break
		}
	}
	s.mu.Unlock()
	if err := s.w.writeHeadersWithPriority(ctx, s, fields, endStream, prio); err != nil {
		return err
	}
	if endStream {
		s.mu.Lock()
		s.localEnded = true
		s.mu.Unlock()
		if c, ok := s.w.(*Conn); ok {
			c.markStreamDone(s.id)
		}
	}
	return nil
}

// SendData sends a single DATA frame. The caller is responsible for
// chunking p to fit the peer's MaxFrameSize. When endStream is true the
// request side is half-closed.
// SendData sends a single DATA frame. The caller must call SendHeaders
// first; the request side is half-closed when endStream is true.
// SendData sends a DATA frame, automatically chunking the payload to
// the peer's MAX_FRAME_SIZE and respecting both per-stream and
// connection-level outbound flow control (RFC 7540 §6.9). Blocks until
// enough send-window credit is available, the context is cancelled, or
// the connection closes. The caller must call SendHeaders first.
func (s *Stream) SendData(ctx context.Context, p []byte, endStream bool) error {
	s.mu.Lock()
	if s.pushed {
		s.mu.Unlock()
		// RFC 9113 §5.1: a server-pushed stream is receive-only for the client.
		return ErrPushedStreamReadOnly
	}
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	s.mu.Unlock()
	if err := s.w.writeData(ctx, s, p, endStream); err != nil {
		return err
	}
	if endStream {
		s.mu.Lock()
		s.localEnded = true
		s.mu.Unlock()
		if c, ok := s.w.(*Conn); ok {
			c.markStreamDone(s.id)
		}
	}
	return nil
}

// Recv blocks until the next event for this stream is ready, the stream
// terminates, or ctx is cancelled.
func (s *Stream) Recv(ctx context.Context) (StreamEvent, error) {
	// Register as a reader: that is what keeps recycleStream off the struct
	// until this returns, and it is the load-bearing half — the select below
	// blocks, so it cannot hold the mutex, and a recycle landing under it
	// would rewrite the very channels being selected on and pool the struct
	// for another request to claim.
	//
	// Snapshotting the two channels in the same critical section is belt and
	// braces. With the registration in place nothing rewrites them while we
	// are here, so a mutation that reads the fields after unlocking does not
	// fail any test. It is kept because it makes this function's safety local:
	// it holds whatever a future change does to where the reset runs, and this
	// pair of fields has now produced two shipped races.
	s.mu.Lock()
	s.recvActive++
	events, resetSignal := s.events, s.resetSignal
	s.mu.Unlock()
	defer s.recvLeave()

	select {
	case e, ok := <-events:
		if !ok {
			return StreamEvent{}, ErrStreamClosed
		}
		return e, nil
	case <-resetSignal:
		code := frame.ErrCode(s.resetCode.Load())
		return StreamEvent{Type: EventReset, RSTCode: code, EndStream: true}, nil
	case <-ctx.Done():
		return StreamEvent{}, ctx.Err()
	}
}

// Close cancels the stream. If neither side has reached END_STREAM, sends
// RST_STREAM(CANCEL). Idempotent for the common case: a repeat Close() is a
// no-op while the recycled struct still sits in the pool. Callers must not
// retain a *Stream past Close — allocStream re-arms the guard for the next
// lifetime, so a Close on a stale reference to a re-allocated struct is not
// protected (no in-tree caller does this).
//
// A cleanly-completed stream (bothEnded) is recycled via a rendezvous with
// the reader's markStreamDone rather than directly here: markStreamDone is
// what evicts the stream from the connection's registry, and only after
// that eviction can no other goroutine look the struct up and touch it.
// Recycling here unconditionally — as soon as Close merely observes
// bothEnded — raced markStreamDone's own read/write of the same fields,
// because recycleStream mutates every field of *Stream with no lock held.
// appClosed/connDone are the two halves of that handshake: whichever of
// Close/markStreamDone finishes second with the struct is the one that
// recycles it.
func (s *Stream) Close() error {
	// released is the idempotency guard. It survives recycleStream (which
	// resets closed/w/... for pool reuse), so a repeat Close while the struct
	// is still pooled returns here instead of dereferencing the nil-ed w.
	if !s.released.CompareAndSwap(false, true) {
		return nil
	}
	s.mu.Lock()
	s.appClosed = true
	closed := s.closed // e.g. push() set this on event-channel overflow
	bothEnded := s.localEnded && s.remoteEnded
	connDone := s.connDone
	w := s.w
	if !closed && !bothEnded {
		// Abandoning an incomplete stream: our RST is the sole teardown: the
		// reader will never see END_STREAM to reach markStreamDone's own path.
		s.closed = true
	}
	s.mu.Unlock()
	if closed {
		// Already closed (RST already sent by push overflow, or a peer
		// reset/GOAWAY already tore this stream down); never pooled from here.
		return nil
	}
	if connDone {
		// markStreamDone already evicted the stream from the registry and
		// found appClosed false at the time — so it left recycling to us. We
		// are the second (and last) party to touch the struct, unless a reader
		// is still inside Recv, in which case recycleStream hands it on.
		recycleStream(s)
		return nil
	}
	if bothEnded {
		// Clean completion is in flight or already handled: either
		// markStreamDone hasn't run its eviction yet (it will recycle once it
		// does, since appClosed is now true) or it already recycled directly.
		// Either way we must not touch the struct.
		return nil
	}
	return w.writeRSTStream(s, frame.ErrCodeCancel)
}
