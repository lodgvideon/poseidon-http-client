package conn

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
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
	Headers   []header.Field // EventHeaders / EventTrailers / EventInterimHeaders
	Data      []byte         // EventData
	EndStream bool           // any event closing the response side
	RSTCode   frame.ErrCode  // EventReset

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
	writeHeadersWithPriority(ctx context.Context, s *Stream, fields []header.Field, endStream bool, prio *frame.Priority) error
	writeData(ctx context.Context, s *Stream, wantGen uint64, p []byte, endStream bool) error
	writeRSTStream(s *Stream, code frame.ErrCode) error

	// markStreamDone retires the stream's concurrency slot once its local side
	// has ended. Reached by downcast until now, which meant a fake writer
	// silently skipped it and every test built on one exercised a lifecycle
	// production never runs: no slot release, no send-waiter wakeup, no recycle
	// rendezvous. handler.go records the same pattern being purged from the
	// dispatch path once already; this half regrew it.
	markStreamDone(id uint32)

	// writeRSTStreamBestEffort sends RST_STREAM under a short write deadline, for
	// the fire-and-forget goroutine that cannot be allowed to block on a stuck
	// transport. Same reason it belongs here: the downcast made it a no-op for a
	// fake, so the overflow path under test was not the overflow path that ships.
	writeRSTStreamBestEffort(s *Stream, code frame.ErrCode)
}

// Stream is one in-flight HTTP/2 stream.
type Stream struct {
	// gen names the request that currently owns this pooled struct. It is
	// incremented — never reset — by resetForPoolLocked, the single point where
	// ownership ends, so a StreamRef minted for lifetime N is refused for every
	// later lifetime AND for the whole window the struct sits unclaimed in the
	// pool. Seeded to 1 by newStream so the zero StreamRef can never match.
	//
	// Bumping at RELEASE rather than at re-allocation is what makes the pooled
	// window safe: resetForPoolLocked clears closed and localEnded and nils w,
	// so a stale send against a recycled-but-unclaimed struct would otherwise
	// pass every gate and nil-deref the writer.
	//
	// Read under s.mu everywhere it gates an operation. It is atomic only so
	// StreamRef.valid can be used on paths that have not taken the lock yet;
	// a check that decides whether to act MUST hold s.mu, or it races the
	// recycle it is trying to exclude.
	gen atomic.Uint64

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

	// resetCode stores the ErrCode delivered with the forced reset. Written
	// once, by whichever signalReset call wins resetSignalled.
	resetCode atomic.Uint32

	// resetSignalled guarantees exactly one close(resetSignal). It cannot be
	// inferred from resetCode: NO_ERROR is 0, so a CAS(0 -> code) guard admits
	// every caller once code is 0 and closes the channel twice.
	resetSignalled atomic.Bool

	// eventsClosed records that shutdownStreams closed s.events. It is the only
	// reason the recycle path has to replace the channel rather than reuse it,
	// and replacing a 272-slot channel — grpc's default — costs 24 KiB on every
	// RPC, in the path whose whole purpose is to avoid allocating.
	eventsClosed atomic.Bool

	// endSignal is closed when the terminal DATA frame could not be enqueued.
	// It is the clean-completion sibling of resetSignal: that frame carries no
	// payload, only the fact that the response is over, so losing the stream
	// over it would destroy a response the peer delivered in full.
	endSignal    chan struct{}
	endSignalled atomic.Bool

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
	s := &Stream{
		id:          id,
		w:           w,
		events:      make(chan StreamEvent, eventBuf),
		recvWindow:  recvWindow,
		resetSignal: make(chan struct{}),
		endSignal:   make(chan struct{}),
	}
	s.gen.Store(1) // never 0: the zero StreamRef must match nothing
	return s
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
	// The guard is its own flag, not a CAS on resetCode. CAS(0 -> code) is not
	// idempotent when code is itself 0: it succeeds, leaves resetCode at 0, and
	// so succeeds again for the next caller — closing an already-closed channel
	// and panicking the reader. NO_ERROR is 0, and NO_ERROR is exactly the code
	// RFC 9113 §8.1 has a server send after a complete response, so the one
	// value that broke the contract was the common one.
	if !s.resetSignalled.CompareAndSwap(false, true) {
		return
	}
	s.resetCode.Store(uint32(code))
	close(s.resetSignal)
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
	// Replace the channel only when shutdownStreams closed it. A closed
	// channel survives the drain above, so reusing one would hand the next
	// request a stream whose first Recv reports ErrStreamClosed and whose
	// first delivery panics the connection reader with a send on a closed
	// channel.
	//
	// It used to be replaced unconditionally, to orphan "a stale reference
	// held by a goroutine from the previous stream lifetime". That reasoning
	// does not hold: no writer in this package captures the channel. push,
	// deliverEnd, endWithReset and shutdownStreams all read s.events at send
	// time, so a late writer lands in the NEW channel whether it was replaced
	// or not — the orphaning prevented nothing, while costing 24 KiB per RPC
	// at grpc's 272-slot default and 1.1 KiB at conn's own 8. What stops a
	// late writer is the id gate in endWithReset and the closed flag that
	// keeps an overflowed stream out of the pool entirely.
	if s.eventsClosed.Load() {
		s.events = make(chan StreamEvent, cap(s.events))
		s.eventsClosed.Store(false)
	}
	// Ownership ends here, so the handle for it dies here. INCREMENT, never
	// reset: this function is a wall of zeroing assignments and setting gen to 0
	// among them would collapse every lifetime onto one value and silently
	// reopen issue #370 with nothing failing. TestStaleRef_* is what catches it.
	s.gen.Add(1)
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
	// Same rule for the two signal channels, and the flag that guards each
	// close is exactly "this channel is closed", so it doubles as the test.
	if s.resetSignalled.Load() {
		s.resetSignal = make(chan struct{})
		s.resetSignalled.Store(false)
	}
	s.resetCode.Store(0)
	if s.endSignalled.Load() {
		s.endSignal = make(chan struct{})
		s.endSignalled.Store(false)
	}
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
	// A terminal DATA frame with an empty payload carries nothing except the
	// fact that the response is over, and pushLocked's overflow path costs the
	// whole stream: it resets and delivers EventReset for a response the peer
	// wrote in full. That is the common shape rather than a corner — a flushing
	// server whose chunks exactly fill the channel loses everything to a
	// zero-byte marker, with every byte of the body already delivered. Say it
	// out of band instead. Only EventData qualifies: a trailer block ends the
	// stream too, and carries fields the caller must still receive.
	if end && e.Type == EventData && len(e.Data) == 0 {
		select {
		case s.events <- e:
			return true
		default:
		}
		s.signalEnd()
		// Reported as not-enqueued so the caller returns the pooled buffer.
		// Nothing was dropped: the event held nothing.
		return false
	}
	return s.pushLocked(e)
}

// signalEnd marks the response complete for a consumer that could not be handed
// the terminal event. Exactly one close, guarded by its own flag for the reason
// spelled out on signalReset.
func (s *Stream) signalEnd() {
	if s.endSignalled.CompareAndSwap(false, true) {
		close(s.endSignal)
	}
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

// pushIfID is push gated on the stream still being the one the caller looked up.
//
// A *Stream is pooled, so a caller that resolved it by id and then did any work
// before delivering can find the struct recycled and re-issued to another
// request in between — and an ungated push would hand that request an event it
// was never sent. endWithReset carries the same gate for the two teardown paths;
// this is the delivery-path counterpart, for a caller whose event is not
// terminal.
//
// Returns false when the id no longer matches, which is not an enqueue failure:
// the stream the event belonged to is gone, so there is nothing to deliver to
// and nothing to recycle. Callers must distinguish it from pushLocked's false,
// which means the channel was full and the stream has been reset.
func (s *Stream) pushIfID(wantID uint32, e StreamEvent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id != wantID {
		return false
	}
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
//
// The code is CANCEL, not REFUSED_STREAM. This teardown is the client giving
// up on a response it cannot buffer, which is what RFC 9113 §7 defines CANCEL
// for — "The endpoint uses this error code to indicate that the stream is no
// longer needed". REFUSED_STREAM asserts something else entirely, and
// something false: §8.7 gives it the meaning "the stream is being closed prior
// to any processing having occurred. Any request that was sent on the reset
// stream can be safely retried." Here the server processed the request and
// answered it — we simply could not hold the answer.
//
// That mattered in two places, both of which believed the code. client's retry
// classifier read the RFC guarantee literally and replayed requests the server
// had already executed (measured: one DELETE run three times), and grpc's
// status map turned it into UNAVAILABLE, gRPC's canonical retryable code, for
// the same reason. Neither is patched around: the classifier and the map are
// right, and were being told a lie. The two REFUSED_STREAM resets this package
// still sends are the ones where the claim is true — a stream stranded by
// GOAWAY, which the peer really never processed, and a refused PUSH_PROMISE,
// where §8.4.2 sanctions "either the CANCEL or REFUSED_STREAM code".
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
		s.w.writeRSTStreamBestEffort(s, frame.ErrCodeCancel)
	}()
	// Try to deliver EventReset via channel; if full, signal via resetSignal.
	select {
	case s.events <- StreamEvent{
		Type:      EventReset,
		RSTCode:   frame.ErrCodeCancel,
		EndStream: true,
	}:
	default:
		s.signalReset(frame.ErrCodeCancel)
	}
	return false
}

// sendHeadersAndData is SendHeaders followed by SendData, fused into one
// transport write when the connection can do it (#451).
//
// The guards are SendHeaders': the stream must own this lifetime, must not be a
// pushed (receive-only) stream, and must not already be closed or half-closed.
// The HEAD bookkeeping is the same too — a HEAD request's response is defined to
// carry no content whatever it declares (RFC 9110 §9.3.2), and the flag has to be
// set before any response can arrive.
//
// Falls back to the two separate calls when the writer is not a real Conn (tests
// fake streamWriter), which keeps this a pure optimisation rather than a second
// code path with its own semantics.
func (s *Stream) sendHeadersAndData(ctx context.Context, wantGen uint64, fields []header.Field, p []byte, endStream bool) error {
	c, ok := s.w.(*Conn)
	if !ok {
		if err := s.sendHeadersWithPriority(ctx, wantGen, fields, false, nil); err != nil {
			return err
		}
		return s.sendData(ctx, wantGen, p, endStream)
	}

	s.mu.Lock()
	if s.gen.Load() != wantGen {
		s.mu.Unlock()
		return ErrStaleStream
	}
	if s.pushed {
		s.mu.Unlock()
		return ErrPushedStreamReadOnly
	}
	if s.closed || s.localEnded {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	for i := range fields {
		if string(fields[i].Name) == ":method" {
			s.reqIsHead = string(fields[i].Value) == "HEAD"
			break
		}
	}
	s.mu.Unlock()

	if err := c.writeHeadersAndData(ctx, s, wantGen, fields, p, endStream); err != nil {
		return err
	}
	if endStream {
		s.mu.Lock()
		s.localEnded = true
		id := s.id
		s.mu.Unlock()
		c.markStreamDone(id)
	}
	return nil
}

// SendHeadersWithPriority sends a HEADERS frame with optional
// PRIORITY fields embedded (RFC 7540 §6.3). When prio is non-nil
// the HEADERS frame carries the PRIORITY flag plus a 5-byte
// priority payload. StreamID 0 in prio means root stream (no
// parent). When prio is nil the frame is emitted identically to
// SendHeaders.
func (s *Stream) sendHeadersWithPriority(ctx context.Context, wantGen uint64, fields []header.Field, endStream bool, prio *frame.Priority) error {
	s.mu.Lock()
	if s.gen.Load() != wantGen {
		s.mu.Unlock()
		return ErrStaleStream
	}
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
		s.endLocalAndRetire()
	}
	return nil
}

// sendData is SendData with the caller's lifetime presented.
//
// The check here is at the door only. writeData releases s.mu and can park in
// acquireSendCredits for an unbounded time, so the door check proves "the
// handle was live on entry" and not "the frame lands on the stream you meant";
// the wake path carries wantGen for that.
func (s *Stream) sendData(ctx context.Context, wantGen uint64, p []byte, endStream bool) error {
	s.mu.Lock()
	if s.gen.Load() != wantGen {
		s.mu.Unlock()
		return ErrStaleStream
	}
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
	if err := s.w.writeData(ctx, s, wantGen, p, endStream); err != nil {
		return err
	}
	if endStream {
		s.endLocalAndRetire()
	}
	return nil
}

// endLocalAndRetire latches the local end of the stream and retires its slot,
// reading the id in the SAME s.mu section that sets localEnded.
//
// The snapshot is the point. conn.go's write path documents the rule — read the
// id and the lifetime together, because a struct whose local and remote sides
// have both ended can be recycled and handed to another request between the two
// reads — and sendHeadersAndData followed it while its three siblings read
// s.id bare after unlocking. A reader could not tell proven-safe from oversight.
// Now there is one place that does it, and it does it under the lock.
func (s *Stream) endLocalAndRetire() {
	s.mu.Lock()
	s.localEnded = true
	id := s.id
	s.mu.Unlock()
	s.w.markStreamDone(id)
}

// Recv blocks until the next event for this stream is ready, the stream
// terminates, or ctx is cancelled.
// Recv is the unguarded receive kept for callers that still hold a *Stream.
//
// It cannot tell a stale reference from a live one — the receiver IS the
// struct — so a reference retained past Close and re-entered after the struct
// was handed to another request receives THAT request's events (issue #370).
// Use the StreamRef returned by Conn.NewStream, whose Recv refuses it.

// recv is Recv with the caller's lifetime presented. wantGen is compared under
// the same s.mu section that registers the reader, so a recycle cannot land
// between the check and the registration.
func (s *Stream) recv(ctx context.Context, wantGen uint64) (StreamEvent, error) {
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
	// Refuse a reader whose stream has already been Closed. The registration
	// above protects a goroutine that is INSIDE Recv; it does nothing for one
	// between two Recv calls, which is the ordinary shape of a read loop —
	// client's response-body reader calls Recv once per Read. A Close landing
	// in that gap pools the struct, and the next call would otherwise register
	// on it, inflating the recvActive of whatever request claims it next and
	// deferring that request's recycle behind a reader that has nothing to do
	// with it. It also stops the caller parking on the orphaned channel until
	// its context expires: ErrStreamClosed is both the truth and immediate.
	//
	// This does NOT make a stale reference safe. If the struct is re-allocated
	// before the stale Recv runs, allocStream re-arms released and this gate
	// admits it — measured, it then receives the NEXT request's events. Closing
	// that window needs the caller to present the lifetime it thinks it holds,
	// which Recv cannot infer from a receiver that IS the struct; the send side
	// has the same hole. "Callers must not retain a *Stream past Close" is still
	// a real obligation, not a formality.
	if s.gen.Load() != wantGen {
		// The struct has been recycled since this handle was minted. Whatever it
		// is carrying now belongs to another request.
		s.mu.Unlock()
		return StreamEvent{}, ErrStaleStream
	}
	if s.released.Load() {
		s.mu.Unlock()
		return StreamEvent{}, ErrStreamClosed
	}
	s.recvActive++
	events, resetSignal, endSignal := s.events, s.resetSignal, s.endSignal
	s.mu.Unlock()
	defer s.recvLeave()

	select {
	case e, ok := <-events:
		if !ok {
			return StreamEvent{}, ErrStreamClosed
		}
		return e, nil
	case <-resetSignal:
		// Deliver anything already buffered first. resetSignal is closed only
		// on the full-channel fallback — every caller of signalReset reaches it
		// from the default arm of a send into s.events — so a ready resetSignal
		// means, by construction, that undelivered events are sitting in the
		// channel. A plain three-way select has no priority and Go picks
		// uniformly among ready cases, which made each Recv a coin flip and an
		// N-event response survive with probability 2^-N. For a complete
		// response followed by the RST_STREAM(NO_ERROR) of RFC 9113 §8.1 that
		// is a discarded response, which "Clients MUST NOT discard responses as
		// a result of receiving such a RST_STREAM" forbids outright.
		//
		// Draining before the select instead would fix the same thing and cost
		// more: ctx.Done() would then lose to a channel that never empties
		// during a fast download, deferring cancellation for the length of the
		// transfer. Preferring events only in this arm leaves events-vs-ctx
		// exactly as it was. The reset is terminal and is not going anywhere;
		// the buffered events are finite, so it is reported as soon as they run
		// out.
		select {
		case e, ok := <-events:
			if ok {
				return e, nil
			}
		default:
		}
		code := frame.ErrCode(s.resetCode.Load())
		return StreamEvent{Type: EventReset, RSTCode: code, EndStream: true}, nil
	case <-endSignal:
		// Buffered events first, for the reason above: the body arrived before
		// the marker that ends it, and reporting the end early would truncate
		// the very response this signal exists to preserve.
		select {
		case e, ok := <-events:
			if ok {
				return e, nil
			}
		default:
		}
		return StreamEvent{Type: EventData, EndStream: true}, nil
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

// close is Close with the caller's lifetime presented.
//
// This is the destructive one. Without the check, a Close from a finished
// request passes the released latch — allocStream re-arms it — marks the NEW
// owner closed, wakes its blocked writer with an error, and puts
// RST_STREAM(CANCEL) on the wire for the new request's stream id. No race
// needed; see TestStaleRef_CloseResetsTheNextRequest.
//
// The gen check and the released CAS both happen under s.mu, which is what
// makes them mutually ordered against resetForPoolLocked. Checking gen outside
// the lock would leave exactly the window this closes.
func (s *Stream) close(wantGen uint64) error {
	s.mu.Lock()
	if s.gen.Load() != wantGen {
		s.mu.Unlock()
		return ErrStaleStream
	}
	// released is the idempotency guard. It survives recycleStream (which
	// resets closed/w/... for pool reuse), so a repeat Close while the struct
	// is still pooled returns here instead of dereferencing the nil-ed w.
	if !s.released.CompareAndSwap(false, true) {
		s.mu.Unlock()
		return nil
	}
	s.appClosed = true
	closed := s.closed // e.g. push() set this on event-channel overflow
	bothEnded := s.localEnded && s.remoteEnded
	connDone := s.connDone
	w := s.w
	nowClosed := false
	if !closed && !bothEnded {
		// Abandoning an incomplete stream: our RST is the sole teardown: the
		// reader will never see END_STREAM to reach markStreamDone's own path.
		s.closed = true
		nowClosed = true
	}
	s.mu.Unlock()
	if nowClosed {
		// Wake a writer parked in acquireSendCredits so it observes s.closed and
		// bails, exactly as OnRSTStream does for a peer reset. The bail-out check
		// was written for that path and its broadcast came with it; this path
		// sets the same flag and inherited neither, so a Send blocked on
		// flow-control credit slept through the Close that was meant to abandon
		// it and woke only when its own context expired. A condition variable's
		// predicate is only as live as its broadcasts.
		//
		// Done with no lock held: the documented order is fcOutMu before
		// Stream.mu, and wakeSendWaiters takes fcOutMu.
		if c, ok := w.(*Conn); ok {
			c.wakeSendWaiters()
		}
	}
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

// StreamRef is a handle to ONE lifetime of a pooled Stream.
//
// conn.Stream structs are recycled through a per-connection pool, so a *Stream
// retained past Close names a struct that another request may already own.
// Nothing on the struct can tell the difference — the receiver IS the struct —
// so the caller has to present the lifetime it believes it holds. A StreamRef
// is that presentation: every method on it refuses, with ErrStaleStream, once
// the struct has been recycled.
//
// It is returned by Conn.NewStream and Conn.LookupStream. Copy it freely; it is
// a small value and holds no lock. The zero StreamRef is valid to hold and
// refuses every call, since a live Stream's generation is never 0.
type StreamRef struct {
	s   *Stream
	gen uint64
}

// ref mints a handle for the stream's current lifetime.
func (s *Stream) ref() StreamRef { return StreamRef{s: s, gen: s.gen.Load()} }

// Stream returns the underlying stream, which is valid to touch only while
// Valid reports true — and only immediately, since nothing holds the lifetime
// still between the two calls. It exists for the few callers that need a field
// off the struct; prefer the methods.
func (r StreamRef) Stream() *Stream { return r.s }

// Valid reports whether the handle still names the lifetime it was minted for.
// It is advisory: a recycle can land the instant after it returns. The methods
// re-check under the lock, which is the check that decides anything.
func (r StreamRef) Valid() bool { return r.s != nil && r.s.gen.Load() == r.gen }

// ID returns the stream id, or 0 when the handle is stale.
func (r StreamRef) ID() uint32 {
	if !r.Valid() {
		return 0
	}
	return r.s.ID()
}

// Recv blocks until the next event for this lifetime is ready, the stream
// terminates, or ctx is cancelled. It returns ErrStaleStream once the struct
// has been recycled, rather than handing over the next request's events.
func (r StreamRef) Recv(ctx context.Context) (StreamEvent, error) {
	if r.s == nil {
		return StreamEvent{}, ErrStaleStream
	}
	return r.s.recv(ctx, r.gen)
}

// SendData sends DATA for this lifetime, chunking the payload to the peer's
// MAX_FRAME_SIZE and respecting both per-stream and connection-level outbound
// flow control (RFC 7540 §6.9). It blocks until enough send-window credit is
// available, the context is cancelled, or the connection closes. SendHeaders
// must have been called first; endStream half-closes the request side.
func (r StreamRef) SendData(ctx context.Context, p []byte, endStream bool) error {
	if r.s == nil {
		return ErrStaleStream
	}
	return r.s.sendData(ctx, r.gen, p, endStream)
}

// SendHeaders sends a HEADERS frame for this lifetime. The first call on a
// stream is where the connection assigns its ID, under the writer mutex, so the
// on-wire order matches RFC 7540 §5.1.1's monotonic-id rule. It always emits
// END_HEADERS (this codec does not split into CONTINUATION); endStream
// half-closes the request side.
func (r StreamRef) SendHeaders(ctx context.Context, fields []header.Field, endStream bool) error {
	return r.SendHeadersWithPriority(ctx, fields, endStream, nil)
}

// SendHeadersAndData sends HEADERS and body as ONE transport write when the send
// windows allow it, which takes a unary request from two writes to one (#451).
//
// It is a pure optimisation of SendHeaders-then-SendData and is safe to use
// wherever both would be called back to back with nothing in between: when the
// credit for the whole body is not immediately available, or the writer is not a
// real Conn, it does exactly those two calls instead. Never blocks for credit
// while holding the write lock.
func (r StreamRef) SendHeadersAndData(ctx context.Context, fields []header.Field, p []byte, endStream bool) error {
	if r.s == nil {
		return ErrStaleStream
	}
	return r.s.sendHeadersAndData(ctx, r.gen, fields, p, endStream)
}

// SendHeadersWithPriority is SendHeaders with PRIORITY fields embedded.
func (r StreamRef) SendHeadersWithPriority(ctx context.Context, fields []header.Field, endStream bool, prio *frame.Priority) error {
	if r.s == nil {
		return ErrStaleStream
	}
	return r.s.sendHeadersWithPriority(ctx, r.gen, fields, endStream, prio)
}

// Close releases this lifetime. A handle whose stream has already been recycled
// is refused, so a late Close cannot reset the request that claimed the struct
// next.
func (r StreamRef) Close() error {
	if r.s == nil {
		return ErrStaleStream
	}
	return r.s.close(r.gen)
}
