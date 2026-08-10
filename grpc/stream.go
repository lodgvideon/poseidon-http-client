package grpc

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ErrSendClosed is reported when Send is called after CloseSend.
var ErrSendClosed = errors.New("grpc: send side already closed")

// ErrStreamClosed is reported by every Stream method called after Close.
var ErrStreamClosed = errors.New("grpc: stream closed by the caller")

// maxMetadataFields caps how many fields a single response header or trailer
// block may carry. conn's MAX_HEADER_LIST_SIZE bounds the block's *encoded*
// size, but this package copies the decoded block into memory that lives as
// long as the Stream, and a hpack.HeaderField is 56 bytes against a 33-byte
// minimum charge — so the byte cap alone lets a peer pin substantially more
// than it spends. gRPC metadata runs to tens of entries; a thousand is already
// far past any real use.
const maxMetadataFields = 1024

// Stream is one gRPC call.
//
// The send side (Send, CloseSend) and the receive side (Recv, Header) may be
// driven from two different goroutines concurrently — that is what makes
// bidirectional streaming work, and conn.Stream supports it because a writer
// blocked on HTTP/2 flow-control credit does not hold the connection's write
// lock. Each side on its own is single-goroutine: Send is serialised by an
// internal mutex, while the receive side must be driven by one goroutine only.
type Stream struct {
	s   conn.StreamRef
	dec decoder

	sendMu  sync.Mutex
	sendBuf []byte
	sentEnd bool
	// sendErr latches the first send failure. conn.writeData chunks a message
	// across several DATA frames and flushes each, so a failure partway leaves
	// a truncated message on the wire; letting a later Send append to that
	// would put its length prefix where the server expects the tail of the
	// previous message and resynchronise the stream onto garbage.
	sendErr error

	// discardMD skips cloning the response header and trailer blocks out of
	// conn's pooled slab. Set by DiscardMetadata, and by Invoke for itself: the
	// unary path calls neither Header nor Trailer, so those four allocations per
	// RPC are garbage the moment the call returns.
	//
	// It does NOT change how the call is decided. finish still reads the block —
	// the live one rather than the copy — and copies grpc-status and grpc-message
	// out of it, so status handling is identical either way.
	discardMD bool

	// Receive-side state. Owned by the single receiving goroutine.
	header      []conn.HeaderField
	trailer     []conn.HeaderField
	status      Status
	httpStatus  int
	headersSeen bool
	done        bool
	// err is the sticky transport-level failure (context cancellation, a
	// closed connection, a malformed message). It outranks status.
	err error

	// bufs is the pooled pair backing sendBuf and dec.buf, returned to the pool
	// by Close. nil when this stream was built without one.
	bufs *streamBufs

	closeOnce sync.Once
	// closed is read by every method, so it cannot live under sendMu or in the
	// receive-side state. conn recycles a Stream into its pool on Close once
	// both directions have ended — and the reader goroutine sets remoteEnded at
	// frame receipt, before this layer has consumed the event — so touching
	// s.s after Close risks reading a struct that has been handed to another
	// RPC on the same connection.
	closed atomic.Bool
}

// Send writes one message. It may be called repeatedly for a client-streaming
// or bidirectional call, and concurrently with Recv. It blocks while the
// stream or connection send window is exhausted, until ctx is done.
func (s *Stream) Send(ctx context.Context, msg []byte) error {
	if s.closed.Load() {
		return ErrStreamClosed
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	if s.sentEnd {
		return ErrSendClosed
	}
	if err := s.writeMessage(ctx, msg, false); err != nil {
		s.sendErr = err
		return err
	}
	return nil
}

// writeMessage puts one length-prefixed message on the wire as a single gRPC
// message, without copying the message to place its five-byte header in front of
// it: the header and the caller's bytes travel as two buffers of one DATA
// payload.
//
// Falls back to the copying form when the stream has no pooled scratch, so the
// send path does not depend on the pool having handed one out.
func (s *Stream) writeMessage(ctx context.Context, msg []byte, endStream bool) error {
	if s.bufs == nil {
		buf, err := AppendMessage(s.sendBuf[:0], msg)
		if err != nil {
			return err
		}
		s.sendBuf = buf
		return s.s.SendData(ctx, buf, endStream)
	}
	prefix, err := AppendMessagePrefix(s.sendBuf[:0], len(msg))
	if err != nil {
		return err
	}
	s.sendBuf = prefix
	s.bufs.vec[0], s.bufs.vec[1] = prefix, msg
	serr := s.s.SendDataV(ctx, s.bufs.vec[:], endStream)
	// Drop the caller's message before returning: the scratch outlives the call,
	// and a stream parked between sends must not keep the last message alive.
	s.bufs.vec[0], s.bufs.vec[1] = nil, nil
	return serr
}

// SendLast writes the final message and half-closes the request side in the
// same DATA frame, telling the server no more messages follow. It is Send
// followed by CloseSend, minus a frame: the empty END_STREAM frame CloseSend
// sends carries no payload but costs its own flush, which on a TLS connection
// is a separate record with its own header and AEAD tag, and — since Go enables
// TCP_NODELAY — usually a separate segment too. For a small message that
// overhead is comparable to the message.
//
// Use it wherever the last message is known in advance: that is every unary
// call, and the end of every client-streaming one. Send followed by CloseSend
// remains correct and remains supported, for a caller that only learns the
// request is over after the last message has already gone.
//
// Unlike CloseSend it is strict about a peer that has already torn the stream
// down. CloseSend can report that as success because telling a peer that has
// stopped listening that nothing more follows is a no-op; this call carries a
// message, and a message that did not reach the server is a real failure.
func (s *Stream) SendLast(ctx context.Context, msg []byte) error {
	if s.closed.Load() {
		return ErrStreamClosed
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	if s.sentEnd {
		return ErrSendClosed
	}
	// sentEnd is latched only on success, for the reason CloseSend gives: a
	// failure partway through leaves DATA on the wire without END_STREAM, and
	// recording the request as half-closed would stop a later CloseSend from
	// finishing the job.
	if err := s.writeMessage(ctx, msg, true); err != nil {
		s.sendErr = err
		return err
	}
	s.sentEnd = true
	return nil
}

// CloseSend half-closes the request side, telling the server no more messages
// follow. It is idempotent. A server-streaming call sends one message then
// CloseSend; a bidirectional call may CloseSend while still receiving.
//
// A caller that knows which message is the last should send it with SendLast
// instead, which folds this half-close into that message's DATA frame.
//
// It returns nil when the peer has already torn the stream down — the RFC 9113
// §8.1 case a server creates by answering without reading the request body.
// Nothing is half-closed in that case and no END_STREAM reaches the wire, but
// there is nothing left to tell the peer either, and failing here would make
// callers discard a response the server has already sent. The reset is not
// swallowed: it decides the call through Recv and Status. A caller that needs
// to know whether its request was delivered in full must read the outcome, not
// this error.
func (s *Stream) CloseSend(ctx context.Context) error {
	if s.closed.Load() {
		return ErrStreamClosed
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.sentEnd {
		return nil
	}
	if s.sendErr != nil {
		return benignHalfClose(s.sendErr)
	}
	// An empty DATA frame with END_STREAM: it consumes no flow-control
	// credit, so it cannot block behind an exhausted send window.
	// sentEnd is latched only on success — setting it first would make a retry
	// after a failed write return nil and report a half-close that never
	// reached the peer.
	if err := s.s.SendData(ctx, nil, true); err != nil {
		s.sendErr = err
		return benignHalfClose(err)
	}
	s.sentEnd = true
	return nil
}

// benignHalfClose reports a half-close that failed only because the peer had
// already torn the stream down as success.
//
// RFC 9113 §8.1 permits a server that has written a complete response to send
// RST_STREAM(NO_ERROR), asking the client to stop sending the request body.
// Both net/http2 and grpc-go's server do exactly that for any handler that
// does not drain the body — the common case for a unary method. conn
// implements §5.1 faithfully and closes the stream on any reset, so that
// benign signal reaches this layer as conn.ErrStreamClosed on a call the
// server has already answered, with the response sitting complete in the
// stream's event buffer.
//
// Telling a peer that has stopped listening that no more messages follow is a
// no-op, so failing the half-close only makes callers discard a response they
// already have. The reset is not swallowed: it reaches the receive side as
// EventReset, where a real code (CANCEL, REFUSED_STREAM, INTERNAL_ERROR)
// becomes the call's Status. Send stays strict — a message that did not reach
// the server is a genuine failure, and sendErr keeps it latched.
func benignHalfClose(err error) error {
	if errors.Is(err, conn.ErrStreamClosed) {
		return nil
	}
	return err
}

// Recv returns the next message from the server. It returns io.EOF when the
// server completed the call successfully, and a *Status when the call failed —
// so a caller loops until any error, then inspects it:
//
//	for {
//		msg, err := s.Recv(ctx)
//		if errors.Is(err, io.EOF) { break }   // status OK
//		if err != nil { return err }
//		use(msg)
//	}
//
// The returned slice is a fresh copy owned by the caller. RecvInto is the same
// call without that allocation, for a caller that already has a buffer.
func (s *Stream) Recv(ctx context.Context) ([]byte, error) {
	return s.RecvInto(ctx, nil)
}

// RecvInto is Recv appending into dst instead of allocating, in the same
// idiom as AppendMessage on the send side:
//
//	for {
//		buf, err = s.RecvInto(ctx, buf[:0])
//		if errors.Is(err, io.EOF) { break }   // status OK
//		if err != nil { return err }
//		use(buf)
//	}
//
// The message is appended to dst[:0], so any length dst carries is discarded and
// only its capacity is used. The result is caller-owned and stays valid across
// later calls — unlike the decoder's own buffer, which the next Recv overwrites.
//
// Every other part of Recv's contract is unchanged: io.EOF for a call that ended
// with status OK, a *Status for one that did not, and the sticky transport
// failure that makes every later call report the same thing.
//
// On error the returned slice is dst[:0], not nil. That matters for the loop
// above: the terminal iteration is an error, and returning nil there would hand
// the caller's buffer to the garbage collector on every stream — which is the
// allocation this method exists to avoid. Recv passes dst=nil and so still
// returns nil, keeping its own contract exactly.
func (s *Stream) RecvInto(ctx context.Context, dst []byte) ([]byte, error) {
	dst = dst[:0]
	if s.closed.Load() {
		return dst, ErrStreamClosed
	}
	for {
		msg, ok, err := s.dec.Next()
		if err != nil {
			return dst, s.fail(err)
		}
		if ok {
			return append(dst, msg...), nil
		}
		if s.err != nil {
			return dst, s.err
		}
		if s.done {
			return dst, s.terminal()
		}
		if err := s.pump(ctx); err != nil {
			return dst, err
		}
	}
}

// Header returns the response metadata, pulling events until the server's
// header block arrives. It returns the terminal error when the call failed
// before any headers were sent.
func (s *Stream) Header(ctx context.Context) ([]conn.HeaderField, error) {
	if s.closed.Load() {
		return nil, ErrStreamClosed
	}
	for !s.headersSeen {
		if s.err != nil {
			return nil, s.err
		}
		if s.done {
			return nil, s.terminal()
		}
		if err := s.pump(ctx); err != nil {
			return nil, err
		}
	}
	return s.header, nil
}

// Trailer returns the trailing metadata. It is meaningful only after Recv has
// reported the end of the stream; before that it returns nil.
//
// It reads receive-side state, so it must be called from the same goroutine
// that drives Recv. Reading it from the sending goroutine of a bidirectional
// call is a data race.
func (s *Stream) Trailer() []conn.HeaderField { return s.trailer }

// Status returns the RPC's terminal status. It is meaningful only after Recv
// has reported the end of the stream.
//
// Like Trailer, it must be called from the goroutine that drives Recv.
func (s *Stream) Status() Status { return s.status }

// Close releases the stream. When the call has not completed it sends
// RST_STREAM(CANCEL), which is how a client abandons an RPC early. Idempotent.
//
// Every other method fails with ErrStreamClosed afterwards. That is not
// politeness: conn.Stream.Close recycles the struct into its connection's pool
// once both directions have ended, and the reader goroutine marks the remote
// side ended when the frame arrives rather than when this layer consumes the
// event — so "both ended" is routinely true while the trailers are still
// queued. Calling into a recycled struct would read another RPC's events.
//
// Close must not be called while another goroutine is inside Recv on the same
// Stream. Cancel that goroutine's context first, then Close.
func (s *Stream) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		err = s.s.Close()
		s.releaseBufs()
	})
	return err
}

// maxPooledStreamBuf bounds what a stream may hand back. A call that received a
// 64 MiB message would otherwise park 64 MiB in the pool for the life of the
// process on the strength of one outlier.
const maxPooledStreamBuf = 64 << 10

// streamBufs is the pair of buffers a Stream would otherwise regrow from zero on
// every RPC: the send-side length-prefix scratch and the decoder's reassembly
// buffer.
//
// The BUFFERS are pooled and the Stream is not, which is the whole design. A
// pooled Stream would have to re-arm its closed flag for the next owner, and a
// caller holding one from a finished RPC would then pass every guard and read
// the next call's messages — the same shape as issue #370 one layer down, which
// cost an API change to close. Leaving the struct alone keeps closed a permanent
// latch: once Close has run, every method on that Stream refuses forever, so a
// stale reference can never reach the buffers this pool hands to someone else.
type streamBufs struct {
	send []byte
	dec  []byte
	// vec is the two-element vector every send builds: the five-byte prefix and
	// the caller's message, handed to the transport as one DATA payload without
	// joining them.
	//
	// It lives here rather than on Stream because Stream is deliberately NOT
	// pooled — openStream does &Stream{} per RPC — so a slice field on it would
	// start nil and its first append would allocate on every single call. An
	// array on the pooled struct costs nothing per RPC and nothing per send.
	vec [2][]byte
}

var streamBufPool = sync.Pool{New: func() any { return new(streamBufs) }}

// acquireBufs attaches pooled buffers to the stream.
func (s *Stream) acquireBufs() {
	b, _ := streamBufPool.Get().(*streamBufs)
	if b == nil {
		return
	}
	s.bufs = b
	s.sendBuf = b.send[:0]
	s.dec.own = b.dec[:0]
	s.dec.buf = s.dec.own
}

// releaseBufs returns the buffers, dropping either one that grew past the cap.
// Called once, from Close's sync.Once, so there is no double-Put to guard.
func (s *Stream) releaseBufs() {
	// End any borrow first: a decoder still aliasing a DATA chunk holds that
	// chunk's pooled slab, and dropping the decoder without releasing it would
	// keep the buffer out of circulation for good.
	s.dec.release()

	b := s.bufs
	if b == nil {
		return
	}
	s.bufs = nil
	b.send, b.dec = s.sendBuf[:0], s.dec.own[:0]
	b.vec[0], b.vec[1] = nil, nil // never park a caller's message in the pool
	if cap(b.send) > maxPooledStreamBuf {
		b.send = nil
	}
	if cap(b.dec) > maxPooledStreamBuf {
		b.dec = nil
	}
	// Drop this stream's own view of them first: after the Put they belong to
	// whoever draws them next, and a stale reference must not be able to reach
	// through this struct to memory another RPC is writing.
	s.sendBuf, s.dec.buf, s.dec.own = nil, nil, nil
	streamBufPool.Put(b)
}

// terminal converts the recorded end-of-stream state into what Recv returns:
// io.EOF for a successful call, the *Status otherwise.
func (s *Stream) terminal() error {
	// The recorded status comes first. A peer that aborts mid-message — trailers
	// carrying RESOURCE_EXHAUSTED, or RST_STREAM(ENHANCE_YOUR_CALM) — leaves
	// bytes in the decoder as a *consequence* of the abort, and reporting the
	// truncation instead would replace the peer's diagnosis with our own and
	// turn a retriable code into a non-retriable INTERNAL.
	if err := s.status.Err(); err != nil {
		return err
	}
	if s.dec.Pending() > 0 {
		return s.fail(&Status{
			Code:    Internal,
			Message: "server closed the stream in the middle of a message",
		})
	}
	return io.EOF
}

// fail records err as the sticky transport failure and returns it, so every
// later Recv reports the same thing rather than resuming a broken stream.
func (s *Stream) fail(err error) error {
	if s.err == nil {
		s.err = err
	}
	return s.err
}

// pump reads exactly one stream event and folds it into the receive state.
func (s *Stream) pump(ctx context.Context) error {
	ev, err := s.s.Recv(ctx)
	if err != nil {
		return s.fail(err)
	}
	switch ev.Type {
	case conn.EventInterimHeaders:
		// A 1xx is not a gRPC response; the final HEADERS still follows.
		putHeaderSlab(ev.Slab)

	case conn.EventHeaders:
		s.onHeaders(ev)

	case conn.EventData:
		// Borrow the chunk when nothing is pending, so a DATA frame carrying a
		// whole message is parsed in place instead of copied. The decoder then
		// owns the slab and returns it when the borrow ends — see the ownership
		// rule on PushBorrowed.
		if !s.dec.PushBorrowed(ev.Data, ev.DataSlab) {
			s.dec.Push(ev.Data)
			putDataSlab(ev.DataSlab)
		}
		if ev.EndStream {
			// DATA carrying END_STREAM means no trailers follow, so the
			// server never sent grpc-status.
			s.finish(nil)
		}

	case conn.EventTrailers:
		// Same rule as onHeaders: finish reads the block and copies what it keeps,
		// so it can run on the live fields when the clone is skipped.
		live := ev.Headers
		if !s.discardMD {
			s.trailer = cloneFields(live)
			live = s.trailer
		}
		s.finish(live)
		putHeaderSlab(ev.Slab)

	case conn.EventReset:
		s.status = Status{
			Code:    statusFromRST(ev.RSTCode),
			Message: "stream reset by peer, HTTP/2 error code " + strconv.FormatUint(uint64(ev.RSTCode), 10),
		}
		s.done = true

	case conn.EventPushPromise:
		// Push is disabled by default and meaningless for gRPC; drop it.
		putHeaderSlab(ev.Slab)
	}
	return nil
}

// onHeaders folds the final response header block into the receive state and
// handles the Trailers-Only shape, where a single HEADERS frame with
// END_STREAM carries both :status and grpc-status. That shape is how gRPC
// servers report most errors, and it arrives as EventHeaders rather than
// EventTrailers because no response header block preceded it.
func (s *Stream) onHeaders(ev conn.StreamEvent) {
	if s.headersSeen {
		// conn classifies a second block as trailers, so this is currently
		// unreachable; the guard is kept because client/client.go keeps the
		// same one, and because silently overwriting the response headers is
		// the worst possible failure mode if that classification ever changes.
		return
	}
	// live is the decoded block, pointing into conn's pooled slab. The slab goes
	// back when this function returns — deferred rather than returned inline,
	// because everything below reads the block and an early return would
	// otherwise leave one path reading memory the pool had already handed on.
	//
	// Everything read from it here copies what it keeps: pseudoStatus returns an
	// int, finish converts the two fields it reads with string(), and
	// validContentType compares. Only the clone outlives this call.
	defer putHeaderSlab(ev.Slab)
	live := ev.Headers
	if !s.discardMD {
		s.header = cloneFields(live)
		live = s.header
	}
	s.headersSeen = true
	s.httpStatus = pseudoStatus(live)

	if ev.EndStream {
		// Trailers-Only: the one block is both. Copy rather than alias, so a
		// caller mutating what Header() returned cannot change what Trailer()
		// returns.
		if !s.discardMD {
			s.trailer = append([]conn.HeaderField(nil), s.header...)
		}
		s.finish(live)
		return
	}
	if s.httpStatus != 200 {
		// A non-200 is a failed call and nothing that follows can rescue it —
		// but the status-mapping table is defined for use "only for clients
		// that received a response that did not include grpc-status", and
		// grpc-java's HTTP-error path puts grpc-status and grpc-message in
		// exactly this block. finish prefers them and falls back to the table,
		// so the server's own diagnosis survives.
		s.finish(live)
		return
	}
	if !validContentType(live) {
		s.status = Status{
			Code:    Internal,
			Message: "server response is missing an application/grpc content-type",
		}
		s.done = true
	}
}

// finish derives the terminal status from a field block that should carry
// grpc-status, and marks the stream done. fields may be nil, which is the case
// where the server ended the stream with DATA and sent no trailers at all.
func (s *Stream) finish(fields []conn.HeaderField) {
	s.done = true
	badHTTP := s.httpStatus != 0 && s.httpStatus != 200
	if v, ok := findField(fields, "grpc-status"); ok {
		code := parseStatusCode(string(v))
		// A non-200 response may not declare itself successful. The preference
		// for the peer's own grpc-status is about whose diagnosis wins, not
		// about whether one can contradict the transport it arrived on: the
		// gRPC protocol fixes ":status 200" for every conforming response, so
		// a non-200 carrying grpc-status OK is impossible by construction and
		// the mapping table never contemplates it. Accepting it reported a
		// call the client had already classified as failed as a *successful*
		// one — and on this path the body has already been dropped, so what
		// the caller got was an empty success indistinguishable from a real
		// one, on a value the peer chose. Note "00" parses to OK as well,
		// which is why the guard is on the parsed code rather than the text.
		if !badHTTP || code != OK {
			s.status = Status{Code: code}
			if m, ok := findField(fields, "grpc-message"); ok {
				s.status.Message = decodeMessage(string(m))
			}
			return
		}
		s.status = Status{
			Code:    statusFromHTTP(s.httpStatus),
			Message: "server returned HTTP status " + strconv.Itoa(s.httpStatus) + " with grpc-status OK",
		}
		return
	}
	// No grpc-status: the mapping table is the whole answer, including its 200
	// row, which is UNKNOWN precisely because a truly successful response would
	// have carried a grpc-status.
	if s.httpStatus != 0 {
		s.status = Status{
			Code:    statusFromHTTP(s.httpStatus),
			Message: "server returned HTTP status " + strconv.Itoa(s.httpStatus) + " without a grpc-status",
		}
		return
	}
	s.status = Status{
		Code:    Unknown,
		Message: "server closed the stream without sending grpc-status",
	}
}

// cloneFields copies a decoded header block out of conn's pooled slab into
// caller-owned memory, using one backing array for the whole block. Copying
// here is what lets the slab go back to the pool immediately, which in turn
// keeps the Stream free of any buffer-lifetime contract.
func cloneFields(src []conn.HeaderField) []conn.HeaderField {
	if len(src) == 0 {
		return nil
	}
	if len(src) > maxMetadataFields {
		src = src[:maxMetadataFields]
	}
	n := 0
	for i := range src {
		n += len(src[i].Name) + len(src[i].Value)
	}
	backing := make([]byte, 0, n)
	out := make([]conn.HeaderField, len(src))
	for i := range src {
		start := len(backing)
		backing = append(backing, src[i].Name...)
		mid := len(backing)
		backing = append(backing, src[i].Value...)
		out[i] = conn.HeaderField{
			Name:  backing[start:mid:mid],
			Value: backing[mid:len(backing):len(backing)],
		}
	}
	return out
}

// putHeaderSlab returns a decoded header slab to conn's pool. It is the single
// return site for header slabs in this package, which is what rules out a
// double-Put. nil-safe.
func putHeaderSlab(slab *[]byte) {
	if slab != nil {
		*slab = (*slab)[:0]
		conn.GetHeaderSlabPool().Put(slab)
	}
}

// putDataSlab returns a DATA payload buffer to conn's pool. Safe to call as
// soon as Decoder.Push has copied the bytes out. Single return site, nil-safe.
func putDataSlab(slab *[]byte) {
	if slab != nil {
		conn.GetDataBufPool().Put(slab)
	}
}

// findField returns the value of the first field named name.
func findField(fields []conn.HeaderField, name string) ([]byte, bool) {
	for i := range fields {
		if string(fields[i].Name) == name {
			return fields[i].Value, true
		}
	}
	return nil, false
}

// pseudoStatus returns the numeric :status of a response block, or 0 when it
// is absent or unparsable.
func pseudoStatus(fields []conn.HeaderField) int {
	v, ok := findField(fields, ":status")
	if !ok {
		return 0
	}
	// Exactly three digits (RFC 9113 §8.3.2). Accumulating an unbounded digit
	// string would let "000200" read as 200, and would wrap a long enough one
	// back onto 200 outright — laundering a peer-chosen string into success.
	if len(v) != 3 {
		return 0
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// validContentType reports whether the response carries a gRPC content-type.
// The specification allows a subtype suffix ("application/grpc+proto"), so the
// check is a prefix match with the delimiter enforced.
func validContentType(fields []conn.HeaderField) bool {
	v, ok := findField(fields, "content-type")
	if !ok {
		return false
	}
	ct := string(v)
	if !strings.HasPrefix(ct, "application/grpc") {
		return false
	}
	rest := ct[len("application/grpc"):]
	return rest == "" || rest[0] == '+' || rest[0] == ';'
}
