package grpc

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// ErrSendClosed is reported when Send is called after CloseSend.
var ErrSendClosed = errors.New("grpc: send side already closed")

// Stream is one gRPC call.
//
// The send side (Send, CloseSend) and the receive side (Recv, Header) may be
// driven from two different goroutines concurrently — that is what makes
// bidirectional streaming work, and conn.Stream supports it because a writer
// blocked on HTTP/2 flow-control credit does not hold the connection's write
// lock. Each side on its own is single-goroutine: Send is serialised by an
// internal mutex, while the receive side must be driven by one goroutine only.
type Stream struct {
	s   *conn.Stream
	dec Decoder

	sendMu  sync.Mutex
	sendBuf []byte
	sentEnd bool

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

	closeOnce sync.Once
}

// Send writes one message. It may be called repeatedly for a client-streaming
// or bidirectional call, and concurrently with Recv. It blocks while the
// stream or connection send window is exhausted, until ctx is done.
func (s *Stream) Send(ctx context.Context, msg []byte) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.sentEnd {
		return ErrSendClosed
	}
	buf, err := AppendMessage(s.sendBuf[:0], msg)
	if err != nil {
		return err
	}
	s.sendBuf = buf
	return s.s.SendData(ctx, buf, false)
}

// CloseSend half-closes the request side, telling the server no more messages
// follow. It is idempotent. A server-streaming call sends one message then
// CloseSend; a bidirectional call may CloseSend while still receiving.
func (s *Stream) CloseSend(ctx context.Context) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.sentEnd {
		return nil
	}
	s.sentEnd = true
	// An empty DATA frame with END_STREAM: it consumes no flow-control
	// credit, so it cannot block behind an exhausted send window.
	return s.s.SendData(ctx, nil, true)
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
// The returned slice is a fresh copy owned by the caller.
func (s *Stream) Recv(ctx context.Context) ([]byte, error) {
	for {
		msg, ok, err := s.dec.Next()
		if err != nil {
			return nil, s.fail(err)
		}
		if ok {
			return append([]byte(nil), msg...), nil
		}
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
}

// Header returns the response metadata, pulling events until the server's
// header block arrives. It returns the terminal error when the call failed
// before any headers were sent.
func (s *Stream) Header(ctx context.Context) ([]conn.HeaderField, error) {
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
func (s *Stream) Trailer() []conn.HeaderField { return s.trailer }

// Status returns the RPC's terminal status. It is meaningful only after Recv
// has reported the end of the stream.
func (s *Stream) Status() Status { return s.status }

// Close releases the stream. When the call has not completed it sends
// RST_STREAM(CANCEL), which is how a client abandons an RPC early. Idempotent.
func (s *Stream) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.s.Close() })
	return err
}

// terminal converts the recorded end-of-stream state into what Recv returns:
// io.EOF for a successful call, the *Status otherwise.
func (s *Stream) terminal() error {
	if n := s.dec.Pending(); n > 0 {
		return s.fail(&Status{
			Code:    Internal,
			Message: "server closed the stream in the middle of a message",
		})
	}
	if err := s.status.Err(); err != nil {
		return err
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
		s.dec.Push(ev.Data)
		putDataSlab(ev.DataSlab)
		if ev.EndStream {
			// DATA carrying END_STREAM means no trailers follow, so the
			// server never sent grpc-status.
			s.finish(nil)
		}

	case conn.EventTrailers:
		s.trailer = cloneFields(ev.Headers)
		putHeaderSlab(ev.Slab)
		s.finish(s.trailer)

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
	s.header = cloneFields(ev.Headers)
	putHeaderSlab(ev.Slab)
	s.headersSeen = true
	s.httpStatus = pseudoStatus(s.header)

	if ev.EndStream {
		s.trailer = s.header
		s.finish(s.header)
		return
	}
	if s.httpStatus != 200 {
		// A non-200 with a continuing body is still a failed call: the
		// HTTP-to-gRPC mapping applies and nothing that follows can rescue it.
		s.status = Status{
			Code:    statusFromHTTP(s.httpStatus),
			Message: "server returned HTTP status " + strconv.Itoa(s.httpStatus),
		}
		s.done = true
		return
	}
	if !validContentType(s.header) {
		s.status = Status{
			Code:    Internal,
			Message: "server response is missing a application/grpc content-type",
		}
		s.done = true
	}
}

// finish derives the terminal status from a field block that should carry
// grpc-status, and marks the stream done. fields may be nil, which is the case
// where the server ended the stream with DATA and sent no trailers at all.
func (s *Stream) finish(fields []conn.HeaderField) {
	s.done = true
	if v, ok := findField(fields, "grpc-status"); ok {
		s.status = Status{Code: parseStatusCode(string(v))}
		if m, ok := findField(fields, "grpc-message"); ok {
			s.status.Message = decodeMessage(string(m))
		}
		return
	}
	// No grpc-status. A non-200 HTTP status is the documented fallback;
	// otherwise the server broke the contract.
	if s.httpStatus != 0 && s.httpStatus != 200 {
		s.status = Status{
			Code:    statusFromHTTP(s.httpStatus),
			Message: "server returned HTTP status " + strconv.Itoa(s.httpStatus) + " without a grpc-status",
		}
		return
	}
	s.status = Status{
		Code:    Internal,
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
