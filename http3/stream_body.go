package http3

import (
	"context"
	"errors"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// roundTripStream sends the request on stream and reads only as far as the final
// response HEADERS, returning the response head plus a BodyReader that pulls the
// DATA (and trailer) frames incrementally. It shares the request-send path
// (sendRequest) and the frame validation (dispatchFrame) with the buffered
// roundTrip; the sole difference is that DATA payloads are handed to the BodyReader
// via respBuilder.onData rather than buffered whole. Its caller (DoStream) aborts
// the stream on a non-nil error; once the head is returned the BodyReader owns the
// abort.
func (c *Client) roundTripStream(ctx context.Context, stream quicStream, req *Request) (resp *Response, brOut *BodyReader, err error) {
	frame, eerr := c.encodeRequestHeaders(req)
	if eerr != nil {
		return nil, nil, eerr
	}
	if serr := c.sendRequest(ctx, stream, req, frame); serr != nil {
		return nil, nil, serr
	}

	br := &BodyReader{c: c, stream: stream, ctx: ctx, req: req}
	br.rb.dec = &br.dec // per-request QPACK decoder (2d); its scratch aliases decoded slices
	br.rb.streamID = stream.ID()
	//nolint:fatcontext // Stored, not derived: rb.ctx is overwritten by whoever
	// currently owns the read (here, then by each Next below), so no chain is
	// built up. It exists because a QPACK-blocked decode has to be bounded by
	// the caller that is waiting on it.
	br.rb.ctx = ctx // bounds a blocked pre-head HEADERS decode (§2.1.3, Q4)
	br.rb.onData = br.stashData
	br.fr.SetMaxFrameLen(maxResponseBytes)
	// If an interim/final HEADERS referenced the dynamic table before a pre-head
	// error aborts the stream, notify the encoder (RFC 9204 §4.4.2). Once the head
	// is returned the BodyReader owns this (BodyReader.abort).
	defer func() {
		if err != nil && br.rb.refDynamic {
			c.qpackCancelStream(br.rb.streamID)
		}
	}()

	// Read until the final (non-1xx) response HEADERS arrive. Only HEADERS frames
	// are valid before then (DATA before the final response is a connection error,
	// enforced in dispatchFrame), so no body bytes are consumed here; any DATA or
	// trailer frames delivered in the same burst stay buffered for the BodyReader.
	for {
		if err := c.consumeUntilResponse(&br.fr, &br.rb); err != nil {
			return nil, nil, err
		}
		if br.rb.resp != nil {
			break
		}
		ended, rerr := c.recvStep(ctx, stream, &br.fr)
		if rerr != nil {
			return nil, nil, rerr
		}
		if ended {
			// The stream finished cleanly without ever carrying a final response.
			return nil, nil, ErrH3Message // RFC 9114 §4.1.2
		}
	}
	br.resp = br.rb.resp
	br.resp.Interim = br.rb.interim
	return br.resp, br, nil
}

// consumeUntilResponse dispatches buffered frames until the final response HEADERS
// set rb.resp, or the buffer drains (ErrNeedMore). It leaves any frames buffered
// past the final HEADERS (a DATA or trailer frame delivered in the same burst) in
// fr for the BodyReader to read. Errors are the same scoped connection/stream
// errors dispatchFrame returns.
// consumeUntilResponse parses buffered frames until the final response arrives.
func (c *Client) consumeUntilResponse(fr *FrameReader, rb *respBuilder) error {
	return c.consume(fr, rb, true)
}

// ResponseBody is the incremental body of a streaming HTTP/3 response, returned by
// Client.DoStream. The caller drives Next until BodyEvent.End (or an error) and
// MUST call Close when abandoning it early, so the request stream is aborted and
// its resources released. It is satisfied by *BodyReader; exposing it as an
// interface lets callers (and their tests) substitute the concrete reader.
type ResponseBody interface {
	// Next drives the receive loop until the next body event.
	Next(ctx context.Context) (BodyEvent, error)
	// Trailers returns the trailing field section, or nil if none was sent. It is
	// populated once Next has reported the trailer event or a clean End.
	Trailers() []header.Field
	// Close abandons the body, aborting the stream if it was not read to a clean end.
	Close() error
}

// BodyEvent is one observation from a streaming response body (BodyReader.Next).
// Exactly one of Data or Trailers is populated, or the body has ended with neither:
//   - Data non-nil  → a DATA chunk; it aliases the reader's frame buffer and is
//     valid only until the next Next or Close call, so copy it to retain the bytes.
//   - Trailers non-nil → the trailing field section; End is always true (trailers
//     are the last thing on the stream). Trailers own their backing memory.
//   - Data nil, Trailers nil, End true → the body finished cleanly with no trailers.
type BodyEvent struct {
	Data     []byte
	Trailers []header.Field
	End      bool
}

// BodyReader pulls a streaming HTTP/3 response body one frame at a time. It is
// returned by Client.DoStream after the response head; the caller drives it with
// Next until End (or an error) and MUST call Close when abandoning it early. It is
// NOT safe for concurrent use — a single response body is read by one goroutine.
type BodyReader struct {
	c      *Client
	stream quicStream
	ctx    context.Context
	req    *Request
	resp   *Response

	fr  FrameReader
	dec qpack.Decoder
	rb  respBuilder

	pending     []byte // last DATA payload stashed by stashData, awaiting Next
	havePending bool
	bodyLen     int64 // total DATA bytes observed, for the content-length check

	emittedTrailers bool
	done            bool
	err             error // terminal error, returned by Next after done
	aborted         bool
}

// stashData is respBuilder.onData for the streaming path: it records the DATA
// payload for the next Next call. Only one DATA frame is ever outstanding because
// Next reads exactly one frame per pull and returns as soon as a chunk is stashed.
func (r *BodyReader) stashData(payload []byte) error {
	r.pending = payload
	r.havePending = true
	r.bodyLen += int64(len(payload))
	return nil
}

// Next drives the receive loop until the next body event: a DATA chunk, the
// trailer section, or a clean end (BodyEvent.End). It returns a non-nil error —
// *StreamResetError for a server RESET_STREAM, ErrH3Control / ErrResponseTooLarge
// for a protocol or size violation, or ctx.Err() on cancel — and aborts the stream
// on any error. After a clean end or an error, further calls return the same
// terminal state.
func (r *BodyReader) Next(ctx context.Context) (BodyEvent, error) {
	//nolint:fatcontext // Overwrite, not derive: each Next replaces the field
	// with its own ctx rather than wrapping the previous one, so repeated calls
	// do not grow a context chain.
	r.rb.ctx = ctx // this Next's ctx bounds a blocked trailer-section decode (§2.1.3, Q4)
	for {
		if r.havePending {
			d := r.pending
			r.pending, r.havePending = nil, false
			return BodyEvent{Data: d}, nil
		}
		if r.rb.trailersSeen && !r.emittedTrailers {
			r.emittedTrailers = true
			if err := r.finish(); err != nil {
				return BodyEvent{}, err
			}
			return BodyEvent{Trailers: r.resp.Trailers, End: true}, nil
		}
		if r.done {
			if r.err != nil {
				return BodyEvent{}, r.err
			}
			return BodyEvent{End: true}, nil
		}

		typ, payload, rerr := r.fr.ReadFrame()
		if errors.Is(rerr, ErrNeedMore) {
			ended, err := r.refill(ctx)
			if err != nil {
				return BodyEvent{}, r.fail(err)
			}
			if ended {
				if ferr := r.finish(); ferr != nil {
					return BodyEvent{}, ferr
				}
				return BodyEvent{End: true}, nil
			}
			continue
		}
		if rerr != nil {
			return BodyEvent{}, r.fail(ErrResponseTooLarge) // oversized frame
		}
		if err := r.c.dispatchFrame(&r.rb, typ, payload); err != nil {
			return BodyEvent{}, r.fail(err)
		}
	}
}

// recvStep advances a stream's receive side by exactly one step: drain what the
// reader has delivered, or park until it delivers more, or conclude.
//
// It returns ended=true only when the stream finished cleanly with nothing left
// buffered. Otherwise it returns (false, nil) — the caller re-parses and calls
// again — or an error for a server reset or a truncated final frame (RFC 9114
// §7.1).
//
// This tail was written three times nearly line for line: the buffered roundTrip
// loop, the wait-for-response loop, and BodyReader.refill, whose own comment said
// it "mirrors the tail of the buffered roundTrip receive loop". The async-finished
// re-drain below was a bug fix, and a fix in machinery that exists in triplicate
// is one copy away from being half-applied. (All three did carry it by the time
// this merge happened — the divergence #497 describes had already been closed by
// hand, which is the cost this removes rather than a bug it fixes.)
func (c *Client) recvStep(ctx context.Context, stream quicStream, fr *FrameReader) (ended bool, err error) {
	if data := stream.Recv(); len(data) > 0 {
		fr.Feed(data)
		return false, nil
	}
	// One locked snapshot of the receive side (docs/HTTP3_DESIGN.md §3.4, F5).
	finished, reset, code := stream.RecvState()
	if !finished {
		// Park until the reader signals more data, ctx is cancelled, or the
		// connection terminates (§3.3). Level-triggered: the caller re-reads the
		// predicate on the next step.
		if werr := stream.WaitReadable(ctx); werr != nil {
			return false, werr
		}
		return false, nil
	}
	// Finished. The reader is asynchronous, so finished can flip between the drain
	// above and this point; drain once more before concluding. finished means the
	// FIN is in, so nothing can arrive after this.
	if data := stream.Recv(); len(data) > 0 {
		fr.Feed(data)
		return false, nil
	}
	if reset {
		// The server aborted with RESET_STREAM (RFC 9000 §3.5); surface it so the
		// caller can tell a rejected (retryable) request from a completed one
		// (RFC 9114 §4.1.1).
		return false, &StreamResetError{Code: code}
	}
	if fr.Buffered() > 0 {
		return false, c.connError(H3FrameError) // truncated final frame (§7.1)
	}
	return true, nil
}

// refill reads more stream bytes into the frame reader — one recvStep against
// this reader's own stream.
func (r *BodyReader) refill(ctx context.Context) (ended bool, err error) {
	return r.c.recvStep(ctx, r.stream, &r.fr)
}

// finish validates the completed body (the content-length equality check, RFC 9114
// §4.1.2) and marks the reader done. On a mismatch it aborts the stream with
// H3_MESSAGE_ERROR and records the terminal error.
func (r *BodyReader) finish() error {
	if !contentLengthMatches(r.resp, r.req.Method, r.bodyLen) {
		return r.fail(ErrH3Message)
	}
	r.done = true
	return nil
}

// fail records err as the terminal error, marks the reader done, and aborts the
// request stream (unless a prior fail already did). It returns err so callers can
// `return BodyEvent{}, r.fail(err)`.
func (r *BodyReader) fail(err error) error {
	r.done = true
	r.err = err
	r.abort(err)
	return err
}

// abort issues STOP_SENDING + RESET_STREAM once, so an abandoned or errored body
// releases the server-side stream. A connection error (ErrH3Control) has already
// torn down the whole connection, but aborting the stream too is harmless. If the
// stream referenced the dynamic table, it also emits a Stream Cancellation so the
// encoder can release the outstanding references (RFC 9204 §4.4.2). refDynamic is
// set only when a decoded section resolved a dynamic reference.
func (r *BodyReader) abort(err error) {
	if r.aborted {
		return
	}
	r.aborted = true
	abortStream(r.stream, err)
	if r.rb.refDynamic {
		r.c.qpackCancelStream(r.rb.streamID)
	}
}

// Trailers returns the trailing field section received after the body, or nil if
// the server sent none. It is populated once Next has reported the trailer event or
// a clean End; before then it is nil.
func (r *BodyReader) Trailers() []header.Field {
	if r.resp == nil {
		return nil
	}
	return r.resp.Trailers
}

// Close abandons the body. If it was not read to a clean end it aborts the request
// stream with H3_REQUEST_CANCELLED; otherwise it is a no-op. Idempotent.
func (r *BodyReader) Close() error {
	if r.done && r.err == nil {
		return nil // finished cleanly; nothing to abort
	}
	r.done = true
	r.abort(nil) // caller abandoned mid-stream → H3_REQUEST_CANCELLED
	return nil
}
