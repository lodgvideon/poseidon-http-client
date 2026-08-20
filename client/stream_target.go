package client

import (
	"context"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// streamTarget is what a streaming entry point fills in once the response head
// has arrived. *Response (Do with BodyMode == BodyStream) and *StreamResponse
// (DoStream) implement it.
//
// It exists so the shared prologue CALLS the target rather than handing values
// back for each caller to apply. That distinction is the whole point: the two
// tails used to be hand-copies of each other, and the copy drifted — `do` set
// `done: ev.EndStream` on its body reader and `doStream` set `sr.drained` from
// the same event, one decision under two names in two places. A returned value
// can be ignored silently; a method parameter cannot, so adding one here breaks
// both implementations at compile time.
//
// Both concrete types are pointers, so the interface value carries a pointer and
// costs no allocation to build.
type streamTarget interface {
	// headersOut is where parseStatus writes the regular header fields. It is
	// separate from beginResponse because the status is not known until those
	// fields have been parsed.
	headersOut() *[]conn.HeaderField

	// beginResponse takes ownership of the response stream and the pool slot.
	//
	// endedOnHeaders reports that the response ended on its HEADERS frame — a
	// 204/304, a HEAD, or any status-only reply. Such a response has no DATA or
	// trailer event to end the body on, so a reader that does not know it pumps
	// one event too many: it then blocks until the context expires, or reports a
	// benign RST_STREAM(NO_ERROR) as a *StreamResetError. Missing this on one of
	// the two paths is the bug that motivated this interface.
	//
	// blk may be nil; when it is not, the target takes ownership of it and is
	// responsible for releasing it.
	beginResponse(ctx context.Context, rs respStream, rel releaser, status int, blk *conn.HeaderBlock, endedOnHeaders bool)
}

// beginStreaming runs everything the two streaming entry points share: open the
// incremental reader, wait for the final (non-1xx) response head, parse the
// status, then hand the stream to the target.
//
// It returns when the response head arrived, which is the streaming path's
// time-to-first-byte; the zero Time means none arrived. Reporting the instant
// rather than a duration keeps the choice of what to measure it against with
// the caller, which is the one holding the attempt's start time.
//
// On every error path it closes what it opened and releases the pool slot, so a
// caller that gets an error owns nothing.
func (c *Client) beginStreaming(ctx context.Context, s protoStream, rel releaser, sendCut error, target streamTarget) (time.Time, error) {
	rs, err := beginRespStream(ctx, s)
	if err != nil {
		_ = s.Close()
		rel.release()
		return time.Time{}, preferSendCut(err, sendCut)
	}
	ev, err := recvFinalHeaders(ctx, rs)
	if err != nil {
		_ = rs.Close()
		rel.release()
		return time.Time{}, preferSendCut(err, sendCut)
	}
	headersAt := time.Now()
	status, perr := parseStatus(ev.Headers, target.headersOut())
	if perr != nil {
		_ = rs.Close()
		rel.release()
		return headersAt, perr
	}
	target.beginResponse(ctx, rs, rel, status, ev.Block, ev.EndStream)
	return headersAt, nil
}

// headersOut implements streamTarget.
func (r *Response) headersOut() *[]conn.HeaderField { return &r.Headers }

// beginResponse implements streamTarget: it wires the incremental body reader
// the caller reads through Response.BodyReader.
func (r *Response) beginResponse(ctx context.Context, rs respStream, rel releaser, status int, blk *conn.HeaderBlock, endedOnHeaders bool) {
	r.Status = status
	if blk != nil {
		r.blocks = append(r.blocks, blk)
	}
	// The reader gets its own cancellable context, not the caller's ctx directly.
	// Close cancels it, which is what unblocks a Read parked in Recv: closing the
	// stream alone does not wake that goroutine, so an abort through the
	// io.ReadCloser left the reader hanging until the caller's own deadline fired.
	bodyCtx, bodyCancel := context.WithCancel(ctx)
	r.BodyReader = &responseBodyReader{
		ctx:     bodyCtx,
		cancel:  bodyCancel,
		stream:  rs,
		release: rel,
		resp:    r,
		done:    endedOnHeaders,
		lg:      armLeakGuard("Response.BodyReader"),
	}
}

// headersOut implements streamTarget.
func (sr *StreamResponse) headersOut() *[]conn.HeaderField { return &sr.Headers }

// beginResponse implements streamTarget.
func (sr *StreamResponse) beginResponse(ctx context.Context, rs respStream, rel releaser, status int, blk *conn.HeaderBlock, endedOnHeaders bool) {
	sr.Status = status
	if blk != nil {
		sr.blocks = append(sr.blocks, blk) // transfer block ownership
	}
	sr.stream = rs
	sr.release = rel
	// Pre-merge the caller's context with an abort Close can fire. conn.Stream
	// Recv parks on the event channel and the stream's own signals, so closing the
	// stream does not wake a reader already blocked in it; this is what does.
	// Building it here rather than per Recv keeps the streaming path
	// allocation-free for a caller that passes this same context back.
	sr.doCtx = ctx
	sr.recvCtx, sr.abortCancel = context.WithCancel(ctx)
	sr.lg = armLeakGuard("StreamResponse")
	sr.drained = endedOnHeaders
}
