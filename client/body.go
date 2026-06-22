package client

import (
	"context"
	"io"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// responseBodyReader streams response DATA frames as an io.ReadCloser.
// Constructed by do() when Request.StreamBody is true; ownership
// transfers to Response.BodyReader.
type responseBodyReader struct {
	ctx       context.Context
	stream    *conn.Stream
	release   func()    // returns conn to pool; called exactly once in Close
	resp      *Response // written with trailers when EventTrailers arrives
	buf       []byte    // unconsumed tail of last DATA event (aliases curData)
	curData   *[]byte   // pooled buffer backing buf/last event; recycled on next event/Close
	closeOnce sync.Once
	done      bool
}

// recycleData returns the current pooled DATA buffer to the pool. Safe only
// when buf (which aliases it) has been fully consumed — true whenever a new
// Recv is about to run (the buf>0 fast path returns before reaching Recv).
func (r *responseBodyReader) recycleData() {
	if r.curData != nil {
		conn.GetDataBufPool().Put(r.curData)
		r.curData = nil
	}
}

// Read implements io.Reader. Blocks on stream.Recv until DATA arrives,
// fills p, and saves any surplus in r.buf for the next call. Returns
// io.EOF when END_STREAM or EventTrailers is observed.
func (r *responseBodyReader) Read(p []byte) (int, error) {
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}
	if r.done {
		return 0, io.EOF
	}
	for {
		ev, err := r.stream.Recv(r.ctx)
		if err != nil {
			return 0, err
		}
		switch ev.Type {
		case conn.EventData:
			// The previous frame's surplus was fully consumed before we
			// reached this Recv, so its pooled buffer is safe to recycle.
			r.recycleData()
			r.curData = ev.DataSlab
			n := copy(p, ev.Data)
			if n < len(ev.Data) {
				r.buf = ev.Data[n:] // aliases r.curData until consumed
			}
			if ev.EndStream {
				r.done = true
				if n == len(ev.Data) {
					return n, io.EOF
				}
			}
			return n, nil
		case conn.EventTrailers:
			r.recycleData()
			if r.resp != nil {
				r.resp.Trailers = append(r.resp.Trailers[:0], ev.Headers...)
			}
			r.done = true
			return 0, io.EOF
		case conn.EventReset:
			r.recycleData()
			r.done = true
			return 0, &StreamResetError{Code: ev.RSTCode}
		case conn.EventHeaders:
			continue // spurious mid-stream HEADERS; skip
		}
	}
}

// Close releases the stream and returns the conn to the pool. Sends
// RST_STREAM(CANCEL) when the body has not been fully drained.
// Idempotent.
func (r *responseBodyReader) Close() error {
	var err error
	r.closeOnce.Do(func() {
		err = r.stream.Close()
		r.recycleData()
		r.release()
	})
	return err
}
