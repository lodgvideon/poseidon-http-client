package http3

import (
	"errors"
	"sync"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

// Unidirectional stream types (RFC 9114 §6.2 / §11.2.4, RFC 9204 §4.2). The
// first varint on a unidirectional QUIC stream selects the stream's role.
const (
	StreamTypeControl      uint64 = 0x00
	StreamTypePush         uint64 = 0x01
	StreamTypeQPACKEncoder uint64 = 0x02
	StreamTypeQPACKDecoder uint64 = 0x03
)

// ErrNeedMore reports that the next stream item is not yet fully buffered; the
// caller feeds more stream bytes and retries. It is a benign control signal, not
// a protocol error.
var ErrNeedMore = errors.New("http3: need more bytes")

// ErrH3FrameTooLarge reports that a frame's declared length exceeds the reader's
// configured cap (RFC 9114 §7.1 / H3_EXCESSIVE_LOAD). It is a fatal protocol
// error, not a benign need-more signal.
var ErrH3FrameTooLarge = errors.New("http3: frame length exceeds cap")

// ReadStreamType reads the leading stream-type varint of a unidirectional stream
// (RFC 9114 §6.2), returning the type and the bytes consumed. It returns
// ErrNeedMore if the varint is not yet fully present.
func ReadStreamType(b []byte) (typ uint64, n int, err error) {
	typ, n = bytesx.ReadVarint(b)
	if n == 0 {
		return 0, 0, ErrNeedMore
	}
	return typ, n, nil
}

// FrameReader incrementally parses whole HTTP/3 frames from a byte stream that
// arrives in pieces (each QUIC STREAM frame delivers more bytes). It buffers
// until a frame is complete, so it suits the control stream and other
// bounded-frame streams; large DATA bodies are streamed by a later phase rather
// than buffered whole.
//
// buf is the whole backing array from its head and off is where the unconsumed
// window starts, rather than buf being re-sliced past each frame it hands out.
// Keeping the head is what lets Feed reclaim the consumed prefix instead of
// abandoning that capacity and reallocating (see Feed), and what lets the array
// be recycled between requests (see acquire). Re-slicing forward cost a growth
// chain per response: the reader climbed a new ladder for every body because the
// capacity behind the window was unreachable.
type FrameReader struct {
	buf []byte
	// bufp holds the pooled array acquire took, so release can return that same
	// header to the pool without allocating one. It is nil for a zero-value
	// reader, which never touches the pool.
	bufp   *[]byte
	off    int    // start of the unconsumed window within buf
	maxLen uint64 // reject a declared frame length above this (0 = unlimited)
}

// SetMaxFrameLen bounds the declared length of a frame the reader will buffer, so
// a peer cannot force unbounded buffering by declaring a huge length. Zero (the
// default) is unlimited; the control-stream reader sets a small cap.
func (r *FrameReader) SetMaxFrameLen(max uint64) { r.maxLen = max }

// Feed appends received stream bytes to the reader's buffer.
//
// When the array's spare tail cannot hold b, the unconsumed window slides back
// to the head first, overwriting bytes ReadFrame has already handed out. That is
// what the payload lifetime documented on ReadFrame buys: a caller needing its
// bytes past the next Feed must copy them out, which the buffered receive path
// does (dispatchFrame's FrameData case).
//
// Compacting only when the tail is spent — rather than on every Feed — keeps the
// move amortized O(1) per byte, the same amortization append already has.
// Compacting eagerly would re-copy a large half-received frame once per arriving
// QUIC packet, which is quadratic in the frame's size.
func (r *FrameReader) Feed(b []byte) {
	if r.off > 0 && cap(r.buf)-len(r.buf) < len(b) {
		r.buf = r.buf[:copy(r.buf, r.buf[r.off:])] // overlapping, dst before src: memmove
		r.off = 0
	}
	r.buf = append(r.buf, b...)
}

// ReadFrame returns the next complete frame's type and payload, or ErrNeedMore
// if it is not yet fully buffered. It returns ErrH3FrameTooLarge if the frame's
// declared length exceeds the reader's cap. The returned payload aliases the
// reader's buffer and is valid only until the next Feed or ReadFrame call.
func (r *FrameReader) ReadFrame() (typ uint64, payload []byte, err error) {
	win := r.buf[r.off:]
	typ, length, n, herr := ParseFrameHeader(win)
	if herr != nil {
		return 0, nil, ErrNeedMore
	}
	if r.maxLen != 0 && length > r.maxLen {
		return 0, nil, ErrH3FrameTooLarge // do not buffer an oversized frame
	}
	if uint64(len(win)-n) < length { // payload not fully buffered yet
		return 0, nil, ErrNeedMore
	}
	total := n + int(length) // safe: length <= len(win)-n, so no overflow
	start, end := r.off+n, r.off+total
	// Cap the payload's capacity to its length so a caller's append cannot
	// reach into the reader's next buffered frames.
	payload = r.buf[start:end:end]
	r.off = end
	return typ, payload, nil
}

// Buffered reports how many unconsumed bytes remain buffered.
func (r *FrameReader) Buffered() int { return len(r.buf) - r.off }

// frameBufSize is the backing array a cold pool hands out. A response's frames
// are one field section (a few hundred bytes) plus its body, so this is a body
// size: 16 KiB covers the great majority of them outright, and a larger one just
// grows the array once and recycles it at that size.
const frameBufSize = 16 << 10

// maxPooledFrameBuf bounds what goes back into the pool, since a pooled array
// never shrinks and one outlier response should not pin its footprint there.
//
// It has to sit ABOVE the responses this client actually buffers, not just above
// the common one: the copy dispatchFrame now pays for the body is only repaid by
// reuse, so a size the pool refuses to circulate pays the copy and gets nothing
// back — measured as a regression against adopting the reader's bytes at 64 KiB
// when the cap was 64 KiB. 256 KiB covers the buffered-response sizes in the
// profile that motivated this (issue #342 tops out near 250 KiB) and still keeps
// a multi-megabyte body from settling into the pool.
const maxPooledFrameBuf = 16 * frameBufSize

// frameBufPool recycles FrameReader backing arrays across requests. Reuse is
// what makes the receive path cheap: within one response the reader must grow
// from nothing to the largest frame it buffers, so a per-request array pays a
// fresh growth chain — measured at roughly four times the body's own size for a
// body arriving one QUIC packet at a time. Recycling pays it once.
var frameBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, frameBufSize)
		return &b
	},
}

// acquire gives the reader a recycled backing array. The caller MUST pair it
// with release, and MUST NOT let anything outlive the reader that aliases a
// payload ReadFrame returned — the array goes back to the pool and a later
// request overwrites it. Only the buffered receive path (roundTrip) uses this:
// it owns every byte it hands out, because dispatchFrame copies the body and the
// QPACK decode copies each header field. The streaming path returns payloads
// that alias this buffer to its caller, so it keeps a plain per-request reader.
func (r *FrameReader) acquire() {
	p, _ := frameBufPool.Get().(*[]byte)
	if p == nil {
		return // pool misconfigured; a nil-buffer reader still works, it just grows
	}
	r.bufp, r.buf, r.off = p, (*p)[:0], 0
}

// release returns an acquired array to the pool and empties the reader. It is
// idempotent, so it is safe to defer next to an early return path.
func (r *FrameReader) release() {
	// buf, not *p: Feed's append may have replaced the acquired array with a
	// larger one, and that grown array is the one worth recycling.
	p, buf := r.bufp, r.buf
	r.bufp, r.buf, r.off = nil, nil, 0
	if p == nil {
		return
	}
	if cap(buf) > maxPooledFrameBuf {
		// Outsized: drop the grown array, but return the one Get handed out. It is
		// untouched — Feed's append replaced it rather than writing through it — so
		// keeping it costs nothing and the pool stays populated. Dropping the entry
		// too emptied the pool one large response at a time, and every request after
		// that paid New.
		frameBufPool.Put(p)
		return
	}
	*p = buf[:0]
	frameBufPool.Put(p)
}

// AppendClientControlStream appends the bytes a client writes to open its
// control stream: the control stream-type (0x00) followed by the mandatory
// first SETTINGS frame (RFC 9114 §6.2.1 — SETTINGS MUST be the first frame on
// the control stream).
func AppendClientControlStream(dst []byte, settings []Setting) []byte {
	dst = appendV(dst, StreamTypeControl)
	return AppendSettings(dst, settings)
}

// AppendClientQPACKStream appends the leading stream-type varint a client writes
// to open one of its QPACK instruction streams — the encoder stream
// (StreamTypeQPACKEncoder, 0x02) or the decoder stream (StreamTypeQPACKDecoder,
// 0x03) (RFC 9204 §4.2). Unlike the control stream these carry no frame header:
// the type varint is followed directly by a raw byte stream of QPACK
// instructions.
func AppendClientQPACKStream(dst []byte, streamType uint64) []byte {
	return appendV(dst, streamType)
}
