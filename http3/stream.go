package http3

import (
	"errors"

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
type FrameReader struct {
	buf []byte
}

// Feed appends received stream bytes to the reader's buffer.
func (r *FrameReader) Feed(b []byte) { r.buf = append(r.buf, b...) }

// ReadFrame returns the next complete frame's type and payload, or ErrNeedMore
// if it is not yet fully buffered. The returned payload aliases the reader's
// buffer and is valid only until the next Feed or ReadFrame call.
func (r *FrameReader) ReadFrame() (typ uint64, payload []byte, err error) {
	typ, length, n, herr := ParseFrameHeader(r.buf)
	if herr != nil {
		return 0, nil, ErrNeedMore
	}
	// TODO(flow-control): cap length against a max frame size before buffering,
	// once receive-side flow control lands — a huge declared length otherwise
	// forces unbounded buffering.
	if uint64(len(r.buf)-n) < length { // payload not fully buffered yet
		return 0, nil, ErrNeedMore
	}
	total := n + int(length) // safe: length <= len(r.buf)-n, so no overflow
	// Cap the payload's capacity to its length so a caller's append cannot
	// reach into the reader's next buffered frames.
	payload = r.buf[n:total:total]
	r.buf = r.buf[total:]
	return typ, payload, nil
}

// Buffered reports how many unconsumed bytes remain buffered.
func (r *FrameReader) Buffered() int { return len(r.buf) }

// AppendClientControlStream appends the bytes a client writes to open its
// control stream: the control stream-type (0x00) followed by the mandatory
// first SETTINGS frame (RFC 9114 §6.2.1 — SETTINGS MUST be the first frame on
// the control stream).
func AppendClientControlStream(dst []byte, settings []Setting) []byte {
	dst = appendV(dst, StreamTypeControl)
	return AppendSettings(dst, settings)
}
