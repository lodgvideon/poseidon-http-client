package grpc

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// prefixLen is the gRPC message prefix: 1 compressed-flag byte + a 4-byte
// big-endian length. Every message on the wire carries exactly one.
const prefixLen = 5

// DefaultMaxMessageSize is the largest message Decoder accepts by default,
// matching gRPC's own 4 MiB default receive limit. A larger message is
// rejected before its bytes are buffered, so a hostile length prefix cannot
// make the client allocate on the peer's say-so.
const DefaultMaxMessageSize = 4 << 20

// ErrCompressed is reported when a peer sets the compressed flag on a message.
// The client advertises grpc-accept-encoding: identity, so a compressed
// message is a protocol violation rather than a supported case.
var ErrCompressed = errors.New("grpc: peer sent a compressed message but only identity encoding was advertised")

// ErrMessageTooLarge is reported when a length prefix exceeds the decoder's
// configured limit.
var ErrMessageTooLarge = errors.New("grpc: message exceeds the configured maximum size")

// AppendMessage appends msg to dst as one length-prefixed gRPC message with
// the compressed flag clear, and returns the extended slice. It returns an
// error when msg is longer than a uint32 length prefix can describe.
func AppendMessage(dst, msg []byte) ([]byte, error) {
	if uint64(len(msg)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%w: %d bytes", ErrMessageTooLarge, len(msg))
	}
	dst = append(dst, 0) // compressed flag: identity
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(msg)))
	return append(dst, msg...), nil
}

// Decoder reassembles length-prefixed gRPC messages from the DATA chunks a
// conn.Stream delivers. A message may span any number of chunks, and one
// chunk may carry any number of messages — neither boundary is aligned with
// the other on the wire.
//
// The zero Decoder is ready to use and applies DefaultMaxMessageSize.
type Decoder struct {
	buf []byte // pending bytes: buf[off:] is undelivered
	off int
	// Max is the largest message accepted. Zero means DefaultMaxMessageSize.
	// Set it before the first Push.
	Max int
}

// Push appends a DATA chunk to the decoder's pending bytes. The chunk is
// copied, so the caller may return its backing buffer to a pool immediately
// after Push returns.
func (d *Decoder) Push(chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	d.compact()
	d.buf = append(d.buf, chunk...)
}

// compact slides undelivered bytes to the front once the consumed prefix has
// grown past half the buffer, so a long-lived stream does not grow buf without
// bound.
func (d *Decoder) compact() {
	if d.off == 0 {
		return
	}
	if d.off == len(d.buf) {
		d.buf = d.buf[:0]
		d.off = 0
		return
	}
	if d.off*2 >= len(d.buf) {
		n := copy(d.buf, d.buf[d.off:])
		d.buf = d.buf[:n]
		d.off = 0
	}
}

// Next returns the next complete message. ok is false when the pending bytes
// hold no complete message yet and the caller should Push more. The returned
// slice aliases the decoder's buffer and stays valid only until the next Push
// or Next — copy it to retain it.
func (d *Decoder) Next() (msg []byte, ok bool, err error) {
	avail := d.buf[d.off:]
	if len(avail) < prefixLen {
		return nil, false, nil
	}
	if avail[0] != 0 {
		return nil, false, ErrCompressed
	}
	n := binary.BigEndian.Uint32(avail[1:prefixLen])
	limit := d.Max
	if limit <= 0 {
		limit = DefaultMaxMessageSize
	}
	if uint64(n) > uint64(limit) {
		return nil, false, fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, n, limit)
	}
	if uint64(len(avail)-prefixLen) < uint64(n) {
		return nil, false, nil
	}
	d.off += prefixLen + int(n)
	return avail[prefixLen : prefixLen+n], true, nil
}

// Pending reports the number of buffered bytes that do not yet form a complete
// message. A non-zero value when the stream ends means the peer truncated a
// message mid-flight.
func (d *Decoder) Pending() int { return len(d.buf) - d.off }

// Reset drops all pending bytes, keeping the allocated buffer for reuse.
func (d *Decoder) Reset() {
	d.buf = d.buf[:0]
	d.off = 0
}
