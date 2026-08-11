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

// AppendMessagePrefix appends only the five-byte header for a message of msgLen
// bytes, leaving the message itself where it already is.
//
// It is the other half of AppendMessage, for a sender that can put two buffers
// on the wire as one message rather than one contiguous buffer. The copy
// AppendMessage performs is the entire message, every time, to place five bytes
// in front of it; for a large message that is the dominant cost of sending, and
// it is pure overhead — the bytes are already in memory in the right order,
// just in a different allocation.
func AppendMessagePrefix(dst []byte, msgLen int) ([]byte, error) {
	if uint64(msgLen) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%w: %d bytes", ErrMessageTooLarge, msgLen)
	}
	if cap(dst)-len(dst) < prefixLen {
		// Size it exactly rather than letting append pick. This buffer is pooled
		// and now only ever holds a prefix, so the one allocation it makes should
		// be the right one — append growing 1 → 2 → 4 → 8 would both allocate more
		// than needed and do it several times on the first use of each pooled
		// buffer.
		dst = make([]byte, 0, prefixLen)
	}
	dst = append(dst, 0) // compressed flag: identity
	return binary.BigEndian.AppendUint32(dst, uint32(msgLen)), nil
}

// decoder reassembles length-prefixed gRPC messages from the DATA chunks a
// conn.Stream delivers. A message may span any number of chunks, and one
// chunk may carry any number of messages — neither boundary is aligned with
// the other on the wire.
//
// It is deliberately unexported: Next returns a slice that is only valid until
// the next call, which is a silent-corruption footgun in an exported type, and
// Stream owns the only instance. AppendMessage stays exported because building
// the other side of the wire (a test server, a fixture) genuinely needs it.
//
// The zero decoder is ready to use and applies DefaultMaxMessageSize.
type decoder struct {
	// buf holds the pending bytes; buf[off:] is undelivered. It is either own,
	// or — while borrowed is set — a chunk belonging to the caller.
	buf []byte
	off int
	// max is the largest message accepted. Zero means DefaultMaxMessageSize.
	max int

	// own is the decoder's own growable buffer. Kept across borrows so its
	// capacity survives them; buf aliases it whenever nothing is borrowed.
	own []byte

	// borrowed is the pooled slab backing buf while the decoder aliases a
	// caller's chunk. See PushBorrowed for the ownership rule.
	borrowed *[]byte
	// borrowing distinguishes "borrowing a chunk whose slab is nil" (the pool
	// was cold, so there is nothing to return) from "not borrowing".
	borrowing bool
}

// PushBorrowed hands the decoder a DATA chunk WITHOUT copying it, for the common
// case where one DATA frame carries whole messages. It reports whether the
// borrow was taken; when it returns false the caller must fall back to Push.
//
// OWNERSHIP RULE. On a true return:
//   - the decoder's pending bytes ALIAS chunk, so the caller must not write to
//     it or reuse it;
//   - the decoder owns slab and returns it to the pool itself, so the caller
//     must NOT call putDataSlab;
//   - the borrow ends at the next Push/PushBorrowed — which copies whatever is
//     still undelivered into the decoder's own buffer first — or at release().
//
// This is safe against handing an alias out to callers because RecvInto always
// copies the message into the caller's destination: nothing the decoder returns
// outlives the borrow. A future zero-copy Recv would break that and must end
// the borrow itself.
func (d *decoder) PushBorrowed(chunk []byte, slab *[]byte) bool {
	if len(chunk) == 0 || d.Pending() != 0 {
		// Undelivered bytes are already pending, so this chunk continues a
		// message and has to be appended to them, not aliased.
		return false
	}
	d.endBorrow()
	d.buf, d.off = chunk, 0
	d.borrowed, d.borrowing = slab, true
	return true
}

// endBorrow ends a borrow: whatever is still undelivered is copied into the
// decoder's own buffer, and the borrowed slab goes back to the pool.
func (d *decoder) endBorrow() {
	if !d.borrowing {
		return
	}
	d.own = append(d.own[:0], d.buf[d.off:]...)
	d.buf, d.off = d.own, 0
	putDataSlab(d.borrowed)
	d.borrowed, d.borrowing = nil, false
}

// release ends any borrow, returning the slab. Called when the stream is done
// with the decoder; a borrow held past that would keep a pooled buffer out of
// circulation for as long as the stream object lived.
func (d *decoder) release() { d.endBorrow() }

// Push appends a DATA chunk to the decoder's pending bytes. The chunk is
// copied, so the caller may return its backing buffer to a pool immediately
// after Push returns. Prefer PushBorrowed, which skips the copy when it can.
func (d *decoder) Push(chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	d.endBorrow()
	d.compact()
	d.own = append(d.own, chunk...)
	d.buf = d.own
}

// limit returns the effective maximum message size.
func (d *decoder) limit() int {
	if d.max <= 0 {
		return DefaultMaxMessageSize
	}
	return d.max
}

// overLimit reports whether the pending bytes have grown past what one
// maximum-size message can occupy. Stream.Recv drains with Next before every
// Push, so it cannot reach this — but the bound belongs in the decoder rather
// than in its caller's call order, because a caller that pushes twice without
// draining would otherwise have no bound at all.
func (d *decoder) overLimit() bool {
	return d.Pending() > d.limit()+prefixLen
}

// compact slides undelivered bytes to the front once the consumed prefix has
// grown past half the buffer, so a long-lived stream does not grow buf without
// bound.
// Only ever called with buf == own (Push ends any borrow first), so sliding the
// bytes here cannot touch a caller's chunk.
func (d *decoder) compact() {
	if d.off == 0 {
		return
	}
	if d.off == len(d.own) {
		d.own = d.own[:0]
		d.buf, d.off = d.own, 0
		return
	}
	if d.off*2 >= len(d.own) {
		n := copy(d.own, d.own[d.off:])
		d.own = d.own[:n]
		d.buf, d.off = d.own, 0
	}
}

// Next returns the next complete message. ok is false when the pending bytes
// hold no complete message yet and the caller should Push more. The returned
// slice aliases the decoder's buffer and stays valid only until the next Push
// or Next — copy it to retain it.
func (d *decoder) Next() (msg []byte, ok bool, err error) {
	avail := d.buf[d.off:]
	if len(avail) < prefixLen {
		return nil, false, nil
	}
	if avail[0] != 0 {
		return nil, false, ErrCompressed
	}
	n := binary.BigEndian.Uint32(avail[1:prefixLen])
	limit := d.limit()
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
func (d *decoder) Pending() int { return len(d.buf) - d.off }
