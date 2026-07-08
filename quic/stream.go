package quic

import "sort"

// Stream is a QUIC stream. For an HTTP/3 client the relevant streams are
// client-initiated bidirectional streams (one request/response exchange each).
type Stream struct {
	id   uint64
	conn *Conn
	recv recvStream

	sendOffset     uint64 // next byte offset to send = bytes already sent on this stream
	sendMax        uint64 // absolute per-stream send ceiling (§4.1); init = peer bidi_remote
	sdBlockedLimit uint64 // last sendMax a STREAM_DATA_BLOCKED was emitted for
	sdBlockedSet   bool   // whether a STREAM_DATA_BLOCKED has been emitted yet
	finSent        bool   // FIN latch (§4.5): the final size is fixed once set
}

// ID returns the stream's QUIC stream identifier.
func (s *Stream) ID() uint64 { return s.id }

// OpenStream opens the next client-initiated bidirectional stream (RFC 9000
// §2.1: IDs 0, 4, 8, … — the low two bits are zero). It returns
// ErrTooManyStreams if opening another stream would exceed the peer's
// advertised initial_max_streams_bidi limit (§4.6). It does not send anything
// until the caller writes to the stream.
func (c *Conn) OpenStream() (*Stream, error) {
	if c.openedBidi >= c.peer.InitialMaxStreamsBidi {
		return nil, ErrTooManyStreams
	}
	id := c.nextBidiStreamID
	c.nextBidiStreamID += 4
	c.openedBidi++
	s := &Stream{id: id, conn: c, sendMax: c.peer.InitialMaxStreamDataBidiRemote}
	if c.streams == nil {
		c.streams = map[uint64]*Stream{}
	}
	c.streams[id] = s
	return s, nil
}

// streamChunk is a run of received stream bytes buffered until the bytes before
// it arrive.
type streamChunk struct {
	offset uint64
	data   []byte
}

// recvStream reassembles the bytes of one stream's receive direction from
// STREAM frames that may arrive out of order or overlapping (RFC 9000 §2.2). It
// keeps the contiguous prefix in data and buffers later chunks until the gap
// fills.
type recvStream struct {
	data      []byte        // contiguous bytes received from offset 0
	pending   []streamChunk // buffered chunks beyond len(data), sorted by offset
	fin       bool
	finalSize uint64
}

// receive incorporates a STREAM frame's data at the given stream offset. fin
// marks that this frame carries the last byte (the final size is offset+len).
func (r *recvStream) receive(offset uint64, data []byte, fin bool) {
	if fin {
		r.fin = true
		r.finalSize = offset + uint64(len(data))
	}
	r.insert(offset, data)
}

func (r *recvStream) insert(offset uint64, data []byte) {
	have := uint64(len(r.data))
	if offset+uint64(len(data)) <= have {
		return // wholly duplicate
	}
	if offset <= have {
		r.data = append(r.data, data[have-offset:]...)
		r.absorb()
		return
	}
	r.pending = append(r.pending, streamChunk{offset, append([]byte(nil), data...)})
	sort.Slice(r.pending, func(i, j int) bool { return r.pending[i].offset < r.pending[j].offset })
}

// absorb folds any buffered chunks that are now contiguous into data.
func (r *recvStream) absorb() {
	for len(r.pending) > 0 {
		c := r.pending[0]
		have := uint64(len(r.data))
		if c.offset+uint64(len(c.data)) <= have {
			r.pending = r.pending[1:] // duplicate
			continue
		}
		if c.offset > have {
			break // still a gap
		}
		r.data = append(r.data, c.data[have-c.offset:]...)
		r.pending = r.pending[1:]
	}
}

// bytes returns the contiguous received prefix (offset 0 onward).
func (r *recvStream) bytes() []byte { return r.data }

// complete reports whether the whole stream has been received: the FIN has
// arrived and every byte up to the final size is contiguous.
func (r *recvStream) complete() bool {
	return r.fin && uint64(len(r.data)) == r.finalSize && len(r.pending) == 0
}
