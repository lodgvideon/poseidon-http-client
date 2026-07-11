package quic

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
	sendReset      bool   // RESET_STREAM sent — the send side is aborted (§3.2)
	recvMax        uint64 // per-stream receive limit we advertise; raised via MAX_STREAM_DATA
	recvHighest    uint64 // highest byte offset received on this stream (flow control, §4.1)
	recvReset      bool   // peer sent RESET_STREAM — the receive side is aborted (§3.5)
	recvResetCode  uint64 // the application error code carried by that RESET_STREAM (§19.4)
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
	s := &Stream{id: id, conn: c, sendMax: c.peer.InitialMaxStreamDataBidiRemote, recvMax: DefaultStreamRecvWindow}
	if c.streams == nil {
		c.streams = map[uint64]*Stream{}
	}
	c.streams[id] = s
	return s, nil
}

// OpenUniStream opens the next client-initiated unidirectional stream (RFC 9000
// §2.1: IDs 2, 6, 10, …) — the HTTP/3 control and QPACK streams, which are
// send-only from the opener. It returns ErrTooManyStreams if opening another
// would exceed the peer's advertised initial_max_streams_uni limit (§4.6).
func (c *Conn) OpenUniStream() (*Stream, error) {
	if c.openedUni >= c.peer.InitialMaxStreamsUni {
		return nil, ErrTooManyStreams
	}
	id := 2 + c.openedUni*4
	c.openedUni++
	s := &Stream{id: id, conn: c, sendMax: c.peer.InitialMaxStreamDataUni, recvMax: DefaultStreamRecvWindow}
	if c.streams == nil {
		c.streams = map[uint64]*Stream{}
	}
	c.streams[id] = s
	return s, nil
}

// Recv returns the stream bytes that have become contiguous since the previous
// Recv call (the newly available prefix of the received data), for reading a
// response. It returns an empty slice when nothing new is available. The result
// aliases the stream's buffer and should be consumed before the next Recv.
// Consuming bytes frees receive-flow-control window, which may queue
// MAX_STREAM_DATA / MAX_DATA to grant the peer more credit (RFC 9000 §4.1).
func (s *Stream) Recv() []byte {
	data := s.recv.read()
	if len(data) > 0 {
		s.conn.onStreamConsumed(s, uint64(len(data)))
	}
	return data
}

// Finished reports whether the receive side is complete — either the FIN has
// arrived with every byte contiguous (RFC 9000 §2.2), or the peer aborted the
// stream with RESET_STREAM (§3.5).
func (s *Stream) Finished() bool { return s.recvReset || s.recv.complete() }

// ResetReceived reports whether the peer aborted its send side with RESET_STREAM
// (RFC 9000 §3.5) — an abrupt end that, unlike a clean FIN, may fall in the
// middle of a frame. Callers distinguish it from a clean, complete receive.
func (s *Stream) ResetReceived() bool { return s.recvReset }

// ResetCode returns the application error code the peer carried in its
// RESET_STREAM (RFC 9000 §19.4); it is meaningful only when ResetReceived is
// true. An HTTP/3 caller maps it to a request-level error (RFC 9114 §8.1).
func (s *Stream) ResetCode() uint64 { return s.recvResetCode }

// maybeRetire drops a stream from the routing map once both directions have
// reached a terminal state — the FIN has been sent and the receive side is
// finished (fully received or reset) — so a long-lived connection carrying many
// requests does not accumulate finished streams. finSent is mutually exclusive
// with sendReset (Reset is a no-op once the FIN is sent), so a retired stream is
// never one whose STREAM data the retransmitter must suppress (§13.3), and its
// received bytes stay counted in connection flow control. The map only routes
// inbound frames; the caller's *Stream and its buffered data are unaffected, and
// a late retransmit for the id is ignored as a stream we never opened.
func (c *Conn) maybeRetire(s *Stream) {
	if s.finSent && (s.recvReset || s.recv.complete()) {
		delete(c.streams, s.id)
	}
}

// Reset abruptly terminates the send side of the stream with an application error
// code, sending a RESET_STREAM frame (RFC 9000 §3.2, §19.4). No further data may
// be sent (Send returns ErrStreamReset). It is idempotent and a no-op once the
// stream's FIN has been sent — the peer already has the whole request.
func (s *Stream) Reset(errCode uint64) error {
	if s.finSent || s.sendReset {
		return nil
	}
	if s.conn.oneRTTSealer == nil {
		return ErrNotEstablished
	}
	s.sendReset = true
	// finalSize is the number of bytes already sent (§19.4). Retransmitted until
	// acknowledged (§13.3) via the retrans descriptor.
	rf := retransFrame{kind: retransReset, streamID: s.id, errCode: errCode, offset: s.sendOffset}
	return s.conn.writeAppFrames(AppendResetStream(nil, s.id, errCode, s.sendOffset), []retransFrame{rf})
}

// StopSending requests that the peer abort the send side of the stream (its data
// flow toward us) with an application error code, sending a STOP_SENDING frame
// (RFC 9000 §3.5, §19.5). Used to abandon a response the caller no longer wants,
// so the peer stops filling our receive buffer. Idempotent.
func (s *Stream) StopSending(errCode uint64) error {
	if s.recvReset {
		return nil
	}
	if s.conn.oneRTTSealer == nil {
		return ErrNotEstablished
	}
	s.recvReset = true
	s.conn.maybeRetire(s) // receive side terminal; if our FIN is also sent, drop from the map
	// Retransmitted until acknowledged (§13.3): it has no self-healing successor.
	rf := retransFrame{kind: retransStopSending, streamID: s.id, errCode: errCode}
	return s.conn.writeAppFrames(AppendStopSending(nil, s.id, errCode), []retransFrame{rf})
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
	readOff   int           // bytes of data already returned by read
	highest   uint64        // highest byte offset received (offset+len), for §4.5 checks
	fin       bool
	finalSize uint64
}

// receive incorporates a STREAM frame's data at the given stream offset. fin
// marks that this frame carries the last byte (the final size is offset+len). It
// returns ErrFinalSize if the frame violates the stream's final size (RFC 9000
// §4.5): data past a declared final size, a conflicting FIN, or a FIN below data
// already received.
func (r *recvStream) receive(offset uint64, data []byte, fin bool) error {
	end := offset + uint64(len(data))
	if r.fin && (end > r.finalSize || (fin && end != r.finalSize)) {
		return ErrFinalSize // beyond, or inconsistent with, a fixed final size
	}
	if fin {
		if end < r.highest {
			return ErrFinalSize // a FIN below data already received
		}
		r.fin, r.finalSize = true, end
	}
	if end > r.highest {
		r.highest = end
	}
	r.insert(offset, data)
	return nil
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
	r.bufferGap(offset, data)
}

// bufferGap stores [offset, offset+len(data)) among the out-of-order chunks,
// merging it with any chunk it overlaps or abuts so pending stays sorted by
// offset and non-overlapping — one chunk per gap, no byte buffered twice. A peer
// that resends the same gapped frame, or overlapping gapped frames, therefore
// cannot inflate pending past the receive-flow-control window: without this a
// duplicate append (recvHighest does not advance, so flow control never rejects
// it) grew the buffer without bound. Overlapping input yields to bytes already
// held, which a conformant peer guarantees are identical (RFC 9000 §2.2). The
// merge is a single ordered pass, replacing the previous append-then-resort
// (O(n log n) per frame, quadratic against a peer sending many tiny gaps).
func (r *recvStream) bufferGap(offset uint64, data []byte) {
	lo, hi := offset, offset+uint64(len(data))
	out := make([]streamChunk, 0, len(r.pending)+1)
	placed := false
	for _, c := range r.pending {
		cLo, cHi := c.offset, c.offset+uint64(len(c.data))
		if cHi < lo || cLo > hi { // disjoint from the (possibly grown) new interval
			if cLo > hi && !placed { // first chunk beyond the new one: emit it in order
				out = append(out, streamChunk{lo, append([]byte(nil), data...)})
				placed = true
			}
			out = append(out, c)
			continue
		}
		// Overlap or adjacency: fuse c into the new interval, keeping already-held
		// bytes on any overlap.
		nLo, nHi := min(lo, cLo), max(hi, cHi)
		buf := make([]byte, nHi-nLo)
		copy(buf[lo-nLo:], data)   // new bytes first
		copy(buf[cLo-nLo:], c.data) // existing bytes win on overlap
		lo, hi, data = nLo, nHi, buf
	}
	if !placed {
		out = append(out, streamChunk{lo, append([]byte(nil), data...)})
	}
	r.pending = out
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

// read returns the contiguous bytes not yet returned by a prior read, advancing
// the read cursor to the end of the contiguous prefix.
func (r *recvStream) read() []byte {
	d := r.data[r.readOff:]
	r.readOff = len(r.data)
	return d
}

// complete reports whether the whole stream has been received: the FIN has
// arrived and every byte up to the final size is contiguous.
func (r *recvStream) complete() bool {
	return r.fin && uint64(len(r.data)) == r.finalSize && len(r.pending) == 0
}
