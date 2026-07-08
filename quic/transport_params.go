package quic

import "github.com/lodgvideon/poseidon-http-client/internal/bytesx"

// TransportParams holds the peer's QUIC transport parameters that a client needs
// to bound its own sending (RFC 9000 §18.2). Only the parameters the send path
// consumes are retained; others are parsed for validation but discarded.
type TransportParams struct {
	// InitialMaxData is the connection-level send limit: the maximum total
	// bytes the client may send in STREAM frames across all streams (0x04).
	InitialMaxData uint64
	// InitialMaxStreamDataBidiRemote is the per-stream send limit for streams
	// the client initiates — client-initiated bidirectional "request" streams
	// (0x06). The peer names the parameter from its own perspective, so
	// "remote" means opened by the receiver of the parameter (the client).
	InitialMaxStreamDataBidiRemote uint64
	// InitialMaxStreamsBidi is the maximum number of bidirectional streams the
	// client is permitted to open (0x08).
	InitialMaxStreamsBidi uint64
	// InitialMaxStreamDataUni is the per-stream send limit for unidirectional
	// streams the client opens — the HTTP/3 control and QPACK streams (0x07).
	InitialMaxStreamDataUni uint64
	// InitialMaxStreamsUni is the maximum number of unidirectional streams the
	// client is permitted to open (0x09).
	InitialMaxStreamsUni uint64
}

// Transport-parameter identifiers the parser dispatches on (RFC 9000 §18.2).
const (
	tpMaxUDPPayloadSize              uint64 = 0x03
	tpInitialMaxData                 uint64 = 0x04
	tpInitialMaxStreamDataBidiLocal  uint64 = 0x05
	tpInitialMaxStreamDataBidiRemote uint64 = 0x06
	tpInitialMaxStreamDataUni        uint64 = 0x07
	tpInitialMaxStreamsBidi          uint64 = 0x08
	tpInitialMaxStreamsUni           uint64 = 0x09
	tpActiveConnectionIDLimit        uint64 = 0x0e
	tpInitialSourceConnectionID      uint64 = 0x0f
)

// ParseTransportParams decodes the peer's transport parameters (RFC 9000 §18).
// Each parameter is an identifier, a length, and that many value bytes; the
// integer-valued parameters encode their value as one QUIC varint whose encoded
// length must equal the declared length. A duplicate identifier, a truncated or
// over-long field, a malformed integer encoding, or an invalid value is a
// TRANSPORT_PARAMETER_ERROR (§7.4). Unknown identifiers (including GREASE) are
// ignored.
func ParseTransportParams(raw []byte) (TransportParams, error) {
	var tp TransportParams
	seen := make(map[uint64]struct{})
	p := 0
	for p < len(raw) {
		id, n := bytesx.ReadVarint(raw[p:])
		if n == 0 {
			return tp, ErrTransportParameter
		}
		p += n
		length, n := bytesx.ReadVarint(raw[p:])
		if n == 0 {
			return tp, ErrTransportParameter
		}
		p += n
		if uint64(len(raw)-p) < length {
			return tp, ErrTransportParameter
		}
		value := raw[p : p+int(length)]
		p += int(length)
		if _, dup := seen[id]; dup {
			return tp, ErrTransportParameter
		}
		seen[id] = struct{}{}
		if err := tp.set(id, value); err != nil {
			return tp, err
		}
	}
	return tp, nil
}

// set stores or validates one parameter. Unhandled identifiers are ignored.
func (tp *TransportParams) set(id uint64, value []byte) error {
	switch id {
	case tpInitialMaxData:
		v, ok := tpReadUint(value)
		if !ok {
			return ErrTransportParameter
		}
		tp.InitialMaxData = v
	case tpInitialMaxStreamDataBidiRemote:
		v, ok := tpReadUint(value)
		if !ok {
			return ErrTransportParameter
		}
		tp.InitialMaxStreamDataBidiRemote = v
	case tpInitialMaxStreamsBidi:
		v, ok := tpReadUint(value)
		if !ok {
			return ErrTransportParameter
		}
		tp.InitialMaxStreamsBidi = v
	case tpInitialMaxStreamDataUni:
		v, ok := tpReadUint(value)
		if !ok {
			return ErrTransportParameter
		}
		tp.InitialMaxStreamDataUni = v
	case tpInitialMaxStreamsUni:
		v, ok := tpReadUint(value)
		if !ok {
			return ErrTransportParameter
		}
		tp.InitialMaxStreamsUni = v
	case tpMaxUDPPayloadSize:
		v, ok := tpReadUint(value)
		if !ok || v < 1200 {
			return ErrTransportParameter // §7.4: values below 1200 are invalid
		}
	case tpActiveConnectionIDLimit:
		v, ok := tpReadUint(value)
		if !ok || v < 2 {
			return ErrTransportParameter // §7.4: values below 2 are invalid
		}
	}
	return nil
}

// tpReadUint decodes an integer parameter value. The QUIC varint must consume
// exactly the whole value (RFC 9000 §18): a Length that disagrees with the
// varint's own length prefix is a malformed encoding.
func tpReadUint(value []byte) (uint64, bool) {
	v, n := bytesx.ReadVarint(value)
	if n == 0 || n != len(value) {
		return 0, false
	}
	return v, true
}

// LocalTransportParams are the transport parameters a client advertises to the
// server (RFC 9000 §18.2, client role). They tell the server the limits within
// which it may send — most importantly the credit for the response, which
// arrives on a client-initiated bidirectional stream and so is governed by the
// client's own initial_max_stream_data_bidi_local (0x05).
type LocalTransportParams struct {
	// InitialMaxData is the connection-level receive limit (0x04).
	InitialMaxData uint64
	// InitialMaxStreamDataBidiLocal is the per-stream receive limit for streams
	// the client opens — the response body arrives here (0x05).
	InitialMaxStreamDataBidiLocal uint64
	// InitialMaxStreamDataUni is the per-stream receive limit for server-opened
	// unidirectional streams — the server control and QPACK streams (0x07).
	InitialMaxStreamDataUni uint64
	// InitialMaxStreamsUni is how many unidirectional streams the server may
	// open (0x09); HTTP/3 needs at least three (control + QPACK encoder/decoder).
	InitialMaxStreamsUni uint64
	// SourceConnectionID is the client's chosen source connection ID (0x0f),
	// which the server verifies (§7.3). It may be zero-length.
	SourceConnectionID []byte
}

// AppendTransportParams encodes the client's transport parameters (RFC 9000 §18)
// into the wire form carried in the TLS quic_transport_parameters extension.
func AppendTransportParams(dst []byte, p LocalTransportParams) []byte {
	dst = appendTPInt(dst, tpInitialMaxData, p.InitialMaxData)
	dst = appendTPInt(dst, tpInitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiLocal)
	dst = appendTPInt(dst, tpInitialMaxStreamDataUni, p.InitialMaxStreamDataUni)
	dst = appendTPInt(dst, tpInitialMaxStreamsUni, p.InitialMaxStreamsUni)
	// initial_source_connection_id carries raw CID bytes, not a varint.
	dst = appendV(dst, tpInitialSourceConnectionID)
	dst = appendV(dst, uint64(len(p.SourceConnectionID)))
	return append(dst, p.SourceConnectionID...)
}

// appendTPInt encodes one integer transport parameter (id, length, varint value).
func appendTPInt(dst []byte, id, v uint64) []byte {
	dst = appendV(dst, id)
	dst = appendV(dst, uint64(bytesx.VarintLen(v)))
	return appendV(dst, v)
}
