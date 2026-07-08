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
	tpInitialMaxStreamDataBidiRemote uint64 = 0x06
	tpInitialMaxStreamDataUni        uint64 = 0x07
	tpInitialMaxStreamsBidi          uint64 = 0x08
	tpInitialMaxStreamsUni           uint64 = 0x09
	tpActiveConnectionIDLimit        uint64 = 0x0e
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
