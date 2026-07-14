package quic

import (
	"time"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

// Transport-parameter defaults (RFC 9000 §18.2) for values absent from the
// peer's parameters.
const (
	defaultAckDelayExponent uint64        = 3
	defaultMaxAckDelay      time.Duration = 25 * time.Millisecond
	// maxIdleTimeoutMillis caps a peer's max_idle_timeout so scaling it to a
	// nanosecond Duration cannot overflow int64 (RFC 9000 §18.2 sets no upper
	// bound). It is math.MaxInt64 / 1e6 ≈ 292 years — effectively unbounded.
	maxIdleTimeoutMillis uint64 = uint64(1<<63-1) / uint64(time.Millisecond)
)

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
	// InitialMaxStreamDataBidiLocal is the per-stream receive limit the peer
	// applies to bidirectional streams the peer itself initiates (0x05). A server
	// uses it as its send limit on a client-initiated request stream. The client
	// send path does not consume it; it is retained for the server role.
	InitialMaxStreamDataBidiLocal uint64
	// InitialMaxStreamsBidi is the maximum number of bidirectional streams the
	// client is permitted to open (0x08).
	InitialMaxStreamsBidi uint64
	// InitialMaxStreamDataUni is the per-stream send limit for unidirectional
	// streams the client opens — the HTTP/3 control and QPACK streams (0x07).
	InitialMaxStreamDataUni uint64
	// InitialMaxStreamsUni is the maximum number of unidirectional streams the
	// client is permitted to open (0x09).
	InitialMaxStreamsUni uint64
	// InitialSourceConnectionID is the server's source connection ID as echoed in
	// its transport parameters (0x0f). The client authenticates it against the
	// server's actual SCID (§7.3). HaveInitialSourceConnectionID records whether
	// the parameter was present, so an absent one (a §7.3 error) is distinguished
	// from a zero-length connection ID.
	InitialSourceConnectionID     []byte
	HaveInitialSourceConnectionID bool
	// OriginalDestinationConnectionID (0x00) is the Destination Connection ID of the
	// client's first Initial; the client authenticates it against the value it chose
	// (§7.3). RetrySourceConnectionID (0x10) must be present exactly when a Retry was
	// received and must equal that Retry's Source Connection ID (§7.3). The Have*
	// flags distinguish an absent parameter (a §7.3 error) from a zero-length CID.
	OriginalDestinationConnectionID     []byte
	HaveOriginalDestinationConnectionID bool
	RetrySourceConnectionID             []byte
	HaveRetrySourceConnectionID         bool
	// AckDelayExponent is the peer's ack_delay_exponent (0x0a): the ACK Delay
	// field the peer sends is scaled by 2^AckDelayExponent microseconds. Default 3.
	AckDelayExponent uint64
	// MaxAckDelay is the peer's max_ack_delay (0x0b): the longest it will delay an
	// acknowledgement. It bounds the ack delay used in RTT estimation and feeds the
	// PTO. Default 25 ms.
	MaxAckDelay time.Duration
	// MaxIdleTimeout is the peer's max_idle_timeout (0x01): after this much idle
	// time the peer will close the connection. The effective idle timeout is the
	// minimum of the two endpoints' non-zero values (§10.1). Zero (absent or
	// explicit) means the peer imposes no idle timeout.
	MaxIdleTimeout time.Duration
	// StatelessResetToken is the peer's stateless_reset_token (0x02), the 16-byte
	// token tied to the connection ID it used during the handshake. A datagram
	// ending in it signals a stateless reset (§10.3). HaveStatelessResetToken
	// records whether the parameter was present.
	StatelessResetToken     [16]byte
	HaveStatelessResetToken bool
}

// Transport-parameter identifiers the parser dispatches on (RFC 9000 §18.2).
const (
	tpMaxIdleTimeout                  uint64 = 0x01
	tpStatelessResetToken             uint64 = 0x02
	tpMaxUDPPayloadSize               uint64 = 0x03
	tpInitialMaxData                  uint64 = 0x04
	tpInitialMaxStreamDataBidiLocal   uint64 = 0x05
	tpInitialMaxStreamDataBidiRemote  uint64 = 0x06
	tpInitialMaxStreamDataUni         uint64 = 0x07
	tpInitialMaxStreamsBidi           uint64 = 0x08
	tpInitialMaxStreamsUni            uint64 = 0x09
	tpAckDelayExponent                uint64 = 0x0a
	tpMaxAckDelay                     uint64 = 0x0b
	tpActiveConnectionIDLimit         uint64 = 0x0e
	tpInitialSourceConnectionID       uint64 = 0x0f
	tpOriginalDestinationConnectionID uint64 = 0x00
	tpRetrySourceConnectionID         uint64 = 0x10
	tpPreferredAddress                uint64 = 0x0d
)

// preferredAddressFixedLen is the length of the fixed prefix of a preferred_address
// transport parameter (RFC 9000 §18.2): IPv4 address (4) + IPv4 port (2) + IPv6
// address (16) + IPv6 port (2), followed by a 1-byte Connection ID Length.
const preferredAddressFixedLen = 24

// maxConnIDLen is the largest connection ID a QUIC v1 endpoint may use (RFC 9000
// §17.2); a preferred_address connection ID beyond it is malformed.
const maxConnIDLen = 20

// ParseTransportParams decodes the peer's transport parameters (RFC 9000 §18).
// Each parameter is an identifier, a length, and that many value bytes; the
// integer-valued parameters encode their value as one QUIC varint whose encoded
// length must equal the declared length. A duplicate identifier, a truncated or
// over-long field, a malformed integer encoding, or an invalid value is a
// TRANSPORT_PARAMETER_ERROR (§7.4). Unknown identifiers (including GREASE) are
// ignored.
func ParseTransportParams(raw []byte) (TransportParams, error) {
	tp := TransportParams{AckDelayExponent: defaultAckDelayExponent, MaxAckDelay: defaultMaxAckDelay}
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
// setStreamData handles the connection- and stream-level flow-control and
// stream-count transport parameters, which are all plain (bounded) varints. It
// reports whether id named one of them, so set can dispatch the rest.
func (tp *TransportParams) setStreamData(id uint64, value []byte) (handled bool, err error) {
	switch id {
	case tpInitialMaxData:
		return true, tp.setUint(value, &tp.InitialMaxData)
	case tpInitialMaxStreamDataBidiRemote:
		return true, tp.setUint(value, &tp.InitialMaxStreamDataBidiRemote)
	case tpInitialMaxStreamDataBidiLocal:
		return true, tp.setUint(value, &tp.InitialMaxStreamDataBidiLocal)
	case tpInitialMaxStreamDataUni:
		return true, tp.setUint(value, &tp.InitialMaxStreamDataUni)
	case tpInitialMaxStreamsBidi:
		// A max_streams value greater than 2^60 is invalid (RFC 9000 §4.6): it
		// implies a stream ID that cannot be expressed as a QUIC varint.
		return true, tp.setBoundedUint(value, &tp.InitialMaxStreamsBidi, maxStreamsLimit)
	case tpInitialMaxStreamsUni:
		return true, tp.setBoundedUint(value, &tp.InitialMaxStreamsUni, maxStreamsLimit)
	}
	return false, nil
}

func (tp *TransportParams) set(id uint64, value []byte) error { //nolint:gocyclo // flat dispatch over transport-parameter IDs — dense by count, not branching logic
	if handled, err := tp.setStreamData(id, value); handled {
		return err
	}
	switch id {
	case tpMaxIdleTimeout:
		v, ok := tpReadUint(value)
		if !ok {
			return ErrTransportParameter
		}
		// The value is in milliseconds (§18.2); any value is valid, 0 disables it.
		// Cap before scaling to nanoseconds so an absurd value cannot overflow the
		// int64 Duration into a negative — such a value is effectively no timeout.
		if v > maxIdleTimeoutMillis {
			v = maxIdleTimeoutMillis
		}
		tp.MaxIdleTimeout = time.Duration(v) * time.Millisecond
	case tpStatelessResetToken:
		// A raw 16-byte token, not a varint (RFC 9000 §18.2).
		if len(value) != 16 {
			return ErrTransportParameter
		}
		copy(tp.StatelessResetToken[:], value)
		tp.HaveStatelessResetToken = true
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
	case tpAckDelayExponent:
		v, ok := tpReadUint(value)
		if !ok || v > 20 {
			return ErrTransportParameter // §18.2: values above 20 are invalid
		}
		tp.AckDelayExponent = v
	case tpMaxAckDelay:
		v, ok := tpReadUint(value)
		if !ok || v >= 1<<14 {
			return ErrTransportParameter // §18.2: values of 2^14 ms or greater are invalid
		}
		tp.MaxAckDelay = time.Duration(v) * time.Millisecond
	case tpInitialSourceConnectionID, tpOriginalDestinationConnectionID, tpRetrySourceConnectionID:
		// Raw connection-ID bytes (not a varint), authenticated in §7.3.
		tp.setConnectionID(id, value)
	case tpPreferredAddress:
		return validatePreferredAddress(value)
	}
	return nil
}

// validatePreferredAddress checks the structure of a preferred_address transport
// parameter (RFC 9000 §18.2). The client never migrates to the server's preferred
// address, so the address is not retained, but a malformed parameter — a length
// that does not match its embedded Connection ID Length, or a zero-length
// connection ID (which §9.6 forbids) — is a TRANSPORT_PARAMETER_ERROR.
func validatePreferredAddress(value []byte) error {
	if len(value) < preferredAddressFixedLen+1 {
		return ErrTransportParameter // too short to hold the Connection ID Length byte
	}
	cidLen := int(value[preferredAddressFixedLen])
	if cidLen == 0 || cidLen > maxConnIDLen {
		return ErrTransportParameter // §9.6: the connection ID MUST NOT be zero length
	}
	// Fixed prefix + length byte + CID + 16-byte stateless reset token.
	if len(value) != preferredAddressFixedLen+1+cidLen+16 {
		return ErrTransportParameter
	}
	return nil
}

// setConnectionID records one of the raw connection-ID transport parameters and
// marks it present (RFC 9000 §7.3, §18.2).
func (tp *TransportParams) setConnectionID(id uint64, value []byte) {
	cid := append([]byte(nil), value...)
	switch id {
	case tpInitialSourceConnectionID:
		tp.InitialSourceConnectionID, tp.HaveInitialSourceConnectionID = cid, true
	case tpOriginalDestinationConnectionID:
		tp.OriginalDestinationConnectionID, tp.HaveOriginalDestinationConnectionID = cid, true
	case tpRetrySourceConnectionID:
		tp.RetrySourceConnectionID, tp.HaveRetrySourceConnectionID = cid, true
	}
}

// setUint decodes an integer parameter value and stores it in dst, returning a
// TRANSPORT_PARAMETER_ERROR on a malformed encoding (RFC 9000 §18).
func (tp *TransportParams) setUint(value []byte, dst *uint64) error {
	v, ok := tpReadUint(value)
	if !ok {
		return ErrTransportParameter
	}
	*dst = v
	return nil
}

// setBoundedUint is setUint with an inclusive upper bound: a value above max is a
// TRANSPORT_PARAMETER_ERROR (RFC 9000 §7.4).
func (tp *TransportParams) setBoundedUint(value []byte, dst *uint64, max uint64) error {
	v, ok := tpReadUint(value)
	if !ok || v > max {
		return ErrTransportParameter
	}
	*dst = v
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
	// MaxIdleTimeout is the idle timeout the client advertises, in milliseconds
	// (0x01, §10.1). Zero omits the parameter, leaving the effective timeout to the
	// server's advertised value.
	MaxIdleTimeout uint64
}

// AppendTransportParams encodes the client's transport parameters (RFC 9000 §18)
// into the wire form carried in the TLS quic_transport_parameters extension.
func AppendTransportParams(dst []byte, p LocalTransportParams) []byte {
	if p.MaxIdleTimeout > 0 {
		dst = appendTPInt(dst, tpMaxIdleTimeout, p.MaxIdleTimeout) // §10.1, milliseconds
	}
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
