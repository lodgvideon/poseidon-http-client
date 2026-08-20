// Package trace holds the wire-observation vocabulary the HTTP versions share.
//
// It exists for the same reason header does: the thing being described — a
// frame crossing the wire, in a direction, with a length — is not specific to
// any one RFC, and the alternative was for http1 to import frame (RFC 7540) to
// borrow a Direction constant. Nothing here reads or writes a byte; the
// protocol packages own their own wire formats and name their own frame types,
// and this package owns only the shape they report through and one built-in
// renderer for it.
//
// A Tracer is set on a protocol package's options struct — conn.ConnOptions
// for HTTP/2 — and fires for every frame in both directions. The cost when no
// tracer is set is one nil check per frame; see the doc on Tracer for what an
// implementation must and must not do.
package trace

// Direction says which way a frame crossed the wire, from this endpoint's
// point of view.
type Direction uint8

const (
	// DirIn marks a frame this endpoint received.
	DirIn Direction = iota
	// DirOut marks a frame this endpoint sent.
	DirOut
)

// String renders the direction as the arrow used in frame logs: "<-" for
// inbound, "->" for outbound.
func (d Direction) String() string {
	if d == DirOut {
		return "->"
	}
	return "<-"
}

// Protocol identifies the wire protocol that produced an event, so one Tracer
// can serve a client speaking several at once.
type Protocol uint8

const (
	// ProtoH1 is HTTP/1.1 (RFC 9112).
	ProtoH1 Protocol = iota
	// ProtoH2 is HTTP/2 (RFC 9113).
	ProtoH2
	// ProtoH3 is HTTP/3 (RFC 9114).
	ProtoH3
	// ProtoQUIC is the QUIC transport beneath HTTP/3 (RFC 9000).
	ProtoQUIC
	// ProtoUnknown is for a report about an exchange whose protocol was never
	// settled — an ALPN transport whose dial failed before the handshake, say.
	//
	// It is appended rather than made the zero value, which is what it would be
	// in a vacuum: ProtoH1 has been 0 since this type existed, and renumbering
	// it would silently relabel every stored or transmitted value. The
	// consequence is that a zero Protocol still reads as h1, so a producer of
	// these has to set the field deliberately on every path — see
	// client.exchangeStats.
	ProtoUnknown
)

// String renders the protocol as its ALPN-style short name.
func (p Protocol) String() string {
	switch p {
	case ProtoH1:
		return "h1"
	case ProtoH2:
		return "h2"
	case ProtoH3:
		return "h3"
	case ProtoQUIC:
		return "quic"
	case ProtoUnknown:
		return "?"
	default:
		return "?"
	}
}

// Detail is a bitmask of which type-specific fields of a FrameInfo the emitter
// actually filled in.
//
// It is not redundant with checking those fields against zero: zero is a
// meaningful value for most of them. A GOAWAY carrying NO_ERROR (0x0) and a
// GOAWAY whose code the emitter did not decode are the same bytes in
// FrameInfo.ErrCode and completely different events — losing that distinction
// is the shape of the bug in #570.
type Detail uint16

const (
	// DetailErrCode marks FrameInfo.ErrCode and ErrCodeName as filled.
	DetailErrCode Detail = 1 << iota
	// DetailLastStreamID marks FrameInfo.LastStreamID as filled.
	DetailLastStreamID
	// DetailIncrement marks FrameInfo.Increment as filled.
	DetailIncrement
	// DetailPromisedID marks FrameInfo.PromisedID as filled.
	DetailPromisedID
	// DetailParams marks FrameInfo.Params as filled.
	DetailParams
)

// Has reports whether every bit in want is set.
func (d Detail) Has(want Detail) bool { return d&want == want }

// UnknownName is what a protocol package returns from its naming helpers for a
// code it does not define. Renderers treat it as "print the number instead" —
// RFC 9113 §5.5 obliges an endpoint to ignore frame types it does not know, so
// seeing one is normal, not an error, and the number is the useful part.
const UnknownName = "UNKNOWN"

// MaxParams bounds Params. HTTP/2 defines seven SETTINGS identifiers and
// HTTP/3 three; the ceiling is per-identifier, not per-wire-occurrence, because
// both RFCs make the last value of a repeated identifier the only one that
// counts.
const MaxParams = 16

// Param is one decoded settings parameter. Name is the identifier's spelling
// when the emitter recognises it and empty when it does not; ID carries the
// wire value either way.
type Param struct {
	ID    uint64
	Name  string
	Value uint64
}

// Params is a fixed-capacity carrier for the parameters of a settings-like
// frame.
//
// It is an array rather than a slice so that filling one costs no allocation:
// the emitter keeps a Params on its own already-heap-resident state and hands
// out a pointer to it, which is why FrameInfo.Params must not be retained past
// the TraceFrame call.
type Params struct {
	P [MaxParams]Param
	N int
}

// Reset empties p for reuse without releasing its backing array.
func (p *Params) Reset() { p.N = 0 }

// Add appends one parameter, silently dropping it once MaxParams are held. The
// drop is deliberate: a debug path must not panic on a peer that sends more
// identifiers than this implementation knows about.
func (p *Params) Add(id uint64, name string, value uint64) {
	if p.N >= MaxParams {
		return
	}
	p.P[p.N] = Param{ID: id, Name: name, Value: value}
	p.N++
}

// All returns the live prefix of p. The result aliases p and is valid only for
// as long as p is.
func (p *Params) All() []Param { return p.P[:p.N] }

// FrameInfo describes one frame crossing the wire.
//
// The first block of fields applies to every protocol. The second is
// type-specific and meaningful only where Detail says the emitter filled it in.
//
// Naming is the emitting package's job, not the Tracer's: TypeName, FlagNames
// and ErrCodeName arrive already resolved so that a renderer needs no table of
// another RFC's constants, and so that resolving them costs no allocation
// (every one is a compile-time constant string or the empty string).
//
// Header field values and DATA payloads are deliberately absent. A frame log is
// the thing people paste into a public issue, and authorization and cookie live
// in the header block; the framing layer sees only compressed bytes anyway.
type FrameInfo struct {
	Proto Protocol
	Dir   Direction

	// Type is the wire type code, and TypeName its spelling ("HEADERS") or
	// "UNKNOWN" for a type the emitter does not define.
	Type     uint64
	TypeName string

	// Flags is the raw flag bits, and FlagNames their spelling joined by "|"
	// ("END_STREAM|END_HEADERS"), empty when no defined flag is set.
	Flags     uint64
	FlagNames string

	// StreamID is 0 for connection-level frames. Length is the payload length
	// as it appears in the frame header, excluding that header.
	StreamID uint64
	Length   uint32

	// Detail says which of the fields below the emitter filled in.
	Detail Detail

	// ErrCode is an RST_STREAM or GOAWAY error code, ErrCodeName its spelling.
	ErrCode     uint64
	ErrCodeName string
	// LastStreamID is a GOAWAY's last-processed stream.
	LastStreamID uint64
	// Increment is a WINDOW_UPDATE's flow-control credit.
	Increment uint64
	// PromisedID is a PUSH_PROMISE's promised stream.
	PromisedID uint64

	// Params holds a settings frame's decoded parameters. It points at storage
	// the emitter reuses: valid only for the duration of the TraceFrame call,
	// like every slice in this codebase's frame handlers. Copy to retain.
	Params *Params
}

// Tracer observes frames as they cross the wire. It is the seam below
// client.Hooks: hooks describe requests, a Tracer describes framing.
//
// Implementations MUST NOT block. TraceFrame fires on the connection's reader
// goroutine and, outbound, while the connection's write lock is held, so a slow
// tracer does not merely lag — it stalls the connection it is observing.
// Buffer, or drop, but do not wait. TextTracer does both.
//
// Implementations MUST NOT retain FrameInfo.Params past the call; it aliases
// storage the emitter reuses for the next frame. Every other field is a scalar
// or a constant string and is safe to keep.
//
// TraceFrame may be called concurrently: a connection's reader and writer are
// different goroutines, and one Tracer is typically shared by every connection
// a client owns.
type Tracer interface {
	TraceFrame(FrameInfo)
}
