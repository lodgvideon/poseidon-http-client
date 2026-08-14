package frame

import "github.com/lodgvideon/poseidon-http-client/internal/bufx"

// Direction is which way a frame crossed the wire.
type Direction uint8

// Direction values.
const (
	// DirRecv marks a frame read off the wire by ReadFrame.
	DirRecv Direction = iota
	// DirSend marks a frame written to the wire by one of the Write methods.
	DirSend
)

// String returns "recv" or "send". Renderers that prefer arrows draw their own;
// this is the label a structured consumer wants.
func (d Direction) String() string {
	if d == DirSend {
		return "send"
	}
	return "recv"
}

// FrameInfo describes one frame crossing the wire. It is what a Tracer is
// handed, and it is a value struct rather than a set of arguments so that
// adding a field later is not an interface break.
//
// The decoded-detail fields below are populated only for the frame types that
// carry them and hold their zero value otherwise. They are not a second parser:
// on the read side they are filled straight from the wire bytes, independently
// of whether dispatch went on to accept the frame, which is the point — a frame
// that killed the connection is the one you most want to read.
//
// NOTHING IN HERE MAY BE RETAINED past the TraceFrame call. Payload aliases the
// Framer's read buffer and the struct itself is reused for the next frame; a
// tracer that keeps either watches it mutate. Copy what you need.
type FrameInfo struct {
	// Header is the 9-byte frame header (RFC 7540 §4.1) as it appeared on the
	// wire. Length is the wire length, padding and pad-length byte included.
	Header FrameHeader

	// Dir is the direction the frame moved.
	Dir Direction

	// ErrCode is the code carried by RST_STREAM (§6.4) and GOAWAY (§6.8).
	ErrCode ErrCode

	// LastStreamID is GOAWAY's last-stream-id (§6.8).
	LastStreamID uint32

	// PromisedID is PUSH_PROMISE's promised stream id (§6.6).
	PromisedID uint32

	// WindowIncrement is WINDOW_UPDATE's flow-control increment (§6.9).
	WindowIncrement uint32

	// Ping is PING's eight opaque bytes (§6.7).
	Ping [8]byte

	// Settings holds the parameters of a SETTINGS frame (§6.5). N is zero for a
	// SETTINGS ACK and for every other frame type. Unlike the decoder's own
	// view, unknown identifiers are kept: a GREASE setting (RFC 8701) the codec
	// drops is still something a trace should show. The store holds 16 distinct
	// identifiers and a frame carrying more is truncated there — §6.5 puts no
	// bound on the count, and a trace must not grow a heap allocation because a
	// peer sent a long SETTINGS.
	Settings SettingsParams

	// Payload is the frame payload exactly as it came off the wire — padding,
	// pad-length byte and header-block fragment included, nothing stripped.
	//
	// It is set on DirRecv only. On the send side the frame is assembled from
	// the caller's own buffers, sometimes several of them (WriteDataV), and a
	// field that were sometimes-nil-sometimes-not would be worse than absent.
	//
	// IT MAY CONTAIN SECRETS. A DATA payload is the request or response body and
	// a HEADERS fragment is an HPACK-coded field block that includes
	// `authorization` and `cookie`. The built-in text tracer therefore prints
	// nothing from here unless explicitly asked; anything else that reads it is
	// making the same choice on purpose.
	Payload []byte
}

// Tracer observes every frame a Framer reads or writes. Install one with
// Framer.SetTracer; the nil Tracer (the default) costs one nil compare per
// frame and nothing else.
//
// Three rules, all of them load-bearing:
//
//   - MUST NOT BLOCK. TraceFrame fires on the connection's reader goroutine on
//     the way in, and on the write side while the connection's write lock is
//     held. A tracer that waits on a channel or a slow io.Writer stalls the
//     whole connection, not one stream. Buffer, and drop rather than wait.
//   - MUST NOT RETAIN fi, or any slice reachable from it, past the call. The
//     Framer reuses one FrameInfo per direction and Payload aliases its read
//     buffer. This is the same contract Handler already states.
//   - MUST BE SAFE FOR CONCURRENT USE. The read side and the write side are
//     different goroutines and are not serialised against each other; a single
//     Tracer sees both.
//
// Panics propagate into the connection, exactly as Handler's do.
type Tracer interface {
	TraceFrame(fi *FrameInfo)
}

// SetTracer installs t as the observer of every frame this Framer reads or
// writes; nil disables tracing.
//
// Call it before the Framer is in use. It is a plain field store — cheap, but
// not atomic — so swapping a tracer while a connection is running races with
// the reader goroutine. A tracer that needs to be switched on and off at
// runtime should stay installed and gate itself.
func (f *Framer) SetTracer(t Tracer) { f.tracer = t }

// clearDetail resets the decoded-detail fields between frames so that, say, the
// error code of a GOAWAY cannot survive into the next PING's event.
func (fi *FrameInfo) clearDetail() {
	fi.ErrCode = 0
	fi.LastStreamID = 0
	fi.PromisedID = 0
	fi.WindowIncrement = 0
	fi.Ping = [8]byte{}
	fi.Settings = SettingsParams{}
}

// emitRecv reports a frame just read off the wire. Caller has checked f.tracer.
//
// It fires BEFORE dispatch and before the §6.10 field-block continuity check,
// so the trace contains the frame that caused a teardown as well as every frame
// the codec ignores outright (RFC 7540 §5.5 unknown types) — both are invisible
// from the Handler side, and both are what a bug report needs.
func (f *Framer) emitRecv(fh FrameHeader, payload []byte) {
	fi := &f.traceIn
	fi.Dir = DirRecv
	fi.Header = fh
	fi.clearDetail()
	fi.Payload = payload
	fillRecvDetail(fi, payload)
	f.tracer.TraceFrame(fi)
	// Drop the alias so a tracer that ignored the retention rule sees nil rather
	// than a buffer being overwritten under it.
	fi.Payload = nil
}

// emitSend reports a frame on its way out, using whatever decoded detail the
// calling Write method staged into f.traceOut, and clears that staging.
// Caller has checked f.tracer.
func (f *Framer) emitSend(h FrameHeader) {
	fi := &f.traceOut
	fi.Dir = DirSend
	fi.Header = h
	fi.Payload = nil
	f.tracer.TraceFrame(fi)
	fi.clearDetail()
}

// be32 reads a big-endian uint32. The dispatch path open-codes this shift at
// four sites; the trace path is not hot enough to earn a fifth copy.
func be32(b []byte) uint32 {
	_ = b[3] // BCE hint
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// fillRecvDetail decodes the scalar fields a human reading a trace wants, from
// the raw payload.
//
// Every read is length-guarded and no violation is reported, because this is a
// renderer and not a validator: a RST_STREAM with a three-byte payload is a
// FRAME_SIZE_ERROR that dispatchRSTStream will raise a moment later, and the
// job here is to show what arrived, not to have an opinion about it. A short
// frame simply leaves the field at zero.
func fillRecvDetail(fi *FrameInfo, payload []byte) {
	//exhaustive:ignore // Only the frame types with scalar detail worth naming
	// appear here. DATA, HEADERS, CONTINUATION, PRIORITY, ALTSVC and ORIGIN
	// carry variable-length bodies that Payload already exposes verbatim.
	switch fi.Header.Type {
	case FrameRSTStream:
		if len(payload) >= 4 {
			fi.ErrCode = ErrCode(be32(payload))
		}
	case FrameGoAway:
		if len(payload) >= 8 {
			fi.LastStreamID = bufx.ReadUint31(payload[:4])
			fi.ErrCode = ErrCode(be32(payload[4:8]))
		}
	case FrameWindowUpdate:
		if len(payload) >= 4 {
			fi.WindowIncrement = bufx.ReadUint31(payload[:4])
		}
	case FramePing:
		if len(payload) >= 8 {
			copy(fi.Ping[:], payload[:8])
		}
	case FramePushPromise:
		body := payload
		if fi.Header.Flags&FlagPushPromisePadded != 0 {
			if len(body) < 1 {
				return
			}
			body = body[1:]
		}
		if len(body) >= 4 {
			fi.PromisedID = bufx.ReadUint31(body[:4])
		}
	case FrameSettings:
		if fi.Header.Flags&FlagSettingsAck != 0 {
			return
		}
		for i := 0; i+6 <= len(payload) && fi.Settings.N < maxSettingsPairs; i += 6 {
			fi.Settings.set(
				SettingID(uint16(payload[i])<<8|uint16(payload[i+1])),
				be32(payload[i+2:i+6]),
			)
		}
	}
}
