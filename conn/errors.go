package conn

import "errors"

// Sentinel errors. Hot-path code MUST NOT use fmt.Errorf — only these.
var (
	ErrConnClosed       = errors.New("poseidon/conn: connection closed")
	ErrGoAwayReceived   = errors.New("poseidon/conn: peer sent GOAWAY")
	ErrStreamReset      = errors.New("poseidon/conn: stream reset by peer")
	ErrStreamMaxStreams = errors.New("poseidon/conn: peer SETTINGS_MAX_CONCURRENT_STREAMS reached")
	ErrInvalidState     = errors.New("poseidon/conn: operation invalid in current stream state")
	ErrPushUnsupported  = errors.New("poseidon/conn: server push disabled")
	ErrTLSNoH2          = errors.New("poseidon/conn: TLS ALPN did not negotiate h2")
)
