package quic

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"hash"
)

// hashForSuite returns the KDF hash constructor and AEAD key length for a cipher
// suite (RFC 9001 §5.1). AES-GCM suites are supported; others (ChaCha20-Poly1305)
// return ErrCryptoSuite. It is the single source of truth for the suite→hash/len
// mapping used by both key derivation and the "quic ku" key-update ratchet.
func hashForSuite(suite uint16) (func() hash.Hash, int, error) {
	switch suite {
	case tls.TLS_AES_128_GCM_SHA256:
		return sha256.New, 16, nil
	case tls.TLS_AES_256_GCM_SHA384:
		return sha512.New384, 32, nil
	default:
		return nil, 0, ErrCryptoSuite
	}
}

// KeysFromSecret derives packet-protection keys (key/iv/hp) from a TLS traffic
// secret and the negotiated cipher suite (RFC 9001 §5.1). The suite selects the
// KDF hash and key length. AES-GCM suites are supported; ChaCha20-Poly1305 is
// deferred (its header protection needs a non-stdlib primitive) and returns
// ErrCryptoSuite.
func KeysFromSecret(suite uint16, secret []byte) (PacketKeys, error) {
	h, keyLen, err := hashForSuite(suite)
	if err != nil {
		return PacketKeys{}, err
	}
	return deriveKeys(h, secret, keyLen), nil
}

// HandshakeSink receives TLS handshake progress from a TLSHandshake pump. The
// byte slices passed in alias crypto/tls-owned memory valid only until the next
// pump step — copy to retain. Any non-nil error aborts the pump.
type HandshakeSink interface {
	// WriteCrypto is called with handshake bytes to send in CRYPTO frames at
	// the given encryption level.
	WriteCrypto(level tls.QUICEncryptionLevel, data []byte) error
	// SetReadKeys / SetWriteKeys deliver the traffic secret and cipher suite for
	// a level so the caller can install packet-protection keys.
	SetReadKeys(level tls.QUICEncryptionLevel, suite uint16, secret []byte) error
	SetWriteKeys(level tls.QUICEncryptionLevel, suite uint16, secret []byte) error
	// PeerTransportParameters delivers the peer's raw QUIC transport parameters.
	PeerTransportParameters(params []byte) error
	// HandshakeComplete is called once the TLS handshake finishes.
	HandshakeComplete() error
}

// TLSHandshake drives the TLS 1.3 handshake for a QUIC connection via
// crypto/tls.QUICConn (RFC 9001 §4). The transport engine feeds it inbound
// CRYPTO bytes and pumps events into its own HandshakeSink; crypto/tls owns the
// TLS state machine and surfaces the secrets and handshake bytes QUIC needs.
type TLSHandshake struct {
	conn *tls.QUICConn
	tp   []byte
}

// NewClientHandshake creates a client-side QUIC TLS handshake. cfg must set
// ServerName; it is cloned and forced to TLS 1.3 with ALPN "h3" when unset. tp
// is the client's serialized QUIC transport parameters (RFC 9000 §7.4).
func NewClientHandshake(cfg *tls.Config, tp []byte) *TLSHandshake {
	c := cfg.Clone()
	c.MinVersion = tls.VersionTLS13
	if len(c.NextProtos) == 0 {
		c.NextProtos = []string{"h3"}
	}
	return &TLSHandshake{conn: tls.QUICClient(&tls.QUICConfig{TLSConfig: c}), tp: tp}
}

// NewServerHandshake creates a server-side QUIC TLS handshake. cfg must carry
// the server's certificate(s); it is cloned and forced to TLS 1.3, with ALPN
// "h3" filled in when unset. tp is the server's serialized QUIC transport
// parameters (RFC 9000 §7.4), which crypto/tls emits in the EncryptedExtensions.
// It is the server-role counterpart to NewClientHandshake: the transport engine
// feeds it the client's Initial CRYPTO and pumps events the same way.
func NewServerHandshake(cfg *tls.Config, tp []byte) *TLSHandshake {
	c := cfg.Clone()
	c.MinVersion = tls.VersionTLS13
	if len(c.NextProtos) == 0 {
		c.NextProtos = []string{"h3"}
	}
	return &TLSHandshake{conn: tls.QUICServer(&tls.QUICConfig{TLSConfig: c}), tp: tp}
}

// Start begins the handshake (the client generates its ClientHello). Pump must
// be called afterwards to drain the resulting events.
func (h *TLSHandshake) Start(ctx context.Context) error { return h.conn.Start(ctx) }

// HandleCrypto feeds inbound CRYPTO-frame bytes for an encryption level into the
// TLS state machine (RFC 9001 §4.1.3).
func (h *TLSHandshake) HandleCrypto(level tls.QUICEncryptionLevel, data []byte) error {
	return h.conn.HandleData(level, data)
}

// Pump drains all currently-available TLS events into sink, installing this
// side's transport parameters when TLS asks for them.
func (h *TLSHandshake) Pump(sink HandshakeSink) error {
	for {
		ev := h.conn.NextEvent()
		switch ev.Kind {
		case tls.QUICNoEvent:
			return nil
		case tls.QUICTransportParametersRequired:
			h.conn.SetTransportParameters(h.tp)
		case tls.QUICTransportParameters:
			if err := sink.PeerTransportParameters(ev.Data); err != nil {
				return err
			}
		case tls.QUICWriteData:
			if err := sink.WriteCrypto(ev.Level, ev.Data); err != nil {
				return err
			}
		case tls.QUICSetReadSecret:
			if err := sink.SetReadKeys(ev.Level, ev.Suite, ev.Data); err != nil {
				return err
			}
		case tls.QUICSetWriteSecret:
			if err := sink.SetWriteKeys(ev.Level, ev.Suite, ev.Data); err != nil {
				return err
			}
		case tls.QUICHandshakeDone:
			if err := sink.HandshakeComplete(); err != nil {
				return err
			}
		}
	}
}

// ConnectionState returns the TLS connection state (ALPN, cipher suite, peer
// certificates) once the handshake has progressed enough to populate it.
func (h *TLSHandshake) ConnectionState() tls.ConnectionState { return h.conn.ConnectionState() }

// Close releases the TLS handshake state.
func (h *TLSHandshake) Close() error { return h.conn.Close() }
