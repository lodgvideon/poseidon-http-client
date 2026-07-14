package quic

import (
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/tls"
	"hash"
)

// quicV1InitialSalt is the RFC 9001 §5.2 version-1 salt used to derive Initial
// secrets from the client's Destination Connection ID.
var quicV1InitialSalt = []byte{
	0x38, 0x76, 0x2c, 0xf7, 0xf5, 0x59, 0x34, 0xb3, 0x4d, 0x17,
	0x9a, 0xe6, 0xa4, 0xc8, 0x0c, 0xad, 0xcc, 0xbb, 0x7f, 0x0a,
}

// PacketKeys holds the packet-protection material derived from one traffic
// secret (RFC 9001 §5.1): the AEAD key, the 12-byte IV, and the
// header-protection key. Suite is the negotiated TLS cipher suite the keys are
// for; it selects the AEAD and header-protection algorithm when a Sealer/Opener
// is built (a zero Suite is treated as AEAD_AES_128_GCM for hand-built keys).
type PacketKeys struct {
	Key   []byte
	IV    []byte
	HP    []byte
	Suite uint16
}

// hkdfExpandLabel implements HKDF-Expand-Label (RFC 8446 §7.1) with the TLS 1.3
// "tls13 " label prefix and an empty context, returning length bytes. Used off
// the hot path (once per key install), so allocation is not a concern.
func hkdfExpandLabel(h func() hash.Hash, secret []byte, label string, length int) []byte {
	full := "tls13 " + label
	info := make([]byte, 0, 3+len(full)+1)
	info = append(info, byte(length>>8), byte(length), byte(len(full)))
	info = append(info, full...)
	info = append(info, 0) // zero-length context
	out, err := hkdf.Expand(h, secret, string(info), length)
	if err != nil {
		// Expand only errors on an absurd length (> 255*HashLen); our lengths
		// (12/16/32) never hit it.
		panic("quic: hkdf expand: " + err.Error())
	}
	return out
}

// deriveKeys derives the packet-protection key/iv/hp from a traffic secret
// (RFC 9001 §5.1). keyLen is the AEAD key length (16 for AES-128-GCM); the
// header-protection key length matches it, and the IV is always 12 bytes.
func deriveKeys(h func() hash.Hash, secret []byte, keyLen int) PacketKeys {
	return PacketKeys{
		Key: hkdfExpandLabel(h, secret, "quic key", keyLen),
		IV:  hkdfExpandLabel(h, secret, "quic iv", 12),
		HP:  hkdfExpandLabel(h, secret, "quic hp", keyLen),
	}
}

// InitialKeys derives the client and server Initial packet keys from the
// client's original Destination Connection ID (RFC 9001 §5.2). Initial packets
// always use AEAD_AES_128_GCM with SHA-256, regardless of the TLS suite later
// negotiated.
func InitialKeys(dcid []byte) (client, server PacketKeys) {
	initialSecret, err := hkdf.Extract(sha256.New, dcid, quicV1InitialSalt)
	if err != nil {
		panic("quic: hkdf extract: " + err.Error())
	}
	clientSecret := hkdfExpandLabel(sha256.New, initialSecret, "client in", sha256.Size)
	serverSecret := hkdfExpandLabel(sha256.New, initialSecret, "server in", sha256.Size)
	client = deriveKeys(sha256.New, clientSecret, 16)
	server = deriveKeys(sha256.New, serverSecret, 16)
	client.Suite = tls.TLS_AES_128_GCM_SHA256
	server.Suite = tls.TLS_AES_128_GCM_SHA256
	return client, server
}
