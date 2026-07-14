package quic

import (
	"crypto/tls"
	"hash"
	"time"
)

// AEAD usage limits (RFC 9001 §6.6) are SUITE-DEPENDENT, and the two limits move
// in opposite directions between suites. The confidentiality limit bounds packets
// sealed under one key; the integrity limit bounds packets that fail
// authentication across all keys. AES-GCM is 2^23 / 2^52; AEAD_CHACHA20_POLY1305
// is 2^62 / 2^36 — a far larger confidentiality count but a SMALLER integrity
// count. So the AES-GCM confidentiality limit is safe (merely conservative) for
// ChaCha20, but the AES-GCM integrity limit is NOT: reusing 2^52 for ChaCha20
// would tolerate 2^16x more forged packets than §6.6 permits. aeadLimits selects
// the pair by suite; the named constants are the AES-GCM values, used for AES
// connections and as the fallback for hand-built test conns with no key-update
// state.
const (
	aeadConfidentialityLimit = uint64(1) << 23
	aeadIntegrityLimit       = uint64(1) << 52
)

// aeadLimits returns the RFC 9001 §6.6 (confidentiality, integrity) AEAD usage
// limits for a cipher suite. AEAD_CHACHA20_POLY1305 permits far more sealed
// packets (2^62) but far fewer authentication failures (2^36) than AES-GCM, so the
// two suites cannot share one integrity limit.
func aeadLimits(suite uint16) (confidentiality, integrity uint64) {
	if suite == tls.TLS_CHACHA20_POLY1305_SHA256 {
		return uint64(1) << 62, uint64(1) << 36
	}
	return aeadConfidentialityLimit, aeadIntegrityLimit // AES-GCM (RFC 9001 §6.6)
}

// aeadSuite is the negotiated 1-RTT cipher suite. A hand-built connection with no
// key-update state has no negotiated suite, so it falls back to AES-128-GCM (whose
// limits the AEAD-limit tests assert against).
func (c *Conn) aeadSuite() uint16 {
	if c.ku != nil {
		return c.ku.suite
	}
	return tls.TLS_AES_128_GCM_SHA256
}

// confidentialityLimit is the §6.6 max packets sealed under the current 1-RTT key
// for the negotiated suite. integrityLimit is the §6.6 max failed authentications.
func (c *Conn) confidentialityLimit() uint64 { conf, _ := aeadLimits(c.aeadSuite()); return conf }
func (c *Conn) integrityLimit() uint64       { _, integ := aeadLimits(c.aeadSuite()); return integ }

// keyUpdate holds RFC 9001 §6 key-update state for the application (1-RTT) space.
// The current-generation read Opener and write Sealer live on the Conn
// (c.keys.OneRTT and c.oneRTTSealer); this type owns everything needed to ratchet
// to the next generation and to decrypt reordered packets from the previous one.
//
// The header-protection key is NOT rotated on a key update (RFC 9001 §6.1), so
// readHP/writeHP are captured once at install and reused for every generation on
// both the receive and send paths — re-deriving "quic hp" from a ratcheted secret
// would break header protection for all but the first post-update packet.
type keyUpdate struct {
	suite  uint16
	h      func() hash.Hash
	keyLen int
	phase  bool // current Key Phase bit (the value we send and expect to receive)

	// read (server → client) generations
	readSecret     []byte          // current-generation read traffic secret (copied)
	nextReadSecret []byte          // pre-derived next-generation read secret
	readHP         headerProtector // shared header-protection key (not rotated)
	next           *Opener         // pre-derived next-generation read Opener
	prev           *Opener         // previous-generation read Opener (nil until first update)
	prevUntil      time.Time       // discard deadline for prev (RFC 9001 §6.3)
	boundary       uint64          // lowest packet number of the current read generation
	haveBoundary   bool

	// write (client → server) generations
	writeSecret     []byte
	nextWriteSecret []byte
	writeHP         headerProtector
	nextSealer      *Sealer // pre-derived next-generation write Sealer
}

// quicKuSecret derives the next-generation traffic secret (RFC 9001 §6.1):
// secret_{n+1} = HKDF-Expand-Label(secret_n, "quic ku", "", Hash.length).
func quicKuSecret(h func() hash.Hash, secret []byte) []byte {
	return hkdfExpandLabel(h, secret, "quic ku", len(secret))
}

// openerWithHP builds an Opener from keys but forces its header protector to hp,
// so a ratcheted generation keeps the original (un-rotated) HP key.
func openerWithHP(keys PacketKeys, hp headerProtector) (*Opener, error) {
	op, err := NewOpener(keys)
	if err != nil {
		return nil, err
	}
	op.hp = hp
	return op, nil
}

// sealerWithHP builds a Sealer from keys but forces its header protector to hp
// (see openerWithHP).
func sealerWithHP(keys PacketKeys, hp headerProtector) (*Sealer, error) {
	s, err := NewSealer(keys)
	if err != nil {
		return nil, err
	}
	s.hp = hp
	return s, nil
}

// initAppReadKU records the application read secret and pre-derives the next
// read generation (RFC 9001 §6). hp is the current generation's header-protection
// key, retained and shared across generations.
func (c *Conn) initAppReadKU(suite uint16, secret []byte, hp headerProtector) error {
	h, keyLen, err := hashForSuite(suite)
	if err != nil {
		return err
	}
	if c.ku == nil {
		c.ku = &keyUpdate{}
	}
	c.ku.suite, c.ku.h, c.ku.keyLen = suite, h, keyLen
	c.ku.readSecret = append([]byte(nil), secret...)
	c.ku.readHP = hp
	c.ku.nextReadSecret = quicKuSecret(h, c.ku.readSecret)
	nk, err := KeysFromSecret(suite, c.ku.nextReadSecret)
	if err != nil {
		return err
	}
	c.ku.next, err = openerWithHP(nk, hp)
	return err
}

// initAppWriteKU records the application write secret and pre-derives the next
// write generation.
func (c *Conn) initAppWriteKU(suite uint16, secret []byte, hp headerProtector) error {
	h, keyLen, err := hashForSuite(suite)
	if err != nil {
		return err
	}
	if c.ku == nil {
		c.ku = &keyUpdate{}
	}
	c.ku.suite, c.ku.h, c.ku.keyLen = suite, h, keyLen
	c.ku.writeSecret = append([]byte(nil), secret...)
	c.ku.writeHP = hp
	c.ku.nextWriteSecret = quicKuSecret(h, c.ku.writeSecret)
	nk, err := KeysFromSecret(suite, c.ku.nextWriteSecret)
	if err != nil {
		return err
	}
	c.ku.nextSealer, err = sealerWithHP(nk, hp)
	return err
}

// commitKeyUpdate applies a peer-initiated key update (RFC 9001 §6.2): the
// current read/write generations become the previous/retained ones, the
// pre-derived next generations become current, the Key Phase flips so the client
// sends the new phase, and the following generation is pre-derived. pn is the
// packet number that triggered the update — the boundary below which reordered
// old-generation packets are routed to prev. Assumes c.mu is held (it swaps the
// sealer and read openers the send path reads under the same lock).
func (c *Conn) commitKeyUpdate(pn uint64) {
	ku := c.ku
	// Retain the superseded read generation for reordered old-phase packets.
	ku.prev = c.keys.OneRTT
	ku.prevUntil = c.clock().Add(3 * c.ptoPeriod())
	// Promote the pre-derived next generation to current (read + write).
	c.keys.OneRTT = ku.next
	c.oneRTTSealer = ku.nextSealer
	c.appSendCount = 0 // a fresh write key resets the confidentiality counter (§6.6)
	ku.readSecret = ku.nextReadSecret
	ku.writeSecret = ku.nextWriteSecret
	ku.phase = !ku.phase
	ku.boundary = pn
	ku.haveBoundary = true
	// Pre-derive the following generation, keeping the shared HP keys.
	ku.nextReadSecret = quicKuSecret(ku.h, ku.readSecret)
	ku.nextWriteSecret = quicKuSecret(ku.h, ku.writeSecret)
	if nk, err := KeysFromSecret(ku.suite, ku.nextReadSecret); err == nil {
		ku.next, _ = openerWithHP(nk, ku.readHP)
	}
	if nk, err := KeysFromSecret(ku.suite, ku.nextWriteSecret); err == nil {
		ku.nextSealer, _ = sealerWithHP(nk, ku.writeHP)
	}
}

// appSendPhase is the Key Phase bit the client currently sends on 1-RTT packets.
func (c *Conn) appSendPhase() bool {
	return c.ku != nil && c.ku.phase
}

// discardStaleKeys drops the retained previous-generation read Opener once its
// 3×PTO retention window has elapsed (RFC 9001 §6.3). Assumes c.mu is held
// (called from the Poll receive path).
func (c *Conn) discardStaleKeys() {
	if c.ku != nil && c.ku.prev != nil && c.clock().After(c.ku.prevUntil) {
		c.ku.prev = nil
	}
}
