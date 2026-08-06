package quic

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/tls"
	"encoding/binary"

	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/chacha20poly1305"
)

// Sealer applies QUIC packet protection on the send path (RFC 9001 §5): AEAD
// encryption of the frame payload, then header protection of the first byte and
// packet-number field. One Sealer is built per encryption level from that
// level's PacketKeys. Not safe for concurrent use.
type Sealer struct {
	aead cipher.AEAD
	hp   headerProtector
	iv   [12]byte
	// Per-Sealer scratch for the AEAD nonce and the header-protection mask.
	// Both used to be built on the stack and returned by value, but both are
	// then handed to an interface method (cipher.AEAD.Seal, headerProtector),
	// so the compiler moved them to the heap — 32 B and 2 allocs on every
	// packet sent. Heap-resident fields make a slice of them free. Safe because
	// a Sealer is used by one writer at a time, the same contract the AEAD
	// itself has; see the type doc.
	nonce [12]byte
	mask  [5]byte
}

// Opener removes QUIC packet protection on the receive path.
type Opener struct {
	aead cipher.AEAD
	hp   headerProtector
	iv   [12]byte
	// Receive-side counterparts of Sealer's scratch — same reason, same
	// single-user contract (one reader goroutine per connection).
	nonce [12]byte
	mask  [5]byte
}

// newAEAD builds the AEAD and header protector for a set of PacketKeys, selecting
// the algorithm from k.Suite (RFC 9001 §5.1, §5.4): AEAD_AES_128_GCM /
// AEAD_AES_256_GCM with AES-ECB header protection for the AES-GCM suites (a zero
// Suite is treated as AES-128-GCM for hand-built keys), and AEAD_CHACHA20_POLY1305
// with ChaCha20 header protection for TLS_CHACHA20_POLY1305_SHA256. Wrong key
// sizes return ErrCryptoKey; any other suite returns ErrCryptoSuite.
func newAEAD(k PacketKeys) (cipher.AEAD, headerProtector, [12]byte, error) {
	var iv [12]byte
	if len(k.IV) != 12 {
		return nil, nil, iv, ErrCryptoKey
	}
	copy(iv[:], k.IV)

	switch k.Suite {
	case tls.TLS_CHACHA20_POLY1305_SHA256:
		aead, err := chacha20poly1305.New(k.Key)
		if err != nil {
			return nil, nil, iv, ErrCryptoKey
		}
		if len(k.HP) != chacha20.KeySize {
			return nil, nil, iv, ErrCryptoKey
		}
		var hpKey [32]byte
		copy(hpKey[:], k.HP)
		return aead, &chachaHeaderProtector{key: hpKey}, iv, nil
	case tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, 0:
		block, err := aes.NewCipher(k.Key)
		if err != nil {
			return nil, nil, iv, ErrCryptoKey
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, nil, iv, ErrCryptoKey
		}
		hp, err := aes.NewCipher(k.HP)
		if err != nil {
			return nil, nil, iv, ErrCryptoKey
		}
		return aead, &aesHeaderProtector{block: hp}, iv, nil
	default:
		return nil, nil, iv, ErrCryptoSuite
	}
}

// NewSealer builds a Sealer for the packet-protection suite named by k.Suite
// (AES-GCM or ChaCha20-Poly1305) from a set of PacketKeys.
func NewSealer(k PacketKeys) (*Sealer, error) {
	aead, hp, iv, err := newAEAD(k)
	if err != nil {
		return nil, err
	}
	return &Sealer{aead: aead, hp: hp, iv: iv}, nil
}

// NewOpener builds an Opener from a set of PacketKeys.
func NewOpener(k PacketKeys) (*Opener, error) {
	aead, hp, iv, err := newAEAD(k)
	if err != nil {
		return nil, err
	}
	return &Opener{aead: aead, hp: hp, iv: iv}, nil
}

// putNonce writes the AEAD nonce into dst: the 12-byte IV XORed with the full
// 62-bit packet number, left-padded with zeros (RFC 9001 §5.3).
//
// Writes through a pointer rather than returning [12]byte because the result is
// sliced into cipher.AEAD.Seal/Open — an interface call — which makes a
// by-value return escape to the heap on every packet.
func putNonce(dst *[12]byte, iv [12]byte, pn uint64) {
	*dst = iv
	for i := 0; i < 8; i++ {
		dst[11-i] ^= byte(pn >> (8 * i))
	}
}

// Seal protects a packet and appends it to dst. hdr is the unprotected header
// including the pnLen-byte packet number (it is the AEAD associated data);
// pnOffset is the packet-number offset within hdr; pn is the full packet
// number; payload is the frame bytes. The returned packet is hdr followed by
// the AEAD ciphertext+tag, with the first byte and packet number
// header-protected. The payload plus tag must be long enough to sample header
// protection (RFC 9001 §5.4.2); Initial packets are padded to satisfy this.
func (s *Sealer) Seal(dst, hdr []byte, pnOffset, pnLen int, pn uint64, payload []byte) ([]byte, error) {
	putNonce(&s.nonce, s.iv, pn)
	start := len(dst)
	dst = append(dst, hdr...)
	dst = s.aead.Seal(dst, s.nonce[:], payload, hdr)
	if err := sealHeader(s.hp, dst[start:], pnOffset, pnLen, &s.mask); err != nil {
		return nil, err
	}
	return dst, nil
}

// Open removes header protection and AEAD-decrypts a received packet in place.
// pkt is the full protected packet (its first byte and packet number are
// unprotected in place); pnOffset is the packet-number offset; largestAcked is
// the largest packet number already acknowledged in this packet-number space
// (for reconstruction, RFC 9000 §A.3). It returns the full packet number, the
// packet-number length, and the decrypted payload, or an error.
func (o *Opener) Open(pkt []byte, pnOffset int, largestAcked uint64) (pn uint64, pnLen int, payload []byte, err error) {
	pn, pnLen, _, err = o.unprotectHeader(pkt, pnOffset, largestAcked)
	if err != nil {
		return 0, 0, nil, err
	}
	payload, err = o.openAEAD(pkt, pnOffset, pnLen, pn)
	if err != nil {
		return 0, 0, nil, err
	}
	return pn, pnLen, payload, nil
}

// unprotectHeader removes header protection in place and reconstructs the packet
// number, returning it, its length, and the short-header Key Phase bit
// (RFC 9001 §6). The header-protection key is not rotated on a key update
// (RFC 9001 §6.1), so any generation's Opener unprotects the header identically;
// the caller then chooses the AEAD generation for openAEAD. It must be called at
// most once per packet (it mutates the header in place).
func (o *Opener) unprotectHeader(pkt []byte, pnOffset int, largestAcked uint64) (pn uint64, pnLen int, keyPhase bool, err error) {
	pnLen, err = openHeader(o.hp, pkt, pnOffset, &o.mask)
	if err != nil {
		return 0, 0, false, err
	}
	var truncated uint64
	for i := 0; i < pnLen; i++ {
		truncated = truncated<<8 | uint64(pkt[pnOffset+i])
	}
	pn = decodePacketNumber(largestAcked, truncated, pnLen)
	keyPhase = pkt[0]&0x04 != 0
	return pn, pnLen, keyPhase, nil
}

// openAEAD AEAD-decrypts, in place, a packet whose header protection has already
// been removed by unprotectHeader and whose packet number is known. Attempting it
// more than once on the same packet corrupts the buffer (GCM writes into the
// ciphertext), so the caller picks exactly one key generation per packet.
func (o *Opener) openAEAD(pkt []byte, pnOffset, pnLen int, pn uint64) ([]byte, error) {
	putNonce(&o.nonce, o.iv, pn)
	hdr := pkt[:pnOffset+pnLen]
	ct := pkt[pnOffset+pnLen:]
	payload, err := o.aead.Open(ct[:0], o.nonce[:], ct, hdr)
	if err != nil {
		return nil, ErrCryptoDecrypt
	}
	return payload, nil
}

// headerProtector derives the 5-byte header-protection mask from a 16-byte
// ciphertext sample (RFC 9001 §5.4.1). Only these 5 bytes are ever consumed:
// mask[0] masks the header's low bits and mask[1:1+pnLen] masks the packet
// number. How the sample becomes the mask is the one suite-specific part of
// packet protection, so it is the seam between the AES-GCM suites and
// ChaCha20-Poly1305.
type headerProtector interface {
	// headerMask writes the 5-byte mask into out. It takes a pointer rather
	// than returning [5]byte because this is an interface method: a by-value
	// return escapes to the heap on every packet.
	headerMask(sample []byte, out *[5]byte)
}

// aesHeaderProtector derives the mask by single-block ECB encryption of the
// sample under the header-protection key (RFC 9001 §5.4.3), for the AES-GCM
// suites.
type aesHeaderProtector struct {
	block cipher.Block
	full  [16]byte // scratch for the ECB block; see headerMask
}

func (h *aesHeaderProtector) headerMask(sample []byte, out *[5]byte) {
	// h.full, not a local: cipher.Block.Encrypt is an interface call, so a
	// stack array sliced into it escapes. Pointer receiver for the same reason —
	// a value receiver would copy the scratch back onto the stack.
	h.block.Encrypt(h.full[:], sample)
	copy(out[:], h.full[:])
}

// chachaHeaderProtector derives the mask from a ChaCha20 key stream keyed by the
// header-protection key, using the sample's first 4 bytes as the little-endian
// block counter and its remaining 12 bytes as the nonce (RFC 9001 §5.4.4).
// The ChaCha20 variant still allocates a cipher per packet:
// chacha20.NewUnauthenticatedCipher has no API to re-key an existing cipher for
// a new nonce, and the nonce is the per-packet sample. AES-GCM is the default
// suite and the one the zero-alloc benchmark covers.
type chachaHeaderProtector struct{ key [32]byte }

func (h *chachaHeaderProtector) headerMask(sample []byte, out *[5]byte) {
	counter := binary.LittleEndian.Uint32(sample[0:4])
	c, err := chacha20.NewUnauthenticatedCipher(h.key[:], sample[4:16])
	if err != nil {
		// The key is a fixed 32 bytes and the nonce slice is exactly 12 bytes, so
		// the constructor cannot fail; a non-nil error would be a programming bug.
		panic("quic: chacha20 header protection: " + err.Error())
	}
	c.SetCounter(counter)
	*out = [5]byte{}
	c.XORKeyStream(out[:], out[:]) // key stream XOR zeros = the first 5 key-stream bytes
}

// headerSample returns the 16-byte header-protection sample, taken 4 bytes past
// the packet-number offset (RFC 9001 §5.4.2), or ErrCryptoSample if the packet is
// too short to contain it.
func headerSample(pkt []byte, pnOffset int) ([]byte, error) {
	off := pnOffset + 4
	if off+16 > len(pkt) {
		return nil, ErrCryptoSample
	}
	return pkt[off : off+16], nil
}

// sealHeader masks the first byte and packet number of pkt using the header
// protector hp and a sample of the protected payload (RFC 9001 §5.4).
func sealHeader(hp headerProtector, pkt []byte, pnOffset, pnLen int, mask *[5]byte) error {
	sample, err := headerSample(pkt, pnOffset)
	if err != nil {
		return err
	}
	hp.headerMask(sample, mask)
	maskHeader(pkt, pnOffset, pnLen, mask)
	return nil
}

// openHeader reverses sealHeader and returns the packet-number length recovered
// from the now-unmasked first byte.
func openHeader(hp headerProtector, pkt []byte, pnOffset int, mask *[5]byte) (pnLen int, err error) {
	sample, err := headerSample(pkt, pnOffset)
	if err != nil {
		return 0, err
	}
	hp.headerMask(sample, mask)
	// Unmask the first byte first to learn the packet-number length.
	if pkt[0]&0x80 != 0 {
		pkt[0] ^= mask[0] & 0x0f // long header: low 4 bits
	} else {
		pkt[0] ^= mask[0] & 0x1f // short header: low 5 bits (incl. key phase)
	}
	pnLen = int(pkt[0]&0x03) + 1
	if pnOffset+pnLen > len(pkt) {
		return 0, ErrCryptoSample
	}
	for i := 0; i < pnLen; i++ {
		pkt[pnOffset+i] ^= mask[1+i]
	}
	return pnLen, nil
}

// applyHeaderProtection masks a packet's header with an AES header-protection
// block cipher (RFC 9001 §5.4.3). It is the AES-typed entry point; the Sealer
// routes through sealHeader with the connection's headerProtector.
func applyHeaderProtection(block cipher.Block, pkt []byte, pnOffset, pnLen int) error {
	var mask [5]byte
	return sealHeader(&aesHeaderProtector{block: block}, pkt, pnOffset, pnLen, &mask)
}

// removeHeaderProtection reverses applyHeaderProtection (see sealHeader/openHeader).
func removeHeaderProtection(block cipher.Block, pkt []byte, pnOffset int) (pnLen int, err error) {
	var mask [5]byte
	return openHeader(&aesHeaderProtector{block: block}, pkt, pnOffset, &mask)
}

// maskHeader XORs the 5-byte header-protection mask into the first byte (low 4
// bits for a long header, low 5 for a short header) and the packet number.
func maskHeader(pkt []byte, pnOffset, pnLen int, mask *[5]byte) {
	if pkt[0]&0x80 != 0 {
		pkt[0] ^= mask[0] & 0x0f
	} else {
		pkt[0] ^= mask[0] & 0x1f
	}
	for i := 0; i < pnLen; i++ {
		pkt[pnOffset+i] ^= mask[1+i]
	}
}

// decodePacketNumber reconstructs the full packet number from the truncated
// on-wire value and the largest acknowledged packet number (RFC 9000 §A.3).
func decodePacketNumber(largestAcked, truncated uint64, pnLen int) uint64 {
	pnBits := uint(pnLen * 8)
	pnWin := uint64(1) << pnBits
	pnHalfWin := pnWin / 2
	pnMask := pnWin - 1
	expected := largestAcked + 1
	candidate := (expected &^ pnMask) | truncated
	if candidate+pnHalfWin <= expected && candidate < (1<<62)-pnWin {
		return candidate + pnWin
	}
	if candidate > expected+pnHalfWin && candidate >= pnWin {
		return candidate - pnWin
	}
	return candidate
}
