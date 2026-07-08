package quic

import (
	"crypto/aes"
	"crypto/cipher"
)

// Sealer applies QUIC packet protection on the send path (RFC 9001 §5): AEAD
// encryption of the frame payload, then header protection of the first byte and
// packet-number field. One Sealer is built per encryption level from that
// level's PacketKeys. Not safe for concurrent use.
type Sealer struct {
	aead cipher.AEAD
	hp   cipher.Block
	iv   [12]byte
}

// Opener removes QUIC packet protection on the receive path.
type Opener struct {
	aead cipher.AEAD
	hp   cipher.Block
	iv   [12]byte
}

func newAEAD(k PacketKeys) (cipher.AEAD, cipher.Block, [12]byte, error) {
	var iv [12]byte
	if len(k.IV) != 12 {
		return nil, nil, iv, ErrCryptoKey
	}
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
	copy(iv[:], k.IV)
	return aead, hp, iv, nil
}

// NewSealer builds a Sealer for AEAD_AES_128_GCM (or AES-256-GCM) packet
// protection from a set of PacketKeys.
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

// makeNonce builds the AEAD nonce: the 12-byte IV XORed with the full 62-bit
// packet number, left-padded with zeros (RFC 9001 §5.3).
func makeNonce(iv [12]byte, pn uint64) [12]byte {
	n := iv
	for i := 0; i < 8; i++ {
		n[11-i] ^= byte(pn >> (8 * i))
	}
	return n
}

// Seal protects a packet and appends it to dst. hdr is the unprotected header
// including the pnLen-byte packet number (it is the AEAD associated data);
// pnOffset is the packet-number offset within hdr; pn is the full packet
// number; payload is the frame bytes. The returned packet is hdr followed by
// the AEAD ciphertext+tag, with the first byte and packet number
// header-protected. The payload plus tag must be long enough to sample header
// protection (RFC 9001 §5.4.2); Initial packets are padded to satisfy this.
func (s *Sealer) Seal(dst, hdr []byte, pnOffset, pnLen int, pn uint64, payload []byte) ([]byte, error) {
	nonce := makeNonce(s.iv, pn)
	start := len(dst)
	dst = append(dst, hdr...)
	dst = s.aead.Seal(dst, nonce[:], payload, hdr)
	if err := applyHeaderProtection(s.hp, dst[start:], pnOffset, pnLen); err != nil {
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
	pnLen, err = removeHeaderProtection(o.hp, pkt, pnOffset)
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
	nonce := makeNonce(o.iv, pn)
	hdr := pkt[:pnOffset+pnLen]
	ct := pkt[pnOffset+pnLen:]
	payload, err := o.aead.Open(ct[:0], nonce[:], ct, hdr)
	if err != nil {
		return nil, ErrCryptoDecrypt
	}
	return payload, nil
}

// applyHeaderProtection masks the first byte and packet number using a sample of
// the protected payload (RFC 9001 §5.4).
func applyHeaderProtection(block cipher.Block, pkt []byte, pnOffset, pnLen int) error {
	mask, err := headerMask(block, pkt, pnOffset)
	if err != nil {
		return err
	}
	maskHeader(pkt, pnOffset, pnLen, mask)
	return nil
}

// removeHeaderProtection reverses applyHeaderProtection and returns the
// packet-number length recovered from the now-unmasked first byte.
func removeHeaderProtection(block cipher.Block, pkt []byte, pnOffset int) (pnLen int, err error) {
	mask, err := headerMask(block, pkt, pnOffset)
	if err != nil {
		return 0, err
	}
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

// headerMask computes the 5-byte-relevant header-protection mask from a 16-byte
// sample of the packet taken 4 bytes past the packet-number offset (RFC 9001
// §5.4.2). For AES the mask is a single-block ECB encryption of the sample.
func headerMask(block cipher.Block, pkt []byte, pnOffset int) ([16]byte, error) {
	var mask [16]byte
	sampleOff := pnOffset + 4
	if sampleOff+16 > len(pkt) {
		return mask, ErrCryptoSample
	}
	block.Encrypt(mask[:], pkt[sampleOff:sampleOff+16])
	return mask, nil
}

func maskHeader(pkt []byte, pnOffset, pnLen int, mask [16]byte) {
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
