package quic

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
)

// Retry integrity protection uses a fixed AES-128-GCM key and nonce for QUIC v1
// (RFC 9001 §5.8).
var (
	retryKey   = [16]byte{0xbe, 0x0c, 0x69, 0x0b, 0x9f, 0x66, 0x57, 0x5a, 0x1d, 0x76, 0x6b, 0x54, 0xe3, 0x68, 0xc8, 0x4e}
	retryNonce = [12]byte{0x46, 0x15, 0x99, 0xd3, 0x5d, 0x63, 0x2b, 0xf2, 0x23, 0x98, 0x25, 0xbb}
)

// verifyRetryIntegrity checks a Retry packet's 16-byte Retry Integrity Tag
// (RFC 9001 §5.8). The tag authenticates the Retry Pseudo-Packet: the original
// destination connection ID (the DCID the client chose for its first Initial),
// length-prefixed, followed by the Retry packet with its tag removed. It is an
// AES-128-GCM tag over that associated data with an empty plaintext.
func verifyRetryIntegrity(odcid, pkt []byte) bool {
	if len(pkt) < 16 {
		return false
	}
	body := pkt[:len(pkt)-16]
	tag := pkt[len(pkt)-16:]
	pseudo := make([]byte, 0, 1+len(odcid)+len(body))
	pseudo = append(pseudo, byte(len(odcid)))
	pseudo = append(pseudo, odcid...)
	pseudo = append(pseudo, body...)

	block, err := aes.NewCipher(retryKey[:])
	if err != nil {
		return false
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return false
	}
	want := aead.Seal(nil, retryNonce[:], nil, pseudo) // empty plaintext → the 16-byte tag
	return len(want) == 16 && subtle.ConstantTimeCompare(want, tag) == 1
}

// handleRetry processes a received Retry packet (RFC 9000 §17.2.5). A client
// discards a Retry once it has already processed a Retry or any packet from the
// server (§17.2.5.2), and discards one whose integrity tag fails, whose token is
// empty, or whose Source Connection ID equals its Initial's Destination
// Connection ID. On a valid Retry it adopts the server's connection ID, re-derives
// the Initial keys, stores the token for subsequent Initials, and re-queues the
// ClientHello for resend. Any failure is a silent discard, never fatal.
func (c *Conn) handleRetry(hdr Header, pkt []byte) {
	// c.gotServerCID is set once a server Initial/Handshake packet has been
	// processed; after that a Retry is too late and MUST be discarded (§17.2.5.2),
	// which also prevents an injected Retry from corrupting the adopted DCID.
	if c.handledRetry || c.gotServerCID || len(hdr.Token) == 0 || bytesEqual(hdr.SCID, c.dcid) {
		return
	}
	if !verifyRetryIntegrity(c.dcid, pkt) {
		return
	}
	newDCID := append([]byte(nil), hdr.SCID...)
	client, server := InitialKeys(newDCID)
	is, err := NewSealer(client)
	if err != nil {
		return
	}
	io, err := NewOpener(server)
	if err != nil {
		return
	}
	c.handledRetry = true
	c.dcid = newDCID
	c.initialSealer = is
	c.keys.Initial = io
	c.retryToken = append([]byte(nil), hdr.Token...)
	c.requeueInitialForRetry()
}

// requeueInitialForRetry abandons the outstanding Initial packet(s) and re-queues
// their frames for resend under the post-Retry keys, connection ID, and token.
// Packet numbers are not reset (RFC 9000 §17.2.5.3): the resend takes the next
// number, so this only moves the frames to the retransmit queue.
func (c *Conn) requeueInitialForRetry() {
	for pn, p := range c.sent[spaceInitial].packets {
		c.retransQueue[spaceInitial] = append(c.retransQueue[spaceInitial], p.frames...)
		if p.ackEliciting {
			c.removeInFlight(p.size)
		}
		delete(c.sent[spaceInitial].packets, pn)
	}
}

// bytesEqual reports byte-slice equality (avoids importing bytes for one use).
func bytesEqual(a, b []byte) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare(a, b) == 1
}
