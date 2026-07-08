package quic

// KeySet holds the receive-side Openers for each encryption level. The
// connection installs each as the TLS handshake surfaces the corresponding read
// secret (RFC 9001 §4). A nil Opener means keys for that level are not yet
// available, so packets at that level are buffered/skipped.
type KeySet struct {
	Initial   *Opener
	Handshake *Opener
	OneRTT    *Opener
}

func (k *KeySet) openerFor(t PacketType) *Opener {
	switch t {
	case PacketInitial:
		return k.Initial
	case PacketHandshake:
		return k.Handshake
	case PacketShort:
		return k.OneRTT
	default:
		return nil
	}
}

// DatagramResult reports what ProcessDatagram did with one UDP datagram.
type DatagramResult struct {
	Processed          int  // packets decrypted and dispatched to the handler
	Skipped            int  // packets skipped: keys unavailable, or decryption failed
	Retry              bool // a Retry packet was present
	VersionNegotiation bool
}

// ProcessDatagram parses every QUIC packet coalesced in a single UDP datagram
// (RFC 9000 §12.2), removes protection with the matching level's Opener, and
// dispatches each packet's frames to h in order. dcidLen is the local
// connection-ID length (to locate a short-header DCID). largestAcked returns the
// largest packet number already acknowledged in a packet type's number space,
// for reconstruction (§A.3).
//
// Packets whose keys are not yet installed, or that fail to authenticate (for
// example a reordered packet from a discarded key phase), are skipped rather
// than failing the datagram — a datagram may legitimately mix readable and
// not-yet-readable packets. A structurally malformed header stops parsing and
// returns ErrPacketEncoding. Retry and Version Negotiation packets carry no
// protected payload and are only flagged for the caller to handle.
func ProcessDatagram(datagram []byte, dcidLen int, ks *KeySet, largestAcked func(PacketType) uint64, h FrameHandler) (DatagramResult, error) {
	var res DatagramResult
	rest := datagram
	for len(rest) > 0 {
		hdr, err := ParseHeader(rest, dcidLen)
		if err != nil {
			return res, err
		}
		pkt := rest[:hdr.PacketLen]
		rest = rest[hdr.PacketLen:]

		switch hdr.Type {
		case PacketRetry:
			res.Retry = true
			continue
		case PacketVersionNegotiation:
			res.VersionNegotiation = true
			continue
		}

		op := ks.openerFor(hdr.Type)
		if op == nil {
			res.Skipped++ // keys not yet available (e.g. 1-RTT before handshake completes)
			continue
		}
		_, _, payload, err := op.Open(pkt, hdr.PNOffset, largestAcked(hdr.Type))
		if err != nil {
			res.Skipped++ // authentication failed; skip this packet
			continue
		}
		if err := ParseFrames(payload, h); err != nil {
			return res, err
		}
		res.Processed++
	}
	return res, nil
}
