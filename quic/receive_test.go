package quic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func zeroLargest(PacketType) uint64 { return 0 }

// craftServerInitial seals a server Initial packet carrying payload, as the
// client would receive it (server sends with the server Initial keys).
func craftServerInitial(t *testing.T, serverKeys PacketKeys, dcid, scid []byte, pn uint64, payload []byte) []byte {
	t.Helper()
	return craftServerInitialVersion(t, serverKeys, dcid, scid, pn, payload, QUICVersion1)
}

// craftServerInitialVersion is craftServerInitial with an explicit Version field,
// for the RFC 9000 §5.2 "discard a packet using another version" check.
func craftServerInitialVersion(t *testing.T, serverKeys PacketKeys, dcid, scid []byte, pn uint64, payload []byte, version uint32) []byte {
	t.Helper()
	sealer, err := NewSealer(serverKeys)
	require.NoError(t, err, "build the server Initial sealer the fixture packet is sealed with")
	pnLen := 4
	length := uint64(pnLen + len(payload) + 16)
	hdr, pnOff := AppendLongHeader(nil, PacketInitial, version, dcid, scid, nil, pnLen, length)
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	sealed, err := sealer.Seal(nil, hdr, pnOff, pnLen, pn, payload)
	require.NoErrorf(t, err, "Seal: %v", err)
	return sealed
}

func serverInitialKeySet(t *testing.T, dcid []byte) *keySet {
	t.Helper()
	_, server := InitialKeys(dcid)
	op, err := NewOpener(server)
	require.NoError(t, err, "build the server Initial opener processDatagram decrypts with")
	return &keySet{Initial: op}
}

// TestProcessDatagram_ServerInitial decrypts a server Initial packet and
// dispatches its frames.
func TestProcessDatagram_ServerInitial(t *testing.T) {
	dcid := []byte{0x83, 0x94, 0xc8, 0xf0, 0x3e, 0x51, 0x57, 0x08}
	_, server := InitialKeys(dcid)
	payload := AppendAck(nil, 0, 0, 0, nil)
	payload = AppendCrypto(payload, 0, []byte("serverhello"))
	dg := craftServerInitial(t, server, nil, []byte{0xaa}, 1, payload)

	var rec recHandler
	res, err := processDatagram(dg, 0, serverInitialKeySet(t, dcid), zeroLargest, &rec)

	require.NoErrorf(t, err, "processDatagram: %v", err)
	assert.Equalf(t, 1, res.Processed, "result = %+v, want 1 processed", res)
	assert.Equalf(t, 0, res.Skipped, "result = %+v, want 1 processed", res)
	assert.Equal(t, []string{
		"ack largest=0 delay=0 first=0",
		"crypto off=0 data=73657276657268656c6c6f",
	}, rec.log, "the ACK and CRYPTO frames of the Initial must both be dispatched, in order")
}

// TestProcessDatagram_Coalesced walks two Initial packets coalesced in one
// datagram.
func TestProcessDatagram_Coalesced(t *testing.T) {
	dcid := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	_, server := InitialKeys(dcid)
	p1 := craftServerInitial(t, server, nil, []byte{0xaa}, 1, appendPing(make([]byte, 0, 32)))
	p2 := craftServerInitial(t, server, nil, []byte{0xaa}, 2, AppendMaxData(make([]byte, 0, 32), 4096))
	dg := append(append([]byte{}, p1...), p2...)

	var rec recHandler
	res, err := processDatagram(dg, 0, serverInitialKeySet(t, dcid), zeroLargest, &rec)

	require.NoErrorf(t, err, "processDatagram: %v", err)
	assert.Equalf(t, 2, res.Processed, "processed %d, want 2 (%+v)", res.Processed, res)
	assert.Equal(t, []string{"ping", "maxdata 4096"}, rec.log,
		"both coalesced packets' frames must be dispatched, in wire order")
}

// TestProcessDatagram_SkipNoKeys skips a 1-RTT packet when Application keys are
// not yet installed.
func TestProcessDatagram_SkipNoKeys(t *testing.T) {
	shortPkt := []byte{0x40, 0x11, 0x22, 0x33} // short header, dcidLen 0

	var rec recHandler
	res, err := processDatagram(shortPkt, 0, &keySet{}, zeroLargest, &rec)

	require.NoError(t, err, "a packet we hold no keys for is skipped, not an error")
	assert.Equalf(t, 0, res.Processed, "result = %+v, want 0 processed / 1 skipped", res)
	assert.Equalf(t, 1, res.Skipped, "result = %+v, want 0 processed / 1 skipped", res)
	assert.Emptyf(t, rec.log, "dispatched frames from an undecryptable packet: %v", rec.log)
}

// TestProcessDatagram_AuthFailure skips a packet that fails AEAD authentication.
func TestProcessDatagram_AuthFailure(t *testing.T) {
	dcid := []byte{9, 9, 9, 9, 9, 9, 9, 9}
	_, server := InitialKeys(dcid)
	dg := craftServerInitial(t, server, nil, []byte{0xaa}, 1, appendPing(make([]byte, 0, 32)))
	dg[len(dg)-1] ^= 0xff // corrupt the tag

	var rec recHandler
	res, err := processDatagram(dg, 0, serverInitialKeySet(t, dcid), zeroLargest, &rec)

	require.NoError(t, err, "an unauthenticated packet is skipped, not an error (RFC 9001 §5.5)")
	assert.Equalf(t, 0, res.Processed, "result = %+v, want 0 processed / 1 skipped", res)
	assert.Equalf(t, 1, res.Skipped, "result = %+v, want 0 processed / 1 skipped", res)
}

// TestProcessDatagram_Retry flags a Retry packet without dispatching frames.
func TestProcessDatagram_Retry(t *testing.T) {
	pkt := []byte{0xf0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0xaa}
	pkt = append(pkt, 't', 'o', 'k')
	for i := 0; i < 16; i++ {
		pkt = append(pkt, 0xcc)
	}

	var rec recHandler
	res, err := processDatagram(pkt, 0, &keySet{}, zeroLargest, &rec)

	require.NoError(t, err, "a Retry packet is reported through the result, not as an error")
	assert.Truef(t, res.Retry, "result = %+v, want Retry with 0 processed", res)
	assert.Equalf(t, 0, res.Processed, "result = %+v, want Retry with 0 processed", res)
}

// TestProcessDatagram_Malformed stops on a structurally invalid header.
func TestProcessDatagram_Malformed(t *testing.T) {
	_, err := processDatagram([]byte{0xc0, 0x00}, 0, &keySet{}, zeroLargest, &recHandler{})

	require.ErrorIsf(t, err, ErrPacketEncoding,
		"a structurally invalid header = %v, want ErrPacketEncoding", err)
}
