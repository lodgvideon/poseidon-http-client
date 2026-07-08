package quic

import (
	"errors"
	"testing"
)

func zeroLargest(PacketType) uint64 { return 0 }

// craftServerInitial seals a server Initial packet carrying payload, as the
// client would receive it (server sends with the server Initial keys).
func craftServerInitial(t *testing.T, serverKeys PacketKeys, dcid, scid []byte, pn uint64, payload []byte) []byte {
	t.Helper()
	sealer, err := NewSealer(serverKeys)
	if err != nil {
		t.Fatal(err)
	}
	pnLen := 4
	length := uint64(pnLen + len(payload) + 16)
	hdr, pnOff := AppendLongHeader(nil, PacketInitial, QUICVersion1, dcid, scid, nil, pnLen, length)
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	sealed, err := sealer.Seal(nil, hdr, pnOff, pnLen, pn, payload)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return sealed
}

func serverInitialKeySet(t *testing.T, dcid []byte) *KeySet {
	t.Helper()
	_, server := InitialKeys(dcid)
	op, err := NewOpener(server)
	if err != nil {
		t.Fatal(err)
	}
	return &KeySet{Initial: op}
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
	res, err := ProcessDatagram(dg, 0, serverInitialKeySet(t, dcid), zeroLargest, &rec)
	if err != nil {
		t.Fatalf("ProcessDatagram: %v", err)
	}
	if res.Processed != 1 || res.Skipped != 0 {
		t.Fatalf("result = %+v, want 1 processed", res)
	}
	eq(t, rec.log, []string{
		"ack largest=0 delay=0 first=0",
		"crypto off=0 data=73657276657268656c6c6f",
	})
}

// TestProcessDatagram_Coalesced walks two Initial packets coalesced in one
// datagram.
func TestProcessDatagram_Coalesced(t *testing.T) {
	dcid := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	_, server := InitialKeys(dcid)

	p1 := craftServerInitial(t, server, nil, []byte{0xaa}, 1, AppendPing(make([]byte, 0, 32)))
	p2 := craftServerInitial(t, server, nil, []byte{0xaa}, 2, AppendMaxData(make([]byte, 0, 32), 4096))
	dg := append(append([]byte{}, p1...), p2...)

	var rec recHandler
	res, err := ProcessDatagram(dg, 0, serverInitialKeySet(t, dcid), zeroLargest, &rec)
	if err != nil {
		t.Fatalf("ProcessDatagram: %v", err)
	}
	if res.Processed != 2 {
		t.Fatalf("processed %d, want 2 (%+v)", res.Processed, res)
	}
	eq(t, rec.log, []string{"ping", "maxdata 4096"})
}

// TestProcessDatagram_SkipNoKeys skips a 1-RTT packet when Application keys are
// not yet installed.
func TestProcessDatagram_SkipNoKeys(t *testing.T) {
	shortPkt := []byte{0x40, 0x11, 0x22, 0x33} // short header, dcidLen 0
	var rec recHandler
	res, err := ProcessDatagram(shortPkt, 0, &KeySet{}, zeroLargest, &rec)
	if err != nil {
		t.Fatal(err)
	}
	if res.Processed != 0 || res.Skipped != 1 {
		t.Fatalf("result = %+v, want 0 processed / 1 skipped", res)
	}
	if len(rec.log) != 0 {
		t.Fatalf("dispatched frames from an undecryptable packet: %v", rec.log)
	}
}

// TestProcessDatagram_AuthFailure skips a packet that fails AEAD authentication.
func TestProcessDatagram_AuthFailure(t *testing.T) {
	dcid := []byte{9, 9, 9, 9, 9, 9, 9, 9}
	_, server := InitialKeys(dcid)
	dg := craftServerInitial(t, server, nil, []byte{0xaa}, 1, AppendPing(make([]byte, 0, 32)))
	dg[len(dg)-1] ^= 0xff // corrupt the tag

	var rec recHandler
	res, err := ProcessDatagram(dg, 0, serverInitialKeySet(t, dcid), zeroLargest, &rec)
	if err != nil {
		t.Fatal(err)
	}
	if res.Processed != 0 || res.Skipped != 1 {
		t.Fatalf("result = %+v, want 0 processed / 1 skipped", res)
	}
}

// TestProcessDatagram_Retry flags a Retry packet without dispatching frames.
func TestProcessDatagram_Retry(t *testing.T) {
	pkt := []byte{0xf0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0xaa}
	pkt = append(pkt, 't', 'o', 'k')
	for i := 0; i < 16; i++ {
		pkt = append(pkt, 0xcc)
	}
	var rec recHandler
	res, err := ProcessDatagram(pkt, 0, &KeySet{}, zeroLargest, &rec)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Retry || res.Processed != 0 {
		t.Fatalf("result = %+v, want Retry with 0 processed", res)
	}
}

// TestProcessDatagram_Malformed stops on a structurally invalid header.
func TestProcessDatagram_Malformed(t *testing.T) {
	if _, err := ProcessDatagram([]byte{0xc0, 0x00}, 0, &KeySet{}, zeroLargest, &recHandler{}); !errors.Is(err, ErrPacketEncoding) {
		t.Fatal("want ErrPacketEncoding")
	}
}
