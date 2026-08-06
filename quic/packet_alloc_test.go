package quic

import (
	"reflect"
	"testing"
)

func allocBenchKeys() PacketKeys {
	k, _ := InitialKeys([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	return k
}

// TestConnFrameHandler_ResetClearsEveryField guards the hazard created by
// reusing one connFrameHandler across packets: a field left set carries a
// previous packet's state into the next one — a stale sawAck runs loss
// detection against ranges this packet never carried.
//
// Structural on purpose. A field added to connFrameHandler later and forgotten
// in reset fails this test rather than shipping as a silent cross-packet leak.
func TestConnFrameHandler_ResetClearsEveryField(t *testing.T) {
	var h connFrameHandler
	h.ackEliciting = true
	h.sawAck = true
	h.ackLow = 1234
	h.priorInFlight = 5678

	c := &Conn{}
	h.reset(c, 2)

	if h.c != c || h.space != 2 {
		t.Fatalf("reset did not re-arm identity: c=%p space=%d", h.c, h.space)
	}
	v := reflect.ValueOf(h)
	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		name := typ.Field(i).Name
		switch name {
		case "c", "space", "nopFrameHandler":
			continue // identity, set by reset; nopFrameHandler is stateless
		}
		if !v.Field(i).IsZero() {
			t.Errorf("reset left %s non-zero (%v) — it will leak into the next packet", name, v.Field(i))
		}
	}
}

// BenchmarkSealPacket and BenchmarkOpenPacket exist to hold the QUIC packet
// crypto path inside the bench-gate's zero-allocation guarantee. Both used to
// allocate — the AEAD nonce and the header-protection mask were built on the
// stack and returned by value into interface calls (cipher.AEAD.Seal,
// headerProtector.headerMask), so both escaped: 32 B and 2 allocs on every
// packet sent. Nothing caught it because the package had no packet-crypto
// benchmark at all, only send-path ones that self-skip.
func BenchmarkSealPacket(b *testing.B) {
	s, err := NewSealer(allocBenchKeys())
	if err != nil {
		b.Fatal(err)
	}
	hdr := make([]byte, 20)
	payload := make([]byte, 1100)
	dst := make([]byte, 0, 1500)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Seal(dst[:0], hdr, 16, 4, uint64(i), payload)
	}
}

func BenchmarkOpenPacket(b *testing.B) {
	k := allocBenchKeys()
	s, _ := NewSealer(k)
	o, _ := NewOpener(k)
	hdr := make([]byte, 20)
	payload := make([]byte, 1100)
	pkt, err := s.Seal(nil, hdr, 16, 4, 7, payload)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, len(pkt))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(buf, pkt)
		_, _, _, _ = o.unprotectHeader(buf, 16, 0)
	}
}
