package quic

import (
	"bytes"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
)

// putVarint encodes v as a QUIC varint.
func putVarint(v uint64) []byte {
	b := make([]byte, bytesx.VarintLen(v))
	bytesx.WriteVarint(b, v)
	return b
}

// tpInt encodes one integer transport parameter (id + length + varint value).
func tpInt(id, val uint64) []byte {
	value := putVarint(val)
	out := putVarint(id)
	out = append(out, putVarint(uint64(len(value)))...)
	return append(out, value...)
}

// tpBytes encodes one transport parameter whose value is raw bytes, e.g. a
// connection ID (id + length + bytes).
func tpBytes(id uint64, value []byte) []byte {
	out := putVarint(id)
	out = append(out, putVarint(uint64(len(value)))...)
	return append(out, value...)
}

func concat(chunks ...[]byte) []byte {
	var out []byte
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out
}

func TestConformance_RFC9000_Sec18_TransportParamsParse(t *testing.T) {
	raw := concat(
		tpInt(tpInitialMaxData, 100000),
		tpInt(tpInitialMaxStreamDataBidiRemote, 50000),
		tpInt(tpInitialMaxStreamsBidi, 10),
		tpInt(tpMaxUDPPayloadSize, 1500), // valid (>=1200), parsed then discarded
	)
	tp, err := ParseTransportParams(raw)
	if err != nil {
		t.Fatal(err)
	}
	if tp.InitialMaxData != 100000 {
		t.Errorf("InitialMaxData = %d, want 100000", tp.InitialMaxData)
	}
	if tp.InitialMaxStreamDataBidiRemote != 50000 {
		t.Errorf("bidi_remote = %d, want 50000", tp.InitialMaxStreamDataBidiRemote)
	}
	if tp.InitialMaxStreamsBidi != 10 {
		t.Errorf("streams_bidi = %d, want 10", tp.InitialMaxStreamsBidi)
	}
}

// TestConformance_RFC9000_Sec182_BidiRemoteBoundsClientStream pins the critical
// direction fact: a client's request stream is bounded by the server's
// initial_max_stream_data_bidi_remote (0x06), NOT _bidi_local (0x05).
func TestConformance_RFC9000_Sec182_BidiRemoteBoundsClientStream(t *testing.T) {
	raw := concat(
		tpInt(0x05, 999), // bidi_local — must NOT bound our stream
		tpInt(tpInitialMaxStreamDataBidiRemote, 7000), // bidi_remote — the one that does
		tpInt(tpInitialMaxStreamsBidi, 4),
	)
	tp, err := ParseTransportParams(raw)
	if err != nil {
		t.Fatal(err)
	}
	c := &Conn{peer: tp}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if s.sendMax != 7000 {
		t.Fatalf("stream sendMax = %d, want 7000 (bidi_remote, not bidi_local 999)", s.sendMax)
	}
}

func TestConformance_RFC9000_Sec74_DuplicateParam(t *testing.T) {
	raw := concat(tpInt(tpInitialMaxData, 10), tpInt(tpInitialMaxData, 20))
	if _, err := ParseTransportParams(raw); err != ErrTransportParameter {
		t.Fatalf("err = %v, want ErrTransportParameter", err)
	}
}

func TestConformance_RFC9000_Sec74_MalformedParam(t *testing.T) {
	// (a) declared Length runs past the end of the buffer.
	pastEnd := concat(putVarint(tpInitialMaxData), putVarint(8), []byte{0x00, 0x00})
	if _, err := ParseTransportParams(pastEnd); err != ErrTransportParameter {
		t.Fatalf("past-end err = %v, want ErrTransportParameter", err)
	}
	// (b) integer value whose Length disagrees with the varint's own length.
	badInt := concat(putVarint(tpInitialMaxData), putVarint(3), []byte{0x01, 0x00, 0x00})
	if _, err := ParseTransportParams(badInt); err != ErrTransportParameter {
		t.Fatalf("bad-int err = %v, want ErrTransportParameter", err)
	}
	// (c) truncated identifier / length mid-parameter.
	truncated := putVarint(tpInitialMaxData) // id present, no length byte
	if _, err := ParseTransportParams(truncated); err != ErrTransportParameter {
		t.Fatalf("truncated err = %v, want ErrTransportParameter", err)
	}
}

func TestConformance_RFC9000_Sec74_InvalidValue(t *testing.T) {
	if _, err := ParseTransportParams(tpInt(tpMaxUDPPayloadSize, 1000)); err != ErrTransportParameter {
		t.Fatalf("max_udp_payload_size<1200 err = %v, want ErrTransportParameter", err)
	}
	if _, err := ParseTransportParams(tpInt(tpActiveConnectionIDLimit, 1)); err != ErrTransportParameter {
		t.Fatalf("active_connection_id_limit<2 err = %v, want ErrTransportParameter", err)
	}
}

func TestTransportParams_UnknownAndGREASEIgnored(t *testing.T) {
	raw := concat(
		tpInt(0x555, 1),             // unknown identifier
		tpInt(31*4+27, 2),           // GREASE identifier (31N+27)
		tpInt(tpInitialMaxData, 42), // a known param after them still parses
	)
	tp, err := ParseTransportParams(raw)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if tp.InitialMaxData != 42 {
		t.Fatalf("InitialMaxData = %d, want 42", tp.InitialMaxData)
	}
}

// TestConformance_RFC9000_Sec18_TransportParamsEncode checks the client's
// transport-parameter encoder: every advertised parameter is present with the
// right value, and the peer's own decoder (ParseTransportParams) accepts the
// encoding — including the raw-bytes initial_source_connection_id (§7.3).
func TestConformance_RFC9000_Sec18_TransportParamsEncode(t *testing.T) {
	p := LocalTransportParams{
		InitialMaxData:                1 << 20,
		InitialMaxStreamDataBidiLocal: 1 << 16,
		InitialMaxStreamDataUni:       1 << 14,
		InitialMaxStreamsUni:          3,
		SourceConnectionID:            []byte{0xaa, 0xbb, 0xcc},
	}
	enc := AppendTransportParams(nil, p)

	// Walk the encoding into id -> raw value.
	got := map[uint64][]byte{}
	for rest := enc; len(rest) > 0; {
		id, n := bytesx.ReadVarint(rest)
		rest = rest[n:]
		length, n := bytesx.ReadVarint(rest)
		rest = rest[n:]
		got[id] = rest[:length]
		rest = rest[length:]
	}
	for id, want := range map[uint64]uint64{
		tpInitialMaxData:                1 << 20,
		tpInitialMaxStreamDataBidiLocal: 1 << 16,
		tpInitialMaxStreamDataUni:       1 << 14,
		tpInitialMaxStreamsUni:          3,
	} {
		v, ok := tpReadUint(got[id])
		if !ok || v != want {
			t.Errorf("param %#x = %d (ok=%v), want %d", id, v, ok, want)
		}
	}
	if !bytes.Equal(got[tpInitialSourceConnectionID], []byte{0xaa, 0xbb, 0xcc}) {
		t.Errorf("initial_source_connection_id = %x, want aabbcc", got[tpInitialSourceConnectionID])
	}

	// The peer's decoder accepts our encoding and reads back the send limits.
	tp, err := ParseTransportParams(enc)
	if err != nil {
		t.Fatal(err)
	}
	if tp.InitialMaxData != 1<<20 || tp.InitialMaxStreamDataUni != 1<<14 || tp.InitialMaxStreamsUni != 3 {
		t.Fatalf("round-trip parsed = %+v", tp)
	}

	// Zero-value params (incl. a zero-length source CID) must still encode and parse.
	if _, err := ParseTransportParams(AppendTransportParams(nil, LocalTransportParams{})); err != nil {
		t.Fatalf("zero-value params must parse: %v", err)
	}
}

func TestTransportParams_UniLimits(t *testing.T) {
	raw := concat(tpInt(tpInitialMaxStreamDataUni, 8000), tpInt(tpInitialMaxStreamsUni, 3))
	tp, err := ParseTransportParams(raw)
	if err != nil {
		t.Fatal(err)
	}
	if tp.InitialMaxStreamDataUni != 8000 {
		t.Errorf("InitialMaxStreamDataUni = %d, want 8000", tp.InitialMaxStreamDataUni)
	}
	if tp.InitialMaxStreamsUni != 3 {
		t.Errorf("InitialMaxStreamsUni = %d, want 3", tp.InitialMaxStreamsUni)
	}
}

func TestTransportParams_AbsentDefaults(t *testing.T) {
	tp, err := ParseTransportParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	if tp.InitialMaxData != 0 || tp.InitialMaxStreamDataBidiRemote != 0 || tp.InitialMaxStreamsBidi != 0 {
		t.Fatalf("absent flow-control params must default to 0, got %+v", tp)
	}
}
