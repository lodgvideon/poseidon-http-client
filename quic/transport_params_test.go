package quic

import (
	"bytes"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/internal/bytesx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	require.NoError(t, err, "ParseTransportParams on a well-formed encoding")
	assert.EqualValuesf(t, 100000, tp.InitialMaxData,
		"InitialMaxData = %d, want 100000", tp.InitialMaxData)
	assert.EqualValuesf(t, 50000, tp.InitialMaxStreamDataBidiRemote,
		"bidi_remote = %d, want 50000", tp.InitialMaxStreamDataBidiRemote)
	assert.EqualValuesf(t, 10, tp.InitialMaxStreamsBidi,
		"streams_bidi = %d, want 10", tp.InitialMaxStreamsBidi)
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
	require.NoError(t, err, "ParseTransportParams on the bidi_local/bidi_remote pair")
	c := &Conn{peer: tp}

	s, err := c.OpenStream()

	require.NoError(t, err, "OpenStream under the parsed peer limits")
	assert.EqualValuesf(t, 7000, s.sendMax,
		"stream sendMax = %d, want 7000 (bidi_remote, not bidi_local 999)", s.sendMax)
}

func TestConformance_RFC9000_Sec74_DuplicateParam(t *testing.T) {
	raw := concat(tpInt(tpInitialMaxData, 10), tpInt(tpInitialMaxData, 20))

	_, err := ParseTransportParams(raw)

	assert.Truef(t, err == ErrTransportParameter,
		"err = %v, want ErrTransportParameter", err)
}

func TestConformance_RFC9000_Sec74_MalformedParam(t *testing.T) {
	// (a) declared Length runs past the end of the buffer.
	pastEnd := concat(putVarint(tpInitialMaxData), putVarint(8), []byte{0x00, 0x00})
	// (b) integer value whose Length disagrees with the varint's own length.
	badInt := concat(putVarint(tpInitialMaxData), putVarint(3), []byte{0x01, 0x00, 0x00})
	// (c) truncated identifier / length mid-parameter.
	truncated := putVarint(tpInitialMaxData) // id present, no length byte

	_, errPastEnd := ParseTransportParams(pastEnd)
	_, errBadInt := ParseTransportParams(badInt)
	_, errTruncated := ParseTransportParams(truncated)

	assert.Truef(t, errPastEnd == ErrTransportParameter,
		"past-end err = %v, want ErrTransportParameter", errPastEnd)
	assert.Truef(t, errBadInt == ErrTransportParameter,
		"bad-int err = %v, want ErrTransportParameter", errBadInt)
	assert.Truef(t, errTruncated == ErrTransportParameter,
		"truncated err = %v, want ErrTransportParameter", errTruncated)
}

func TestConformance_RFC9000_Sec74_InvalidValue(t *testing.T) {
	_, errPayload := ParseTransportParams(tpInt(tpMaxUDPPayloadSize, 1000))
	_, errCIDLimit := ParseTransportParams(tpInt(tpActiveConnectionIDLimit, 1))

	assert.Truef(t, errPayload == ErrTransportParameter,
		"max_udp_payload_size<1200 err = %v, want ErrTransportParameter", errPayload)
	assert.Truef(t, errCIDLimit == ErrTransportParameter,
		"active_connection_id_limit<2 err = %v, want ErrTransportParameter", errCIDLimit)
}

func TestTransportParams_UnknownAndGREASEIgnored(t *testing.T) {
	raw := concat(
		tpInt(0x555, 1),             // unknown identifier
		tpInt(31*4+27, 2),           // GREASE identifier (31N+27)
		tpInt(tpInitialMaxData, 42), // a known param after them still parses
	)
	tp, err := ParseTransportParams(raw)

	require.NoError(t, err, "unknown and GREASE identifiers must be skipped, not rejected")
	assert.EqualValuesf(t, 42, tp.InitialMaxData,
		"InitialMaxData = %d, want 42", tp.InitialMaxData)
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
		assert.Truef(t, ok && v == want, "param %#x = %d (ok=%v), want %d", id, v, ok, want)
	}
	assert.Truef(t, bytes.Equal(got[tpInitialSourceConnectionID], []byte{0xaa, 0xbb, 0xcc}),
		"initial_source_connection_id = %x, want aabbcc", got[tpInitialSourceConnectionID])

	// The peer's decoder accepts our encoding and reads back the send limits.
	tp, err := ParseTransportParams(enc)
	require.NoError(t, err, "our own encoding must parse with the peer's decoder")
	assert.Truef(t, tp.InitialMaxData == 1<<20 && tp.InitialMaxStreamDataUni == 1<<14 && tp.InitialMaxStreamsUni == 3,
		"round-trip parsed = %+v", tp)

	// Zero-value params (incl. a zero-length source CID) must still encode and parse.
	_, err = ParseTransportParams(AppendTransportParams(nil, LocalTransportParams{}))
	assert.NoError(t, err, "zero-value params must parse")
}

func TestTransportParams_UniLimits(t *testing.T) {
	raw := concat(tpInt(tpInitialMaxStreamDataUni, 8000), tpInt(tpInitialMaxStreamsUni, 3))

	tp, err := ParseTransportParams(raw)

	require.NoError(t, err, "ParseTransportParams on the unidirectional limits")
	assert.EqualValuesf(t, 8000, tp.InitialMaxStreamDataUni,
		"InitialMaxStreamDataUni = %d, want 8000", tp.InitialMaxStreamDataUni)
	assert.EqualValuesf(t, 3, tp.InitialMaxStreamsUni,
		"InitialMaxStreamsUni = %d, want 3", tp.InitialMaxStreamsUni)
}

func TestTransportParams_AbsentDefaults(t *testing.T) {
	tp, err := ParseTransportParams(nil)

	require.NoError(t, err, "an empty transport-parameter block must parse")
	assert.Truef(t, tp.InitialMaxData == 0 && tp.InitialMaxStreamDataBidiRemote == 0 && tp.InitialMaxStreamsBidi == 0,
		"absent flow-control params must default to 0, got %+v", tp)
}
