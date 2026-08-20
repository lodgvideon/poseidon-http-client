package quic

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recHandler records each parsed frame as a formatted string for assertion.
type recHandler struct{ log []string }

func (r *recHandler) add(s string, a ...any) error {
	r.log = append(r.log, fmt.Sprintf(s, a...))
	return nil
}

func (r *recHandler) OnPadding(n int) error { return r.add("padding %d", n) }
func (r *recHandler) OnPing() error         { return r.add("ping") }
func (r *recHandler) OnAck(l, d, f uint64) error {
	return r.add("ack largest=%d delay=%d first=%d", l, d, f)
}
func (r *recHandler) OnAckRange(g, n uint64) error  { return r.add("ackrange gap=%d len=%d", g, n) }
func (r *recHandler) OnAckECN(a, b, c uint64) error { return r.add("ackecn %d %d %d", a, b, c) }
func (r *recHandler) OnResetStream(s, e, f uint64) error {
	return r.add("reset id=%d err=%d final=%d", s, e, f)
}
func (r *recHandler) OnStopSending(s, e uint64) error   { return r.add("stopsending id=%d err=%d", s, e) }
func (r *recHandler) OnCrypto(o uint64, d []byte) error { return r.add("crypto off=%d data=%x", o, d) }
func (r *recHandler) OnNewToken(t []byte) error         { return r.add("newtoken %x", t) }
func (r *recHandler) OnStream(s, o uint64, f bool, d []byte) error {
	return r.add("stream id=%d off=%d fin=%v data=%x", s, o, f, d)
}
func (r *recHandler) OnMaxData(m uint64) error { return r.add("maxdata %d", m) }
func (r *recHandler) OnMaxStreamData(s, m uint64) error {
	return r.add("maxstreamdata id=%d max=%d", s, m)
}
func (r *recHandler) OnMaxStreams(u bool, m uint64) error {
	return r.add("maxstreams uni=%v max=%d", u, m)
}
func (r *recHandler) OnDataBlocked(l uint64) error { return r.add("datablocked %d", l) }
func (r *recHandler) OnStreamDataBlocked(s, l uint64) error {
	return r.add("streamdatablocked id=%d lim=%d", s, l)
}
func (r *recHandler) OnStreamsBlocked(u bool, l uint64) error {
	return r.add("streamsblocked uni=%v lim=%d", u, l)
}
func (r *recHandler) OnNewConnectionID(s, rp uint64, c []byte, t *[16]byte) error {
	return r.add("newcid seq=%d retire=%d cid=%x token=%x", s, rp, c, *t)
}
func (r *recHandler) OnRetireConnectionID(s uint64) error { return r.add("retirecid %d", s) }
func (r *recHandler) OnPathChallenge(d *[8]byte) error    { return r.add("pathchallenge %x", *d) }
func (r *recHandler) OnPathResponse(d *[8]byte) error     { return r.add("pathresponse %x", *d) }
func (r *recHandler) OnConnectionClose(app bool, e, ft uint64, reason []byte) error {
	return r.add("close app=%v err=%d frametype=%d reason=%s", app, e, ft, reason)
}
func (r *recHandler) OnHandshakeDone() error { return r.add("handshakedone") }

func parse(t *testing.T, payload []byte) []string {
	t.Helper()
	var r recHandler
	require.NoErrorf(t, ParseFrames(payload, &r), "ParseFrames(%x)", payload)
	return r.log
}

func eq(t *testing.T, got, want []string) {
	t.Helper()
	require.Lenf(t, got, len(want), "got %d frames %v, want %d %v", len(got), got, len(want), want)
	for i := range want {
		assert.Equalf(t, want[i], got[i], "frame %d = %q, want %q", i, got[i], want[i])
	}
}

// TestFrames_RoundTrip writes one of each writable frame into a packet payload,
// parses it back, and checks each frame survives in order.
func TestFrames_RoundTrip(t *testing.T) {
	var b []byte
	b = appendPing(b)
	b = AppendCrypto(b, 0, []byte("hello"))
	b = AppendStream(b, 4, 0, false, []byte("GET"))
	b = AppendStream(b, 8, 100, true, []byte("body"))
	b = AppendAck(b, 10, 3, 2, []AckRange{{Gap: 1, Length: 3}})
	b = AppendMaxData(b, 1<<20)
	b = AppendMaxStreamData(b, 4, 65535)
	b = AppendMaxStreams(b, true, 100)
	b = appendDataBlocked(b, 42)
	b = appendStreamDataBlocked(b, 4, 7)
	b = appendStreamsBlocked(b, false, 5)
	b = AppendResetStream(b, 4, 0x10e, 12)
	b = appendStopSending(b, 8, 0x101)
	b = appendNewToken(b, []byte{0xde, 0xad})
	b = appendRetireConnectionID(b, 3)
	b = appendPathResponse(b, [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	b = AppendConnectionClose(b, false, ErrCodeProtocolViolation, FrameStreamBase, []byte("bad"))
	b = AppendConnectionClose(b, true, 0x100, 0, nil)
	b = appendHandshakeDone(b)
	b = appendPadding(b, 4)

	want := []string{
		"ping",
		"crypto off=0 data=68656c6c6f",
		"stream id=4 off=0 fin=false data=474554",
		"stream id=8 off=100 fin=true data=626f6479",
		"ack largest=10 delay=3 first=2",
		"ackrange gap=1 len=3",
		"maxdata 1048576",
		"maxstreamdata id=4 max=65535",
		"maxstreams uni=true max=100",
		"datablocked 42",
		"streamdatablocked id=4 lim=7",
		"streamsblocked uni=false lim=5",
		"reset id=4 err=270 final=12",
		"stopsending id=8 err=257",
		"newtoken dead",
		"retirecid 3",
		"pathresponse 0102030405060708",
		"close app=false err=10 frametype=8 reason=bad",
		"close app=true err=256 frametype=0 reason=",
		"handshakedone",
		"padding 4",
	}

	got := parse(t, b)

	eq(t, got, want)
}

// TestConformance_RFC9000_Sec19_StreamFrame decodes a hand-built STREAM frame
// (type 0x0e = OFF+LEN, no FIN) from the §19.8 layout.
func TestConformance_RFC9000_Sec19_StreamFrame(t *testing.T) {
	// 0x0e | id=4 | offset=64 (0x4040) | len=3 | "abc"
	in := []byte{0x0e, 0x04, 0x40, 0x40, 0x03, 'a', 'b', 'c'}

	got := parse(t, in)

	eq(t, got, []string{"stream id=4 off=64 fin=false data=616263"})
}

// TestConformance_RFC9000_Sec193_AckFrame decodes a hand-built ACK frame with
// one additional range (§19.3).
func TestConformance_RFC9000_Sec193_AckFrame(t *testing.T) {
	// 0x02 | largest=10 | delay=0 | rangeCount=1 | firstRange=2 | gap=1 | len=3
	in := []byte{0x02, 0x0a, 0x00, 0x01, 0x02, 0x01, 0x03}

	got := parse(t, in)

	eq(t, got, []string{"ack largest=10 delay=0 first=2", "ackrange gap=1 len=3"})
}

// TestConformance_RFC9000_Sec193_AckECN decodes the ECN variant (0x03) with its
// three ECT0/ECT1/CE counts, in RFC order.
func TestConformance_RFC9000_Sec193_AckECN(t *testing.T) {
	// 0x03 | largest=5 | delay=0 | rangeCount=0 | firstRange=5 | ect0=1 ect1=2 ce=3
	in := []byte{0x03, 0x05, 0x00, 0x00, 0x05, 0x01, 0x02, 0x03}

	got := parse(t, in)

	eq(t, got, []string{"ack largest=5 delay=0 first=5", "ackecn 1 2 3"})
}

// TestConformance_RFC9000_Sec1919_ConnectionClose decodes both the transport
// (0x1c, with a Frame Type field) and application (0x1d, without) variants.
func TestConformance_RFC9000_Sec1919_ConnectionClose(t *testing.T) {
	transport := []byte{0x1c, 0x07, 0x08, 0x02, 'h', 'i'} // err=7 frametype=8 reason="hi"
	app := []byte{0x1d, 0x41, 0x00, 0x00}                 // err=0x100 (varint 0x4100), no reason

	gotTransport, gotApp := parse(t, transport), parse(t, app)

	eq(t, gotTransport, []string{"close app=false err=7 frametype=8 reason=hi"})
	eq(t, gotApp, []string{"close app=true err=256 frametype=0 reason="})
}

// TestConformance_RFC9000_Sec1915_NewConnectionID decodes a NEW_CONNECTION_ID
// with an 8-byte CID and the 16-byte reset token (§19.15).
func TestConformance_RFC9000_Sec1915_NewConnectionID(t *testing.T) {
	in := []byte{0x18, 0x01, 0x00, 0x08}
	in = append(in, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88) // CID
	for i := 0; i < 16; i++ {
		in = append(in, 0xab) // reset token
	}

	got := parse(t, in)

	eq(t, got, []string{"newcid seq=1 retire=0 cid=1122334455667788 token=abababababababababababababababab"})
}

// TestConformance_RFC9000_Sec191_Padding coalesces a run of zero bytes into one
// PADDING report, then continues parsing (§19.1).
func TestConformance_RFC9000_Sec191_Padding(t *testing.T) {
	in := []byte{0x00, 0x00, 0x00, 0x01}

	got := parse(t, in)

	eq(t, got, []string{"padding 3", "ping"})
}

func TestParseFrames_Malformed(t *testing.T) {
	cases := map[string][]byte{
		"unknown_type":        {0x3f}, // 0x3f is not an assigned frame type
		"crypto_truncated":    {0x06, 0x00, 0x05, 'a', 'b'},
		"stream_len_past_end": {0x0a, 0x04, 0x05, 'x'}, // LEN=5 but 1 byte
		"ack_truncated":       {0x02, 0x0a},
		"newcid_bad_len":      {0x18, 0x01, 0x00, 0x21}, // CID length 33 > 20
		"varint_past_end":     {0x10, 0x40},             // MAX_DATA with 2-byte varint prefix, 1 byte
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			err := ParseFrames(in, nopFrameHandler{})

			assert.Truef(t, errors.Is(err, ErrFrameEncoding),
				"ParseFrames(%x) = %v, want ErrFrameEncoding", in, err)
		})
	}
}

func TestParseFrames_HandlerError(t *testing.T) {
	boom := errors.New("boom")
	h := errHandler{err: boom}

	err := ParseFrames([]byte{0x01}, h)

	assert.Truef(t, errors.Is(err, boom), "err = %v, want boom", err)
}

type errHandler struct {
	nopFrameHandler
	err error
}

func (h errHandler) OnPing() error { return h.err }

// TestParseStream_NoLength covers a STREAM frame without the LEN bit (data runs
// to the end of the packet) and the OFF+FIN combination.
func TestParseStream_NoLength(t *testing.T) {
	noFlags := []byte{0x08, 0x04, 'h', 'i'} // no OFF/LEN/FIN: id=4, data "hi" to end
	offFin := []byte{0x0d, 0x04, 0x01, 'x'} // OFF+FIN (no LEN): id=4, off=1, data "x" to end

	gotNoFlags, gotOffFin := parse(t, noFlags), parse(t, offFin)

	eq(t, gotNoFlags, []string{"stream id=4 off=0 fin=false data=6869"})
	eq(t, gotOffFin, []string{"stream id=4 off=1 fin=true data=78"})
}

// TestConformance_RFC9000_Sec1917_PathChallenge decodes PATH_CHALLENGE (§19.17).
func TestConformance_RFC9000_Sec1917_PathChallenge(t *testing.T) {
	in := []byte{0x1a, 1, 2, 3, 4, 5, 6, 7, 8}

	got := parse(t, in)

	eq(t, got, []string{"pathchallenge 0102030405060708"})
}

func TestParseFrames_MoreMalformed(t *testing.T) {
	cases := map[string][]byte{
		"stopsending_truncated":    {0x05, 0x04},
		"resetstream_truncated":    {0x04, 0x04, 0x00},
		"maxstreamdata_truncated":  {0x11, 0x04},
		"maxstreams_truncated":     {0x12},
		"datablocked_truncated":    {0x14},
		"streamdatablocked_trunc":  {0x15, 0x04},
		"streamsblocked_truncated": {0x16},
		"retire_truncated":         {0x19},
		"path_truncated":           {0x1a, 0x01, 0x02},
		"pathresp_truncated":       {0x1b, 0x01},
		"newtoken_empty":           {0x07, 0x00},
		"newtoken_truncated":       {0x07, 0x05, 'a'},
		"newcid_token_truncated":   {0x18, 0x01, 0x00, 0x02, 0xaa, 0xbb, 0x00}, // 2-byte CID, token < 16
		"newcid_retire_past_seq":   {0x18, 0x01, 0x02, 0x08},                   // retire(2) > seq(1)
		"connclose_truncated":      {0x1c, 0x07},
		"connclose_app_truncated":  {0x1d},
		"stream_id_truncated":      {0x08, 0x40}, // 2-byte varint id prefix, 1 byte
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			err := ParseFrames(in, nopFrameHandler{})

			assert.Truef(t, errors.Is(err, ErrFrameEncoding),
				"ParseFrames(%x) = %v, want ErrFrameEncoding", in, err)
		})
	}
}

func BenchmarkParseFrames(b *testing.B) {
	var buf []byte
	buf = AppendStream(buf, 4, 0, true, []byte("GET / HTTP/3"))
	buf = AppendAck(buf, 10, 3, 5, []AckRange{{Gap: 1, Length: 2}})
	buf = AppendMaxData(buf, 1<<20)
	h := nopFrameHandler{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ParseFrames(buf, h); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendStream(b *testing.B) {
	dst := make([]byte, 0, 64)
	data := []byte("GET / HTTP/3")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = AppendStream(dst[:0], 4, 128, true, data)
	}
}

func BenchmarkAppendAck(b *testing.B) {
	dst := make([]byte, 0, 64)
	ranges := []AckRange{{Gap: 1, Length: 2}, {Gap: 3, Length: 4}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = AppendAck(dst[:0], 100, 5, 10, ranges)
	}
}
