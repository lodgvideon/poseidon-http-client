package quic

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// capturePC records every datagram written; reads return EOF.
type capturePC struct{ pkts [][]byte }

func (p *capturePC) Write(b []byte) (int, error) {
	p.pkts = append(p.pkts, append([]byte(nil), b...))
	return len(b), nil
}
func (p *capturePC) Read([]byte) (int, error) { return 0, io.EOF }
func (p *capturePC) Close() error             { return nil }

// frameCollector records the send-relevant frames decoded off the wire.
type frameCollector struct {
	nopFrameHandler
	streams     []streamRec
	sdBlocked   []blockedRec
	dataBlocked []uint64
}

type streamRec struct {
	id, offset uint64
	fin        bool
	data       []byte
}
type blockedRec struct{ id, limit uint64 }

func (h *frameCollector) OnStream(id, off uint64, fin bool, data []byte) error {
	h.streams = append(h.streams, streamRec{id, off, fin, append([]byte(nil), data...)})
	return nil
}
func (h *frameCollector) OnStreamDataBlocked(id, limit uint64) error {
	h.sdBlocked = append(h.sdBlocked, blockedRec{id, limit})
	return nil
}
func (h *frameCollector) OnDataBlocked(limit uint64) error {
	h.dataBlocked = append(h.dataBlocked, limit)
	return nil
}

// sendTestConn builds a Conn with an installed 1-RTT sealer, an open stream, a
// capturing PacketConn, and the paired Opener to decrypt what it writes.
func sendTestConn(t *testing.T, streamMax, connMax uint64) (*Conn, *Stream, *capturePC, *Opener) {
	t.Helper()
	dcid := []byte("sendtst0")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	require.NoError(t, err, "NewSealer for the fixture's 1-RTT keys")
	opener, err := NewOpener(keys)
	require.NoError(t, err, "NewOpener for the fixture's 1-RTT keys")
	pc := &capturePC{}
	c := &Conn{
		pc:           pc,
		dcid:         dcid,
		oneRTTSealer: sealer,
		peer: TransportParams{
			InitialMaxStreamsBidi:          8,
			InitialMaxData:                 connMax,
			InitialMaxStreamDataBidiRemote: streamMax,
		},
		connMax: connMax,
	}
	s, err := c.OpenStream()
	require.NoError(t, err, "OpenStream on the fixture connection")
	return c, s, pc, opener
}

// collectFrames decrypts every captured datagram and dispatches its frames.
func collectFrames(t *testing.T, c *Conn, opener *Opener, pc *capturePC) *frameCollector {
	t.Helper()
	h := &frameCollector{}
	for i, pkt := range pc.pkts {
		hdr, err := ParseHeader(pkt, len(c.dcid))
		require.NoErrorf(t, err, "pkt %d ParseHeader", i)
		_, _, payload, err := opener.Open(pkt, hdr.PNOffset, 0)
		require.NoErrorf(t, err, "pkt %d Open", i)
		require.NoErrorf(t, ParseFrames(payload, h), "pkt %d ParseFrames", i)
	}
	return h
}

// TestConn_SendStream_Wire is the load-bearing send round-trip: a request body
// sent over 1-RTT decrypts back to exactly one STREAM frame with the FIN set.
func TestConn_SendStream_Wire(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 1<<20, 1<<20)
	body := []byte("GET / HTTP/3\r\n\r\n")

	n, err := s.Send(body, true)

	require.NoError(t, err, "Send the request body with FIN")
	require.Equalf(t, len(body), n, "sent %d, want %d", n, len(body))
	h := collectFrames(t, c, op, pc)
	require.Lenf(t, h.streams, 1, "got %d STREAM frames, want 1", len(h.streams))
	f := h.streams[0]
	assert.Truef(t, f.id == s.ID() && f.offset == 0 && f.fin && bytes.Equal(f.data, body),
		"frame = {id:%d off:%d fin:%v data:%q}, want {id:%d off:0 fin:true data:%q}",
		f.id, f.offset, f.fin, f.data, s.ID(), body)
	assert.True(t, s.finSent, "finSent not set after FIN")
}

func TestConformance_RFC9000_Sec41_SendClampsToMinLimit(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 1<<20, 40) // connection credit is the binding limit

	n, err := s.Send(make([]byte, 100), false)

	require.NoError(t, err, "Send past the connection credit")
	assert.Equalf(t, 40, n, "sent %d, want 40 (min of stream/conn credit)", n)
	assert.Truef(t, c.connSent == 40 && s.sendOffset == 40,
		"connSent=%d sendOffset=%d, want 40/40", c.connSent, s.sendOffset)
	h := collectFrames(t, c, op, pc)
	assert.Truef(t, len(h.streams) == 1 && len(h.streams[0].data) == 40,
		"want one 40-byte STREAM frame, got %+v", h.streams)
	assert.Truef(t, len(h.dataBlocked) == 1 && h.dataBlocked[0] == 40,
		"dataBlocked=%v, want [40]", h.dataBlocked)
}

func TestConformance_RFC9000_Sec1913_StreamDataBlocked(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 0, 1<<20) // zero per-stream credit

	n, err := s.Send([]byte("data"), false)
	// Informational; emitted once per distinct limit — a retry adds no frame.
	_, retryErr := s.Send([]byte("data"), false)

	require.Truef(t, n == 0 && err == nil, "Send = (%d,%v), want (0,nil)", n, err)
	require.NoError(t, retryErr, "the retry at the same limit must not error")
	h := collectFrames(t, c, op, pc)
	assert.Emptyf(t, h.streams, "no STREAM frames expected, got %d", len(h.streams))
	assert.Truef(t, len(h.sdBlocked) == 1 && h.sdBlocked[0].limit == 0 && h.sdBlocked[0].id == s.ID(),
		"sdBlocked=%v, want one at id %d limit 0", h.sdBlocked, s.ID())
}

func TestConformance_RFC9000_Sec1912_DataBlocked(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 1<<20, 0) // zero connection credit

	n, err := s.Send([]byte("data"), false)

	require.Truef(t, n == 0 && err == nil, "Send = (%d,%v), want (0,nil)", n, err)
	h := collectFrames(t, c, op, pc)
	assert.Truef(t, len(h.dataBlocked) == 1 && h.dataBlocked[0] == 0,
		"dataBlocked=%v, want [0]", h.dataBlocked)
}

func TestConformance_RFC9000_Sec199_MaxDataRaisesLimit(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 1<<20, 0)
	_, err := s.Send([]byte("hello"), false) // blocked at conn level
	require.NoError(t, err, "the first Send is expected to block, not fail")
	h := &connFrameHandler{c: c}

	require.NoError(t, h.OnMaxData(3), "OnMaxData(3) raises the connection limit")
	_ = h.OnMaxData(2)                       // non-increasing: ignored (§4.1)
	n, err := s.Send([]byte("hello"), false) // retry the still-unsent buffer

	assert.EqualValuesf(t, 3, c.connMax, "connMax=%d, want 3 (non-increasing ignored)", c.connMax)
	require.NoError(t, err, "Send after the raised limit")
	assert.Equalf(t, 3, n, "sent %d after MAX_DATA=3, want 3", n)
	col := collectFrames(t, c, op, pc)
	var data []byte
	for _, f := range col.streams {
		data = append(data, f.data...)
	}
	assert.Equalf(t, "hel", string(data), "sent stream data %q, want %q", data, "hel")
}

// TestConformance_RFC9000_Sec1910_MaxStreamDataRaisesLimit also covers the
// FIN-withheld-while-blocked rule: a FIN is not sent until all data is admitted.
func TestConformance_RFC9000_Sec1910_MaxStreamDataRaisesLimit(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 3, 1<<20) // per-stream credit 3
	h := &connFrameHandler{c: c}

	n, err := s.Send([]byte("hello"), true)
	require.NoError(t, err, "the first Send is expected to block, not fail")
	finWithheld := !s.finSent
	require.NoError(t, h.OnMaxStreamData(s.ID(), 5), "raise the stream credit to 5")
	n2, err2 := s.Send([]byte("lo"), true) // remainder + FIN

	assert.Equalf(t, 3, n, "sent %d, want 3", n)
	assert.True(t, finWithheld, "FIN must be withheld while data is flow-control blocked")
	require.NoError(t, err2, "the remainder Send after the raised limit")
	assert.Truef(t, n2 == 2 && s.finSent,
		"remainder send = (%d, finSent=%v), want (2, true)", n2, s.finSent)
	col := collectFrames(t, c, op, pc)
	var data []byte
	var sawFin bool
	for _, f := range col.streams {
		data = append(data, f.data...)
		sawFin = sawFin || f.fin
	}
	assert.Truef(t, string(data) == "hello" && sawFin,
		"reassembled %q sawFin=%v, want \"hello\" true", data, sawFin)
	assert.Truef(t, len(col.sdBlocked) == 1 && col.sdBlocked[0].limit == 3,
		"sdBlocked=%v, want one at limit 3", col.sdBlocked)
}

// TestConformance_RFC9000_Sec45_FinBypassesFlowControl verifies the FIN-accounting
// rule: a FIN consumes no flow-control credit, so a bare end-of-stream marker is
// sent even when both the stream and connection are at zero credit.
func TestConformance_RFC9000_Sec45_FinBypassesFlowControl(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 0, 0) // zero credit at both levels

	n, err := s.Send(nil, true)

	require.NoError(t, err, "a bare FIN must be sendable at zero credit")
	assert.Truef(t, n == 0 && s.finSent,
		"bare FIN send = (%d, finSent=%v), want (0, true)", n, s.finSent)
	h := collectFrames(t, c, op, pc)
	assert.Truef(t, len(h.streams) == 1 && h.streams[0].fin && len(h.streams[0].data) == 0,
		"want one zero-length STREAM+FIN, got %+v", h.streams)
	assert.Truef(t, len(h.sdBlocked) == 0 && len(h.dataBlocked) == 0,
		"a bare FIN must not emit BLOCKED frames")
}

func TestConformance_RFC9000_Sec45_FinalSizeLatch(t *testing.T) {
	_, s, _, _ := sendTestConn(t, 1<<20, 1<<20)
	_, err := s.Send([]byte("hi"), true)
	require.NoError(t, err, "the first Send establishes the final size")

	_, errAfterFin := s.Send([]byte("more"), true)

	assert.True(t, s.finSent, "finSent not set")
	assert.Truef(t, errAfterFin == ErrStreamFinished,
		"Send after FIN err = %v, want ErrStreamFinished", errAfterFin)
}

func TestConformance_RFC9000_Sec198_StreamSendChunking(t *testing.T) {
	c, s, pc, op := sendTestConn(t, 1<<20, 1<<20)
	body := make([]byte, 3000)
	for i := range body {
		body[i] = byte(i)
	}

	n, err := s.Send(body, true)

	require.NoError(t, err, "Send a body larger than one datagram")
	require.Equalf(t, len(body), n, "sent %d, want %d", n, len(body))
	h := collectFrames(t, c, op, pc)
	require.GreaterOrEqualf(t, len(h.streams), 2,
		"body larger than one datagram must split; got %d frames", len(h.streams))
	var reassembled []byte
	for i, f := range h.streams {
		assert.Equalf(t, uint64(len(reassembled)), f.offset,
			"frame %d offset=%d, want %d (monotonic, contiguous)", i, f.offset, len(reassembled))
		assert.Equalf(t, i == len(h.streams)-1, f.fin,
			"frame %d fin=%v, want %v (FIN only on last)", i, f.fin, i == len(h.streams)-1)
		reassembled = append(reassembled, f.data...)
	}
	assert.True(t, bytes.Equal(reassembled, body),
		"reassembled body does not match sent body")
}

func TestStream_Send_NotEstablished(t *testing.T) {
	c := &Conn{peer: TransportParams{InitialMaxStreamsBidi: 1}}
	s, err := c.OpenStream()
	require.NoError(t, err, "OpenStream before the 1-RTT keys exist")

	_, err = s.Send([]byte("x"), false)

	assert.Truef(t, err == ErrNotEstablished,
		"Send before 1-RTT keys err = %v, want ErrNotEstablished", err)
}

func TestStream_Grantable_Unit(t *testing.T) {
	cases := []struct {
		name                                   string
		sendMax, sendOffset, connMax, connSent uint64
		remaining                              int
		wantN                                  int
		wantBlock                              blockKind
	}{
		{"stream-clamp", 10, 0, 1000, 0, 50, 10, blockNone},
		{"conn-clamp", 1000, 0, 10, 0, 50, 10, blockNone},
		{"remaining-clamp", 1000, 0, 1000, 0, 3, 3, blockNone},
		{"block-stream", 5, 5, 1000, 0, 10, 0, blockStream},
		{"block-conn", 1000, 0, 5, 5, 10, 0, blockConn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{dcid: make([]byte, 8), connMax: tc.connMax, connSent: tc.connSent}
			s := &Stream{conn: c, sendMax: tc.sendMax, sendOffset: tc.sendOffset}

			n, blk := s.grantable(tc.remaining)

			assert.Truef(t, n == tc.wantN && blk == tc.wantBlock,
				"grantable(%d) = (%d,%v), want (%d,%v)", tc.remaining, n, blk, tc.wantN, tc.wantBlock)
		})
	}
}
