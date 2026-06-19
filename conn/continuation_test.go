package conn

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// newWireConn builds a *Conn whose framer writes to buf, with a fixed
// peer MAX_FRAME_SIZE, just enough to exercise writeHeadersWithPriority's
// wire output without a live peer.
func newWireConn(buf *bytes.Buffer, peerMaxFrame uint32) *Conn {
	fr := frame.NewFramer(buf, bytes.NewReader(nil))
	c := &Conn{
		opts:    ConnOptions{}.defaulted(),
		fr:      fr,
		enc:     hpack.NewEncoder(),
		streams: map[uint32]*Stream{},
		peerSettings: frame.SettingsParams{
			Pairs: [16]frame.SettingPair{
				{ID: frame.SettingMaxFrameSize, Value: peerMaxFrame},
			},
			N: 1,
		},
	}
	return c
}

// blockFrame is a parsed HEADERS/CONTINUATION frame with its payload.
type blockFrame struct {
	ftype   byte
	flags   byte
	payload []byte
}

// parseBlockFrames walks a raw frame stream and returns HEADERS (0x1)
// and CONTINUATION (0x9) frames with payloads.
func parseBlockFrames(t *testing.T, b []byte) []blockFrame {
	t.Helper()
	var out []blockFrame
	for len(b) >= 9 {
		length := int(b[0])<<16 | int(b[1])<<8 | int(b[2])
		ftype := b[3]
		flags := b[4]
		payload := b[9 : 9+length]
		out = append(out, blockFrame{ftype: ftype, flags: flags, payload: payload})
		b = b[9+length:]
	}
	return out
}

// bigFields returns a pseudo-header set plus n filler headers whose
// values are valSize bytes each, producing an HPACK block large enough
// to force a CONTINUATION split.
func bigFields(n, valSize int) []hpack.HeaderField {
	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	val := bytes.Repeat([]byte("x"), valSize)
	for i := 0; i < n; i++ {
		name := []byte("x-big-" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
		fields = append(fields, hpack.HeaderField{Name: name, Value: append([]byte(nil), val...)})
	}
	return fields
}

func TestConformance_RFC7540_Sec6_2_HeadersSplitIntoContinuation(t *testing.T) {
	var buf bytes.Buffer
	c := newWireConn(&buf, 256) // small peer frame → force split
	s := newStream(0, 8, c, 65535)
	c.nextID = 1

	fields := bigFields(20, 60) // encoded block well over 256 bytes
	if err := c.writeHeadersWithPriority(context.Background(), s, fields, true, nil); err != nil {
		t.Fatalf("writeHeadersWithPriority: %v", err)
	}

	frames := parseBlockFrames(t, buf.Bytes())
	if len(frames) < 2 {
		t.Fatalf("got %d frames, want HEADERS + >=1 CONTINUATION", len(frames))
	}
	if frames[0].ftype != byte(frame.FrameHeaders) {
		t.Fatalf("frame 0 type = %d, want HEADERS", frames[0].ftype)
	}
	if frames[0].flags&byte(frame.FlagHeadersEndHeaders) != 0 {
		t.Fatalf("frame 0 must NOT set END_HEADERS when split")
	}
	if frames[0].flags&byte(frame.FlagHeadersEndStream) == 0 {
		t.Fatalf("frame 0 must carry END_STREAM")
	}
	for i := 1; i < len(frames); i++ {
		if frames[i].ftype != byte(frame.FrameContinuation) {
			t.Fatalf("frame %d type = %d, want CONTINUATION", i, frames[i].ftype)
		}
		endH := frames[i].flags&byte(frame.FlagContinuationEndHeaders) != 0
		if i == len(frames)-1 && !endH {
			t.Fatalf("last CONTINUATION must set END_HEADERS")
		}
		if i != len(frames)-1 && endH {
			t.Fatalf("non-final CONTINUATION %d must NOT set END_HEADERS", i)
		}
	}
	var block []byte
	for _, f := range frames {
		block = append(block, f.payload...)
	}
	dec := hpack.NewDecoder()
	count := 0
	if err := dec.DecodeBlock(block, func(hpack.HeaderField) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("DecodeBlock reassembled block: %v", err)
	}
	if count != len(fields) {
		t.Fatalf("decoded %d fields, want %d", count, len(fields))
	}
}

func TestConformance_RFC7540_Sec6_10_ContinuationFlagsAndPadding(t *testing.T) {
	var buf bytes.Buffer
	c := newWireConn(&buf, 256)
	c.opts.Padding = fixedPadding(8) // padding only legal on HEADERS
	s := newStream(0, 8, c, 65535)
	c.nextID = 1

	prio := &frame.Priority{StreamDep: 0, Weight: 15}
	fields := bigFields(20, 60)
	if err := c.writeHeadersWithPriority(context.Background(), s, fields, false, prio); err != nil {
		t.Fatalf("writeHeadersWithPriority: %v", err)
	}

	frames := parseBlockFrames(t, buf.Bytes())
	if len(frames) < 2 {
		t.Fatalf("got %d frames, want split", len(frames))
	}
	if frames[0].flags&byte(frame.FlagHeadersPadded) == 0 {
		t.Fatalf("HEADERS frame must carry PADDED")
	}
	if frames[0].flags&byte(frame.FlagHeadersPriority) == 0 {
		t.Fatalf("HEADERS frame must carry PRIORITY")
	}
	for i := 1; i < len(frames); i++ {
		if frames[i].flags&byte(frame.FlagHeadersPadded) != 0 {
			t.Fatalf("CONTINUATION %d must not be padded", i)
		}
		if frames[i].flags&byte(frame.FlagHeadersPriority) != 0 {
			t.Fatalf("CONTINUATION %d must not carry priority", i)
		}
	}
}

func TestConn_WriteHeaders_BlockFits_SingleFrame(t *testing.T) {
	var buf bytes.Buffer
	c := newWireConn(&buf, 16384) // default-sized frame
	s := newStream(0, 8, c, 65535)
	c.nextID = 1

	fields := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
	if err := c.writeHeadersWithPriority(context.Background(), s, fields, true, nil); err != nil {
		t.Fatalf("writeHeadersWithPriority: %v", err)
	}
	frames := parseBlockFrames(t, buf.Bytes())
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1 (no split for small block)", len(frames))
	}
	if frames[0].ftype != byte(frame.FrameHeaders) {
		t.Fatalf("frame type = %d, want HEADERS", frames[0].ftype)
	}
	if frames[0].flags&byte(frame.FlagHeadersEndHeaders) == 0 {
		t.Fatalf("single HEADERS frame must set END_HEADERS")
	}
}

// fixedPadding returns a PaddingStrategy that always pads HEADERS by n.
func fixedPadding(n uint8) PaddingStrategy {
	return PaddingStrategy{Min: n, Max: n}
}

// TestIntegration_LargeHeaders_SplitAcrossContinuation drives a real
// net/http2.Server with a request whose header block exceeds one 16384-byte
// frame, proving the HEADERS+CONTINUATION split reassembles end-to-end
// (RFC 7540 §6.2 / §6.10).
func TestIntegration_LargeHeaders_SplitAcrossContinuation(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := 0
		for name := range r.Header {
			if strings.HasPrefix(strings.ToLower(name), "x-big-") {
				count++
			}
		}
		w.Header().Set("X-Recv-Count", strconv.Itoa(count))
		w.WriteHeader(200)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	// ~40 headers of ~600 bytes each → block well over one 16384 frame.
	fields := bigFields(40, 600)
	if err := s.SendHeaders(ctx, fields, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}

	ev, err := s.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Type != EventHeaders {
		t.Fatalf("event type = %v, want EventHeaders", ev.Type)
	}
	var status, recv string
	for _, f := range ev.Headers {
		switch string(f.Name) {
		case ":status":
			status = string(f.Value)
		case "x-recv-count":
			recv = string(f.Value)
		}
	}
	if status != "200" {
		t.Fatalf("status = %q, want 200 (CONTINUATION reassembly failed?)", status)
	}
	if recv != "40" {
		t.Fatalf("server received x-recv-count=%q big headers, want 40", recv)
	}
}
