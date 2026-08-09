package grpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// testBuildHeaders borrows and returns a header scratch around buildHeaders the
// way NewStream does, and copies the result out — the scratch's field array is
// cleared when it goes back to the pool, so a caller that kept the slice would
// be reading zeroed entries.
func testBuildHeaders(cc *ClientConn, ctx context.Context, method string, md []conn.HeaderField) []conn.HeaderField {
	sc := headerScratchPool.Get().(*headerScratch)
	defer putHeaderScratch(sc)
	return append([]conn.HeaderField(nil), cc.buildHeaders(ctx, method, md, nil, sc)...)
}

// TestBuildHeaders_CredentialsMarkedSensitive is the regression test for the
// finding that mattered most: hpack.Encoder inserts every non-sensitive field
// into the connection's dynamic table, so an unmarked bearer token would be
// retained for the life of the ClientConn and emitted as a one-byte index on
// every later call — long-lived exposure plus a compression-oracle target.
func TestBuildHeaders_CredentialsMarkedSensitive(t *testing.T) {
	cc := newClientConn(nil, Options{Authority: "example.com"}.defaulted(), false)

	md := []conn.HeaderField{
		{Name: []byte("authorization"), Value: []byte("Bearer secret-token")},
		{Name: []byte("proxy-authorization"), Value: []byte("Basic zzz")},
		{Name: []byte("cookie"), Value: []byte("session=abc")},
		{Name: []byte("x-request-id"), Value: []byte("req-1")},
	}
	hdrs := testBuildHeaders(cc, context.Background(), "/t.S/M", md)

	want := map[string]bool{
		"authorization":       true,
		"proxy-authorization": true,
		"cookie":              true,
		"x-request-id":        false,
	}
	seen := map[string]bool{}
	for _, h := range hdrs {
		n := string(h.Name)
		if _, tracked := want[n]; !tracked {
			continue
		}
		seen[n] = true
		if h.Sensitive() != want[n] {
			t.Errorf("%q Sensitive = %v, want %v", n, h.Sensitive(), want[n])
		}
	}
	for n := range want {
		if !seen[n] {
			t.Errorf("%q missing from the built header block", n)
		}
	}
}

// TestBuildHeaders_CallerSensitiveIsPreserved pins that the default list is a
// floor: a caller marking some other field sensitive keeps that.
func TestBuildHeaders_CallerSensitiveIsPreserved(t *testing.T) {
	cc := newClientConn(nil, Options{Authority: "example.com"}.defaulted(), false)
	md := []conn.HeaderField{{Name: []byte("x-api-key"), Value: []byte("k"), Indexing: conn.IndexNever}}
	for _, h := range testBuildHeaders(cc, context.Background(), "/t.S/M", md) {
		if string(h.Name) == "x-api-key" && !h.Sensitive() {
			t.Fatal("caller-set Sensitive was cleared")
		}
	}
}

func TestEventBufferFor(t *testing.T) {
	cases := []struct {
		name       string
		maxMessage int
		frameSize  uint32
		want       int
	}{
		// 4 MiB + 256 KiB slack over 16 KiB frames.
		{"defaults", DefaultMaxMessageSize, 16384, (DefaultMaxMessageSize + eventBufferSlackBytes) / 16384},
		// A tiny message is dominated by the slack term, which is what keeps a
		// small-message connection from being starved by a burst.
		{"tiny message", 1024, 16384, (1024 + eventBufferSlackBytes) / 16384},
		// The byte budget is capped, so a huge message cannot buy unbounded slots.
		{"huge message", 1 << 30, 16384, maxEventBufferBytes / 16384},
		// A huge frame size must not multiply the slack into hundreds of MiB;
		// it collapses to the floor instead.
		{"huge frames", DefaultMaxMessageSize, 1 << 24, minEventBuffer},
		// A tiny frame size is capped by the slot ceiling.
		{"tiny frames", DefaultMaxMessageSize, 1024, maxEventBuffer},
		{"zero frame size uses the default", DefaultMaxMessageSize, 0,
			(DefaultMaxMessageSize + eventBufferSlackBytes) / 16384},
	}
	for _, c := range cases {
		got := eventBufferFor(c.maxMessage, c.frameSize)
		if got != c.want {
			t.Errorf("%s: eventBufferFor(%d, %d) = %d, want %d", c.name, c.maxMessage, c.frameSize, got, c.want)
		}
		if got > maxEventBuffer || got < minEventBuffer {
			t.Errorf("%s: %d outside [%d, %d]", c.name, got, minEventBuffer, maxEventBuffer)
		}
	}
}

// TestEventBufferFor_BytesStayBounded is the property the slot count exists to
// serve: every queued event pins a pooled DATA buffer of up to one frame, so
// slots x frameSize is what a peer can hold per stream.
func TestEventBufferFor_BytesStayBounded(t *testing.T) {
	for _, frameSize := range []uint32{1 << 10, 16384, 1 << 20, 1 << 24} {
		for _, msg := range []int{1024, DefaultMaxMessageSize, 1 << 30} {
			slots := eventBufferFor(msg, frameSize)
			bytes := uint64(slots) * uint64(frameSize)
			ceiling := uint64(maxEventBufferBytes)
			if fs := uint64(frameSize); fs*minEventBuffer > ceiling {
				// The floor wins: never worse than conn's own default.
				ceiling = fs * minEventBuffer
			}
			if bytes > ceiling {
				t.Errorf("frame=%d msg=%d: %d slots pin %d bytes, past %d",
					frameSize, msg, slots, bytes, ceiling)
			}
		}
	}
}

func TestPseudoStatus_RejectsNonThreeDigit(t *testing.T) {
	mk := func(v string) []conn.HeaderField {
		return []conn.HeaderField{{Name: []byte(":status"), Value: []byte(v)}}
	}
	if got := pseudoStatus(mk("200")); got != 200 {
		t.Fatalf("pseudoStatus(200) = %d", got)
	}
	// "000200" would read as 200 under an unbounded accumulator, and a long
	// enough digit string would wrap back onto 200 outright.
	for _, v := range []string{"000200", "18446744073709551816", "2000", "20", "", "2x0"} {
		if got := pseudoStatus(mk(v)); got == 200 {
			t.Errorf("pseudoStatus(%q) = 200 — a peer-chosen string was laundered into success", v)
		}
	}
}

func TestDecodeMessage_StripsControlBytes(t *testing.T) {
	// conn rejects raw CR/LF in a field value; percent-decoding runs after that
	// check and would put them back, letting a peer forge a line in the
	// caller's log or deliver ANSI escapes to an operator's terminal.
	got := decodeMessage("benign%0A2026-07-31%20INFO%20login%20ok")
	if strings.ContainsAny(got, "\r\n") {
		t.Fatalf("decodeMessage kept a newline: %q", got)
	}
	if got := decodeMessage("esc%1B%5B31m"); strings.ContainsRune(got, 0x1b) {
		t.Fatalf("decodeMessage kept an ESC: %q", got)
	}
	if got := decodeMessage("del%7F"); strings.ContainsRune(got, 0x7f) {
		t.Fatalf("decodeMessage kept DEL: %q", got)
	}
	// Printable escapes still decode.
	if got := decodeMessage("a%20b"); got != "a b" {
		t.Fatalf("decodeMessage(a%%20b) = %q", got)
	}
	// A control byte sitting in the value verbatim, not behind an escape. conn
	// rejects those upstream, so this cannot arrive over a live connection —
	// but the guarantee decodeMessage offers has to hold on its own rather than
	// by trusting a check in another package, and a fuzz seed reaches it
	// directly.
	if got := decodeMessage("a\x04b%20c"); strings.ContainsRune(got, 0x04) {
		t.Fatalf("decodeMessage kept a raw control byte: %q", got)
	}
	if got := decodeMessage("plain\x1b[31m"); strings.ContainsRune(got, 0x1b) {
		t.Fatalf("decodeMessage kept a raw ESC: %q", got)
	}
	// A value with neither escapes nor control bytes is returned untouched.
	if got := decodeMessage("ordinary message"); got != "ordinary message" {
		t.Fatalf("decodeMessage(plain) = %q", got)
	}
}

func TestCloneFields_CapsFieldCount(t *testing.T) {
	src := make([]conn.HeaderField, maxMetadataFields+500)
	for i := range src {
		src[i] = conn.HeaderField{Name: []byte("k"), Value: []byte("v")}
	}
	got := cloneFields(src)
	if len(got) != maxMetadataFields {
		t.Fatalf("cloneFields kept %d fields, want the cap of %d", len(got), maxMetadataFields)
	}
}

func TestCloneFields_CopiesOutOfTheSlab(t *testing.T) {
	backing := []byte("nameVALUE")
	src := []conn.HeaderField{{Name: backing[:4], Value: backing[4:]}}
	got := cloneFields(src)
	copy(backing, "XXXXXXXXX") // simulate the slab being reused after the Put
	if string(got[0].Name) != "name" || string(got[0].Value) != "VALUE" {
		t.Fatalf("cloneFields aliased the slab: %q / %q", got[0].Name, got[0].Value)
	}
}

// TestStream_ClosedGuard pins that no method touches the underlying conn.Stream
// after Close. conn recycles that struct into its connection's pool, so a call
// through a stale pointer can read another RPC's events.
func TestStream_ClosedGuard(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, []byte("ok"))
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := cc.NewStream(ctx, "/t.S/M", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil (idempotent)", err)
	}
	if _, err := s.Recv(ctx); !errors.Is(err, ErrStreamClosed) {
		t.Errorf("Recv after Close = %v, want ErrStreamClosed", err)
	}
	if _, err := s.Header(ctx); !errors.Is(err, ErrStreamClosed) {
		t.Errorf("Header after Close = %v, want ErrStreamClosed", err)
	}
	if err := s.Send(ctx, []byte("x")); !errors.Is(err, ErrStreamClosed) {
		t.Errorf("Send after Close = %v, want ErrStreamClosed", err)
	}
	if err := s.CloseSend(ctx); !errors.Is(err, ErrStreamClosed) {
		t.Errorf("CloseSend after Close = %v, want ErrStreamClosed", err)
	}
}

// TestStream_SendErrorIsSticky pins that a failed send latches. conn.writeData
// chunks a message across several DATA frames and flushes each, so a failure
// partway leaves a truncated message on the wire; letting the next Send append
// to that would put its length prefix where the server expects the tail of the
// previous one and resynchronise the stream onto garbage.
func TestStream_SendErrorIsSticky(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc, err := Dial(ctx, srv.Listener.Addr().String(), Options{
		Conn:      conn.ConnOptions{Dialer: &conn.TLSDialer{Config: cfg}},
		Authority: "example.com",
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	s, err := cc.NewStream(ctx, "/t.S/M", nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()

	// Killing the connection makes every subsequent write fail.
	if err := cc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	first := s.Send(ctx, []byte("one"))
	if first == nil {
		t.Fatal("Send on a closed connection succeeded")
	}
	second := s.Send(ctx, []byte("two"))
	if second == nil {
		t.Fatal("second Send succeeded after the first failed — framing would resynchronise onto garbage")
	}
	if !errors.Is(second, first) && second.Error() != first.Error() {
		t.Fatalf("second Send = %v, want the latched %v", second, first)
	}
	if err := s.CloseSend(ctx); err == nil {
		t.Fatal("CloseSend succeeded after a failed Send")
	}
}

// TestIntegration_NonOKStatusInHeaderBlock covers grpc-java's HTTP-error path:
// a non-200 whose header block already carries grpc-status and grpc-message,
// followed by a body. The status-mapping table is defined for use only when the
// response carried no grpc-status, so the server's own diagnosis must win.
func TestIntegration_NonOKStatusInHeaderBlock(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("grpc-status", fmt.Sprintf("%d", uint32(ResourceExhausted)))
		w.Header().Set("grpc-message", "quota%20exceeded")
		w.WriteHeader(http.StatusBadRequest)
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("over quota"))
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil)
	var st *Status
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %v (%T), want *Status", err, err)
	}
	// Without the header-block lookup this would be INTERNAL, from the 400 row
	// of the mapping table.
	if st.Code != ResourceExhausted {
		t.Fatalf("code = %v, want RESOURCE_EXHAUSTED from the server's own grpc-status", st.Code)
	}
	if st.Message != "quota exceeded" {
		t.Fatalf("message = %q", st.Message)
	}
}

// TestIntegration_ResetStreamMapsToStatus exercises the RST_STREAM path end to
// end. http.ErrAbortHandler makes net/http abort the response without logging,
// which puts an RST_STREAM on the wire.
func TestIntegration_ResetStreamMapsToStatus(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		panic(http.ErrAbortHandler)
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil)
	var st *Status
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %v (%T), want *Status", err, err)
	}
	if st.Code == OK {
		t.Fatal("a reset stream reported OK")
	}
}

// TestIntegration_RawMetadataIsValidated covers the path a caller reaches
// without AppendMetadata, where nothing has lowercased or checked the field.
func TestIntegration_RawMetadataIsValidated(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	cases := []struct {
		name string
		md   conn.HeaderField
		want error
	}{
		{"uppercase name", conn.HeaderField{Name: []byte("Content-Type"), Value: []byte("text/plain")}, ErrInvalidMetadata},
		{"uppercase te", conn.HeaderField{Name: []byte("TE"), Value: []byte("gzip")}, ErrInvalidMetadata},
		{"crlf in value", conn.HeaderField{Name: []byte("x-user"), Value: []byte("a\r\nx-admin: 1")}, ErrInvalidMetadata},
		{"space in name", conn.HeaderField{Name: []byte("bad name"), Value: []byte("v")}, ErrInvalidMetadata},
		{"reserved", conn.HeaderField{Name: []byte("grpc-timeout"), Value: []byte("1S")}, ErrReservedMetadata},
		{"pseudo", conn.HeaderField{Name: []byte(":path"), Value: []byte("/x")}, ErrInvalidMetadata},
	}
	for _, c := range cases {
		_, err := cc.NewStream(context.Background(), "/t.S/M", []conn.HeaderField{c.md})
		if !errors.Is(err, c.want) {
			t.Errorf("%s: NewStream = %v, want %v", c.name, err, c.want)
		}
	}
}

// TestIntegration_TruncatedTrailingMessage covers a server that declares more
// bytes than it delivers and then reports success. Without the pending-bytes
// check the caller would see a clean io.EOF and silently lose a message.
func TestIntegration_TruncatedTrailingMessage(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		// Declares 10 bytes, delivers 2.
		_, _ = w.Write([]byte{0, 0, 0, 0, 10, 'a', 'b'})
		w.(http.Flusher).Flush()
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil)
	var st *Status
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %v (%T), want *Status", err, err)
	}
	if st.Code != Internal {
		t.Fatalf("code = %v, want INTERNAL", st.Code)
	}
	if !strings.Contains(st.Message, "middle of a message") {
		t.Fatalf("message = %q, want the truncation diagnosis", st.Message)
	}
}

// TestIntegration_TrailersOnlyNonOKWithoutStatus is the shape a draining proxy
// produces: a header block with END_STREAM, a non-200 status, and no
// grpc-status anywhere. It is the only path that reaches finish's HTTP-status
// fallback — a non-200 that carries a body returns earlier.
func TestIntegration_TrailersOnlyNonOKWithoutStatus(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil)
	var st *Status
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %v (%T), want *Status", err, err)
	}
	if st.Code != Unavailable {
		t.Fatalf("code = %v, want UNAVAILABLE for a 503 with no grpc-status", st.Code)
	}
}

// TestIntegration_InterimHeadersIgnored pins that a 1xx block does not get
// mistaken for the final response.
func TestIntegration_InterimHeadersIgnored(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		w.WriteHeader(http.StatusEarlyHints)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, []byte("final"))
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(got) != "final" {
		t.Fatalf("response = %q", got)
	}
}

// TestNewClientConn_Validation covers the wrap-an-existing-connection entry
// point, including that Close leaves a connection it does not own alone.
func TestNewClientConn_Validation(t *testing.T) {
	if _, err := NewClientConn(nil, Options{Authority: "example.com"}); err == nil {
		t.Fatal("NewClientConn(nil) = nil error")
	}

	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, []byte("ok"))
		srvFinish(w, OK, "")
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := conn.Dial(ctx, srv.Listener.Addr().String(), conn.ConnOptions{
		Dialer:            &conn.TLSDialer{Config: cfg},
		StreamEventBuffer: 64,
	})
	if err != nil {
		t.Fatalf("conn.Dial: %v", err)
	}
	defer func() { _ = raw.Close() }()

	if _, err := NewClientConn(raw, Options{}); err == nil {
		t.Fatal("NewClientConn without Authority = nil error")
	}
	cc, err := NewClientConn(raw, Options{Authority: "example.com"})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	if cc.Conn() != raw {
		t.Fatal("Conn() did not return the wrapped connection")
	}
	if got, err := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil); err != nil || string(got) != "ok" {
		t.Fatalf("Invoke = %q, %v", got, err)
	}
	// Close must not touch a connection this ClientConn does not own.
	if err := cc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !raw.IsAlive() {
		t.Fatal("Close killed a connection the ClientConn does not own")
	}
}

// TestIntegration_MaxRecvMessageSizeCallOption pins that the per-call override
// beats the connection-wide setting, in both directions.
func TestIntegration_MaxRecvMessageSizeCallOption(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		_ = srvWriteMessage(w, make([]byte, 4096))
		srvFinish(w, OK, "")
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc, err := Dial(ctx, srv.Listener.Addr().String(), Options{
		Conn:               conn.ConnOptions{Dialer: &conn.TLSDialer{Config: cfg}},
		Authority:          "example.com",
		MaxRecvMessageSize: 1024,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = cc.Close() }()

	if _, err := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("connection limit not applied: %v", err)
	}
	got, err := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil, MaxRecvMessageSize(1<<20))
	if err != nil {
		t.Fatalf("per-call override: %v", err)
	}
	if len(got) != 4096 {
		t.Fatalf("len = %d, want 4096", len(got))
	}
}
