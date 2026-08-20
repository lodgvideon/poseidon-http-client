package grpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
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
	want := map[string]bool{
		"authorization":       true,
		"proxy-authorization": true,
		"cookie":              true,
		"x-request-id":        false,
	}

	hdrs := testBuildHeaders(cc, context.Background(), "/t.S/M", md)

	seen := map[string]bool{}
	for _, h := range hdrs {
		n := string(h.Name)
		if _, tracked := want[n]; !tracked {
			continue
		}
		seen[n] = true
		assert.Equalf(t, want[n], h.Sensitive(), "%q Sensitive = %v, want %v", n, h.Sensitive(), want[n])
	}
	for n := range want {
		assert.Truef(t, seen[n], "%q missing from the built header block", n)
	}
}

// TestBuildHeaders_CallerSensitiveIsPreserved pins that the default list is a
// floor: a caller marking some other field sensitive keeps that.
func TestBuildHeaders_CallerSensitiveIsPreserved(t *testing.T) {
	cc := newClientConn(nil, Options{Authority: "example.com"}.defaulted(), false)
	md := []conn.HeaderField{{Name: []byte("x-api-key"), Value: []byte("k"), Indexing: conn.IndexNever}}

	hdrs := testBuildHeaders(cc, context.Background(), "/t.S/M", md)

	for _, h := range hdrs {
		if string(h.Name) == "x-api-key" {
			require.True(t, h.Sensitive(), "caller-set Sensitive was cleared")
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

		assert.Equalf(t, c.want, got, "%s: eventBufferFor(%d, %d) = %d, want %d",
			c.name, c.maxMessage, c.frameSize, got, c.want)
		assert.Truef(t, got <= maxEventBuffer && got >= minEventBuffer,
			"%s: %d outside [%d, %d]", c.name, got, minEventBuffer, maxEventBuffer)
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
			assert.LessOrEqualf(t, bytes, ceiling, "frame=%d msg=%d: %d slots pin %d bytes, past %d",
				frameSize, msg, slots, bytes, ceiling)
		}
	}
}

func TestPseudoStatus_RejectsNonThreeDigit(t *testing.T) {
	mk := func(v string) []conn.HeaderField {
		return []conn.HeaderField{{Name: []byte(":status"), Value: []byte(v)}}
	}
	// "000200" would read as 200 under an unbounded accumulator, and a long
	// enough digit string would wrap back onto 200 outright.
	bad := []string{"000200", "18446744073709551816", "2000", "20", "", "2x0"}

	ok := pseudoStatus(mk("200"))

	require.Equalf(t, 200, ok, "pseudoStatus(200) = %d", ok)
	for _, v := range bad {
		got := pseudoStatus(mk(v))

		assert.NotEqualf(t, 200, got,
			"pseudoStatus(%q) = 200 — a peer-chosen string was laundered into success", v)
	}
}

func TestDecodeMessage_StripsControlBytes(t *testing.T) {
	// conn rejects raw CR/LF in a field value; percent-decoding runs after that
	// check and would put them back, letting a peer forge a line in the
	// caller's log or deliver ANSI escapes to an operator's terminal.
	newline := decodeMessage("benign%0A2026-07-31%20INFO%20login%20ok")
	esc := decodeMessage("esc%1B%5B31m")
	del := decodeMessage("del%7F")
	printable := decodeMessage("a%20b")
	// A control byte sitting in the value verbatim, not behind an escape. conn
	// rejects those upstream, so this cannot arrive over a live connection —
	// but the guarantee decodeMessage offers has to hold on its own rather than
	// by trusting a check in another package, and a fuzz seed reaches it
	// directly.
	rawControl := decodeMessage("a\x04b%20c")
	rawEsc := decodeMessage("plain\x1b[31m")
	plain := decodeMessage("ordinary message")

	require.NotContainsf(t, newline, "\n", "decodeMessage kept a newline: %q", newline)
	require.NotContainsf(t, newline, "\r", "decodeMessage kept a carriage return: %q", newline)
	require.NotContainsf(t, esc, "\x1b", "decodeMessage kept an ESC: %q", esc)
	require.NotContainsf(t, del, "\x7f", "decodeMessage kept DEL: %q", del)
	// Printable escapes still decode.
	require.Equalf(t, "a b", printable, "decodeMessage(a%%20b) = %q", printable)
	require.NotContainsf(t, rawControl, "\x04",
		"decodeMessage kept a raw control byte: %q", rawControl)
	require.NotContainsf(t, rawEsc, "\x1b", "decodeMessage kept a raw ESC: %q", rawEsc)
	// A value with neither escapes nor control bytes is returned untouched.
	require.Equalf(t, "ordinary message", plain, "decodeMessage(plain) = %q", plain)
}

func TestCloneFields_CapsFieldCount(t *testing.T) {
	src := make([]conn.HeaderField, maxMetadataFields+500)
	for i := range src {
		src[i] = conn.HeaderField{Name: []byte("k"), Value: []byte("v")}
	}

	got := cloneFields(src)

	require.Lenf(t, got, maxMetadataFields,
		"cloneFields kept %d fields, want the cap of %d", len(got), maxMetadataFields)
}

func TestCloneFields_CopiesOutOfTheSlab(t *testing.T) {
	backing := []byte("nameVALUE")
	src := []conn.HeaderField{{Name: backing[:4], Value: backing[4:]}}

	got := cloneFields(src)
	copy(backing, "XXXXXXXXX") // simulate the block being reused after release

	require.Truef(t, string(got[0].Name) == "name" && string(got[0].Value) == "VALUE",
		"cloneFields aliased the block: %q / %q", got[0].Name, got[0].Value)
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
	require.NoError(t, err, "NewStream")

	require.NoError(t, s.Close(), "Close")
	secondClose := s.Close()
	_, recvErr := s.Recv(ctx)
	_, headerErr := s.Header(ctx)
	sendErr := s.Send(ctx, []byte("x"))
	closeSendErr := s.CloseSend(ctx)

	require.NoErrorf(t, secondClose, "second Close = %v, want nil (idempotent)", secondClose)
	assert.ErrorIsf(t, recvErr, ErrStreamClosed, "Recv after Close = %v, want ErrStreamClosed", recvErr)
	assert.ErrorIsf(t, headerErr, ErrStreamClosed, "Header after Close = %v, want ErrStreamClosed", headerErr)
	assert.ErrorIsf(t, sendErr, ErrStreamClosed, "Send after Close = %v, want ErrStreamClosed", sendErr)
	assert.ErrorIsf(t, closeSendErr, ErrStreamClosed, "CloseSend after Close = %v, want ErrStreamClosed", closeSendErr)
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
	require.NoError(t, err, "Dial")
	s, err := cc.NewStream(ctx, "/t.S/M", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()

	// Killing the connection makes every subsequent write fail.
	require.NoError(t, cc.Close(), "Close")
	first := s.Send(ctx, []byte("one"))
	second := s.Send(ctx, []byte("two"))
	closeSendErr := s.CloseSend(ctx)

	require.Error(t, first, "Send on a closed connection succeeded")
	require.Error(t, second,
		"second Send succeeded after the first failed — framing would resynchronise onto garbage")
	require.Truef(t, errors.Is(second, first) || second.Error() == first.Error(),
		"second Send = %v, want the latched %v", second, first)
	require.Error(t, closeSendErr, "CloseSend succeeded after a failed Send")
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
	require.Truef(t, errors.As(err, &st), "Invoke error = %v (%T), want *Status", err, err)
	// Without the header-block lookup this would be INTERNAL, from the 400 row
	// of the mapping table.
	require.Equalf(t, ResourceExhausted, st.Code,
		"code = %v, want RESOURCE_EXHAUSTED from the server's own grpc-status", st.Code)
	require.Equalf(t, "quota exceeded", st.Message, "message = %q", st.Message)
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
	require.Truef(t, errors.As(err, &st), "Invoke error = %v (%T), want *Status", err, err)
	require.NotEqual(t, OK, st.Code, "a reset stream reported OK")
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

		assert.ErrorIsf(t, err, c.want, "%s: NewStream = %v, want %v", c.name, err, c.want)
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
	require.Truef(t, errors.As(err, &st), "Invoke error = %v (%T), want *Status", err, err)
	require.Equalf(t, Internal, st.Code, "code = %v, want INTERNAL", st.Code)
	require.Truef(t, strings.Contains(st.Message, "middle of a message"),
		"message = %q, want the truncation diagnosis", st.Message)
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
	require.Truef(t, errors.As(err, &st), "Invoke error = %v (%T), want *Status", err, err)
	require.Equalf(t, Unavailable, st.Code,
		"code = %v, want UNAVAILABLE for a 503 with no grpc-status", st.Code)
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

	require.NoError(t, err, "Invoke")
	require.Equalf(t, "final", string(got), "response = %q", got)
}

// TestNewClientConn_Validation covers the wrap-an-existing-connection entry
// point, including that Close leaves a connection it does not own alone.
func TestNewClientConn_Validation(t *testing.T) {
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
	require.NoError(t, err, "conn.Dial")
	defer func() { _ = raw.Close() }()

	_, nilConnErr := NewClientConn(nil, Options{Authority: "example.com"})
	_, noAuthorityErr := NewClientConn(raw, Options{})
	cc, err := NewClientConn(raw, Options{Authority: "example.com"})

	require.Error(t, nilConnErr, "NewClientConn(nil) = nil error")
	require.Error(t, noAuthorityErr, "NewClientConn without Authority = nil error")
	require.NoError(t, err, "NewClientConn")
	require.Same(t, raw, cc.Conn(), "Conn() did not return the wrapped connection")
	got, invokeErr := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil)
	require.NoErrorf(t, invokeErr, "Invoke = %q, %v", got, invokeErr)
	require.Equalf(t, "ok", string(got), "Invoke = %q", got)
	// Close must not touch a connection this ClientConn does not own.
	require.NoError(t, cc.Close(), "Close")
	require.True(t, raw.IsAlive(), "Close killed a connection the ClientConn does not own")
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
	require.NoError(t, err, "Dial")
	defer func() { _ = cc.Close() }()

	_, connLimitErr := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil)
	got, overrideErr := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil, MaxRecvMessageSize(1<<20))

	require.ErrorIsf(t, connLimitErr, ErrMessageTooLarge, "connection limit not applied: %v", connLimitErr)
	require.NoError(t, overrideErr, "per-call override")
	require.Lenf(t, got, 4096, "len = %d, want 4096", len(got))
}

// TestReset_CodeReachesTheStatusThroughTheWiring pins pump's EventReset arm,
// which TestIntegration_ResetStreamMapsToStatus above does not.
//
// That test's only assertion is "not OK", and "not OK" survives the mapping
// being replaced by the constant OK: the call then ends with a bare io.EOF,
// InvokeInto's empty-response guard turns that into an Internal status of its
// own, errors.As still succeeds and the code is still not OK. The empty-response
// guard covers for the missing mapping.
//
// Two things are different here. The call is driven through NewStream/Recv, so
// no guard sits between the reset and the caller; and the peer sends a code of
// the test's choosing, so the table is pinned THROUGH the wiring rather than
// only in TestStatusFromRST's isolation. net/http2's server cannot do the
// second part — the one reset a handler can force out of it is INTERNAL_ERROR,
// which is also where every unmapped code lands, so it cannot tell
// statusFromRST apart from a function returning the constant Internal.
func TestReset_CodeReachesTheStatusThroughTheWiring(t *testing.T) {
	cases := []struct {
		name string
		code frame.ErrCode
		want Code
	}{
		{"ENHANCE_YOUR_CALM", frame.ErrCodeEnhanceYourCalm, ResourceExhausted},
		{"CANCEL", frame.ErrCodeCancel, Canceled},
		{"REFUSED_STREAM", frame.ErrCodeRefusedStream, Unavailable},
		{"INADEQUATE_SECURITY", frame.ErrCodeInadequateSecurity, PermissionDenied},
		{"INTERNAL_ERROR", frame.ErrCodeInternalError, Internal},
		{"PROTOCOL_ERROR falls through to INTERNAL", frame.ErrCodeProtocolError, Internal},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			peer := newMockGRPCPeer(t)
			code := c.code
			peer.resetAfterHeaders = &code
			cc := dialMockPeer(t, peer, nil)
			ctx := t.Context()
			s, err := cc.NewStream(ctx, "/t.S/M", nil)
			require.NoError(t, err, "NewStream")
			defer func() { _ = s.Close() }()

			_, recvErr := s.Recv(ctx)

			var st *Status
			require.Truef(t, errors.As(recvErr, &st),
				"Recv = %v (%T), want a *Status — a reset whose code is not mapped "+
					"ends the call with a bare io.EOF instead", recvErr, recvErr)
			assert.Equalf(t, c.want, st.Code,
				"RST_STREAM(%#x) surfaced as %v, want %v", uint32(c.code), st.Code, c.want)
			assert.Equalf(t, c.want, s.Status().Code,
				"Status() reports %v after RST_STREAM(%#x), want %v — the recorded "+
					"status and the error a caller sees must be the same verdict",
				s.Status().Code, uint32(c.code), c.want)
			assert.Containsf(t, st.Message, fmt.Sprintf("HTTP/2 error code %d", uint32(c.code)),
				"message = %q, want the peer's own reset code in it — without it a "+
					"caller cannot tell which of the codes mapping to %v arrived",
				st.Message, c.want)
		})
	}
}

// TestIntegration_TruncationDoesNotOverrideTheServersStatus is the combination
// terminal()'s ordering comment is about, and the one nothing produced: a
// non-OK status AND bytes left in the decoder, at the same time.
//
// TestIntegration_TruncatedTrailingMessage covers the truncation branch on its
// own, but its server reports grpc-status 0, so status.Err() is nil and the
// order of the two checks cannot matter. Swap them and every test still passes.
//
// The peer here aborts mid-message and says why. The truncation is a
// CONSEQUENCE of that abort, so reporting it would replace a retriable
// RESOURCE_EXHAUSTED with our own non-retriable INTERNAL — a caller with a
// backoff policy keyed on the code would stop retrying a server that asked it
// to slow down.
func TestIntegration_TruncationDoesNotOverrideTheServersStatus(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = srvReadMessage(r.Body)
		srvBeginResponse(w)
		// Declares 10 bytes, delivers 2, then reports the abort in the trailer.
		_, _ = w.Write([]byte{0, 0, 0, 0, 10, 'a', 'b'})
		w.(http.Flusher).Flush()
		srvFinish(w, ResourceExhausted, "over quota")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := cc.Invoke(ctx, "/t.S/M", []byte("x"), nil)

	var st *Status
	require.Truef(t, errors.As(err, &st), "Invoke error = %v (%T), want *Status", err, err)
	require.Equalf(t, ResourceExhausted, st.Code,
		"code = %v, want RESOURCE_EXHAUSTED — the peer's own diagnosis, not the "+
			"INTERNAL our truncation check would report for the bytes its abort left behind",
		st.Code)
	assert.Equalf(t, "over quota", st.Message,
		"message = %q, want the server's — reporting the truncation would replace it too", st.Message)
}

// TestStream_SendErrorLatchesWithTheConnectionIntact covers what
// TestStream_SendErrorIsSticky above is named after but cannot reach.
//
// That test kills the whole ClientConn, so every later call fails on its own,
// with the same error, and sendErr is never read: dropping the latch changes
// nothing there. The latch is only observable when the first send fails while
// the connection stays usable — otherwise "the second Send failed" has two
// explanations and the test pins neither.
//
// A context that is already done gives exactly that. acquireSendCredits checks
// ctx.Err() before it touches the wire, so the message never leaves and the
// connection is not involved in the failure at all.
func TestStream_SendErrorLatchesWithTheConnectionIntact(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	ctx := t.Context()
	s, err := cc.NewStream(ctx, "/bench.Svc/Echo", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	dead, cancel := context.WithCancel(context.Background())
	cancel() // done before the write starts, so nothing reaches the wire

	first := s.Send(dead, []byte("one"))
	second := s.Send(ctx, []byte("two"))
	closeSendErr := s.CloseSend(ctx)
	control, controlErr := cc.Invoke(ctx, "/bench.Svc/Echo", []byte("still alive"), nil)

	require.Errorf(t, first, "Send on an already-cancelled context succeeded")
	// The precondition the rest of this test rests on. Without it a broken
	// connection would satisfy every check below with no latch at all — which is
	// precisely how the sibling test came to pass without exercising one.
	require.NoErrorf(t, controlErr,
		"a fresh call on the same connection failed (%v): the first Send took the "+
			"connection down with it, so the checks below no longer distinguish the "+
			"latch from a dead transport", controlErr)
	require.Equalf(t, "still alive", string(control),
		"the control call echoed %q — the connection is not carrying traffic normally", control)
	require.Errorf(t, second,
		"second Send succeeded on a healthy connection after the first failed — a "+
			"truncated message is on the wire and this one resynchronises the server onto garbage")
	require.Truef(t, errors.Is(second, first) || second.Error() == first.Error(),
		"second Send = %v, want the latched %v", second, first)
	assert.Errorf(t, closeSendErr,
		"CloseSend = nil after a failed Send: END_STREAM after a truncated message "+
			"tells the server the garbage was the whole request")
}
