package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http3"
)

// fakeH3Client is a test double for *http3.Client. It captures the request it
// receives (deep-copied, since the real path mutates a pooled buffer after the
// call) and returns a canned response/body or error over both the buffered Do and
// the streaming DoStream paths.
type fakeH3Client struct {
	resp  *http3.Response
	body  []byte
	doErr error

	// bodyChunks, when non-nil, is the ordered sequence of DATA chunks DoStream
	// yields (each a separate BodyEvent), so a streaming test can assert incremental
	// delivery. When nil, DoStream yields the whole body as one chunk.
	bodyChunks [][]byte
	// nextErr, when non-nil, is returned by the streaming body's Next after all
	// chunks — modelling a mid-body server reset or protocol error.
	nextErr error

	gotReq     *http3.Request
	lastBody   *fakeH3Body // the body returned by the most recent DoStream
	doCalls    int32
	closes     int32
	deadFlag   int32 // non-zero → Alive reports false (see kill)
	goawayFlag int32 // non-zero → GoingAway reports true (peer sent GOAWAY)
}

func (f *fakeH3Client) Do(_ context.Context, req *http3.Request) (*http3.Response, []byte, error) {
	atomic.AddInt32(&f.doCalls, 1)
	cp := *req
	cp.Headers = append([]conn.HeaderField(nil), req.Headers...)
	cp.Body = append([]byte(nil), req.Body...)
	f.gotReq = &cp
	if f.doErr != nil {
		return nil, nil, f.doErr
	}
	return f.resp, f.body, nil
}

func (f *fakeH3Client) DoStream(_ context.Context, req *http3.Request) (*http3.Response, http3.ResponseBody, error) {
	atomic.AddInt32(&f.doCalls, 1)
	cp := *req
	cp.Headers = append([]conn.HeaderField(nil), req.Headers...)
	cp.Body = append([]byte(nil), req.Body...)
	f.gotReq = &cp
	if f.doErr != nil {
		return nil, nil, f.doErr
	}
	chunks := f.bodyChunks
	if chunks == nil && len(f.body) > 0 {
		chunks = [][]byte{f.body}
	}
	f.lastBody = &fakeH3Body{chunks: chunks, trailers: f.resp.Trailers, nextErr: f.nextErr}
	return f.resp, f.lastBody, nil
}

// dead, when set, makes Alive report false — modelling a QUIC connection that has
// terminated. Read/written atomically so pool tests can flip it from another
// goroutine.
func (f *fakeH3Client) Alive() bool     { return atomic.LoadInt32(&f.deadFlag) == 0 }
func (f *fakeH3Client) GoingAway() bool { return atomic.LoadInt32(&f.goawayFlag) != 0 }

func (f *fakeH3Client) kill() { atomic.StoreInt32(&f.deadFlag, 1) }

func (f *fakeH3Client) Close() error {
	atomic.AddInt32(&f.closes, 1)
	// A closed QUIC client is no longer alive (the real *http3.Client closes its
	// readerDone latch on Close), so reflect that here for the pool's Alive gate.
	atomic.StoreInt32(&f.deadFlag, 1)
	return nil
}

// fakeH3Body is a test double for http3.ResponseBody. It yields the configured
// DATA chunks in order (one per Next), then the trailer section (if any), then a
// clean End — or nextErr after the chunks, modelling a mid-body abort.
type fakeH3Body struct {
	chunks       [][]byte
	trailers     []conn.HeaderField
	nextErr      error
	idx          int
	trailersDone bool
	closes       int32
}

func (b *fakeH3Body) Next(_ context.Context) (http3.BodyEvent, error) {
	if b.idx < len(b.chunks) {
		c := b.chunks[b.idx]
		b.idx++
		return http3.BodyEvent{Data: c}, nil
	}
	if b.nextErr != nil {
		err := b.nextErr
		b.nextErr = nil
		return http3.BodyEvent{}, err
	}
	if len(b.trailers) > 0 && !b.trailersDone {
		b.trailersDone = true
		return http3.BodyEvent{Trailers: b.trailers, End: true}, nil
	}
	return http3.BodyEvent{End: true}, nil
}

func (b *fakeH3Body) Trailers() []conn.HeaderField { return b.trailers }

func (b *fakeH3Body) Close() error {
	atomic.AddInt32(&b.closes, 1)
	return nil
}

// newH3TestClient builds a TransportH3 Client whose dialer yields fake instead
// of a live QUIC connection.
func newH3TestClient(t *testing.T, fake *fakeH3Client, hooks *Hooks) *Client {
	t.Helper()
	opts := ClientOptions{
		Addr:      "h3.example:443",
		Transport: TransportH3,
		TLSConfig: &tls.Config{ServerName: "h3.example"},
	}
	if hooks != nil {
		opts.Hooks = hooks
	}
	c, err := NewClient(opts)
	require.NoError(t, err, "NewClient(TransportH3)")
	sc, ok := c.tr.(*singleH3Conn)
	require.Truef(t, ok, "transport is %T, want *singleH3Conn", c.tr)
	sc.dialFn = func(context.Context, string, *tls.Config) (h3Client, error) { return fake, nil }
	return c
}

func hasHeader(hs []conn.HeaderField, name, value string) bool {
	for _, h := range hs {
		if string(h.Name) == name && string(h.Value) == value {
			return true
		}
	}
	return false
}

// TestH3_Do_BufferedRoundTrip verifies the pseudo-header re-split into typed
// http3.Request fields and the response synthesis back into status + headers +
// body over the buffered Client.Do path.
func TestH3_Do_BufferedRoundTrip(t *testing.T) {
	fake := &fakeH3Client{
		resp: &http3.Response{
			Status:  200,
			Headers: []conn.HeaderField{{Name: []byte("content-type"), Value: []byte("text/plain")}},
		},
		body: []byte("hello h3"),
	}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()

	var resp Response
	err := c.Do(context.Background(), &Request{
		Method:    "GET",
		Path:      "/x",
		Authority: "h3.example",
		BodyMode:  BodyBuffer,
		Headers:   []conn.HeaderField{{Name: []byte("x-test"), Value: []byte("1")}},
	}, &resp)

	require.NoError(t, err, "Do")
	// Pseudo-headers routed to typed http3.Request fields.
	got := fake.gotReq
	require.NotNil(t, got, "the transport never reached http3.Client.Do")
	assert.Equal(t, "GET", got.Method, "re-split :method")
	assert.Equal(t, "https", got.Scheme, "re-split :scheme")
	assert.Equal(t, "h3.example", got.Authority, "re-split :authority")
	assert.Equal(t, "/x", got.Path, "re-split :path")
	// No pseudo-header leaked into the regular Headers slice; x-test survived.
	for _, h := range got.Headers {
		assert.Falsef(t, len(h.Name) > 0 && h.Name[0] == ':',
			"pseudo-header %q leaked into http3.Request.Headers, where HTTP/3 has no place for it",
			h.Name)
	}
	assert.Truef(t, hasHeader(got.Headers, "x-test", "1"),
		"x-test header missing from http3.Request.Headers: %+v", got.Headers)
	// Response synthesised back.
	assert.Equal(t, 200, resp.Status, "synthesised status")
	assert.Equal(t, "hello h3", string(resp.Body), "synthesised body")
	assert.Truef(t, hasHeader(resp.Headers, "content-type", "text/plain"),
		"content-type header missing from resp.Headers: %+v", resp.Headers)
	assert.EqualValues(t, len("hello h3"), resp.BytesReceived, "resp.BytesReceived")
}

// TestH3_Do_RequestBody verifies SendData chunks are buffered into
// http3.Request.Body for a POST.
func TestH3_Do_RequestBody(t *testing.T) {
	fake := &fakeH3Client{resp: &http3.Response{Status: 201}}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()

	var resp Response
	err := c.Do(context.Background(), &Request{
		Method:   "POST",
		Path:     "/upload",
		BodyMode: BodyBuffer,
		Body:     []byte("payload-bytes"),
	}, &resp)

	require.NoError(t, err, "Do")
	require.NotNil(t, fake.gotReq, "the transport never reached http3.Client.Do")
	assert.Equal(t, "payload-bytes", string(fake.gotReq.Body),
		"SendData chunks were not buffered into http3.Request.Body")
	assert.Equal(t, 201, resp.Status, "resp.Status")
}

// TestH3Exchange_Synthesize_HeadersDataTrailers drives the exchange directly and
// asserts the exact synthesised event sequence: EventHeaders (:status first),
// EventData (whole body, EndStream=false because trailers follow), EventTrailers
// (EndStream=true), then ErrStreamClosed.
func TestH3Exchange_Synthesize_HeadersDataTrailers(t *testing.T) {
	fake := &fakeH3Client{
		resp: &http3.Response{
			Status:   200,
			Headers:  []conn.HeaderField{{Name: []byte("x-h"), Value: []byte("v")}},
			Trailers: []conn.HeaderField{{Name: []byte("x-t"), Value: []byte("w")}},
		},
		body: []byte("body-bytes"),
	}
	ex := &h3Exchange{client: fake}
	fields := []conn.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("h")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte("accept"), Value: []byte("*/*")},
	}
	require.NoError(t, ex.SendHeadersWithPriority(context.Background(), fields, true, nil),
		"SendHeadersWithPriority")
	ctx := context.Background()

	ev1, err1 := ex.Recv(ctx)
	ev2, err2 := ex.Recv(ctx)
	ev3, err3 := ex.Recv(ctx)
	_, err4 := ex.Recv(ctx)

	require.NoError(t, err1, "Recv#1")
	assert.Equal(t, conn.EventHeaders, ev1.Type, "Recv#1 type")
	require.NotEmpty(t, ev1.Headers, "Recv#1 carried no header block")
	assert.Equal(t, ":status", string(ev1.Headers[0].Name), "Recv#1 first header name: :status must lead")
	assert.Equal(t, "200", string(ev1.Headers[0].Value), "Recv#1 :status value")
	assert.Truef(t, hasHeader(ev1.Headers, "x-h", "v"), "Recv#1 missing x-h header: %+v", ev1.Headers)
	assert.False(t, ev1.EndStream, "Recv#1 EndStream: data still follows")

	require.NoError(t, err2, "Recv#2")
	assert.Equal(t, conn.EventData, ev2.Type, "Recv#2 type")
	assert.Equal(t, "body-bytes", string(ev2.Data), "Recv#2 data")
	assert.False(t, ev2.EndStream,
		"Recv#2 EndStream: trailers follow, so the DATA event must not end the stream")

	require.NoError(t, err3, "Recv#3")
	assert.Equal(t, conn.EventTrailers, ev3.Type, "Recv#3 type")
	assert.Truef(t, hasHeader(ev3.Headers, "x-t", "w"), "Recv#3 trailers = %+v, want x-t=w", ev3.Headers)
	assert.True(t, ev3.EndStream, "Recv#3 EndStream: the trailer section is the final event")

	assert.Truef(t, errors.Is(err4, conn.ErrStreamClosed),
		"Recv#4 err = %v, want ErrStreamClosed", err4)
	assert.EqualValues(t, 1, atomic.LoadInt32(&fake.doCalls),
		"http3.Client.Do must be driven exactly once per exchange")
}

// TestH3_Do_ResponseTrailers checks trailers surface through drainResponse into
// Response.Trailers when WantTrailers is set.
func TestH3_Do_ResponseTrailers(t *testing.T) {
	fake := &fakeH3Client{
		resp: &http3.Response{
			Status:   200,
			Trailers: []conn.HeaderField{{Name: []byte("grpc-status"), Value: []byte("0")}},
		},
		body: []byte("data"),
	}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()

	var resp Response
	err := c.Do(context.Background(), &Request{
		Method:       "GET",
		Path:         "/",
		BodyMode:     BodyBuffer,
		WantTrailers: true,
	}, &resp)

	require.NoError(t, err, "Do")
	assert.Equal(t, "data", string(resp.Body), "resp.Body")
	assert.Truef(t, hasHeader(resp.Trailers, "grpc-status", "0"),
		"grpc-status trailer missing: %+v — a gRPC caller reads its status from here",
		resp.Trailers)
}

// TestH3_Do_ErrorPassthrough verifies a Do error is returned verbatim so a
// caller can errors.As it — here a retryable *http3.StreamResetError.
func TestH3_Do_ErrorPassthrough(t *testing.T) {
	fake := &fakeH3Client{doErr: &http3.StreamResetError{Code: http3.H3RequestRejected}}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()

	var resp Response
	err := c.Do(context.Background(), &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp)

	require.Error(t, err, "Do must surface the transport error, not swallow it")
	var sre *http3.StreamResetError
	require.Truef(t, errors.As(err, &sre),
		"Do err = %v (%T), want *http3.StreamResetError so the retry layer can classify it",
		err, err)
	assert.True(t, sre.Retryable(),
		"StreamResetError.Retryable() = false, want true for H3_REQUEST_REJECTED")
	assert.EqualValues(t, 1, c.Metrics().Counters.RequestsErrored.Load(), "RequestsErrored")
}

// TestH3_Do_MetricsAndHooks confirms the buffered H3 path drives the same
// metric counters and lifecycle hooks as H2.
func TestH3_Do_MetricsAndHooks(t *testing.T) {
	var dialN, completeN, connCloseN atomic.Int32
	var gotStatus atomic.Int32
	hooks := &Hooks{
		OnDial: func(DialEvent) { dialN.Add(1) },
		OnRequestComplete: func(e RequestCompleteEvent) {
			completeN.Add(1)
			gotStatus.Store(int32(e.Status))
		},
		OnConnClose: func(ConnCloseEvent) { connCloseN.Add(1) },
	}
	fake := &fakeH3Client{resp: &http3.Response{Status: 200}, body: []byte("ok")}
	c := newH3TestClient(t, fake, hooks)

	var resp Response
	doErr := c.Do(context.Background(), &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp)
	closeErr := c.Close()

	require.NoError(t, doErr, "Do")
	m := c.Metrics()
	assert.EqualValues(t, 1, m.Counters.RequestsStarted.Load(), "RequestsStarted")
	assert.EqualValues(t, 1, m.Counters.RequestsSucceeded.Load(), "RequestsSucceeded")
	assert.EqualValues(t, 1, m.Counters.Responses2xx.Load(), "Responses2xx")
	assert.EqualValues(t, 1, m.Counters.DialsAttempted.Load(), "DialsAttempted")
	assert.EqualValues(t, 1, dialN.Load(), "OnDial")
	assert.EqualValues(t, 1, completeN.Load(), "OnRequestComplete")
	assert.EqualValues(t, 200, gotStatus.Load(), "OnRequestComplete status")

	// Close fires OnConnClose + ConnsClosed and closes the underlying client.
	require.NoError(t, closeErr, "Close")
	assert.EqualValues(t, 1, connCloseN.Load(), "OnConnClose on Close")
	assert.EqualValues(t, 1, m.Counters.ConnsClosed.Load(), "ConnsClosed on Close")
	assert.EqualValues(t, 1, atomic.LoadInt32(&fake.closes),
		"Client.Close must close the underlying http3.Client exactly once")
}

// TestH3_RequestTrailers_Unsupported verifies request trailers are rejected on
// the buffered H3 transport rather than silently dropped.
func TestH3_RequestTrailers_Unsupported(t *testing.T) {
	fake := &fakeH3Client{resp: &http3.Response{Status: 200}}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()

	var resp Response
	err := c.Do(context.Background(), &Request{
		Method:   "POST",
		Path:     "/",
		BodyMode: BodyBuffer,
		Body:     []byte("x"),
		Trailers: []conn.HeaderField{{Name: []byte("x-trailer"), Value: []byte("v")}},
	}, &resp)

	assert.Truef(t, errors.Is(err, ErrTrailersUnsupportedH3),
		"Do err = %v, want ErrTrailersUnsupportedH3: silently dropping request trailers "+
			"would ship an incomplete request", err)
}

// TestH3_SplitPseudoHeaders exercises splitPseudoHeaders directly, including the
// unrecognised-pseudo-header rejection.
func TestH3_SplitPseudoHeaders(t *testing.T) {
	t.Run("routes the four known pseudo-headers", func(t *testing.T) {
		method, scheme, authority, path, regular, err := splitPseudoHeaders([]conn.HeaderField{
			{Name: []byte(":method"), Value: []byte("PUT")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":authority"), Value: []byte("a:443")},
			{Name: []byte(":path"), Value: []byte("/p")},
			{Name: []byte("h1"), Value: []byte("v1")},
			{Name: []byte("h2"), Value: []byte("v2")},
		}, nil)

		require.NoError(t, err, "splitPseudoHeaders")
		assert.Equal(t, "PUT", method, ":method")
		assert.Equal(t, "https", scheme, ":scheme")
		assert.Equal(t, "a:443", authority, ":authority")
		assert.Equal(t, "/p", path, ":path")
		require.Lenf(t, regular, 2, "regular = %+v, want exactly the two non-pseudo fields", regular)
		assert.Truef(t, hasHeader(regular, "h1", "v1"), "regular = %+v, want h1=v1 kept", regular)
		assert.Truef(t, hasHeader(regular, "h2", "v2"), "regular = %+v, want h2=v2 kept", regular)
	})

	t.Run("rejects an unrecognised pseudo-header", func(t *testing.T) {
		_, _, _, _, _, err := splitPseudoHeaders([]conn.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":protocol"), Value: []byte("websocket")},
		}, nil)

		assert.Truef(t, errors.Is(err, ErrInvalidRequest),
			"unknown pseudo-header err = %v, want ErrInvalidRequest: silently forwarding "+
				"one HTTP/3 has no mapping for would send a malformed request", err)
	})
}

// TestNewClient_H3_RequiresTLSConfig checks the Dialer carve-out: TransportH3
// needs a TLSConfig, not a conn.Dialer.
func TestNewClient_H3_RequiresTLSConfig(t *testing.T) {
	// No TLSConfig → rejected even without a Dialer.
	_, noTLS := NewClient(ClientOptions{Addr: "h:443", Transport: TransportH3})
	// With TLSConfig and no Dialer → accepted (Dialer carve-out).
	c, err := NewClient(ClientOptions{
		Addr:      "h:443",
		Transport: TransportH3,
		TLSConfig: &tls.Config{ServerName: "h"},
	})

	assert.Error(t, noTLS, "TransportH3 without a TLSConfig has nothing to dial with")
	require.NoError(t, err, "NewClient(TransportH3) with a TLSConfig and no Dialer")
	_, ok := c.tr.(*singleH3Conn)
	assert.Truef(t, ok, "transport is %T, want *singleH3Conn", c.tr)
	_ = c.Close()
}

// TestNewH3Client_WiresTransport verifies the focused constructor sets up a
// TransportH3 client.
func TestNewH3Client_WiresTransport(t *testing.T) {
	c, err := NewH3Client("h3.example:443", &tls.Config{ServerName: "h3.example"}, WithDialBackoff(0))

	require.NoError(t, err, "NewH3Client")
	defer func() { _ = c.Close() }()
	sc, ok := c.tr.(*singleH3Conn)
	require.Truef(t, ok, "transport is %T, want *singleH3Conn", c.tr)
	assert.Equal(t, "h3.example:443", sc.addr, "singleH3Conn addr")
	assert.NotNil(t, sc.tlsConfig, "singleH3Conn tlsConfig was not wired")
	assert.NotNil(t, sc.dialFn, "singleH3Conn dialFn was not wired")
}

// TestH3_DoStream_Incremental drives Client.DoStream over the H3 transport and
// asserts the response head arrives before the body and each DATA chunk surfaces
// as its own StreamEvent, ending with EndStream.
func TestH3_DoStream_Incremental(t *testing.T) {
	fake := &fakeH3Client{
		resp:       &http3.Response{Status: 200, Headers: []conn.HeaderField{{Name: []byte("content-type"), Value: []byte("text/plain")}}},
		bodyChunks: [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")},
	}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()

	var sr StreamResponse
	err := c.DoStream(context.Background(), &Request{Method: "GET", Path: "/stream"}, &sr)
	var got [][]byte
	if err == nil {
		defer func() { _ = sr.Close() }()
		for {
			ev, rerr := sr.Recv(context.Background())
			require.NoError(t, rerr, "Recv")
			if ev.Type == EventData && len(ev.Data) > 0 {
				got = append(got, append([]byte(nil), ev.Data...))
			}
			if ev.EndStream {
				break
			}
		}
	}

	require.NoError(t, err, "DoStream")
	assert.Equal(t, 200, sr.Status, "the response head must arrive before the body")
	assert.Truef(t, hasHeader(sr.Headers, "content-type", "text/plain"),
		"headers missing content-type: %+v", sr.Headers)
	want := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	require.Lenf(t, got, len(want),
		"got %d chunks (%q), want %d: each DATA chunk must surface as its own event "+
			"rather than being coalesced", len(got), got, len(want))
	for i := range want {
		assert.Truef(t, bytes.Equal(got[i], want[i]), "chunk %d = %q, want %q", i, got[i], want[i])
	}
}

// TestH3_DoStream_Trailers verifies a response trailer section surfaces as an
// EventTrailers over the H3 streaming path.
func TestH3_DoStream_Trailers(t *testing.T) {
	fake := &fakeH3Client{
		resp: &http3.Response{
			Status:   200,
			Trailers: []conn.HeaderField{{Name: []byte("grpc-status"), Value: []byte("0")}},
		},
		bodyChunks: [][]byte{[]byte("payload")},
	}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()
	var sr StreamResponse
	require.NoError(t,
		c.DoStream(context.Background(), &Request{Method: "GET", Path: "/"}, &sr), "DoStream")
	defer func() { _ = sr.Close() }()

	tr, err := sr.WaitTrailers(context.Background())

	require.NoError(t, err, "WaitTrailers")
	assert.Truef(t, hasHeader(tr, "grpc-status", "0"),
		"trailers = %+v, want grpc-status=0 — a gRPC caller reads its status from here", tr)
}

// TestH3_DoStream_ResetError verifies a mid-body error from the http3 body reader
// (e.g. a server RESET_STREAM) surfaces from Recv verbatim so retry classification
// via errors.As still works.
func TestH3_DoStream_ResetError(t *testing.T) {
	fake := &fakeH3Client{
		resp:       &http3.Response{Status: 200},
		bodyChunks: [][]byte{[]byte("partial")},
		nextErr:    &http3.StreamResetError{Code: http3.H3RequestRejected},
	}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()
	var sr StreamResponse
	require.NoError(t,
		c.DoStream(context.Background(), &Request{Method: "GET", Path: "/"}, &sr), "DoStream")
	defer func() { _ = sr.Close() }()

	// First Recv delivers the partial chunk; the second surfaces the reset error.
	_, firstErr := sr.Recv(context.Background())
	_, secondErr := sr.Recv(context.Background())

	require.NoError(t, firstErr, "Recv#1 must deliver the partial chunk")
	var rse *http3.StreamResetError
	require.Truef(t, errors.As(secondErr, &rse),
		"Recv#2 err = %v (%T), want *http3.StreamResetError so the retry layer can "+
			"classify a mid-body reset", secondErr, secondErr)
	assert.True(t, rse.Retryable(), "H3_REQUEST_REJECTED should be retryable")
}

// TestH3_BodyStream_ReadIncremental drives Do with BodyMode=BodyStream over the H3
// transport: the whole body reads back correctly, and each Read pulls exactly one
// underlying chunk — the proof that the body is streamed, not buffered whole.
func TestH3_BodyStream_ReadIncremental(t *testing.T) {
	fake := &fakeH3Client{
		resp:       &http3.Response{Status: 200},
		bodyChunks: [][]byte{[]byte("one"), []byte("two"), []byte("three")},
	}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()
	var resp Response
	resp.Reset()
	require.NoError(t,
		c.Do(context.Background(), &Request{Method: "GET", Path: "/", BodyMode: BodyStream}, &resp),
		"Do(BodyStream)")
	require.NotNil(t, resp.BodyReader, "BodyReader is nil for BodyStream over H3")

	// One Read with a buffer large enough for a chunk pulls exactly one chunk from
	// the underlying body — incremental, not the whole body at once.
	buf := make([]byte, 64)
	n, err := resp.BodyReader.Read(buf)
	pulledAfterFirst := fake.lastBody.idx
	rest, restErr := io.ReadAll(resp.BodyReader)
	closeErr := resp.BodyReader.Close()

	if err != nil {
		require.ErrorIs(t, err, io.EOF, "first Read")
	}
	assert.Equal(t, "one", string(buf[:n]), "the first Read must return exactly one chunk")
	assert.Equalf(t, 1, pulledAfterFirst,
		"underlying chunks pulled = %d after the first Read, want 1: the body was "+
			"buffered whole rather than streamed", pulledAfterFirst)
	require.NoError(t, restErr, "ReadAll")
	assert.Equal(t, "onetwothree", "one"+string(rest), "the whole body must read back intact")
	assert.NoError(t, closeErr, "Close")
}

// TestH3_BodyStream_UsesStreamingPath asserts the buffered Do path is NOT taken for
// BodyStream: the streaming DoStream path yields a BodyReader without pre-buffering.
func TestH3_BodyStream_UsesStreamingPath(t *testing.T) {
	fake := &fakeH3Client{resp: &http3.Response{Status: 200}, bodyChunks: [][]byte{[]byte("x")}}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()

	var resp Response
	resp.Reset()
	err := c.Do(context.Background(), &Request{Method: "GET", Path: "/", BodyMode: BodyStream}, &resp)

	require.NoError(t, err, "Do(BodyStream)")
	defer func() { _ = resp.BodyReader.Close() }()
	// The streaming path leaves Response.Body empty; body arrives only via BodyReader.
	assert.Emptyf(t, resp.Body,
		"Response.Body = %q, want empty: the streaming path must not pre-buffer", resp.Body)
	assert.NotNil(t, fake.lastBody,
		"DoStream path not taken: BodyStream fell through to the buffered Do")
}

// h3FailingDial returns a dialFn that always fails and counts its calls, so a
// test can tell "the backoff window suppressed the redial" from "the redial
// happened and failed again" — the two are the same error to the caller.
func h3FailingDial(n *atomic.Int32) func(context.Context, string, *tls.Config) (h3Client, error) {
	return func(context.Context, string, *tls.Config) (h3Client, error) {
		n.Add(1)
		return nil, errors.New("boom")
	}
}

// TestSingleH3Conn_Backoff_RefusesWithinWindow is the HTTP/3 sibling of
// TestSingleConn_Backoff_RefusesWithinWindow. The branch it covers — refuse a
// redial while the backoff window is open — had no test on this transport, and
// the mutation gate found it: four separate mutants of that one condition
// survived the whole suite.
func TestSingleH3Conn_Backoff_RefusesWithinWindow(t *testing.T) {
	var dials atomic.Int32
	s := &singleH3Conn{
		addr:        "h3.example:443",
		tlsConfig:   &tls.Config{ServerName: "h3.example"},
		backoff:     500 * time.Millisecond,
		dialTimeout: time.Second,
		metrics:     &Metrics{},
		dialFn:      h3FailingDial(&dials),
	}
	defer func() { _ = s.close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, err1 := s.acquireClient(ctx)
	_, _, err2 := s.acquireClient(ctx)

	require.Error(t, err1, "the first acquire dials and the dial fails")
	require.Error(t, err2, "the second acquire fails too, inside the backoff window")
	assert.EqualValues(t, 1, dials.Load(),
		"dial count = %d, want 1 — the second acquire is inside the backoff window and must not reach the network; a client that redials on every request turns one dead backend into a dial storm",
		dials.Load())
	assert.ErrorIsf(t, err2, ErrRedialBackoff,
		"second error = %v, want it to wrap ErrRedialBackoff so a caller can tell a suppressed redial from a fresh dial failure", err2)
}

// TestSingleH3Conn_ZeroBackoff_RedialsImmediately is the other equivalence
// class: with no backoff configured the suppression must not engage at all.
// Without this case a condition that always suppresses looks identical to one
// that suppresses correctly.
//
// Two CONDITIONALS_BOUNDARY mutants of that condition still survive these two
// tests, and both are equivalent mutants rather than holes:
//
//   - `s.backoff > 0` -> `>= 0` changes nothing, because with backoff == 0 the
//     third conjunct is `time.Since(...) < 0`, which is false anyway. The
//     branch cannot be entered either way.
//   - `time.Since(s.lastDialAt) < s.backoff` -> `<=` differs only when the
//     elapsed time equals the window to the nanosecond.
//
// Do not chase them with a sleep-tuned test: it would assert on a clock, not on
// behaviour.
func TestSingleH3Conn_ZeroBackoff_RedialsImmediately(t *testing.T) {
	var dials atomic.Int32
	s := &singleH3Conn{
		addr:        "h3.example:443",
		tlsConfig:   &tls.Config{ServerName: "h3.example"},
		backoff:     0,
		dialTimeout: time.Second,
		metrics:     &Metrics{},
		dialFn:      h3FailingDial(&dials),
	}
	defer func() { _ = s.close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, err1 := s.acquireClient(ctx)
	_, _, err2 := s.acquireClient(ctx)

	require.Error(t, err1, "the first acquire dials and the dial fails")
	require.Error(t, err2, "the second acquire dials again and fails again")
	assert.EqualValues(t, 2, dials.Load(),
		"dial count = %d, want 2 — with no backoff configured the second acquire must dial rather than be refused from the cached error",
		dials.Load())
	assert.NotErrorIsf(t, err2, ErrRedialBackoff,
		"second error = %v; with backoff disabled nothing should be suppressed", err2)
}
