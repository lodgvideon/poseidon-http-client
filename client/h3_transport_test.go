package client

import (
	"context"
	"crypto/tls"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/http3"
)

// fakeH3Client is a test double for *http3.Client. It captures the request it
// receives (deep-copied, since the real path mutates a pooled buffer after the
// call) and returns a canned response/body or error.
type fakeH3Client struct {
	resp  *http3.Response
	body  []byte
	doErr error

	gotReq  *http3.Request
	doCalls int32
	closes  int32
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

func (f *fakeH3Client) Close() error {
	atomic.AddInt32(&f.closes, 1)
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
	if err != nil {
		t.Fatalf("NewClient(TransportH3): %v", err)
	}
	sc, ok := c.tr.(*singleH3Conn)
	if !ok {
		t.Fatalf("transport is %T, want *singleH3Conn", c.tr)
	}
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
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	// Pseudo-headers routed to typed http3.Request fields.
	got := fake.gotReq
	if got.Method != "GET" || got.Scheme != "https" || got.Authority != "h3.example" || got.Path != "/x" {
		t.Fatalf("re-split pseudo-headers = {%q %q %q %q}, want {GET https h3.example /x}",
			got.Method, got.Scheme, got.Authority, got.Path)
	}
	// No pseudo-header leaked into the regular Headers slice; x-test survived.
	for _, h := range got.Headers {
		if len(h.Name) > 0 && h.Name[0] == ':' {
			t.Fatalf("pseudo-header %q leaked into http3.Request.Headers", h.Name)
		}
	}
	if !hasHeader(got.Headers, "x-test", "1") {
		t.Fatalf("x-test header missing from http3.Request.Headers: %+v", got.Headers)
	}

	// Response synthesised back.
	if resp.Status != 200 {
		t.Fatalf("resp.Status = %d, want 200", resp.Status)
	}
	if string(resp.Body) != "hello h3" {
		t.Fatalf("resp.Body = %q, want %q", resp.Body, "hello h3")
	}
	if !hasHeader(resp.Headers, "content-type", "text/plain") {
		t.Fatalf("content-type header missing from resp.Headers: %+v", resp.Headers)
	}
	if resp.BytesReceived != int64(len("hello h3")) {
		t.Fatalf("resp.BytesReceived = %d, want %d", resp.BytesReceived, len("hello h3"))
	}
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
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if string(fake.gotReq.Body) != "payload-bytes" {
		t.Fatalf("http3.Request.Body = %q, want %q", fake.gotReq.Body, "payload-bytes")
	}
	if resp.Status != 201 {
		t.Fatalf("resp.Status = %d, want 201", resp.Status)
	}
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
	if err := ex.SendHeadersWithPriority(context.Background(), fields, true, nil); err != nil {
		t.Fatalf("SendHeadersWithPriority: %v", err)
	}
	ctx := context.Background()

	ev, err := ex.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv#1: %v", err)
	}
	if ev.Type != conn.EventHeaders {
		t.Fatalf("Recv#1 type = %s, want headers", ev.Type)
	}
	if len(ev.Headers) == 0 || string(ev.Headers[0].Name) != ":status" || string(ev.Headers[0].Value) != "200" {
		t.Fatalf("Recv#1 first header = %+v, want :status=200 first", ev.Headers)
	}
	if !hasHeader(ev.Headers, "x-h", "v") {
		t.Fatalf("Recv#1 missing x-h header: %+v", ev.Headers)
	}
	if ev.EndStream {
		t.Fatal("Recv#1 EndStream = true, want false (data follows)")
	}

	ev, err = ex.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv#2: %v", err)
	}
	if ev.Type != conn.EventData || string(ev.Data) != "body-bytes" {
		t.Fatalf("Recv#2 = {%s %q}, want {data body-bytes}", ev.Type, ev.Data)
	}
	if ev.EndStream {
		t.Fatal("Recv#2 EndStream = true, want false (trailers follow)")
	}

	ev, err = ex.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv#3: %v", err)
	}
	if ev.Type != conn.EventTrailers || !hasHeader(ev.Headers, "x-t", "w") {
		t.Fatalf("Recv#3 = {%s %+v}, want {trailers x-t=w}", ev.Type, ev.Headers)
	}
	if !ev.EndStream {
		t.Fatal("Recv#3 EndStream = false, want true (final event)")
	}

	if _, err = ex.Recv(ctx); !errors.Is(err, conn.ErrStreamClosed) {
		t.Fatalf("Recv#4 err = %v, want ErrStreamClosed", err)
	}
	if n := atomic.LoadInt32(&fake.doCalls); n != 1 {
		t.Fatalf("http3.Client.Do called %d times, want exactly 1", n)
	}
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
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if string(resp.Body) != "data" {
		t.Fatalf("resp.Body = %q, want %q", resp.Body, "data")
	}
	if !hasHeader(resp.Trailers, "grpc-status", "0") {
		t.Fatalf("grpc-status trailer missing: %+v", resp.Trailers)
	}
}

// TestH3_Do_ErrorPassthrough verifies a Do error is returned verbatim so a
// caller can errors.As it — here a retryable *http3.StreamResetError.
func TestH3_Do_ErrorPassthrough(t *testing.T) {
	fake := &fakeH3Client{doErr: &http3.StreamResetError{Code: http3.H3RequestRejected}}
	c := newH3TestClient(t, fake, nil)
	defer func() { _ = c.Close() }()

	var resp Response
	err := c.Do(context.Background(), &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp)
	if err == nil {
		t.Fatal("Do err = nil, want *http3.StreamResetError")
	}
	var sre *http3.StreamResetError
	if !errors.As(err, &sre) {
		t.Fatalf("Do err = %v (%T), want *http3.StreamResetError", err, err)
	}
	if !sre.Retryable() {
		t.Fatal("StreamResetError.Retryable() = false, want true for H3_REQUEST_REJECTED")
	}
	if c.Metrics().Counters.RequestsErrored.Load() != 1 {
		t.Fatalf("RequestsErrored = %d, want 1", c.Metrics().Counters.RequestsErrored.Load())
	}
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
	if err := c.Do(context.Background(), &Request{Method: "GET", Path: "/", BodyMode: BodyBuffer}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}

	m := c.Metrics()
	if m.Counters.RequestsStarted.Load() != 1 || m.Counters.RequestsSucceeded.Load() != 1 || m.Counters.Responses2xx.Load() != 1 {
		t.Fatalf("counters started=%d succeeded=%d 2xx=%d, want 1/1/1",
			m.Counters.RequestsStarted.Load(), m.Counters.RequestsSucceeded.Load(), m.Counters.Responses2xx.Load())
	}
	if m.Counters.DialsAttempted.Load() != 1 {
		t.Fatalf("DialsAttempted = %d, want 1", m.Counters.DialsAttempted.Load())
	}
	if dialN.Load() != 1 {
		t.Fatalf("OnDial fired %d times, want 1", dialN.Load())
	}
	if completeN.Load() != 1 || gotStatus.Load() != 200 {
		t.Fatalf("OnRequestComplete fired %d times (status %d), want 1 (status 200)", completeN.Load(), gotStatus.Load())
	}

	// Close fires OnConnClose + ConnsClosed and closes the underlying client.
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if connCloseN.Load() != 1 {
		t.Fatalf("OnConnClose fired %d times, want 1", connCloseN.Load())
	}
	if m.Counters.ConnsClosed.Load() != 1 {
		t.Fatalf("ConnsClosed = %d, want 1", m.Counters.ConnsClosed.Load())
	}
	if n := atomic.LoadInt32(&fake.closes); n != 1 {
		t.Fatalf("http3.Client.Close called %d times, want 1", n)
	}
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
	if !errors.Is(err, ErrTrailersUnsupportedH3) {
		t.Fatalf("Do err = %v, want ErrTrailersUnsupportedH3", err)
	}
}

// TestH3_SplitPseudoHeaders exercises splitPseudoHeaders directly, including the
// unrecognised-pseudo-header rejection.
func TestH3_SplitPseudoHeaders(t *testing.T) {
	method, scheme, authority, path, regular, err := splitPseudoHeaders([]conn.HeaderField{
		{Name: []byte(":method"), Value: []byte("PUT")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("a:443")},
		{Name: []byte(":path"), Value: []byte("/p")},
		{Name: []byte("h1"), Value: []byte("v1")},
		{Name: []byte("h2"), Value: []byte("v2")},
	})
	if err != nil {
		t.Fatalf("splitPseudoHeaders: %v", err)
	}
	if method != "PUT" || scheme != "https" || authority != "a:443" || path != "/p" {
		t.Fatalf("split = {%q %q %q %q}, want {PUT https a:443 /p}", method, scheme, authority, path)
	}
	if len(regular) != 2 || !hasHeader(regular, "h1", "v1") || !hasHeader(regular, "h2", "v2") {
		t.Fatalf("regular = %+v, want [h1=v1 h2=v2]", regular)
	}

	if _, _, _, _, _, err = splitPseudoHeaders([]conn.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":protocol"), Value: []byte("websocket")},
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unknown pseudo-header err = %v, want ErrInvalidRequest", err)
	}
}

// TestNewClient_H3_RequiresTLSConfig checks the Dialer carve-out: TransportH3
// needs a TLSConfig, not a conn.Dialer.
func TestNewClient_H3_RequiresTLSConfig(t *testing.T) {
	// No TLSConfig → rejected even without a Dialer.
	if _, err := NewClient(ClientOptions{Addr: "h:443", Transport: TransportH3}); err == nil {
		t.Fatal("NewClient(TransportH3) without TLSConfig = nil error, want failure")
	}
	// With TLSConfig and no Dialer → accepted (Dialer carve-out).
	c, err := NewClient(ClientOptions{
		Addr:      "h:443",
		Transport: TransportH3,
		TLSConfig: &tls.Config{ServerName: "h"},
	})
	if err != nil {
		t.Fatalf("NewClient(TransportH3) with TLSConfig: %v", err)
	}
	if _, ok := c.tr.(*singleH3Conn); !ok {
		t.Fatalf("transport is %T, want *singleH3Conn", c.tr)
	}
	_ = c.Close()
}

// TestNewH3Client_WiresTransport verifies the focused constructor sets up a
// TransportH3 client.
func TestNewH3Client_WiresTransport(t *testing.T) {
	c, err := NewH3Client("h3.example:443", &tls.Config{ServerName: "h3.example"}, WithDialBackoff(0))
	if err != nil {
		t.Fatalf("NewH3Client: %v", err)
	}
	defer func() { _ = c.Close() }()
	sc, ok := c.tr.(*singleH3Conn)
	if !ok {
		t.Fatalf("transport is %T, want *singleH3Conn", c.tr)
	}
	if sc.addr != "h3.example:443" || sc.tlsConfig == nil || sc.dialFn == nil {
		t.Fatalf("singleH3Conn not wired: addr=%q tls=%v dialFn=%v", sc.addr, sc.tlsConfig != nil, sc.dialFn != nil)
	}
}
