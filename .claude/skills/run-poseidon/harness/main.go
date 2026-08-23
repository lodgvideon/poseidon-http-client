// Command harness runs the poseidon client against live local servers.
//
// It is agent tooling for the run-poseidon skill, not library code: it lives
// under .claude/ so `go build ./...`, golangci-lint and the coverage gate never
// see it (the go tool skips directories whose name starts with a dot), while
// `go run ./.claude/skills/run-poseidon/harness` still compiles it against this
// working tree.
//
//	go run ./.claude/skills/run-poseidon/harness smoke   # scenarios over real sockets
//	go run ./.claude/skills/run-poseidon/harness serve   # h2+TLS server for ./examples/loadgen
//
// smoke exits non-zero if any scenario fails; serve prints one
// "URL=https://127.0.0.1:PORT/" line and blocks until SIGINT/SIGTERM.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

func main() {
	cmd := "smoke"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "smoke":
		os.Exit(smoke())
	case "serve":
		serve()
	default:
		fmt.Fprintf(os.Stderr, "usage: harness [smoke|serve]\n")
		os.Exit(2)
	}
}

// ---- the server side ----------------------------------------------------

// mux is the handler set both smoke and serve use. /flaky answers 503 until
// its counter passes failures, which is what makes the retry scenario a
// property of the client rather than of the fixture.
func mux(failures int32) *http.ServeMux {
	var flakyHits atomic.Int32
	m := http.NewServeMux()

	m.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	m.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-proto", r.Proto)
		_, _ = w.Write([]byte("hello, poseidon"))
	})
	m.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		buf := make([]byte, 0, 1024)
		tmp := make([]byte, 512)
		for {
			n, err := r.Body.Read(tmp)
			buf = append(buf, tmp[:n]...)
			if err != nil {
				break
			}
		}
		_, _ = w.Write(buf)
	})
	m.HandleFunc("/stream", func(w http.ResponseWriter, _ *http.Request) {
		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", http.StatusInternalServerError)
			return
		}
		for i := 0; i < 3; i++ {
			_, _ = fmt.Fprintf(w, "chunk-%d;", i)
			fl.Flush()
			time.Sleep(5 * time.Millisecond)
		}
	})
	m.HandleFunc("/flaky", func(w http.ResponseWriter, _ *http.Request) {
		if flakyHits.Add(1) <= failures {
			http.Error(w, "come back later", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("recovered"))
	})
	m.HandleFunc("/notfound", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	})
	return m
}

// startH2 starts a TLS server that negotiates h2 via ALPN and returns it with
// its "host:port".
func startH2(failures int32) (*httptest.Server, string) {
	srv := httptest.NewUnstartedServer(mux(failures))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	return srv, strings.TrimPrefix(srv.URL, "https://")
}

// startH1 starts a cleartext HTTP/1.1 server (no ALPN, no TLS) for the
// PlaintextDialer + TransportH1SingleConn path.
func startH1(failures int32) (*httptest.Server, string) {
	srv := httptest.NewServer(mux(failures))
	return srv, strings.TrimPrefix(srv.URL, "http://")
}

// serve runs the h2 server until a signal arrives, printing the URL that
// ./examples/loadgen should be aimed at.
func serve() {
	srv, addr := startH2(0)
	defer srv.Close()

	fmt.Printf("URL=%s/\n", srv.URL)
	fmt.Printf("ADDR=%s\n", addr)
	_ = os.Stdout.Sync()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	fmt.Println("harness: shutting down")
}

// ---- the client side ----------------------------------------------------

func h2Client(addr string, pool *client.PoolOptions) (*client.Client, error) {
	dialer := &conn.TLSDialer{Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // self-signed httptest cert
	if pool != nil {
		return client.NewPoolClient(addr, dialer, *pool)
	}
	return client.NewSingleConnClient(addr, dialer)
}

type scenario struct {
	name string
	run  func(ctx context.Context) (string, error)
}

func scenarios() []scenario {
	return []scenario{
		{"h2-get", h2Get},
		{"h2-post-echo", h2PostEcho},
		{"h2-stream", h2Stream},
		{"h2-pool-concurrency", h2PoolConcurrency},
		{"h2-non2xx-metrics", h2Non2xx},
		{"h1-plaintext-get", h1Get},
		{"retry-503", retry503},
	}
}

func smoke() int {
	all := scenarios()
	failed := 0
	for _, sc := range all {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()
		detail, err := sc.run(ctx)
		cancel()
		if err != nil {
			fmt.Printf("FAIL %-20s %v\n", sc.name, err)
			failed++
			continue
		}
		fmt.Printf("PASS %-20s %-58s %s\n", sc.name, detail, time.Since(start).Round(time.Millisecond))
	}
	if failed > 0 {
		fmt.Printf("\nsmoke: %d/%d scenarios FAILED\n", failed, len(all))
		return 1
	}
	fmt.Printf("\nsmoke: %d/%d scenarios passed\n", len(all), len(all))
	return 0
}

func h2Get(ctx context.Context) (string, error) {
	srv, addr := startH2(0)
	defer srv.Close()

	c, err := h2Client(addr, nil)
	if err != nil {
		return "", fmt.Errorf("build client: %w", err)
	}
	defer func() { _ = c.Close() }()

	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/hello"), resp); err != nil {
		return "", fmt.Errorf("GET /hello: %w", err)
	}
	if resp.Status != 200 {
		return "", fmt.Errorf("status = %d, want 200", resp.Status)
	}
	if got := string(resp.Body); got != "hello, poseidon" {
		return "", fmt.Errorf("body = %q, want %q", got, "hello, poseidon")
	}
	return fmt.Sprintf("200, %d bytes, server saw %s", len(resp.Body), headerValue(resp, "x-proto")), nil
}

func h2PostEcho(ctx context.Context) (string, error) {
	srv, addr := startH2(0)
	defer srv.Close()

	c, err := h2Client(addr, nil)
	if err != nil {
		return "", fmt.Errorf("build client: %w", err)
	}
	defer func() { _ = c.Close() }()

	payload := strings.Repeat("poseidon-", 4096) // ~36 KiB: past one 16 KiB DATA frame
	resp := &client.Response{}
	if err := c.Do(ctx, client.POST("/echo", []byte(payload)), resp); err != nil {
		return "", fmt.Errorf("POST /echo: %w", err)
	}
	if resp.Status != 200 {
		return "", fmt.Errorf("status = %d, want 200", resp.Status)
	}
	if string(resp.Body) != payload {
		return "", fmt.Errorf("echo mismatch: got %d bytes, sent %d", len(resp.Body), len(payload))
	}
	return fmt.Sprintf("%d bytes over multiple DATA frames", len(payload)), nil
}

func h2Stream(ctx context.Context) (string, error) {
	srv, addr := startH2(0)
	defer srv.Close()

	c, err := h2Client(addr, nil)
	if err != nil {
		return "", fmt.Errorf("build client: %w", err)
	}
	defer func() { _ = c.Close() }()

	sr := &client.StreamResponse{}
	if err := c.DoStream(ctx, client.GET("/stream"), sr); err != nil {
		return "", fmt.Errorf("DoStream /stream: %w", err)
	}
	defer func() { _ = sr.Close() }()

	var body strings.Builder
	events := 0
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, client.ErrStreamEnded) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("Recv: %w", err)
		}
		if ev.Type == client.EventData {
			events++
			body.Write(ev.Data) // copied before the next Recv recycles the buffer
		}
		if ev.EndStream {
			break
		}
	}
	if !strings.Contains(body.String(), "chunk-2;") {
		return "", fmt.Errorf("streamed body = %q, want it to contain chunk-2;", body.String())
	}
	return fmt.Sprintf("status %d, %d DATA events, %q", sr.Status, events, body.String()), nil
}

func h2PoolConcurrency(ctx context.Context) (string, error) {
	srv, addr := startH2(0)
	defer srv.Close()

	c, err := h2Client(addr, &client.PoolOptions{MaxConnsPerHost: 4, MaxStreamsPerConn: 32})
	if err != nil {
		return "", fmt.Errorf("build pool client: %w", err)
	}
	defer func() { _ = c.Close() }()

	const workers, each = 16, 20
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			resp := &client.Response{}
			for j := 0; j < each; j++ {
				resp.Reset()
				if err := c.Do(ctx, client.GET("/hello"), resp); err != nil {
					errs <- err
					return
				}
				if resp.Status != 200 {
					errs <- fmt.Errorf("status %d", resp.Status)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return "", err
	}

	m := c.MetricsSnapshot()
	want := int64(workers * each)
	if m.Counters.Responses2xx != want {
		return "", fmt.Errorf("Responses2xx = %d, want %d", m.Counters.Responses2xx, want)
	}
	st := c.PoolStats()
	return fmt.Sprintf("%d requests, %d conns, p99 %s", want, st.ActiveConns, m.Latency.Request.Quantile(0.99)), nil
}

func h2Non2xx(ctx context.Context) (string, error) {
	srv, addr := startH2(0)
	defer srv.Close()

	c, err := h2Client(addr, nil)
	if err != nil {
		return "", fmt.Errorf("build client: %w", err)
	}
	defer func() { _ = c.Close() }()

	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/notfound"), resp); err != nil {
		return "", fmt.Errorf("GET /notfound: %w", err)
	}
	if resp.Status != 404 {
		return "", fmt.Errorf("status = %d, want 404", resp.Status)
	}
	m := c.MetricsSnapshot()
	if m.Counters.ResponsesNon2xx != 1 || m.Counters.RequestsErrored != 0 {
		return "", fmt.Errorf("non2xx=%d errored=%d, want 1 and 0 (a 404 is a response, not a transport error)",
			m.Counters.ResponsesNon2xx, m.Counters.RequestsErrored)
	}
	return "404 counted as non-2xx, not as an error", nil
}

func h1Get(ctx context.Context) (string, error) {
	srv, addr := startH1(0)
	defer srv.Close()

	c, err := client.NewClient(client.ClientOptions{
		Addr:      addr,
		Transport: client.TransportH1SingleConn,
		ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
	})
	if err != nil {
		return "", fmt.Errorf("build h1 client: %w", err)
	}
	defer func() { _ = c.Close() }()

	resp := &client.Response{}
	if err := c.Do(ctx, client.GET("/hello"), resp); err != nil {
		return "", fmt.Errorf("GET /hello: %w", err)
	}
	if resp.Status != 200 || string(resp.Body) != "hello, poseidon" {
		return "", fmt.Errorf("status=%d body=%q", resp.Status, resp.Body)
	}
	return fmt.Sprintf("200 over %s", headerValue(resp, "x-proto")), nil
}

func retry503(ctx context.Context) (string, error) {
	const failures = 2
	srv, addr := startH2(failures)
	defer srv.Close()

	c, err := h2Client(addr, nil)
	if err != nil {
		return "", fmt.Errorf("build client: %w", err)
	}
	defer func() { _ = c.Close() }()

	var retries atomic.Int32
	c.SetHooks(&client.Hooks{OnRetry: func(client.RetryEvent) { retries.Add(1) }})

	r := c.Retryer(client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(_ error, resp *client.Response) bool {
			return resp != nil && resp.Status == 503
		},
	})

	resp := &client.Response{}
	if err := r.Do(ctx, client.GET("/flaky"), resp); err != nil {
		return "", fmt.Errorf("retryer Do: %w", err)
	}
	if resp.Status != 200 || string(resp.Body) != "recovered" {
		return "", fmt.Errorf("status=%d body=%q, want 200 recovered", resp.Status, resp.Body)
	}
	if got := retries.Load(); got != failures {
		return "", fmt.Errorf("OnRetry fired %d times, want %d", got, failures)
	}
	return fmt.Sprintf("%d 503s replayed, attempt 3 won", failures), nil
}

func headerValue(resp *client.Response, name string) string {
	for _, h := range resp.Headers {
		if strings.EqualFold(string(h.Name), name) {
			return string(h.Value)
		}
	}
	return "?"
}
