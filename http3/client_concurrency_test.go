package http3

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// TestClient_Do_ConcurrentRejected proves the single-request contract is
// enforced, not merely documented: while one Do is in flight, a second
// concurrent Do returns ErrConcurrentUse without touching the connection, and
// the guard is released once the first Do completes.
//
// It is a real concurrency test — the first Do is blocked inside its receive
// loop (via pollHook) with the in-flight guard held, and the second Do races it
// — so it must be run under -race.
func TestClient_Do_ConcurrentRejected(t *testing.T) {
	headersFrame := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	dataFrame := AppendData(nil, []byte("hi"))

	// The request stream yields its HEADERS but is not finished, so the first Do
	// blocks in poll; on release, poll delivers the DATA frame and the FIN so Do
	// can complete.
	req := &fakeStream{recvChunks: [][]byte{headersFrame}, fin: false}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	conn := &fakeConn{req: req}
	conn.pollHook = func(context.Context) error {
		once.Do(func() {
			close(entered) // the first Do is now past the guard and blocked here
			<-release
			req.recvChunks = append(req.recvChunks, dataFrame)
			req.fin = true
		})
		return nil
	}

	client, err := NewClientFake(conn, nil)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		resp *Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, _, err := client.Do(context.Background(), &Request{
			Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"})
		done <- result{resp, err}
	}()

	<-entered // the first Do holds the in-flight guard and is blocked in poll

	// A second, concurrent Do must be rejected loudly rather than corrupt the
	// shared connection / QPACK / control-stream state.
	if _, _, err := client.Do(context.Background(), &Request{
		Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}); !errors.Is(err, ErrConcurrentUse) {
		t.Fatalf("concurrent Do = %v, want ErrConcurrentUse", err)
	}

	close(release) // let the first Do finish
	got := <-done
	if got.err != nil {
		t.Fatalf("first Do = %v, want success", got.err)
	}
	if got.resp == nil || got.resp.Status != 200 {
		t.Fatalf("first Do resp = %+v, want status 200", got.resp)
	}
	// The guard must be released once Do returns, so the next request is admitted.
	if client.inFlight.Load() {
		t.Fatal("in-flight guard still held after Do returned")
	}
}
