package http3

import (
	"context"
	"errors"
	"testing"
)

// TestClient_Do_ConcurrentRejected proves the single-request contract PR 2c keeps
// (inFlight retained): while one Do is in flight — its request sent and parked in
// WaitReadable holding the guard — a second concurrent Do returns ErrConcurrentUse
// without touching the connection, and the guard is released once the first Do
// completes. Run under -race.
func TestClient_Do_ConcurrentRejected(t *testing.T) {
	headersFrame := AppendHeaders(nil, encodeSection(hf(":status", "200")))
	dataFrame := AppendData(nil, []byte("hi"))

	// The request stream yields its HEADERS but no FIN, so the first Do reads the
	// headers then parks in WaitReadable holding the in-flight guard.
	req := &fakeStream{recvChunks: [][]byte{headersFrame}, fin: false}
	conn := &fakeConn{req: req}

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

	waitSent(t, conn) // the first Do holds the guard and is parked reading the response

	// A second, concurrent Do must be rejected loudly rather than corrupt the
	// shared connection / QPACK / control-stream state.
	if _, _, err := client.Do(context.Background(), &Request{
		Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"}); !errors.Is(err, ErrConcurrentUse) {
		t.Fatalf("concurrent Do = %v, want ErrConcurrentUse", err)
	}

	conn.pushRecv(dataFrame, true) // deliver the body + FIN so the first Do completes

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
