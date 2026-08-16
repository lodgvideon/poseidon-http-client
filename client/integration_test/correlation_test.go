//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// The suite checked that a response came back, not that it was THIS request's
// response. Every concurrent test fired the identical `GET /healthz` and asserted
// only the status, so every reply was the same two bytes: deliver stream A's
// response to stream B and both assertions still pass. The tests were structurally
// incapable of failing on mixing (#651).
//
// That matters more here than in most clients. This one pools buffers and decodes
// header blocks into reused storage, so the failure mode is not a crash — it is a
// response, or a header value, that belongs to a neighbouring request. Nothing above
// conn/multistream_test.go (10 streams, 13-byte bodies, below client and the pool)
// could see it.
//
// These give every in-flight request a distinct identity on both channels a response
// can carry — the body and a header — and make each one prove it got its own back.

// idBody builds this request's unique body. Long enough to span more than one DATA
// frame's worth of copying at the sizes the pool works in, and self-describing so a
// failure message names the request the bytes actually came from.
func idBody(i int) []byte {
	var b bytes.Buffer
	for b.Len() < 3000 {
		fmt.Fprintf(&b, "req-%04d-seq-%04d;", i, b.Len())
	}
	return b.Bytes()
}

// TestMatrix_ConcurrentIdentity is the mixing detector. N concurrent requests, each
// POSTing its own body to /echo, each asserting the response carries THAT body.
//
// It rests only on body echo, which every peer in the matrix already implements, so
// it runs everywhere without a fixture change. A response delivered to the wrong
// stream, a pooled buffer handed to two streams, or a slice outliving its owner all
// surface as one request seeing another's bytes.
func TestMatrix_ConcurrentIdentity(t *testing.T) {
	const N = 30
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			var wg sync.WaitGroup
			errs := make(chan error, N)

			wg.Add(N)
			for i := 0; i < N; i++ {
				go func(i int) {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()

					want := idBody(i)
					var resp client.Response
					resp.Reset()
					if err := c.Do(ctx, &client.Request{
						Method:   "POST",
						Path:     "/echo",
						Body:     want,
						BodyMode: client.BodyBuffer,
					}, &resp); err != nil {
						errs <- fmt.Errorf("req %d: Do: %w", i, err)
						return
					}
					if resp.Status != 200 {
						errs <- fmt.Errorf("req %d: status %d", i, resp.Status)
						return
					}
					if !bytes.Equal(resp.Body, want) {
						errs <- fmt.Errorf("req %d got a body that is not its own: %s",
							i, describeMismatch(want, resp.Body))
						return
					}
				}(i)
			}
			wg.Wait()
			close(errs)

			for err := range errs {
				t.Error(err)
			}
		})
	}
}

// describeMismatch names the request a wrong body actually belongs to, which is the
// difference between "corruption somewhere" and "stream i was handed stream j's
// response". Bodies are self-describing (see idBody), so the answer is in the bytes.
func describeMismatch(want, got []byte) string {
	if len(got) == 0 {
		return "the response body was empty"
	}
	head := got
	if len(head) > 24 {
		head = head[:24]
	}
	wantHead := want
	if len(wantHead) > 24 {
		wantHead = wantHead[:24]
	}
	if len(got) != len(want) {
		return fmt.Sprintf("length %d, want %d; starts %q, want %q",
			len(got), len(want), head, wantHead)
	}
	return fmt.Sprintf("same length but different bytes; starts %q, want %q", head, wantHead)
}

// findHeader returns the value of the named response header, matched case-insensitively
// per RFC 9113 §8.2, or "" when absent.
func findHeader(fields []conn.HeaderField, name string) string {
	for _, h := range fields {
		if strings.EqualFold(string(h.Name), name) {
			return string(h.Value)
		}
	}
	return ""
}

// containsFold is strings.Contains with ASCII case folding, because peers are free to
// echo header names in any case.
func containsFold(haystack, needle string) bool {
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(needle))
}

// TestMatrix_ConcurrentHeaderIdentity is the same question asked of the header
// channel, which is the one that carries pooled, reused decode storage — a header
// value is far likelier to be aliased across streams than a body is.
//
// It reads X-Echo-Headers, which test/integration/fixtures/CONTRACT.md has specified
// since it was written and which no test has ever read: repo-wide the string occurred
// exactly twice, the contract line and Undertow's implementation. Asserted
// unconditionally rather than skipped where absent — a peer that claims the contract
// and does not implement it is the finding, not a reason to weaken the test.
func TestMatrix_ConcurrentHeaderIdentity(t *testing.T) {
	const N = 30
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			var wg sync.WaitGroup
			errs := make(chan error, N)

			wg.Add(N)
			for i := 0; i < N; i++ {
				go func(i int) {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()

					id := "poseidon-req-" + strconv.Itoa(i)
					var resp client.Response
					resp.Reset()
					if err := c.Do(ctx, &client.Request{
						Method: "POST",
						Path:   "/echo",
						Body:   []byte(id),
						Headers: []conn.HeaderField{
							{Name: []byte("x-request-id"), Value: []byte(id)},
						},
						BodyMode: client.BodyBuffer,
					}, &resp); err != nil {
						errs <- fmt.Errorf("req %d: Do: %w", i, err)
						return
					}
					if resp.Status != 200 {
						errs <- fmt.Errorf("req %d: status %d", i, resp.Status)
						return
					}
					echoed := findHeader(resp.Headers, "x-echo-headers")
					if echoed == "" {
						errs <- fmt.Errorf("req %d: no X-Echo-Headers on the response — "+
							"CONTRACT.md specifies /echo returns the request headers there, so "+
							"this peer does not implement the contract it claims and the header "+
							"channel is unverifiable against it", i)
						return
					}
					if !containsFold(echoed, id) {
						errs <- fmt.Errorf("req %d: X-Echo-Headers does not carry this request's "+
							"id %q — the server saw a different request's header value: %q",
							i, id, echoed)
					}
				}(i)
			}
			wg.Wait()
			close(errs)

			for err := range errs {
				t.Error(err)
			}
		})
	}
}
