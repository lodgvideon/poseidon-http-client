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
	"github.com/stretchr/testify/assert"
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

// idBodySize is large enough that N concurrent in-flight request bodies exceed the
// 65535-byte connection-level send window RFC 9113 §6.9.2 fixes at handshake —
// 30 × 3006 = 90180 — which is the point.
//
// It was 1500 for a while, on the belief that the overflow was a peer constraint:
// nginx appeared to stop granting connection-level credit, 21 of 30 bodies landed
// (63126 bytes) and the rest hung forever, and curl was reported to fail the same
// way. That reading was wrong twice over. The re-run of the curl leg passes 30/30
// against the same pinned image, and a frame trace showed nginx granting the whole
// 2^31-1 immediately after its SETTINGS — before the client's first HEADERS. The
// client discarded that WINDOW_UPDATE because it arrived while `settingsRecorder`
// was still the frame handler, so its own send window stayed at 65535 forever
// (#701).
//
// So the size is back above the window deliberately: it is the cheapest end-to-end
// guard that the handshake credit survives, against a real peer that actually sends
// one. conn/handshake_windowupdate_test.go pins the mechanism.
const idBodySize = 3006

// idBody builds this request's unique body: self-describing, so a failure message
// can name the request the bytes actually came from rather than only reporting that
// they are wrong.
func idBody(i int) []byte {
	var b bytes.Buffer
	for b.Len() < idBodySize {
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
				assert.NoError(t, err, "a concurrent request did not get its own response back — "+
					"the client pools buffers and reuses header-decode storage, so a mix shows up "+
					"as a neighbouring request's bytes rather than as a crash")
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
				assert.NoError(t, err, "the header channel did not keep concurrent requests "+
					"apart — header values are decoded into pooled, reused storage, so this is "+
					"the channel most likely to alias one stream's value into another")
			}
		})
	}
}
