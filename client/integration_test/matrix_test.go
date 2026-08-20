//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allReadyServers returns every server that passed healthcheck.
// Tests using this automatically parameterize across implementations.
func allReadyServers(t *testing.T) []*TestServer {
	t.Helper()
	var out []*TestServer
	for k := ServerGoHTTP; k < serverKindCount; k++ {
		if srv, ok := allServers[k]; ok && srv.Ready {
			out = append(out, srv)
		}
	}
	if len(out) == 0 {
		t.Skip("no servers ready")
	}
	return out
}

// ── Matrix: basic round-trip ────────────────────────────────────

func TestMatrix_Healthz(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)

			status, body := doGET(t, c, "/healthz", true)

			require.Equalf(t, 200, status, "status: got %d, want 200", status)
			require.Equalf(t, "ok", string(body), "body: got %q, want %q", body, "ok")
		})
	}
}

// TestMatrix_TLS_Healthz exercises the h2-over-TLS leg of every peer that has
// one, which no other matrix test reaches: newTestClient prefers h2c wherever a
// peer offers it, so Undertow and nghttpx were only ever driven over cleartext
// and nginx was the sole peer negotiating ALPN.
//
// The count assertion is the point of the test as much as the requests are. Peers
// are discovered at runtime, so "every peer with a TLS address" is a set that can
// quietly become empty — and an empty loop passes.
func TestMatrix_TLS_Healthz(t *testing.T) {
	tested := 0
	for _, srv := range allReadyServers(t) {
		if srv.TLSAddr == "" {
			continue // the in-process Go reference is h2c only
		}
		tested++
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClientTLS(t, srv)
			require.NotNil(t, c, "newTestClientTLS returned no client for a peer that "+
				"advertises a TLS address, so this subtest would dereference nil rather "+
				"than exercise ALPN")

			status, body := doGET(t, c, "/healthz", true)

			require.Equalf(t, 200, status, "status: got %d, want 200", status)
			require.Equalf(t, "ok", string(body), "body: got %q, want %q", body, "ok")
		})
	}

	// With POSEIDON_IT_SKIP_REMOTE (make it-test-fast) only the in-process Go
	// reference exists and it is h2c-only, so having nothing to test is the
	// expected outcome rather than a failure. Without it, every TLS peer
	// vanishing means the compose stack is not up and a silent pass would be
	// the worst answer available.
	if tested == 0 && skipRemote {
		t.Skip("POSEIDON_IT_SKIP_REMOTE: no TLS peers by design")
	}
	require.NotZero(t, tested, "no peer advertised a TLS address, so nothing here ran over TLS: "+
		"this test would otherwise have passed without testing anything")
}

func TestMatrix_Root(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)

			status, body := doGET(t, c, "/", true)

			require.Equalf(t, 200, status, "status: got %d", status)
			require.NotEmpty(t, body, "body: empty")
		})
	}
}

// ── Matrix: status codes ────────────────────────────────────────

func TestMatrix_StatusCodes(t *testing.T) {
	codes := []int{200, 201, 301, 400, 404, 500}
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			for _, code := range codes {
				t.Run(fmt.Sprintf("code_%d", code), func(t *testing.T) {
					status, _ := doGET(t, c, fmt.Sprintf("/status/%d", code), false)

					require.Equalf(t, code, status, "got %d, want %d", status, code)
				})
			}
		})
	}
}

// ── Matrix: echo (POST) ─────────────────────────────────────────

func TestMatrix_Echo(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			payload := []byte("cross-server echo test")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method:   "POST",
				Path:     "/echo",
				Body:     payload,
				BodyMode: client.BodyBuffer,
			}, &resp)

			require.NoErrorf(t, err, "Do POST /echo: %v", err)
			require.Equalf(t, 200, resp.Status, "status: got %d", resp.Status)
			require.Truef(t, bytes.Equal(resp.Body, payload),
				"body: got %q, want %q", resp.Body, payload)
		})
	}
}

// ── Matrix: connection reuse ────────────────────────────────────

// TestMatrix_ConnectionReuse asserts every peer served all N requests on one
// connection.
//
// Ten 200s used to be the whole assertion, and ten connections produce ten 200s
// just as readily (#893) - the mutation that makes singleConn.acquireConn refuse
// its own cached connection left this green on all four peers. The dial count is
// the only thing here that distinguishes reuse from reconnection, and it is worth
// asking of each implementation separately: whether a peer keeps the connection
// open is a property of that peer as much as of this client.
func TestMatrix_ConnectionReuse(t *testing.T) {
	const N = 10
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			var dials atomic.Int64
			c := newCountingTestClient(t, srv, &dials)

			statuses := make([]int, 0, N)
			for i := 0; i < N; i++ {
				status, _ := doGET(t, c, "/healthz", false)
				statuses = append(statuses, status)
			}

			for i, status := range statuses {
				require.Equalf(t, 200, status, "req %d/%d: status %d", i+1, N, status)
			}
			assert.EqualValuesf(t, 1, dials.Load(),
				"%d sequential requests to this peer completed %d dials, want exactly 1.\n"+
					"Every one of them came back 200 either way, so the statuses above cannot "+
					"tell a reused connection from a fresh one per request", N, dials.Load())
		})
	}
}

// ── Matrix: concurrent requests ─────────────────────────────────

func TestMatrix_Concurrent(t *testing.T) {
	const N = 30
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			var wg sync.WaitGroup
			errs := make(chan error, N)

			wg.Add(N)
			for i := 0; i < N; i++ {
				go func() {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()

					var resp client.Response
					resp.Reset()
					if err := c.Do(ctx, &client.Request{
						Method:   "GET",
						Path:     "/healthz",
						BodyMode: client.BodyBuffer,
					}, &resp); err != nil {
						errs <- fmt.Errorf("Do: %w", err)
						return
					}
					if resp.Status != 200 {
						errs <- fmt.Errorf("status %d", resp.Status)
					}
				}()
			}
			wg.Wait()
			close(errs)

			for err := range errs {
				assert.NoError(t, err, "one of 30 requests multiplexed on a single "+
					"connection to this peer failed")
			}
		})
	}
}

// ── Matrix: chunked streaming ───────────────────────────────────

func TestMatrix_ChunkedBody(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			// 100 × 1KB chunks with 10ms delay = ~1s
			expected := 100 * 1024

			status, body := doGET(t, c, "/chunked", true)

			require.Equalf(t, 200, status, "status: got %d", status)
			require.Lenf(t, body, expected, "body length: got %d, want %d", len(body), expected)
		})
	}
}

// ── Matrix: large body (within window) ──────────────────────────

func TestMatrix_LargeBody_32KB(t *testing.T) {
	const sz = 32 * 1024
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)

			status, body := doGET(t, c, fmt.Sprintf("/large?bytes=%d", sz), true)

			require.Equalf(t, 200, status, "status: got %d", status)
			require.Lenf(t, body, sz, "body length: got %d, want %d", len(body), sz)
		})
	}
}

// ── Matrix: delay + context cancel ──────────────────────────────

func TestMatrix_Delay(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)

			status, body := doGET(t, c, "/delay?ms=100", true)

			require.Equalf(t, 200, status, "status: got %d", status)
			require.Containsf(t, string(body), "delayed", "body: got %q", body)
		})
	}
}

func TestMatrix_ContextCancel(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method: "GET",
				Path:   "/delay?ms=5000",
			}, &resp)

			require.Error(t, err, "expected timeout error, got nil")
		})
	}
}

// ── Matrix: response headers ────────────────────────────────────

func TestMatrix_ResponseHeaders(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     "/healthz",
				BodyMode: client.BodyBuffer,
			}, &resp)

			require.NoErrorf(t, err, "Do: %v", err)
			require.Equalf(t, 200, resp.Status, "status: got %d", resp.Status)
			// Every server should send at least content-type
			require.NotEmpty(t, resp.Headers, "no response headers")
		})
	}
}

// ── Matrix: request headers ─────────────────────────────────────

// TestMatrix_RequestHeaders asks each peer what it saw, instead of only whether
// it answered.
//
// The header went to /healthz and only the status was asserted, so a client that
// dropped every Request.Headers entry stayed green on all four peers (#892).
// X-Echo-Headers off /echo is the channel CONTRACT.md specifies for exactly this
// and every peer implements it. Asked per peer because header encoding is where
// implementations differ: each one runs its own HPACK decoder over what this
// client emitted.
func TestMatrix_RequestHeaders(t *testing.T) {
	const name, value = "x-matrix-test", "cross-server"
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     "/echo",
				BodyMode: client.BodyBuffer,
				Headers: []conn.HeaderField{
					{Name: []byte(name), Value: []byte(value)},
				},
			}, &resp)

			require.NoErrorf(t, err, "Do: %v", err)
			require.Equalf(t, 200, resp.Status, "status: got %d", resp.Status)
			echoed := findHeader(resp.Headers, "x-echo-headers")
			require.NotEmpty(t, echoed, "no X-Echo-Headers on the response - CONTRACT.md "+
				"specifies /echo returns the request headers there, so this peer does not "+
				"implement the contract it claims and the header channel is unverifiable "+
				"against it")
			assert.Truef(t, containsFold(echoed, name),
				"this peer never saw the header name %q; it echoed %q.\n"+
					"A dropped request header costs nothing observable in the status, which "+
					"is all this test used to check", name, echoed)
			assert.Truef(t, containsFold(echoed, value),
				"this peer saw the header name but not the value %q; it echoed %q.\n"+
					"A name carried without its value is the same loss to a caller setting "+
					"an authorization or a routing header", value, echoed)
		})
	}
}

// ── Matrix: metrics ─────────────────────────────────────────────

func TestMatrix_Metrics(t *testing.T) {
	const sz = 8192
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     fmt.Sprintf("/large?bytes=%d", sz),
				BodyMode: client.BodyBuffer,
			}, &resp)

			require.NoErrorf(t, err, "Do: %v", err)
			require.Equalf(t, 200, resp.Status, "status: got %d", resp.Status)
			require.GreaterOrEqualf(t, resp.BytesReceived, int64(sz),
				"BytesReceived: got %d, want >= %d", resp.BytesReceived, sz)
		})
	}
}

// ── Matrix: the connection-flow-control boundary ────────────────
//
// Response sizes in this suite were 32 KiB on the matrix and 1 MiB / 60 KiB on
// go-http alone, so nothing sat AT the 65535-byte connection receive window RFC
// 9113 section 6.9.2 fixes at handshake (#896). That is the boundary where the
// refund matters: 65535 bytes fit the initial window exactly and can arrive
// without a single WINDOW_UPDATE, while 65536 cannot - the last octet is only
// deliverable if this client returned connection-level credit while reading. The
// pair is the test; either size alone is satisfied by a client that refunds and
// by one that never has to.
//
// This is the RESPONSE direction. Request bodies near the same boundary are
// deliberately avoided: nginx stops granting connection-level credit once
// concurrent request bodies fill the window and every in-flight request then
// stalls, which is #701 and not this client. Measured before this test was
// written - all four peers serve both sizes below without complaint.
func TestMatrix_LargeBody_AtTheConnectionWindow(t *testing.T) {
	sizes := []int{65535, 65536}
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			for _, sz := range sizes {
				t.Run(fmt.Sprintf("bytes_%d", sz), func(t *testing.T) {
					status, body := doGET(t, c, fmt.Sprintf("/large?bytes=%d", sz), true)

					require.Equalf(t, 200, status, "status: got %d", status)
					require.Lenf(t, body, sz, "body length: got %d, want %d.\n"+
						"At %d bytes the initial 65535-byte connection window is not enough on "+
						"its own, so a body that arrives short here means connection-level "+
						"credit was never returned and the peer is still waiting to send the "+
						"rest", len(body), sz, sz)
				})
			}
		})
	}
}

// ── Matrix: Content-Encoding ────────────────────────────────────

// TestMatrix_GzipResponseIsDecoded is /gzip's first consumer.
//
// The endpoint has existed since the fixture contract was written and no test
// read it (#896), so client.detectEncoding and decompressFully - which run on
// every buffered response - had no integration coverage, on any peer.
//
// The assertion that carries the test is the length comparison. Response.Body is
// the decoded bytes while Response.BytesReceived counts the octets that actually
// arrived, so a body LONGER than what came off the wire is proof the payload was
// decompressed rather than handed through. That reads identically on every peer,
// which matters because they do not agree on the payload size: nginx compresses
// 10 KiB, Undertow and nghttpx 100 KiB.
func TestMatrix_GzipResponseIsDecoded(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			c := newTestClient(t, srv)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     "/gzip",
				BodyMode: client.BodyBuffer,
			}, &resp)

			require.NoErrorf(t, err, "Do GET /gzip: %v", err)
			require.Equalf(t, 200, resp.Status, "status: got %d", resp.Status)
			require.Equal(t, "gzip", findHeader(resp.Headers, "content-encoding"),
				"this peer did not answer with Content-Encoding: gzip, so nothing below "+
					"exercises the decoder - the client sends accept-encoding on every "+
					"request that has not disabled decompression, and CONTRACT.md has this "+
					"endpoint answer it")
			require.NotEmpty(t, resp.Body, "the decoded body is empty")
			assert.Greaterf(t, int64(len(resp.Body)), resp.BytesReceived,
				"decoded %d bytes from %d received - the body is no longer than the octets "+
					"that arrived, so it was never decompressed and the caller is holding a "+
					"gzip stream it asked the client to decode",
				len(resp.Body), resp.BytesReceived)
			assert.Equalf(t, bytes.Repeat([]byte("x"), len(resp.Body)), resp.Body,
				"the decoded body is not the payload every peer sends here (a run of 'x'); "+
					"first bytes %q", firstBytes(resp.Body, 32))
		})
	}
}

// firstBytes is a short, safe prefix for a failure message about a body that may
// be megabytes of binary.
func firstBytes(b []byte, n int) []byte {
	if len(b) > n {
		return b[:n]
	}
	return b
}

// ── Matrix: the peer that never answers ─────────────────────────

// TestMatrix_NeverRespondsIsADeadlineAndKeepsTheConnection drives /never, the
// third fixture nothing consumed (#896).
//
// /delay?ms=5000 against a 200ms budget - the closest existing case - is a SLOW
// peer: the response is coming, just late. /never is a silent one, and the
// difference shows in what happens after the caller gives up. Two claims, and
// each fails on its own:
//
// The failure must be a context.DeadlineExceeded. That is what Request.Timeout
// and CLIENT_GUIDE promise and what client.isHardStop reads to refuse a replay;
// TestIT_ToxiH1_StalledPeer_IsAContextDeadline pins it for HTTP/1.1 and nothing
// pinned it here, where the stream is abandoned rather than a socket read.
//
// And the connection must survive. A cancelled stream is reset, not fatal to its
// connection, so the request after it belongs on the same one - the dial count is
// what says so, since a reconnect answers 200 exactly as convincingly.
func TestMatrix_NeverRespondsIsADeadlineAndKeepsTheConnection(t *testing.T) {
	for _, srv := range allReadyServers(t) {
		t.Run(srv.Kind.String(), func(t *testing.T) {
			var dials atomic.Int64
			c := newCountingTestClient(t, srv, &dials)
			status, _ := doGET(t, c, "/healthz", false)
			require.Equalf(t, 200, status, "warm-up request: status %d", status)
			require.EqualValuesf(t, 1, dials.Load(), "warm-up completed %d dials, want 1; "+
				"without an established connection the reuse claim below is vacuous",
				dials.Load())
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			var resp client.Response
			resp.Reset()
			err := c.Do(ctx, &client.Request{
				Method:   "GET",
				Path:     "/never",
				BodyMode: client.BodyBuffer,
			}, &resp)

			require.Errorf(t, err, "a request to a peer that never answers succeeded: "+
				"status=%d bodyLen=%d - the fixture responded, so this test proved nothing",
				resp.Status, len(resp.Body))
			assert.Truef(t, errors.Is(err, context.DeadlineExceeded),
				"a request abandoned on a silent peer failed with %v, which is not a "+
					"context.DeadlineExceeded.\n"+
					"That is the error Request.Timeout and CLIENT_GUIDE promise, and the one "+
					"client.isHardStop reads to refuse a replay - anything else looks "+
					"transient to a retry predicate and gets fired at the silent peer again",
				err)
			after, _ := doGET(t, c, "/healthz", false)
			assert.Equalf(t, 200, after,
				"the request after an abandoned one failed with status %d; giving up on one "+
					"stream took the whole connection with it", after)
			assert.EqualValuesf(t, 1, dials.Load(),
				"the request after the abandoned one brought the dial count to %d, want it "+
					"still at 1.\n"+
					"A cancelled stream is reset, not fatal to its connection, so tearing the "+
					"connection down costs a handshake for every timed-out request - and the "+
					"200 above reads the same either way", dials.Load())
		})
	}
}

// ── Matrix: the bodyless statuses, over HTTP/1.1 ────────────────

// h1LegFor returns an HTTP/1.1 client for srv, counting successful dials, or nil
// when this suite has no HTTP/1.1 leg to that peer.
//
// nghttpx is the one exclusion and it is a fixture defect rather than a missing
// port: its backend is Undertow over h2c, Undertow answers /status/204 with a
// ten-byte body - which RFC 9110 section 15.3.5 forbids - and nghttpx refuses to
// re-encode that, closing the connection without responding. Measured on the live
// stack; the same fixture makes nghttpx reset the stream with INTERNAL_ERROR over
// HTTP/2, which is why 204 is not in TestMatrix_StatusCodes either. Undertow's own
// HTTP/1.1 encoder suppresses the body correctly, so the undertow leg below is
// cleartext rather than a proxy hop.
func h1LegFor(t *testing.T, srv *TestServer, dials *atomic.Int64) *client.Client {
	t.Helper()
	// OnDial fires on the dialling goroutine, so the counter is atomic.
	hooks := client.WithHooks(&client.Hooks{OnDial: func(ev client.DialEvent) {
		if ev.Err == nil {
			dials.Add(1)
		}
	}})
	var (
		c   *client.Client
		err error
	)
	switch srv.Kind {
	case ServerGoHTTP, ServerUndertow:
		// Both listeners serve HTTP/1.1 and h2c on one cleartext port; which of
		// the two is spoken is decided by the transport, not by the dial.
		c, err = client.NewH1Client(srv.H2CAddr, &conn.PlaintextDialer{}, hooks)
	case ServerNginx:
		c, err = client.NewH1Client(srv.TLSAddr, &conn.H1TLSDialer{Config: &tls.Config{
			InsecureSkipVerify: true, // same as the rest of this suite: a local test CA
		}}, hooks)
	default:
		return nil
	}
	require.NoErrorf(t, err, "HTTP/1.1 client for %s: %v", srv.Kind, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestMatrix_H1BodylessStatusDoesNotDesyncTheConnection covers the no-body
// equivalence class the status matrix omits (#896).
//
// TestMatrix_StatusCodes walks {200, 201, 301, 400, 404, 500} - six members of
// one class, all of which carry a body - while the go-http-only status test
// includes 204 and the matrix does not, leaving the cross-implementation suite
// strictly weaker than the single-peer test it parallels.
//
// It is asked over HTTP/1.1 because that is where the client has a decision to
// make. RFC 9112 section 6.3 rule 1 makes a 204 or 304 response bodyless whatever
// its head declares, and http1 short-circuits ReadBodyChunk on those two codes;
// over HTTP/2 the peer simply ends the stream and there is no such branch. A
// client that ignores rule 1 does not return a wrong body - it waits for one that
// is never coming, and the connection it waits on is the one the NEXT request
// needs, which is what the follow-up assertion is for.
func TestMatrix_H1BodylessStatusDoesNotDesyncTheConnection(t *testing.T) {
	codes := []int{204, 304}
	tested := 0
	for _, srv := range allReadyServers(t) {
		var dials atomic.Int64
		c := h1LegFor(t, srv, &dials)
		if c == nil {
			continue
		}
		tested++
		t.Run(srv.Kind.String(), func(t *testing.T) {
			for _, code := range codes {
				t.Run(fmt.Sprintf("code_%d", code), func(t *testing.T) {
					status, body := doGET(t, c, fmt.Sprintf("/status/%d", code), true)
					afterStatus := dials.Load()
					followUp, followBody := doGET(t, c, "/healthz", true)

					require.Equalf(t, code, status, "status: got %d, want %d", status, code)
					assert.Emptyf(t, body, "a %d response delivered a %d-byte body: %q.\n"+
						"RFC 9112 section 6.3 rule 1 makes this status bodyless whatever the "+
						"head declares, so anything here came from octets that belong to the "+
						"next message on this connection", code, len(body), body)
					assert.Equalf(t, 200, followUp,
						"the request after a %d failed with status %d - the connection is out "+
							"of step, which is what reading a body that does not exist does to "+
							"an HTTP/1.1 stream", code, followUp)
					assert.Equalf(t, "ok", string(followBody),
						"the request after a %d came back with %q instead of the health "+
							"response; the reader is parsing the wrong octets", code, followBody)
					assert.EqualValuesf(t, afterStatus, dials.Load(),
						"the follow-up request dialled again (%d -> %d), so it did not have to "+
							"survive the previous exchange and this test says nothing about "+
							"whether the %d desynchronised the connection",
						afterStatus, dials.Load(), code)
				})
			}
		})
	}

	require.NotZero(t, tested, "no peer offered an HTTP/1.1 leg, so nothing here ran: "+
		"this test would otherwise have passed without testing anything")
}
