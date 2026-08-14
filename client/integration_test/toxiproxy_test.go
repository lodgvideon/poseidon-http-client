//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
	"github.com/lodgvideon/poseidon-http-client/conn"
)

// Fault injection on the TCP path (#552). The H1/H2 suite ran against nginx and
// undertow on the clean path only — no truncation, no RST, no hang — while the
// H3 side had both a fault server and a lossy relay. This closes that
// sibling-divergence gap.
//
// Toxiproxy sits in front of the existing peers and its toxics are applied per
// test over its HTTP API, so nothing leaks between tests. Two notes on the
// shape of this file:
//
// The API is driven by THIS repo's own HTTP/1.1 client, not by Toxiproxy's Go
// package. A test-only dependency would still be the fifth direct entry in a
// go.mod that has four, in a library whose whole point is having written the
// stack itself — and driving a real JSON API is free extra exercise for the
// client.
//
// The TLS "trap" the issue warns about does not exist here. Toxiproxy
// terminates TCP, not TLS: bytes pass through untouched, h2-over-TLS negotiates
// end to end, and the suite's tls.Config already sets InsecureSkipVerify, so no
// certificate covers the proxy's address in the first place. Verified against
// the live stack before this file was written.

const (
	// largePath declares a Content-Length, which is what makes a truncated body
	// detectable at all — and what makes silently accepting one dangerous.
	largePath = "/large?bytes=65536"
	// chunkedPath has NO Content-Length: over HTTP/1.1 nginx serves it
	// Transfer-Encoding: chunked, so the only thing that says the body ended is
	// the terminating zero-length chunk. Nothing above HTTP/1.1 has this shape —
	// h2 and h3 both frame the end of a body explicitly.
	chunkedPath = "/chunked"
	toxiAPIAddr = "127.0.0.1:18474" // Toxiproxy control API
	toxiTLSAddr = "127.0.0.1:18085" // h2 proxy listen port, upstream = nginx TLS
	toxiH1Addr  = "127.0.0.1:18086" // http/1.1 proxy listen port, same upstream
	nginxUp     = "172.30.0.10:8443"
)

// toxi drives the Toxiproxy control API over the repo's own H1 client.
type toxi struct {
	t *testing.T
	c *client.Client
}

func newToxi(t *testing.T) *toxi {
	t.Helper()
	c, err := client.NewH1Client(toxiAPIAddr, &conn.PlaintextDialer{})
	if err != nil {
		t.Fatalf("H1 client for the Toxiproxy API: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	x := &toxi{t: t, c: c}
	if _, err := x.do("GET", "/version", ""); err != nil {
		t.Skipf("Toxiproxy is not reachable at %s (%v); "+
			"run: docker compose -f test/integration/docker-compose.yml up -d nginx toxiproxy",
			toxiAPIAddr, err)
	}
	return x
}

// do issues one API call and returns the body. A non-2xx is an error, so a
// mis-scripted toxic surfaces here rather than as a confusing test failure
// three steps later.
func (x *toxi) do(method, path, body string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &client.Request{Method: method, Path: path, BodyMode: client.BodyBuffer}
	if body != "" {
		req.Body = []byte(body)
	}
	var resp client.Response
	resp.Reset()
	if err := x.c.Do(ctx, req, &resp); err != nil {
		return "", err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return "", fmt.Errorf("%s %s: status %d: %s", method, path, resp.Status, resp.Body)
	}
	return string(resp.Body), nil
}

// One proxy name per protocol leg, and each is shared by every test on that leg
// on purpose. Tests on a leg all bind its one listen port, so per-test names
// could never coexist anyway, and a single idempotent name means a crashed run
// leaves nothing that makes the next one fail with "address already in use".
//
// Two legs rather than one because a Toxiproxy proxy is one listen address with
// one upstream while the ALPN token is chosen per connection by the client:
// sharing a port would let an h2 test's toxic bite an HTTP/1.1 connection.
const (
	proxyH2 = "poseidon-h2"
	proxyH1 = "poseidon-h1"
)

// proxy (re)creates the named proxy in front of nginx, listening on addr's port
// inside the container, and removes it on cleanup.
func (x *toxi) proxy(name, addr string) {
	x.t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		x.t.Fatalf("proxy %q: bad listen addr %q: %v", name, addr, err)
	}
	_, _ = x.do("DELETE", "/proxies/"+name, "") // ignore: may not exist yet
	if _, err := x.do("POST", "/proxies", fmt.Sprintf(
		`{"name":%q,"listen":"0.0.0.0:%s","upstream":%q,"enabled":true}`, name, port, nginxUp)); err != nil {
		x.t.Fatalf("create proxy %q: %v", name, err)
	}
	x.t.Cleanup(func() { _, _ = x.do("DELETE", "/proxies/"+name, "") })
}

// disable takes the proxy down, which drops every connection it is holding —
// including ones parked idle in a pool, which no toxic reaches. enable brings it
// back on the same listen port.
func (x *toxi) setEnabled(name string, on bool) {
	x.t.Helper()
	if _, err := x.do("POST", "/proxies/"+name,
		fmt.Sprintf(`{"enabled":%t}`, on)); err != nil {
		x.t.Fatalf("set proxy %q enabled=%t: %v", name, on, err)
	}
}

func (x *toxi) disable(name string) { x.t.Helper(); x.setEnabled(name, false) }
func (x *toxi) enable(name string)  { x.t.Helper(); x.setEnabled(name, true) }

// addToxic installs a toxic and returns a function that removes it.
func (x *toxi) addToxic(proxy, name, typ, stream, attrs string) func() {
	x.t.Helper()
	if _, err := x.do("POST", "/proxies/"+proxy+"/toxics", fmt.Sprintf(
		`{"name":%q,"type":%q,"stream":%q,"attributes":%s}`, name, typ, stream, attrs)); err != nil {
		x.t.Fatalf("add toxic %s/%s: %v", typ, name, err)
	}
	return func() { _, _ = x.do("DELETE", "/proxies/"+proxy+"/toxics/"+name, "") }
}

// h2Through builds an h2 client pointed at the proxy's listen port.
func h2Through(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr:          addr,
		DefaultScheme: "https",
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{
				InsecureSkipVerify: true, // same as the rest of this suite: a local test CA
				NextProtos:         []string{"h2"},
			}},
			StreamEventBuffer: 1024,
		},
	})
	if err != nil {
		t.Fatalf("NewClient(%s): %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// getWithin issues one GET through c under a budget of its own and returns
// status, body length, how long it took, and the error. Every test above wants
// a budget generous enough never to be the thing that fails; the stall tests at
// the bottom of this file want the budget to BE the subject, and want to read
// back how much of it was spent.
func getWithin(c *client.Client, path string, budget time.Duration) (int, int, time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	var resp client.Response
	resp.Reset()
	start := time.Now()
	err := c.Do(ctx, &client.Request{Method: "GET", Path: path, BodyMode: client.BodyBuffer}, &resp)
	return resp.Status, len(resp.Body), time.Since(start), err
}

// get issues one GET through c and returns status, body length and error.
func get(c *client.Client, path string) (int, int, error) {
	status, n, _, err := getWithin(c, path, 15*time.Second)
	return status, n, err
}

// TestIT_Toxi_ControlPathIsClean is the control. Every assertion below is about
// a request failing, and a request can fail because the peer is simply broken,
// so the same path must first be shown to work with no toxic installed.
func TestIT_Toxi_ControlPathIsClean(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH2, toxiTLSAddr)

	c := h2Through(t, toxiTLSAddr)
	status, n, err := get(c, "/healthz")
	if err != nil {
		t.Fatalf("clean request through the proxy failed: %v", err)
	}
	if status != 200 || n == 0 {
		t.Fatalf("clean request: status=%d bodyLen=%d, want 200 and a non-empty body", status, n)
	}
}

// TestIT_Toxi_LimitDataMidBody_IsNotASilentShortRead is the case the issue
// singles out and the one where a client can plausibly do the wrong thing
// quietly: headers arrive, half the body arrives, then nothing. The failure
// mode that matters is not an error — it is a SUCCESS carrying a truncated
// body, which a load generator would record as a good response.
func TestIT_Toxi_LimitDataMidBody_IsNotASilentShortRead(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH2, toxiTLSAddr)

	// Establish the full length first, so "short" is measured, not assumed.
	full := h2Through(t, toxiTLSAddr)
	_, wantLen, err := get(full, largePath)
	if err != nil {
		t.Skipf("no clean baseline for the large body through the proxy: %v", err)
	}
	if wantLen == 0 {
		t.Skip("baseline body is empty; nothing to truncate")
	}

	// Cut downstream partway into the body: enough bytes for the response head
	// and some payload, fewer than the whole thing.
	remove := x.addToxic(proxyH2, "half", "limit_data", "downstream",
		fmt.Sprintf(`{"bytes":%d}`, wantLen/2))
	defer remove()

	c := h2Through(t, toxiTLSAddr)
	status, gotLen, err := get(c, largePath)

	if err == nil && gotLen < wantLen {
		t.Fatalf("truncated response reported as success: status=%d, %d of %d bytes, err=nil.\n"+
			"A caller cannot tell this from a complete response, which is exactly the "+
			"failure a load generator must never record as a good result", status, gotLen, wantLen)
	}
	if err == nil {
		t.Fatalf("no error and a full %d-byte body with limit_data at %d — the toxic did "+
			"not bite, so this test proved nothing", gotLen, wantLen/2)
	}
	if strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("failed by timeout (%v); the connection is cut, so this should surface as "+
			"a transport error rather than costing the caller the full deadline", err)
	}
	t.Logf("truncation surfaced as: %v (%d of %d bytes)", err, gotLen, wantLen)
}

// TestIT_Toxi_ResetPeer_NextRequestIsNotPoisoned covers the reuse half, which a
// single-request test cannot express: the damage shows up on request N+1, when
// the pool hands out the connection the peer has just reset.
func TestIT_Toxi_ResetPeer_NextRequestIsNotPoisoned(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH2, toxiTLSAddr)

	c := h2Through(t, toxiTLSAddr)
	if _, _, err := get(c, "/healthz"); err != nil {
		t.Fatalf("first request failed before any toxic was installed: %v", err)
	}

	// Reset the established connection, then take the toxic away again so the
	// NEXT request has a clean path to a fresh connection. What is under test is
	// whether the poisoned connection is evicted rather than reused.
	remove := x.addToxic(proxyH2, "boom", "reset_peer", "downstream", `{"timeout":0}`)
	_, _, errDuring := get(c, largePath)
	remove()

	if errDuring == nil {
		t.Fatal("request during reset_peer succeeded; the toxic did not bite, so the " +
			"eviction below would be untested")
	}

	// The pool must not hand the reset connection to this request.
	status, n, err := get(c, "/healthz")
	if err != nil {
		t.Fatalf("request after the peer reset the connection failed: %v\n"+
			"the damaged connection was handed to the next request instead of being "+
			"evicted, so one peer RST poisons every later request on that pool entry", err)
	}
	if status != 200 || n == 0 {
		t.Fatalf("request after reset: status=%d bodyLen=%d, want 200 with a body", status, n)
	}
}

// h2PoolThrough builds a POOLED h2 client of n connections pointed at the
// proxy's listen port, counting completed dials into dials. Everything above
// uses client.NewClient's default transport, which is TransportSingleConn — so
// until this helper existed no fault in this file reached client/pool.go at all.
//
// HealthCheckPeriod is set against the fault rather than for realism: it is
// pushed past any plausible test duration because handleTick runs evictDead,
// which reaps a dead conn on its own. A sweep landing between the outage and the
// request after it would remove the corpse before the acquire path ever saw it,
// and the assertion below would hold for a reason that has nothing to do with
// what it claims to test.
func h2PoolThrough(t *testing.T, addr string, n int, dials *atomic.Int64) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.ClientOptions{
		Addr:          addr,
		DefaultScheme: "https",
		Transport:     client.TransportPool,
		Pool: &client.PoolOptions{
			MaxConnsPerHost:   n,
			HealthCheckPeriod: time.Hour,
		},
		ConnOpts: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{
				InsecureSkipVerify: true, // same as the rest of this suite: a local test CA
				NextProtos:         []string{"h2"},
			}},
			StreamEventBuffer: 1024,
		},
		// OnDial fires on the dialling goroutine, so the counter is atomic.
		Hooks: &client.Hooks{OnDial: func(client.DialEvent) { dials.Add(1) }},
	})
	if err != nil {
		t.Fatalf("NewClient(pool of %d, %s): %v", n, addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// warmTo brings the pool up to n connections and waits for them to land, so a
// test that needs a second connection in the pool fails here — loudly — rather
// than silently running the one-connection scenario it was written to avoid.
func warmTo(t *testing.T, c *client.Client, n int) {
	t.Helper()
	c.Warmup(n)
	deadline := time.Now().Add(15 * time.Second)
	for {
		live := c.PoolStats().ActiveConns
		if live == n {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("pool warmed to %d connections, want %d; without the second one "+
				"there is no idle sibling to poison and the test below is vacuous", live, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestIT_Toxi_Outage_PoolDoesNotServeThePoisonedSibling is the pooled half of
// this file, and MaxConnsPerHost:2 is the whole test.
//
// client/pool_restart_test.go already kills a peer under a pool and demands
// recovery, but it pins the pool to one connection and says why: "A larger cap
// would let healthy connections mask a poisoned one." That is the right choice
// for the bug it was written for and it leaves this one unreachable. At N=1 the
// pool's only connection is the one the failing request holds, and handleRelease
// evicts a dead conn the moment its stream count reaches zero — the acquire path
// is never asked to judge anything. At N=2 the outage kills both, and a
// connection nobody was using is never released, so it stays in the pool as an
// idle corpse. The next acquire walks straight into it, and the only thing
// keeping it out of the caller's hands is pickLeastLoaded's IsAlive() skip.
//
// Nothing between the outage and the final request may call PoolStats():
// handleStats runs evictDeadSilent, so a metrics read would reap the corpses and
// leave the pick with nothing to get wrong.
func TestIT_Toxi_Outage_PoolDoesNotServeThePoisonedSibling(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH2, toxiTLSAddr)

	var dials atomic.Int64
	c := h2PoolThrough(t, toxiTLSAddr, 2, &dials)
	warmTo(t, c, 2)

	if _, _, err := get(c, "/healthz"); err != nil {
		t.Fatalf("first request failed on a healthy proxy: %v", err)
	}

	// Disabling the proxy, not a toxic, and that is the lever this test turns on.
	// Both reset_peer forms were measured against this fixture and neither works
	// here: the toxic sits on the data pipe and acts on the first chunk that
	// crosses it, so a connection parked idle in a pool is never touched — with
	// {"timeout":0} and with {"timeout":1} alike the sibling came through alive
	// and the fixture check at the bottom of this test caught it. Disabling the
	// proxy drops every connection it holds, idle ones included, which is the
	// only state that puts a corpse in front of pickLeastLoaded.
	x.disable(proxyH2)

	// Give the drop time to be seen by both connection readers. This is a bound,
	// not a race: longer is only ever more certain, and too short cannot make
	// this test pass quietly — a surviving sibling is served without a dial,
	// which the fixture check below turns into a failure.
	time.Sleep(500 * time.Millisecond)
	x.enable(proxyH2)

	// Nothing may read PoolStats from here on — see the doc comment.
	before := dials.Load()
	status, n, err := get(c, "/healthz")
	if err != nil {
		t.Fatalf("request after the peer dropped the pool's connections failed: %v\n"+
			"a poisoned but idle sibling was handed out instead of being skipped, so one "+
			"outage poisons later requests on a multi-connection pool", err)
	}
	if status != 200 || n == 0 {
		t.Fatalf("request after the outage: status=%d bodyLen=%d, want 200 with a body", status, n)
	}

	// The fixture check, not a behaviour claim — and the reason the assertion
	// above means anything. Both pooled connections were dropped and neither was
	// ever released, so both are still in the pool, and the request above can
	// only have been served by a connection dialled after the outage. A recorded
	// dial is what says the pool really was offered a corpse and turned it down;
	// no dial says the drop spared the sibling and the pool simply reused a
	// healthy connection, which proves nothing about the skip.
	if dials.Load() == before {
		t.Fatal("the request after the outage was served without dialling, so the idle " +
			"sibling survived it and no poisoned connection was ever offered to " +
			"pickLeastLoaded — this test proved nothing")
	}
}

// ── the HTTP/1.1 leg ────────────────────────────────────────────────
//
// Everything above rides h2. Until this section the HTTP/1.1 stack saw no
// network-level fault anywhere in the repo — only in-process synthetic ones
// (client/large_transfer_test.go's mid-body FIN+RST, client/h1_dataslab_test.go's
// pool truncation). The H1 client at the top of this file drives Toxiproxy's own
// control API; it is the instrument, never the system under fault. #647.
//
// A separate proxy on a separate port, because ALPN is chosen per connection by
// the client while a Toxiproxy proxy is one listen address: nginx offers both
// tokens on 172.30.0.10:8443 and picks h2 whenever a client offers it, so the
// only thing that makes this leg HTTP/1.1 is conn.H1TLSDialer offering
// "http/1.1" alone. Measured against the live stack before this section was
// written — offering http/1.1 yields http/1.1, offering both yields h2.

// h1PoolThrough builds a POOLED HTTP/1.1 client of n connections pointed at the
// proxy's listen port, counting completed dials into dials. It is the HTTP/1.1
// twin of h2PoolThrough and its HealthCheckPeriod is pushed out for the same
// reason: h1Pool.handleTick runs evictDead, and a sweep landing between a fault
// and the request after it would reap the damaged conn before the acquire path
// ever saw it.
//
// conn.H1TLSDialer, not conn.TLSDialer, is the whole point — see above. The
// client refuses the h2 dialer on an HTTP/1.1 transport anyway
// (ErrALPNProtocolMismatch), which is why this cannot silently become a second
// h2 test.
func h1PoolThrough(t *testing.T, addr string, n int, dials *atomic.Int64) *client.Client {
	t.Helper()
	c, err := client.NewH1PoolClient(addr,
		&conn.H1TLSDialer{Config: &tls.Config{
			InsecureSkipVerify: true, // same as the rest of this suite: a local test CA
		}},
		client.PoolOptions{
			MaxConnsPerHost:   n,
			HealthCheckPeriod: time.Hour,
		},
		// OnDial fires on the dialling goroutine, so the counter is atomic.
		client.WithHooks(&client.Hooks{OnDial: func(client.DialEvent) { dials.Add(1) }}),
	)
	if err != nil {
		t.Fatalf("NewH1PoolClient(pool of %d, %s): %v", n, addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestIT_ToxiH1_ControlPathIsClean is this leg's control, and it carries one
// claim the h2 control does not have to make: that the connection really is
// HTTP/1.1. That is not asserted here because it cannot fail quietly —
// conn.H1TLSDialer rejects a peer selecting anything but "http/1.1"
// (ErrALPNNotHTTP11) and client.TransportH1Pool rejects a dialer that does not
// assert it (ErrALPNProtocolMismatch), so a success below IS an HTTP/1.1
// exchange. Both gates are unit-tested in conn/h1dial_test.go and
// client/h1_alpn_test.go; what is new here is a real dual-protocol peer.
func TestIT_ToxiH1_ControlPathIsClean(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH1, toxiH1Addr)

	var dials atomic.Int64
	c := h1PoolThrough(t, toxiH1Addr, 1, &dials)
	status, n, err := get(c, "/healthz")
	if err != nil {
		t.Fatalf("clean HTTP/1.1 request through the proxy failed: %v", err)
	}
	if status != 200 || n == 0 {
		t.Fatalf("clean request: status=%d bodyLen=%d, want 200 and a non-empty body", status, n)
	}
}

// TestIT_ToxiH1_LimitDataMidBody_IsNotASilentShortRead is the h2 test of the
// same name aimed at the other codec. HTTP/1.1 reaches the wrong answer by a
// different route: h2 knows a body ended because a DATA frame carried
// END_STREAM, while here the only thing separating "the body ended" from "the
// socket ended" is that Content-Length bytes were counted. A client that stops
// at EOF and reports what it has produces a 200 with a short body, which a load
// generator records as a good response.
func TestIT_ToxiH1_LimitDataMidBody_IsNotASilentShortRead(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH1, toxiH1Addr)

	var dials atomic.Int64

	// Establish the full length first, so "short" is measured, not assumed.
	full := h1PoolThrough(t, toxiH1Addr, 1, &dials)
	_, wantLen, err := get(full, largePath)
	if err != nil {
		t.Skipf("no clean baseline for the large body through the proxy: %v", err)
	}
	if wantLen == 0 {
		t.Skip("baseline body is empty; nothing to truncate")
	}

	// Cut downstream partway into the body. The limit counts every byte the
	// server sends, TLS handshake included — measured at ~1.5 KB against this
	// fixture — so half of a 64 KiB body lets the response head and roughly
	// 30 KiB of payload through and closes the socket on the rest. There is no
	// race to lose: the toxic is a byte counter, so the client cannot receive
	// more than the limit however the two sides are scheduled.
	remove := x.addToxic(proxyH1, "half", "limit_data", "downstream",
		fmt.Sprintf(`{"bytes":%d}`, wantLen/2))
	defer remove()

	c := h1PoolThrough(t, toxiH1Addr, 1, &dials)
	status, gotLen, err := get(c, largePath)

	if err == nil && gotLen < wantLen {
		t.Fatalf("truncated response reported as success: status=%d, %d of %d bytes, err=nil.\n"+
			"A caller cannot tell this from a complete response, which is exactly the "+
			"failure a load generator must never record as a good result", status, gotLen, wantLen)
	}
	if err == nil {
		t.Fatalf("no error and a full %d-byte body with limit_data at %d — the toxic did "+
			"not bite, so this test proved nothing", gotLen, wantLen/2)
	}
	if gotLen >= wantLen {
		t.Errorf("errored but delivered the whole %d-byte body; the cut landed past the "+
			"end of the body, so this is not the mid-body case it claims to be", gotLen)
	}
	if strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("failed by timeout (%v); the connection is cut, so this should surface as "+
			"a transport error rather than costing the caller the full deadline", err)
	}
	t.Logf("truncation surfaced as: %v (status=%d, %d of %d bytes)", err, status, gotLen, wantLen)
}

// TestIT_ToxiH1_LimitDataMidChunked_IsNotASilentShortRead is the case with no
// counterpart anywhere else in this file, because no other protocol here has
// this framing. /chunked carries no Content-Length: over HTTP/1.1 the body is a
// chunk sequence and the ONLY thing that says it ended is the terminating
// zero-length chunk. There is no declared length to compare a total against, so
// a decoder that treats EOF as end-of-body has nothing left to catch it — this
// is the sharpest silent-short-read shape the stack has.
func TestIT_ToxiH1_LimitDataMidChunked_IsNotASilentShortRead(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH1, toxiH1Addr)

	var dials atomic.Int64

	full := h1PoolThrough(t, toxiH1Addr, 1, &dials)
	_, wantLen, err := get(full, chunkedPath)
	if err != nil {
		t.Skipf("no clean baseline for the chunked body through the proxy: %v", err)
	}
	if wantLen == 0 {
		t.Skip("baseline chunked body is empty; nothing to truncate")
	}

	remove := x.addToxic(proxyH1, "half", "limit_data", "downstream",
		fmt.Sprintf(`{"bytes":%d}`, wantLen/2))
	defer remove()

	c := h1PoolThrough(t, toxiH1Addr, 1, &dials)
	status, gotLen, err := get(c, chunkedPath)

	if err == nil && gotLen < wantLen {
		t.Fatalf("truncated chunked response reported as success: status=%d, %d of %d bytes.\n"+
			"With no Content-Length the missing terminating chunk is the only signal that "+
			"the body did not finish; accepting the short read hands a load generator a "+
			"result it cannot tell from a complete one", status, gotLen, wantLen)
	}
	if err == nil {
		t.Fatalf("no error and a full %d-byte body with limit_data at %d — the toxic did "+
			"not bite, so this test proved nothing", gotLen, wantLen/2)
	}
	if gotLen >= wantLen {
		t.Errorf("errored but delivered the whole %d-byte body; the cut landed past the "+
			"end of the body, so this is not the mid-body case it claims to be", gotLen)
	}
	if strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("failed by timeout (%v); the connection is cut, so this should surface as "+
			"a transport error rather than costing the caller the full deadline", err)
	}
	t.Logf("chunked truncation surfaced as: %v (status=%d, %d of %d bytes)", err, status, gotLen, wantLen)
}

// TestIT_ToxiH1_CutConnectionIsNotReused is the reuse half, and here the pool is
// under test rather than the codec: h1Pool.handleRelease discards a conn whose
// exchange errored instead of parking it in the idle set, and until now nothing
// had ever handed it a conn a network cut had killed.
//
// MaxConnsPerHost is 1 where the h2 outage test above needs 2, and the
// difference is the toxic's reach rather than a preference. limit_data acts on
// the pipe carrying the request, so it can only damage the connection the
// failing exchange is checked out on — a warmed idle sibling would come through
// untouched and serve the request below with no dial at all, which is exactly
// the state the fixture check at the bottom exists to reject.
func TestIT_ToxiH1_CutConnectionIsNotReused(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH1, toxiH1Addr)

	var dials atomic.Int64
	c := h1PoolThrough(t, toxiH1Addr, 1, &dials)

	if _, _, err := get(c, "/healthz"); err != nil {
		t.Fatalf("first request failed before any toxic was installed: %v", err)
	}
	afterFirst := dials.Load()

	// Cut the exchange, then take the toxic away so the NEXT request has a clean
	// path to a fresh connection. What is under test is whether the cut one is
	// discarded rather than handed out again.
	remove := x.addToxic(proxyH1, "half", "limit_data", "downstream", `{"bytes":8192}`)
	_, _, errDuring := get(c, largePath)
	remove()

	if errDuring == nil {
		t.Fatal("request under limit_data succeeded; the toxic did not bite, so the " +
			"reuse assertion below would be untested")
	}

	status, n, err := get(c, "/healthz")
	if err != nil {
		t.Fatalf("request after the peer cut the connection mid-body failed: %v\n"+
			"the damaged connection was handed to the next request instead of being "+
			"discarded, so one truncated response poisons every later request on that "+
			"pool entry", err)
	}
	if status != 200 || n == 0 {
		t.Fatalf("request after the cut: status=%d bodyLen=%d, want 200 with a body", status, n)
	}

	// The fixture check, not a behaviour claim. The pool holds one connection and
	// the cut killed it, so the request above can only have been served by a
	// connection dialled afterwards. No new dial means the cut left the socket
	// usable and the pool simply reused it, which says nothing about the discard.
	if dials.Load() == afterFirst {
		t.Fatal("the request after the cut was served without dialling, so the connection " +
			"survived the truncation and no damaged conn was ever offered back to the " +
			"pool — this test proved nothing")
	}
}

// ── the stalled peer ────────────────────────────────────────────────
//
// Every fault above ends the connection: limit_data cuts it, disabling the proxy
// drops it, reset_peer RSTs it. Each one gives the client an event to react to.
// The shape none of them can make is the absence of one — a socket that stays
// open, healthy and completely silent, where nothing arrives and nothing ends,
// and the only thing that can finish the request is the caller's own budget.
//
// That is the path through http1.Exchange.ReadResponse's read deadline, and it
// was the one leg of the three that reported the outcome differently: over
// HTTP/2 a stalled peer surfaced as context.DeadlineExceeded, over HTTP/1.1 as
// the socket's own os.ErrDeadlineExceeded, which does not match it.

// stallForever is the timeout toxic's "hold the socket open and send nothing".
// Toxiproxy closes the connection after `timeout` milliseconds and 0 means never,
// so this is the only attribute value that produces silence rather than an
// ending — a nonzero one is just another way to spell the cut that limit_data
// already covers on this leg.
const stallForever = `{"timeout":0}`

// warmH1 puts one established, idle HTTP/1.1 connection in c's pool and proves
// it landed.
//
// This is the lever for both stall tests below, not a convenience. Measured
// against this fixture: with the timeout toxic already installed, a FRESH
// connection stalls inside the TLS handshake, so the request dies in the dialer
// under the dialer's own context, completes no dial, and never enters http1 at
// all — and that failure already reports context.DeadlineExceeded. A test that
// skipped the warm-up would therefore assert exactly what it asserts now and
// prove nothing about the response read. The connection has to exist before the
// silence starts.
func warmH1(t *testing.T, c *client.Client, dials *atomic.Int64) {
	t.Helper()
	status, n, _, err := getWithin(c, "/healthz", 15*time.Second)
	if err != nil {
		t.Fatalf("warm-up request on a clean path failed: %v", err)
	}
	if status != 200 || n == 0 {
		t.Fatalf("warm-up: status=%d bodyLen=%d, want 200 with a body", status, n)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("warm-up completed %d dials, want exactly 1; without one connection "+
			"already established the toxic below stalls the TLS handshake instead of "+
			"the response read", got)
	}
}

// assertStalledInTheResponseRead is the fixture check both stall tests need, and
// it is two questions rather than one: did the request really spend its whole
// budget waiting, and did it wait in http1 rather than in the dialer.
//
// os.ErrDeadlineExceeded is what separates them. A read that blocked on an
// established socket carries the socket's own timeout underneath whatever the
// client layer says about it; the dialer's handshake stall does not — it fails
// on its context and reports that alone. Measured both ways against this
// fixture before either assertion below was written.
func assertStalledInTheResponseRead(t *testing.T, err error, elapsed, budget time.Duration) {
	t.Helper()
	if elapsed < budget {
		t.Fatalf("failed after %v of a %v budget: %v\n"+
			"a socket that sends nothing can only end the request when the budget does, "+
			"so this failed for some other reason", elapsed, budget, err)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("failure carries no socket timeout: %v\n"+
			"the response read never blocked on an established connection — the same "+
			"toxic stalls the TLS handshake when there is no warm connection, and that "+
			"fails in the dialer, so the claim below would hold without http1 being "+
			"reached at all", err)
	}
}

// TestIT_ToxiH1_StalledPeer_IsAContextDeadline pins what a caller is told when
// an HTTP/1.1 peer accepts the request and then says nothing.
//
// The budget is spent inside http1.Exchange.ReadResponse, blocked on the status
// line under the read deadline it installs from ctx. The socket reports that as
// `i/o timeout`, and reported verbatim it is wrong in the way that matters:
// os.ErrDeadlineExceeded does not match context.DeadlineExceeded, so the one
// thing every caller of this client is told to test for was false. Request.Timeout
// promises "the request fails with context.DeadlineExceeded" and CLIENT_GUIDE
// repeats it three times; over HTTP/2 it was true and only this leg disagreed.
func TestIT_ToxiH1_StalledPeer_IsAContextDeadline(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH1, toxiH1Addr)

	var dials atomic.Int64
	c := h1PoolThrough(t, toxiH1Addr, 1, &dials)
	warmH1(t, c, &dials)

	remove := x.addToxic(proxyH1, "stall", "timeout", "downstream", stallForever)
	defer remove()

	const budget = 300 * time.Millisecond
	status, n, elapsed, err := getWithin(c, "/healthz", budget)
	if err == nil {
		t.Fatalf("a request through a peer sending nothing succeeded: status=%d bodyLen=%d — "+
			"the toxic did not bite, so this test proved nothing", status, n)
	}
	assertStalledInTheResponseRead(t, err, elapsed, budget)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a request that spent its whole %v budget on a silent peer failed with %v, "+
			"which is not a context.DeadlineExceeded.\n"+
			"That is the error Request.Timeout and CLIENT_GUIDE both promise, and the one "+
			"client.isHardStop reads to refuse a replay; an HTTP/1.1 stall reported only as "+
			"a net.Error timeout looks transient to every caller that classifies it",
			budget, err)
	}
	t.Logf("stall surfaced as: %v (after %v)", err, elapsed)
}

// TestIT_ToxiH1_StalledPeer_IsNotReplayed is the consequence, and the reason the
// classification above is worth a test of its own rather than a doc fix.
//
// A load generator's IsRetryable is written against the standard library's
// vocabulary — "a net.Error whose Timeout() is true is transient, try again" —
// and that predicate is correct for a dial timeout or a stalled connect. The
// retry layer is what keeps it from also firing on a request that already spent
// its entire budget: shouldRetryErr asks isHardStop first, and a hard stop means
// the user predicate is never consulted at all. An HTTP/1.1 stall arriving as a
// bare socket timeout walked straight past that gate, so a request with a 300ms
// budget was replayed against the same silent peer — spending the budget again,
// and again, for an answer that was never coming.
//
// The assertion is that the predicate is never asked, not that no retry
// happened. It is the same fact one step earlier, and it cannot be reached by
// any route other than isHardStop: a count of zero says the classifier
// recognised this error itself.
func TestIT_ToxiH1_StalledPeer_IsNotReplayed(t *testing.T) {
	x := newToxi(t)
	x.proxy(proxyH1, toxiH1Addr)

	var dials atomic.Int64
	c := h1PoolThrough(t, toxiH1Addr, 1, &dials)
	warmH1(t, c, &dials)

	remove := x.addToxic(proxyH1, "stall", "timeout", "downstream", stallForever)
	defer remove()

	// consulted counts calls made ABOUT AN ERROR. Retryer also consults the
	// predicate about a successful response, which is a different question and
	// would make the count say something else.
	var consulted atomic.Int64
	r := client.NewRetryer(c, client.RetryOptions{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return 0 },
		IsRetryable: func(err error, _ *client.Response) bool {
			if err == nil {
				return false
			}
			consulted.Add(1)
			var ne net.Error
			return errors.As(err, &ne) && ne.Timeout()
		},
	})

	// A budget on the REQUEST, not on ctx: Retryer derives a fresh one per
	// attempt, so a replay is visible as time spent rather than hidden behind a
	// single outer deadline that would end the loop by itself.
	const budget = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	start := time.Now()
	err := r.Do(ctx, &client.Request{
		Method: "GET", Path: "/healthz", BodyMode: client.BodyBuffer, Timeout: budget,
	}, &resp)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("a retried request through a peer sending nothing succeeded: status=%d — "+
			"the toxic did not bite, so this test proved nothing", resp.Status)
	}
	// The claim goes first here, unlike the test above, because a replay changes
	// which attempt's error comes back last: the second one dies in the dialer
	// against the same stalled proxy, so the fixture check below would fire first
	// and report the wrong thing about the right failure.
	if got := consulted.Load(); got != 0 {
		t.Fatalf("IsRetryable was asked about the error %d times, want 0 "+
			"(the call took %v against a per-attempt budget of %v).\n"+
			"A request that spent its whole budget is a hard stop, so shouldRetryErr "+
			"must refuse it before the user predicate is reached. Asking a predicate "+
			"written for net.Error timeouts hands it an exhausted budget dressed as a "+
			"transient failure, and it replays the request against the same silent peer "+
			"(err was: %v)", got, elapsed, budget, err)
	}
	// After the claim, not before it — but it still has to run: a zero count also
	// comes out of a request that never reached the response read at all, since
	// the dialer's own timeout is a hard stop too and would short-circuit the
	// predicate for a completely different reason.
	assertStalledInTheResponseRead(t, err, elapsed, budget)

	t.Logf("stalled retry stopped after %v with: %v", elapsed, err)
}
