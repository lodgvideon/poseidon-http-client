//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"fmt"
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
	largePath   = "/large?bytes=65536"
	toxiAPIAddr = "127.0.0.1:18474" // Toxiproxy control API
	toxiTLSAddr = "127.0.0.1:18085" // proxy listen port, upstream = nginx TLS
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

// proxyName is shared by every test in this file on purpose. All of them bind
// the same listen port, so per-test names could never coexist anyway, and a
// single idempotent name means a crashed run leaves nothing that makes the next
// one fail with "address already in use".
const proxyName = "poseidon-h2"

// proxy (re)creates the proxy in front of nginx and removes it on cleanup.
func (x *toxi) proxy() {
	x.t.Helper()
	_, _ = x.do("DELETE", "/proxies/"+proxyName, "") // ignore: may not exist yet
	if _, err := x.do("POST", "/proxies", fmt.Sprintf(
		`{"name":%q,"listen":"0.0.0.0:18085","upstream":%q,"enabled":true}`, proxyName, nginxUp)); err != nil {
		x.t.Fatalf("create proxy %q: %v", proxyName, err)
	}
	x.t.Cleanup(func() { _, _ = x.do("DELETE", "/proxies/"+proxyName, "") })
}

// disable takes the proxy down, which drops every connection it is holding —
// including ones parked idle in a pool, which no toxic reaches. enable brings it
// back on the same listen port.
func (x *toxi) setEnabled(on bool) {
	x.t.Helper()
	if _, err := x.do("POST", "/proxies/"+proxyName,
		fmt.Sprintf(`{"enabled":%t}`, on)); err != nil {
		x.t.Fatalf("set proxy %q enabled=%t: %v", proxyName, on, err)
	}
}

func (x *toxi) disable() { x.t.Helper(); x.setEnabled(false) }
func (x *toxi) enable()  { x.t.Helper(); x.setEnabled(true) }

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

// get issues one GET through c and returns status, body length and error.
func get(c *client.Client, path string) (int, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var resp client.Response
	resp.Reset()
	err := c.Do(ctx, &client.Request{Method: "GET", Path: path, BodyMode: client.BodyBuffer}, &resp)
	return resp.Status, len(resp.Body), err
}

// TestIT_Toxi_ControlPathIsClean is the control. Every assertion below is about
// a request failing, and a request can fail because the peer is simply broken,
// so the same path must first be shown to work with no toxic installed.
func TestIT_Toxi_ControlPathIsClean(t *testing.T) {
	x := newToxi(t)
	x.proxy()

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
	x.proxy()

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
	remove := x.addToxic(proxyName, "half", "limit_data", "downstream",
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
	x.proxy()

	c := h2Through(t, toxiTLSAddr)
	if _, _, err := get(c, "/healthz"); err != nil {
		t.Fatalf("first request failed before any toxic was installed: %v", err)
	}

	// Reset the established connection, then take the toxic away again so the
	// NEXT request has a clean path to a fresh connection. What is under test is
	// whether the poisoned connection is evicted rather than reused.
	remove := x.addToxic(proxyName, "boom", "reset_peer", "downstream", `{"timeout":0}`)
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
	x.proxy()

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
	x.disable()

	// Give the drop time to be seen by both connection readers. This is a bound,
	// not a race: longer is only ever more certain, and too short cannot make
	// this test pass quietly — a surviving sibling is served without a dial,
	// which the fixture check below turns into a failure.
	time.Sleep(500 * time.Millisecond)
	x.enable()

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
