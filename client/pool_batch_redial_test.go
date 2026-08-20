package client

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/frame"
)

// ————————————————————————————————————————————————————————————————
// #504 item 2: the multiplexing pools re-dial for ONE waiter at a time.
//
// h1Pool.ensureDialForWaiters dials for the whole uncovered batch — #411 made
// that necessary there, and its comment records the pathology it fixed: k
// waiters arrive together, one dial per call serialises them into k sequential
// round trips. The H2 and H3 pools kept the single dial.
//
// They escape the pathology only while one connection multiplexes many waiters.
// With MaxStreamsPerConn: 1 — a supported load-generator configuration, since
// that is how you make each request own a connection — a mass eviction leaves k
// waiters queued behind connections that can serve exactly one caller each, and
// the serial ramp is back.
//
// What must NOT be copied from h1 is its arithmetic. h1's per-connection
// capacity is 1 by protocol, so "waiters minus coverage" IS the dial count.
// Here a connection covers streamCap waiters, so the same expression opens
// MaxConnsPerHost sockets to serve a handful of callers one connection could
// have taken. Nothing defaults IdleTimeout in this repo, so evictIdle is off and
// a surplus socket never goes away — the ramp would be traded for a permanent
// over-allocation. Hence a batch divided by expected per-connection capacity,
// and hence the second test below, which is the one that fails on a naive port.
// ————————————————————————————————————————————————————————————————

// batchRedialPool builds a pool whose dial backoff is closed (so
// ensureDialForWaiters proceeds) and whose per-conn stream cap is the parameter
// under test. The address is unroutable: dialOne's goroutines will fail, but
// every assertion here reads inFlightDials, which ensureDialForWaiters
// increments synchronously before spawning anything.
func batchRedialPool(t *testing.T, maxStreams, maxConns int) *Pool {
	t.Helper()
	p := newPool("127.0.0.1:1", newConnOpts(), PoolOptions{
		MaxConnsPerHost:   maxConns,
		MaxStreamsPerConn: maxStreams,
		HealthCheckPeriod: time.Hour,
		DialBackoff:       0, // no backoff: the dial decision is the thing under test
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// h2LiveConn is h2DeadConn's counterpart: a real *conn.Conn left open, so
// IsAlive reports true and countLive / the spare-capacity walk count it. Real
// rather than hand-built for the same reason h2DeadConn is — conn.Conn's
// liveness flags are unexported.
func h2LiveConn(t *testing.T) *conn.Conn {
	t.Helper()
	cli, srv := net.Pipe()
	stopSrv := make(chan struct{})
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		runFakeH2Server(srv, func(*frame.Framer) { <-stopSrv })
	}()
	t.Cleanup(func() { close(stopSrv); <-srvDone })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := conn.NewClientConn(ctx, cli, conn.ConnOptions{})
	require.NoError(t, err, "NewClientConn")
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func batchRedialWaiters(ctx context.Context, n int) []acquireReq {
	ws := make([]acquireReq, 0, n)
	for i := 0; i < n; i++ {
		ws = append(ws, acquireReq{ctx: ctx, reply: make(chan acquireResp, 1)})
	}
	return ws
}

// TestPool_EnsureDialForWaiters_DialsTheWholeBatchWhenEachConnTakesOne is the
// serial-ramp case: MaxStreamsPerConn 1, so k waiters genuinely need k
// connections and dialling one leaves k-1 of them waiting on a round trip that
// has not started.
func TestPool_EnsureDialForWaiters_DialsTheWholeBatchWhenEachConnTakesOne(t *testing.T) {
	const waiters = 6

	p := batchRedialPool(t, 1, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs := &runState{waiters: batchRedialWaiters(ctx, waiters)}

	p.ensureDialForWaiters(rs)

	assert.Equalf(t, waiters, rs.inFlightDials,
		"inFlightDials = %d after %d waiters were left with no connection, want %d.\n"+
			"One dial per call serialises them: each completes, serves a single waiter, and "+
			"only then does the next start — %d sequential round trips.",
		rs.inFlightDials, waiters, waiters, waiters)
}

// TestPool_EnsureDialForWaiters_OneDialCoversAMultiplexedBatch is the control,
// and the one a naive port of h1's formula fails. Six waiters against a
// connection that will carry a hundred streams need exactly one connection.
func TestPool_EnsureDialForWaiters_OneDialCoversAMultiplexedBatch(t *testing.T) {
	p := batchRedialPool(t, 100, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs := &runState{waiters: batchRedialWaiters(ctx, 6)}

	p.ensureDialForWaiters(rs)

	assert.Equalf(t, 1, rs.inFlightDials,
		"inFlightDials = %d for 6 waiters against a 100-stream connection, want 1.\n"+
			"Copying h1's waiters-minus-coverage arithmetic opens a socket per waiter; "+
			"evictIdle is disabled by default, so those sockets never go away.",
		rs.inFlightDials)
}

// TestPool_EnsureDialForWaiters_CountsSpareCapacityOnLiveConns pins that a
// connection already able to take a caller is coverage, not a candidate for
// another socket.
func TestPool_EnsureDialForWaiters_CountsSpareCapacityOnLiveConns(t *testing.T) {
	p := batchRedialPool(t, 1, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Two live conns at cap 1: one idle (covers a waiter), one busy (does not).
	idle := &managedConn{c: h2LiveConn(t), streamCap: 1, active: 0}
	busy := &managedConn{c: h2LiveConn(t), streamCap: 1, active: 1}
	rs := &runState{
		conns:   []*managedConn{idle, busy},
		waiters: batchRedialWaiters(ctx, 4),
	}

	p.ensureDialForWaiters(rs)

	// 4 waiters, 1 covered by the idle conn -> 3 dials. Room allows it: cap 8,
	// 2 live.
	assert.Equalf(t, 3, rs.inFlightDials,
		"inFlightDials = %d, want 3: 4 waiters minus the one the idle "+
			"connection can take", rs.inFlightDials)
}

// TestPool_EnsureDialForWaiters_StaysUnderMaxConnsPerHost pins the cap, which
// is what stops a large batch from becoming a connection flood.
func TestPool_EnsureDialForWaiters_StaysUnderMaxConnsPerHost(t *testing.T) {
	p := batchRedialPool(t, 1, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs := &runState{waiters: batchRedialWaiters(ctx, 10)}

	p.ensureDialForWaiters(rs)

	assert.Equalf(t, 3, rs.inFlightDials,
		"inFlightDials = %d for 10 waiters with MaxConnsPerHost 3, want 3 — without the cap "+
			"a large batch becomes a connection flood", rs.inFlightDials)
}

// TestPool_EnsureDialForWaiters_CountsDialsAlreadyInFlight pins that the batch
// does not re-dial for waiters an earlier call already covered.
func TestPool_EnsureDialForWaiters_CountsDialsAlreadyInFlight(t *testing.T) {
	p := batchRedialPool(t, 1, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs := &runState{waiters: batchRedialWaiters(ctx, 5), inFlightDials: 2}

	p.ensureDialForWaiters(rs)

	assert.Equalf(t, 5, rs.inFlightDials,
		"inFlightDials = %d, want 5: two of the five waiters already had "+
			"a dial on the way", rs.inFlightDials)
}

// ── H3, the same three properties ───────────────────────────────

func h3BatchPool(maxStreams, maxConns int) *h3Pool {
	// inertH3Pool never starts the actor, so these call dialForWaiters directly
	// and read the counter it increments synchronously — the idiom the other h3
	// white-box tests use, and why none of them Close the pool.
	return inertH3Pool(PoolOptions{
		MaxConnsPerHost:   maxConns,
		MaxStreamsPerConn: maxStreams,
		HealthCheckPeriod: time.Hour,
		DialBackoff:       0,
	}, nil)
}

func h3BatchWaiters(ctx context.Context, n int) []h3AcquireReq {
	ws := make([]h3AcquireReq, 0, n)
	for i := 0; i < n; i++ {
		ws = append(ws, h3AcquireReq{ctx: ctx, reply: make(chan h3AcquireResp, 1)})
	}
	return ws
}

func TestH3Pool_DialForWaiters_DialsTheWholeBatchWhenEachConnTakesOne(t *testing.T) {
	const waiters = 6

	p := h3BatchPool(1, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs := &h3RunState{waiters: h3BatchWaiters(ctx, waiters)}

	p.dialForWaiters(rs)

	assert.Equalf(t, waiters, rs.inFlightDials,
		"inFlightDials = %d, want %d: with one caller per connection, "+
			"one dial per call is a %d-round-trip ramp", rs.inFlightDials, waiters, waiters)
}

func TestH3Pool_DialForWaiters_OneDialCoversAMultiplexedBatch(t *testing.T) {
	p := h3BatchPool(100, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rs := &h3RunState{waiters: h3BatchWaiters(ctx, 6)}

	p.dialForWaiters(rs)

	assert.Equalf(t, 1, rs.inFlightDials,
		"inFlightDials = %d for 6 waiters against a 100-stream connection, want 1",
		rs.inFlightDials)
}

// TestH3Pool_DialForWaiters_ADrainedConnIsNotCoverage pins the one term that
// differs from the H2 helper. A GOAWAY'd connection is still Alive and may sit
// on idle stream slots, but it refuses every new request (RFC 9114 §5.2), so
// counting those slots as coverage would suppress precisely the dial the GOAWAY
// made necessary — the waiters would be "covered" by a connection that will
// never take them.
func TestH3Pool_DialForWaiters_ADrainedConnIsNotCoverage(t *testing.T) {
	p := h3BatchPool(1, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	drained := &barrierH3Client{}
	atomic.StoreInt32(&drained.goaway, 1) // Alive, but refusing new requests
	rs := &h3RunState{
		conns:   []*h3ManagedConn{{cl: drained, streamCap: 4}},
		waiters: h3BatchWaiters(ctx, 3),
	}

	p.dialForWaiters(rs)

	assert.Equalf(t, 3, rs.inFlightDials,
		"inFlightDials = %d, want 3: the drained connection's 4 idle stream "+
			"slots are not capacity, because §5.2 has it refusing every new request",
		rs.inFlightDials)
}
