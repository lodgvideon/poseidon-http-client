package client

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// Every h1Pool eviction site pairs dropping a conn from the slice with closing
// its socket, and those are two independent statements. Only the first is
// observable to a later request, so every assertion the suite had — PoolStats
// counts, "did the next request dial fresh" — is satisfied by an eviction that
// forgets the Close. TestH1Pool_IdleEviction is the clearest case: its doc
// comment says "closes conns idle past IdleTimeout" and its body only waits for
// Stats().ActiveConns to reach 0.
//
// Deleting `_ = mc.c.Close()` from h1Pool.evictIdle left `go test ./client/
// -race` green, twice. The fd and the conn's watchdog goroutine leak once per
// idle eviction, with ConnsClosed still incrementing, so the metrics report a
// pool that is closing exactly what it drops.
//
// The other three sites were mutated the same way and do not need their own
// test:
//
//   - evict (h1_pool.go:609) is already caught, four tests over, by
//     TestH1Pool_NotKeepAlive_DiscardsConn and the three
//     TestH1PoolTransport_* discard tests.
//   - evictDead (h1_pool.go:665) and evictDeadSilent (h1_pool.go:830) are
//     equivalent mutants, not holes. Both fire only under `!mc.c.IsAlive()`,
//     and IsAlive is `!c.closed.Load()` (http1/conn.go:254) over an atomic
//     written in exactly one place: http1.Conn.Close (http1/conn.go:260),
//     which closes the socket in the same call. So `!IsAlive()` already means
//     this side closed the fd, and the Close those two run is an idempotent
//     second one. Deleting it leaks nothing, and a test that went red on it
//     would be pinning a redundant call rather than the property.
//
// evictIdle is the odd one out precisely because it has no liveness test: it
// evicts a healthy, open, idle connection, so its Close is the only thing
// standing between the pool and a leaked descriptor.

// TestH1EvictIdle_PeerSeesTheClose pins that h1Pool.evictIdle closes the socket
// of the conn it drops.
//
// The observable is the leak itself rather than the call: the peer holds the
// other half of the connection and is blocked in Read, which is what a real
// server does with an idle keep-alive conn. Nothing in this test ever writes to
// that socket, so the only thing that can wake that read is the client hanging
// up. A test that asserted "Close was called" instead would pin the
// implementation; this pins what the operating system on the far end observes.
func TestH1EvictIdle_PeerSeesTheClose(t *testing.T) {
	t.Parallel()

	// Both halves are owned here — the conn is handed straight to evictIdle and
	// never enters the pool's run state, so p.Close cannot be what closes it and
	// no other goroutine reads srv.
	cli, srv := net.Pipe()
	t.Cleanup(func() { _ = srv.Close() })

	peerSaw := make(chan error, 1)
	go func() {
		_, err := srv.Read(make([]byte, 1))
		peerSaw <- err
	}()

	p := newH1Pool("ignored:0", newH1FakeDialer(), PoolOptions{
		MaxConnsPerHost: 1,
		IdleTimeout:     10 * time.Millisecond,
	}, nil, nil)
	t.Cleanup(func() { _ = p.Close() })

	// Healthy and open, only stale: evictIdle tests lastUsed, never IsAlive.
	idle := &h1ManagedConn{p: p, c: http1.NewConn(cli), lastUsed: time.Now().Add(-time.Second)}

	kept := p.evictIdle([]*h1ManagedConn{idle})

	// CONTROL: the fixture must actually have reached the eviction path.
	require.Empty(t, kept, "evictIdle kept the conn — the fixture never reached the eviction path")
	select {
	case <-peerSaw:
		// The blocked read returned: the socket is gone.
	case <-time.After(5 * time.Second):
		require.FailNow(t, "peer never saw the connection close",
			"evictIdle dropped the conn from the pool without closing its socket, so the "+
				"descriptor and the conn's watchdog goroutine leak on every idle eviction "+
				"while ConnsClosed still increments")
	}
}
