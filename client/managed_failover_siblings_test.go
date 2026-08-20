package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// The managed acquire loop fails over to the next address only for errors
// isDialOnlyErr accepts; anything else aborts the whole acquire. That predicate
// was pinned on the HTTP/2 managed pool alone — replacing `!isDialOnlyErr(err)`
// with `err != nil` in h1_managed_pool.go or h3_managed_pool.go left the entire
// ./client suite green, so a unification could quietly drop address failover on
// two of three protocols.
//
// These are the h1/h3 counterparts of TestManagedPool_FailsOverOnFirstDialFailure.

// h1FailFirstDialer fails every dial to failAddr and delegates the rest, so the
// managed loop must reach the second address inside one acquire.
type h1FailFirstDialer struct {
	inner    conn.Dialer
	failAddr string
}

func (d *h1FailFirstDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	if addr == d.failAddr {
		return nil, errors.New("simulated dial failure")
	}
	return d.inner.Dial(ctx, addr)
}

func TestH1ManagedPool_FailsOverOnFirstDialFailure(t *testing.T) {
	addrs := h1Addrs(2)
	dialer := &h1FailFirstDialer{inner: newH1FakeDialer(), failAddr: addrs[0].String()}

	mp, err := newH1ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		dialer, h1ManagedPoolOpts(), nil, nil)
	if err != nil {
		t.Fatalf("newH1ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	// RoundRobin starts on addrs[0] — the dead one — so the loop must reach
	// addrs[1] within this single acquire.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, rel, _, err := mp.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire = %v; a live second address was available, so the loop stopped on the dead one", err)
	}
	if c == nil {
		t.Fatal("acquire returned no connection and no error")
	}
	rel(true)
}

func TestH3ManagedPool_FailsOverOnFirstDialFailure(t *testing.T) {
	addrs := h3Addrs(2)
	d := newH3FakeDialer()
	failAddr := addrs[0].String()
	dialFn := func(ctx context.Context, addr string, cfg *tls.Config) (h3Client, error) {
		if addr == failAddr {
			return nil, errors.New("simulated dial failure")
		}
		return d.dial(ctx, addr, cfg)
	}

	mp, err := newH3ManagedPool(StaticResolver(addrs...), RoundRobin(), DrainGraceful,
		&tls.Config{ServerName: "h"},
		PoolOptions{MaxConnsPerHost: 1, MaxStreamsPerConn: 4, HealthCheckPeriod: time.Hour},
		dialFn, nil, nil)
	if err != nil {
		t.Fatalf("newH3ManagedPool: %v", err)
	}
	defer func() { _ = mp.close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, rel, _, err := mp.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire = %v; a live second address was available, so the loop stopped on the dead one", err)
	}
	if c == nil {
		t.Fatal("acquire returned no connection and no error")
	}
	rel()
}
