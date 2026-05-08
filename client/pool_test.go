package client

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func TestPool_Stats_Empty(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 2})
	t.Cleanup(func() { _ = p.Close() })

	s := p.Stats()
	if s.ActiveConns != 0 || s.InFlightStreams != 0 || s.Waiters != 0 || s.InFlightDials != 0 {
		t.Fatalf("empty Stats = %+v", s)
	}
}

func TestPool_Close_Idempotent(t *testing.T) {
	t.Parallel()
	p := newPool("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1})
	if err := p.Close(); err != nil {
		t.Fatalf("first Close = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}
