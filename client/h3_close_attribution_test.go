package client

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// ————————————————————————————————————————————————————————————————
// Closing the pool must say WHY each conn went.
//
// h3RetireReason is documented as the single spelling of the retire rule
// shared by every eviction site, and handleRelease, handleTick and
// handleStats all consult it — each bumping GoAwaysReceived when it answers
// CloseGoAway. handleClose is the one site that does not: it reports
// CloseManual unconditionally.
//
// So a conn the peer had told to go away is filed under "the operator closed
// the pool", and the GOAWAY never reaches the metric. The HTTP/2 pool's
// handleClose does attribute it, which is what makes this an omission rather
// than a protocol difference — HTTP/3 GOAWAY is RFC 9114 §5.2, and the pool
// already tracks it everywhere else.
//
// The existing TestH3Pool_Close_ClosesAllConns counts how many times
// OnConnClose fires, never with what reason, so nothing caught this.
// ————————————————————————————————————————————————————————————————

func TestH3Pool_Close_AttributesGoAway(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	reasons := map[CloseReason]int{}
	hooks := &Hooks{OnConnClose: func(e ConnCloseEvent) {
		mu.Lock()
		reasons[e.Reason]++
		mu.Unlock()
	}}
	hp := new(atomic.Pointer[Hooks])
	hp.Store(hooks)
	metrics := &Metrics{}

	d := newH3FakeDialer()
	p := newH3Pool("h:443", nil, PoolOptions{
		MaxConnsPerHost:   2,
		MaxStreamsPerConn: 1,
	}, d.dial, hp, metrics)

	// Two conns: hold a stream on each so both are still pooled at Close, and
	// so neither can be retired early — h3RetireReason only retires a
	// going-away conn once it has drained.
	for i := 0; i < 2; i++ {
		if _, err := p.acquire(context.Background()); err != nil {
			t.Fatalf("acquire[%d]: %v", i, err)
		}
	}

	conns := d.all()
	if len(conns) != 2 {
		t.Fatalf("dialed %d conns, want 2", len(conns))
	}
	// The peer sends GOAWAY on exactly one of them. The other is healthy, so the
	// test also pins that a plain close is still CloseManual.
	atomic.StoreInt32(&conns[0].goawayFlag, 1)

	_ = p.Close()

	mu.Lock()
	gotGoAway, gotManual := reasons[CloseGoAway], reasons[CloseManual]
	mu.Unlock()

	if gotGoAway != 1 {
		t.Errorf("OnConnClose fired CloseGoAway %d times, want 1;\n"+
			"handleClose reported a conn the peer had GOAWAY'd as an operator close", gotGoAway)
	}
	if gotManual != 1 {
		t.Errorf("OnConnClose fired CloseManual %d times, want 1 (the healthy conn)", gotManual)
	}
	if got := metrics.Counters.GoAwaysReceived.Load(); got != 1 {
		t.Errorf("GoAwaysReceived = %d, want 1;\n"+
			"every other h3 eviction site counts a GOAWAY, so closing the pool must too", got)
	}
}
