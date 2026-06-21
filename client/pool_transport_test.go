package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

func TestPoolTransport_AcquireAfterClose_ReturnsErrPoolClosed(t *testing.T) {
	t.Parallel()
	pt := newPoolTransport("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	if err := pt.close(); err != nil {
		t.Fatalf("close = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _, _, err := pt.openExchange(ctx)
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("openExchange after close = %v, want ErrPoolClosed", err)
	}
}
