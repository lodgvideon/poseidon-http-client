package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolTransport_AcquireAfterClose_ReturnsErrPoolClosed(t *testing.T) {
	t.Parallel()

	pt := newPoolTransport("ignored:0", conn.ConnOptions{}, PoolOptions{MaxConnsPerHost: 1}, nil, nil)
	require.NoError(t, pt.close(), "closing a freshly built pool transport")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, _, _, _, err := pt.openExchange(ctx)

	assert.Truef(t, errors.Is(err, ErrPoolClosed),
		"openExchange after close = %v, want ErrPoolClosed — a caller cannot tell a "+
			"closed pool from a transport failure otherwise", err)
}
