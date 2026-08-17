package conn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConn_NewStream_ConcurrentAllocation_RespectsCap verifies that
// concurrent NewStream callers race the inflight counter without
// exceeding the advertised MaxConcurrentStreams. ID assignment itself
// is exercised by the integration suite, since IDs are now allocated
// at the moment HEADERS goes on the wire (RFC 7540 §5.1.1).
func TestConn_NewStream_ConcurrentAllocation_RespectsCap(t *testing.T) {
	const cap = 8
	c := newFakeConn(cap)
	successes := make(chan struct{}, cap*2)
	tooMany := make(chan struct{}, cap*2)

	var wg sync.WaitGroup
	for i := 0; i < cap*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.NewStream(context.Background())
			switch err {
			case nil:
				successes <- struct{}{}
			case ErrTooManyStreams:
				tooMany <- struct{}{}
			default:
				// assert, not require: FailNow from a non-test goroutine is
				// undefined, and the counts below report the outcome anyway.
				assert.Failf(t, "unexpected NewStream error", "unexpected err: %v", err)
			}
		}()
	}
	wg.Wait()

	close(successes)
	close(tooMany)
	require.Lenf(t, successes, cap, "successes = %d, want %d", len(successes), cap)
	require.Lenf(t, tooMany, cap, "ErrTooManyStreams count = %d, want %d", len(tooMany), cap)
}

// TestConn_NewStream_AfterRelease_ReusesSlot verifies that a stream
// allocated via NewStream but never written to (e.g. cancelled before
// SendHeaders) frees its slot via releaseUnassignedInflight.
func TestConn_NewStream_AfterRelease_ReusesSlot(t *testing.T) {
	c := newFakeConn(2)
	s1, err := c.NewStream(context.Background())
	require.NoErrorf(t, err, "NewStream 1")
	_, err = c.NewStream(context.Background())
	require.NoErrorf(t, err, "NewStream 2")
	_, err = c.NewStream(context.Background())
	require.ErrorIsf(t, err, ErrTooManyStreams, "NewStream 3 err = %v, want ErrTooManyStreams", err)

	c.releaseUnassignedInflight(s1.Stream())

	_, err = c.NewStream(context.Background())
	require.NoErrorf(t, err, "NewStream after release")
}

// newFakeConn builds a *Conn wired only enough to exercise NewStream
// bookkeeping (no real I/O).
func newFakeConn(maxStreams uint32) *Conn {
	return &Conn{
		opts:       ConnOptions{Settings: AdvertisedSettings{MaxConcurrentStreams: maxStreams}}.defaulted(),
		nextID:     1,
		streams:    map[uint32]*Stream{},
		readerDone: make(chan struct{}),
	}
}

// TestIntegration_TenConcurrentStreams_Echo runs 10 streams in parallel
// against the httptest h2 server, each round-tripping its own body.
// This is the headline B.2.1 test: it would fail with ErrTooManyStreams
// under the B.1 single-stream cap.
func TestIntegration_TenConcurrentStreams_Echo(t *testing.T) {
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	c := dialServer(t, srv, cfg)
	defer c.Close()
	const n = 10
	errs := make(chan error, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s, err := c.NewStream(ctx)
			if err != nil {
				errs <- fmt.Errorf("stream %d NewStream: %w", i, err)
				return
			}
			body := []byte(fmt.Sprintf("hello-from-%d", i))
			cl := fmt.Sprintf("%d", len(body))
			if err := s.SendHeaders(ctx, []header.Field{
				{Name: []byte(":method"), Value: []byte("POST")},
				{Name: []byte(":scheme"), Value: []byte("https")},
				{Name: []byte(":authority"), Value: []byte("example.com")},
				{Name: []byte(":path"), Value: []byte("/echo")},
				{Name: []byte("content-length"), Value: []byte(cl)},
			}, false); err != nil {
				errs <- fmt.Errorf("stream %d SendHeaders: %w", i, err)
				return
			}
			if err := s.SendData(ctx, body, true); err != nil {
				errs <- fmt.Errorf("stream %d SendData: %w", i, err)
				return
			}
			var got []byte
			for {
				ev, err := s.Recv(ctx)
				if err != nil {
					errs <- fmt.Errorf("stream %d Recv: %w", i, err)
					return
				}
				if ev.Type == EventData {
					got = append(got, ev.Data...)
				}
				if ev.EndStream {
					break
				}
			}
			if string(got) != string(body) {
				errs <- fmt.Errorf("stream %d echo mismatch: got %q want %q", i, got, body)
				return
			}
		}(i)
	}
	wg.Wait()

	close(errs)
	for err := range errs {
		assert.NoError(t, err, "a concurrent stream did not round-trip its own body")
	}
}
