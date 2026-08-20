package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nonRepeatingPattern returns n bytes where every position carries a distinct
// value modulo 251 (a prime > the 0..250 range), so a corrupted, truncated, or
// misordered reassembly cannot be masked by repeated bytes.
func nonRepeatingPattern(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i % 251)
	}
	return p
}

// streamPatternServer flushes the pattern as `chunk`-sized DATA frames so the
// client observes multiple frame boundaries (one pooled buffer per frame).
func streamPatternServer(t *testing.T, pattern []byte, chunk int) string {
	return h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		for off := 0; off < len(pattern); off += chunk {
			end := off + chunk
			if end > len(pattern) {
				end = len(pattern)
			}
			_, _ = w.Write(pattern[off:end])
			if fl != nil {
				fl.Flush()
			}
		}
	})
}

// A multi-frame body drives the pooled receive path through many distinct
// buffers, so a gross wiring bug — wrong DataSlab pointer, off-by-one in the
// surplus-alias slice, truncation, or double-delivery — corrupts the
// reassembled bytes deterministically.
//
// Receive-window refunds are EAGER: onDataReceived returns window credit on
// frame RECEIPT, not on consumer read, so the connection reader buffers frames
// independently of how fast the consumer drains. StreamEventBuffer is therefore
// sized to absorb every in-flight frame; a smaller channel would drop a frame
// via push() overflow and surface a spurious RST_STREAM(CANCEL) under
// slow CI scheduling (same reasoning as the StreamEventBuffer=1024 large-body
// integration tests).
//
// The race-precise premature-recycle guard (a buffer recycled while a consumer
// slice still aliases it) is `go test -race` on CI plus the conn-layer
// TestOnData_DistinctBuffersWhileOutstanding; that pair owns the concurrency
// invariant, while these tests own deterministic content/wiring correctness for
// both client consumer paths.
const (
	poolChunk  = 8192
	poolFrames = 64
	poolTotal  = poolChunk * poolFrames // 512 KiB across poolFrames frames
)

func poolTestClient(t *testing.T, addr string) *Client {
	co := insecureConnOpts()
	co.StreamEventBuffer = 1024 // absorb the eager-refund burst; never overflow
	c, err := NewClient(ClientOptions{
		Addr:      addr,
		Transport: TransportPool,
		Pool:      &PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 10},
		ConnOpts:  co,
	})
	require.NoError(t, err, "NewClient")
	return c
}

func assertPattern(t *testing.T, got, want []byte) {
	t.Helper()
	if bytes.Equal(got, want) {
		return
	}

	mismatch := -1
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			mismatch = i
			break
		}
	}

	require.Failf(t, "streamed body corrupted",
		"got %d bytes, want %d, first mismatch at index %d", len(got), len(want), mismatch)
}

// TestBodyStream_MultiFrame_NoCorruption guards the responseBodyReader
// (BodyMode=BodyStream) pooled path. It reads through a 7-byte buffer so the
// surplus path — r.buf aliasing the pooled buffer across many Reads before the
// next frame's Recv recycles it — is exercised on every frame. A wrong recycle,
// pointer, or surplus slice corrupts the reassembled non-repeating pattern.
func TestBodyStream_MultiFrame_NoCorruption(t *testing.T) {
	pattern := nonRepeatingPattern(poolTotal)
	addr := streamPatternServer(t, pattern, poolChunk)
	c := poolTestClient(t, addr)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var resp Response
	err := c.Do(ctx, &Request{Method: "GET", Path: "/", BodyMode: BodyStream}, &resp)
	require.NoError(t, err, "Do")
	defer resp.BodyReader.Close()

	got := make([]byte, 0, poolTotal)
	buf := make([]byte, 7) // tiny buffer -> surplus alias path hit every frame
	for {
		n, rerr := resp.BodyReader.Read(buf)
		got = append(got, buf[:n]...)
		if rerr == io.EOF {
			break
		}
		require.NoError(t, rerr, "Read")
	}

	assertPattern(t, got, pattern)
}

// TestStreamResponse_MultiFrame_NoCorruption guards the structurally different
// StreamResponse.Recv path (DoStream). Recv recycles the previous event's
// pooled buffer at the TOP of the next call, so ev.Data is valid only until the
// next Recv. The test copies each ev.Data out (append) before the next Recv and
// compares the reassembly against the non-repeating pattern; a recycle that
// returned the buffer too early, a wrong DataSlab pointer, or a truncated
// payload would corrupt the copy.
func TestStreamResponse_MultiFrame_NoCorruption(t *testing.T) {
	pattern := nonRepeatingPattern(poolTotal)
	addr := streamPatternServer(t, pattern, poolChunk)
	c := poolTestClient(t, addr)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var sr StreamResponse
	err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr)
	require.NoError(t, err, "DoStream")
	defer sr.Close()

	got := make([]byte, 0, poolTotal)
	for {
		ev, rerr := sr.Recv(ctx)
		if errors.Is(rerr, ErrStreamEnded) {
			break
		}
		require.NoError(t, rerr, "Recv")
		if ev.Type == EventData {
			got = append(got, ev.Data...) // copy out before the next Recv recycles curData
		}
	}

	assertPattern(t, got, pattern)
}
