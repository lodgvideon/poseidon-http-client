package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

// nonRepeatingPattern returns n bytes where every position carries a distinct
// value modulo 251 (a prime > the 0..250 range), so a premature recycle that
// re-Gets and overwrites an aliased buffer cannot be masked by repeated bytes.
func nonRepeatingPattern(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i % 251)
	}
	return p
}

// streamPatternServer flushes the pattern as `chunk`-sized DATA frames so the
// client observes multiple frame boundaries.
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

// The body deliberately exceeds the 64 KiB default receive window so DATA
// delivery is *staged* across WINDOW_UPDATE refunds: the connection reader
// keeps calling dataBufPool.Get() throughout the read, instead of buffering
// the whole body before the consumer's first read. That staging is what makes
// a premature recycle observable — a Put buffer gets re-Gotten and overwritten
// while a consumer slice still aliases it. StreamEventBuffer is held at 16,
// comfortably above the ~8 frames the window allows in flight, so the events
// channel never overflows into a spurious RST_STREAM(REFUSED_STREAM).
const (
	poolChunk  = 8192
	poolFrames = 64
	poolTotal  = poolChunk * poolFrames // 512 KiB >> 64 KiB window
)

func poolTestClient(t *testing.T, addr string) *Client {
	co := insecureConnOpts()
	co.StreamEventBuffer = 16
	c, err := NewClient(ClientOptions{
		Addr:      addr,
		Transport: TransportPool,
		Pool:      &PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 10},
		ConnOpts:  co,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
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
	t.Fatalf("streamed body corrupted: got %d bytes, want %d, first mismatch at index %d", len(got), len(want), mismatch)
}

// TestStreamBody_MultiFrame_NoCorruption is the premature-recycle regression
// guard for the DATA-buffer pool on the responseBodyReader (StreamBody=true)
// path. It reads through a 7-byte buffer so the surplus path (r.buf aliasing
// the pooled buffer across Reads) is exercised on every frame, over a body far
// larger than the receive window so recycling and re-Getting overlap for the
// whole stream. If a buffer were recycled while r.buf still aliased it (then
// re-Gotten by a later OnData and overwritten), the reassembled bytes would
// not match the pattern.
func TestStreamBody_MultiFrame_NoCorruption(t *testing.T) {
	pattern := nonRepeatingPattern(poolTotal)
	addr := streamPatternServer(t, pattern, poolChunk)
	c := poolTestClient(t, addr)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var resp Response
	if err := c.Do(ctx, &Request{Method: "GET", Path: "/", StreamBody: true}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.BodyReader.Close()

	got := make([]byte, 0, poolTotal)
	buf := make([]byte, 7) // tiny buffer -> surplus alias path hit every frame
	for {
		n, rerr := resp.BodyReader.Read(buf)
		got = append(got, buf[:n]...)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("Read: %v", rerr)
		}
	}
	assertPattern(t, got, pattern)
}

// TestStreamResponse_MultiFrame_NoCorruption is the premature-recycle guard for
// the structurally different StreamResponse.Recv path (DoStream). Recv recycles
// the previous event's pooled buffer at the TOP of the next call, so ev.Data is
// contractually valid only until the next Recv. The test copies each ev.Data
// out (append) before the next Recv and compares the reassembly against a
// non-repeating pattern; a recycle that fired too early — Putting a buffer a
// later OnData re-Gets and overwrites while ev.Data still aliases it — would
// corrupt the copy. The >window body forces that Get/read overlap for the whole
// stream rather than letting every frame buffer before the first Recv.
func TestStreamResponse_MultiFrame_NoCorruption(t *testing.T) {
	pattern := nonRepeatingPattern(poolTotal)
	addr := streamPatternServer(t, pattern, poolChunk)
	c := poolTestClient(t, addr)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var sr StreamResponse
	if err := c.DoStream(ctx, &Request{Method: "GET", Path: "/"}, &sr); err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	defer sr.Close()

	got := make([]byte, 0, poolTotal)
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, ErrStreamEnded) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Type == EventData {
			got = append(got, ev.Data...) // copy out before the next Recv recycles curData
		}
	}
	assertPattern(t, got, pattern)
}
