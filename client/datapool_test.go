package client

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestStreamBody_MultiFrame_NoCorruption is the premature-recycle regression
// guard for the DATA-buffer pool. It streams a multi-frame body with a
// non-repeating byte pattern and reads it through responseBodyReader with a
// SMALL buffer, so the surplus path (r.buf aliasing the pooled buffer across
// Reads) is exercised on every frame. If a buffer were recycled while r.buf
// still aliased it (then re-Gotten by the next OnData and overwritten), the
// reassembled bytes would not match the pattern.
func TestStreamBody_MultiFrame_NoCorruption(t *testing.T) {
	const chunk = 8192
	const total = 4 * chunk
	pattern := make([]byte, total)
	for i := range pattern {
		pattern[i] = byte(i % 251) // non-repeating within and across frames
	}

	addr := h2TestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		for off := 0; off < total; off += chunk {
			_, _ = w.Write(pattern[off : off+chunk])
			if fl != nil {
				fl.Flush() // push a DATA-frame boundary so multiple frames arrive
			}
		}
	})

	c, err := NewClient(ClientOptions{
		Addr:      addr,
		Transport: TransportPool,
		Pool:      &PoolOptions{MaxConnsPerHost: 2, MaxStreamsPerConn: 10},
		ConnOpts:  insecureConnOpts(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var resp Response
	if err := c.Do(ctx, &Request{Method: "GET", Path: "/", StreamBody: true}, &resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.BodyReader.Close()

	got := make([]byte, 0, total)
	buf := make([]byte, 7) // tiny buffer -> surplus path hit every frame
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

	if !bytes.Equal(got, pattern) {
		mismatch := -1
		for i := 0; i < len(got) && i < len(pattern); i++ {
			if got[i] != pattern[i] {
				mismatch = i
				break
			}
		}
		t.Fatalf("streamed body corrupted: got %d bytes, want %d, first mismatch at index %d", len(got), len(pattern), mismatch)
	}
}
