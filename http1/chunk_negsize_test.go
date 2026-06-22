package http1_test

import (
	"context"
	"strings"
	"testing"
)

// TestReadChunkedChunk_NegativeSizeRejected is a regression test for a
// remotely-triggerable client panic: a server sending a negative hex
// chunk-size line ("-1\r\n") used to flow through strconv.ParseInt (which
// accepts a leading '-') into a negative slice bound buf[:-1], panicking with
// "slice bounds out of range [:-1]". The fix rejects size < 0 with an error.
func TestReadChunkedChunk_NegativeSizeRejected(t *testing.T) {
	resp := "HTTP/1.1 200 OK\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"\r\n" +
		"-1\r\n"
	ex := wireExchange(t, "GET", resp)
	if _, _, err := ex.ReadResponse(context.Background()); err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	buf := make([]byte, 64)

	var panicked bool
	var n int
	var done bool
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		n, done, err = ex.ReadBodyChunk(buf)
	}()

	if panicked {
		t.Fatalf("ReadBodyChunk panicked on a negative chunk size; want a clean error")
	}
	if err == nil {
		t.Fatalf("negative chunk size accepted (n=%d done=%v); want an error", n, done)
	}
	if !strings.Contains(err.Error(), "invalid chunk size") {
		t.Fatalf("unexpected error for negative chunk size: %v", err)
	}
}
