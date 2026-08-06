package client_test

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/client"
)

// TestBodyReader_ConcurrentReadCloseIsSafe drives the shape net/http conditions
// users into: Close from another goroutine to abort a slow read.
//
// Response.BodyReader is a public io.ReadCloser, so that is a legitimate call
// pattern. Before this was serialised, Close recycled the pooled DATA slab while
// an in-flight Read still aliased it through buf — a later request could be
// handed the same slab and overwrite bytes this caller was about to copy out.
// The race detector reported it at the putDataSlab in recycleData against
// Read's use of curData.
//
// This test asserts nothing about content: it exists to fail under -race, and
// to fail by DEADLOCK if Close is ever made to wait for the Read it aborts.
func TestBodyReader_ConcurrentReadCloseIsSafe(t *testing.T) {
	chunk := make([]byte, 32*1024)
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		for i := 0; i < 16; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			time.Sleep(time.Millisecond)
		}
	}))
	c := clientFor(t, addr)

	for iter := 0; iter < 12; iter++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var res client.Response
		if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &res); err != nil {
			cancel()
			t.Fatalf("iter %d Do: %v", iter, err)
		}

		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4096)
			for {
				if _, err := res.BodyReader.Read(buf); err != nil {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(2 * time.Millisecond)
			_ = res.BodyReader.Close()
		}()
		go func() { wg.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(8 * time.Second):
			cancel()
			t.Fatal("Close did not complete while a Read was in flight — the abort is waiting for the read it aborts")
		}
		cancel()
	}
}

// TestBodyReader_ReadAfterCloseReturnsEOF pins that a Read issued after Close
// does not copy out of a slab that has gone back to the pool. Close clears buf
// and marks the reader done, so the answer is EOF rather than another request's
// bytes.
func TestBodyReader_ReadAfterCloseReturnsEOF(t *testing.T) {
	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, addr := newTLSH2Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(payload)
	}))
	c := clientFor(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var res client.Response
	if err := c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &res); err != nil {
		t.Fatalf("Do: %v", err)
	}

	// One short read, so a surplus tail is left in buf aliasing the slab.
	small := make([]byte, 16)
	if _, err := res.BodyReader.Read(small); err != nil && err != io.EOF {
		t.Fatalf("first Read: %v", err)
	}
	if err := res.BodyReader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	n, err := res.BodyReader.Read(small)
	if err != io.EOF || n != 0 {
		t.Fatalf("Read after Close = (%d, %v), want (0, io.EOF)", n, err)
	}
}
