package client_test

import (
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/client"
)

// readCloseProbe counts the iterations where the abort actually landed on an
// in-flight Read.
//
// A timing-shaped test needs this: if the response finished before the closer
// woke, Close aborts nothing and the iteration passes exactly like a real one.
// injected is a lower bound on genuine overlap — the closer samples inRead
// immediately before calling Close — and the control arm below, which never
// aborts, is what shows the counter measures overlap rather than iterations.
type readCloseProbe struct {
	inRead   atomic.Int64
	injected atomic.Int64
}

// slowChunkedBody streams 16 x 32 KiB with a 1 ms gap, so a body takes ~16 ms to
// deliver and an abort fired at 2 ms is inside the window by construction rather
// than by luck.
func slowChunkedBody() http.Handler {
	chunk := make([]byte, 32*1024)
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		for i := 0; i < 16; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			time.Sleep(time.Millisecond)
		}
	})
}

// streamOnce opens one streamed response, drains it in a reader goroutine and
// Closes it from a second one. Both arms run the identical sample-then-Close
// code; only the moment differs. With abort=true the closer fires 2 ms in, while
// the reader is mid-body; with abort=false it waits for the reader to finish
// first, so the same sampler must observe nothing in flight. It returns false if
// reader and closer had not both finished within the timeout, which is the
// deadlock signal.
func streamOnce(t *testing.T, c *client.Client, p *readCloseProbe, abort bool) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var res client.Response
	require.NoError(t, c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &res),
		"Do must return once the response HEADERS arrive; nothing can be measured without a body reader")

	var wg sync.WaitGroup
	readerDone := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer close(readerDone)
		buf := make([]byte, 4096)
		for {
			p.inRead.Add(1)
			_, err := res.BodyReader.Read(buf)
			p.inRead.Add(-1)
			if err != nil {
				break
			}
		}
		// One Read past the abort, deliberately. It is the scheduling point that
		// makes the window observable: this Read takes r.mu and touches closed /
		// buf / curData exactly while Close is releasing the slab, so a Close that
		// recycles outside the lock is an unsynchronised access pair rather than a
		// pair the detector may never see. It is also the documented contract —
		// Read after Close is EOF, never another request's bytes.
		p.inRead.Add(1)
		_, _ = res.BodyReader.Read(buf)
		p.inRead.Add(-1)
	}()
	go func() {
		defer wg.Done()
		if abort {
			time.Sleep(2 * time.Millisecond)
		} else {
			<-readerDone
		}
		if p.inRead.Load() > 0 {
			p.injected.Add(1)
		}
		_ = res.BodyReader.Close()
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(8 * time.Second):
		return false
	}
}

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
// This test asserts nothing about content: it exists to fail under -race, to
// fail by DEADLOCK if Close is ever made to wait for the Read it aborts, and —
// via the probe — to fail rather than pass silently when the abort never
// overlapped a read at all.
func TestBodyReader_ConcurrentReadCloseIsSafe(t *testing.T) {
	_, addr := newTLSH2Server(t, slowChunkedBody())
	c := clientFor(t, addr)
	var p readCloseProbe

	for iter := 0; iter < 12; iter++ {
		require.Truef(t, streamOnce(t, c, &p, true),
			"iter %d: Close did not complete while a Read was in flight — the abort is waiting for the read it aborts", iter)
	}

	assert.Positive(t, p.injected.Load(),
		"no iteration landed Close on an in-flight Read: the run exercised no abort at all, "+
			"so a green here says nothing about Read/Close serialisation")
}

// TestBodyReader_ConcurrentReadCloseProbeControl is the control arm for the
// probe above: same fixture, same closer goroutine, same sampler — only the
// Close is held back until the reader has finished, so nothing is in flight when
// it fires. A probe that counted iterations, or Closes, rather than genuine
// Close-on-an-in-flight-Read would register here too. It must not, or the
// sibling test's non-zero count is meaningless.
func TestBodyReader_ConcurrentReadCloseProbeControl(t *testing.T) {
	_, addr := newTLSH2Server(t, slowChunkedBody())
	c := clientFor(t, addr)
	var p readCloseProbe

	for iter := 0; iter < 3; iter++ {
		require.Truef(t, streamOnce(t, c, &p, false), "iter %d: undisturbed drain did not finish", iter)
	}

	assert.Zero(t, p.injected.Load(),
		"the probe counted an injection in a run that injects nothing — it is counting iterations, "+
			"not Close-on-an-in-flight-Read, and the sibling test's count proves nothing")
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
	require.NoError(t, c.Do(ctx, &client.Request{Method: "GET", Path: "/", BodyMode: client.BodyStream}, &res), "Do")
	// One short read, so a surplus tail is left in buf aliasing the slab.
	small := make([]byte, 16)
	if _, err := res.BodyReader.Read(small); err != nil {
		require.ErrorIs(t, err, io.EOF, "first Read")
	}
	require.NoError(t, res.BodyReader.Close(), "Close")

	n, err := res.BodyReader.Read(small)

	assert.ErrorIsf(t, err, io.EOF,
		"Read after Close = (%d, %v), want (0, io.EOF) — anything else means the caller was handed "+
			"bytes out of a slab already back in the pool", n, err)
	assert.Zerof(t, n, "Read after Close returned %d bytes; the slab is no longer ours to copy from", n)
}
