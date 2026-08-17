package grpc

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// DiscardMetadata skips copying the response header and trailer blocks out of
// conn's pooled block. The risk it creates is precise: grpc-status and
// grpc-message are then read from the LIVE block, which the pool reclaims when
// the event handler returns. Get that wrong and a failing call reports a garbage
// diagnosis instead of the server's — quietly, because the shape of a Status is
// still a Status.
//
// So these test the outcome of failing calls, not just that Header() is nil.

// benignSendLastErr reports whether a SendLast error is the ordinary race with a
// server that answered and finished first, rather than a failure.
//
// There are two sentinels here and their names differ by a package qualifier:
// grpc.ErrStreamClosed ("grpc: stream closed by the caller") and
// conn.ErrStreamClosed ("conn: stream already closed"). The one that surfaces
// depends on who noticed first — the gRPC layer, or the transport whose stream
// the peer's END_STREAM already retired. These tests tolerated only the first, so
// they passed locally and fataled on a loaded CI runner where the server usually
// wins. grpc/conn.go's own send path has always tolerated both.
func benignSendLastErr(err error) bool {
	return errors.Is(err, ErrStreamClosed) || errors.Is(err, conn.ErrStreamClosed)
}

// TestDiscardMetadata_ErrorStatusStillArrives is the one that matters. The
// status travels in trailers, which is exactly the block that is no longer
// copied.
func TestDiscardMetadata_ErrorStatusStillArrives(t *testing.T) {
	const msg = "the server explained itself at some length, with spaces"
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srvBeginResponse(w)
		srvFinish(w, PermissionDenied, msg)
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	_, err := cc.Invoke(t.Context(), "/t.S/M", []byte("q"), nil)

	var st *Status
	require.Truef(t, errors.As(err, &st), "Invoke error = %T %v, want *Status", err, err)
	assert.Equalf(t, PermissionDenied, st.Code, "code = %v, want PermissionDenied", st.Code)
	assert.Equalf(t, msg, st.Message,
		"message = %q, want %q — grpc-message is read from the live block and "+
			"must be copied out of it before the block returns", st.Message, msg)
}

// TestDiscardMetadata_TrailersOnlyStillArrives covers the other block: a
// Trailers-Only response puts the status in the HEADER block, which onHeaders
// no longer copies either.
func TestDiscardMetadata_TrailersOnlyStillArrives(t *testing.T) {
	const msg = "not here"
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("grpc-status", "5") // NotFound
		w.Header().Set("grpc-message", msg)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	_, err := cc.Invoke(t.Context(), "/t.S/M", []byte("q"), nil)

	var st *Status
	require.Truef(t, errors.As(err, &st), "Invoke error = %T %v, want *Status", err, err)
	assert.Truef(t, st.Code == NotFound && st.Message == msg,
		"Trailers-Only gave (%v, %q), want (NotFound, %q)", st.Code, st.Message, msg)
}

// TestDiscardMetadata_NonOKHTTPStillMaps covers the third path through
// onHeaders: a non-200 response, whose status the mapping table derives and
// whose block finish also reads for a server-supplied grpc-status.
func TestDiscardMetadata_NonOKHTTPStillMaps(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)

	_, err := cc.Invoke(t.Context(), "/t.S/M", []byte("q"), nil)

	var st *Status
	require.Truef(t, errors.As(err, &st), "Invoke error = %T %v, want *Status", err, err)
	assert.Equalf(t, Unavailable, st.Code, "503 mapped to %v, want Unavailable", st.Code)
}

// TestDiscardMetadata_HeaderAndTrailerAreNil pins the documented consequence, so
// a caller that opts in is not surprised and a future change that started
// populating them anyway shows up as a failure rather than as silent extra work.
func TestDiscardMetadata_HeaderAndTrailerAreNil(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("x-served-by", "test")
		srvBeginResponse(w)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx := t.Context()

	s, err := cc.NewStream(ctx, "/t.S/M", nil, DiscardMetadata())
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	if err := s.SendLast(ctx, []byte("q")); err != nil && !benignSendLastErr(err) {
		require.NoError(t, err, "SendLast")
	}
	// Drain to the end so the trailer block has been handled.
	for {
		if _, err := s.Recv(ctx); err != nil {
			break
		}
	}
	hdr, _ := s.Header(ctx)
	trl := s.Trailer()

	assert.Nilf(t, hdr, "Header() returned %d fields under DiscardMetadata, want nil", len(hdr))
	assert.Nilf(t, trl, "Trailer() returned %d fields under DiscardMetadata, want nil", len(trl))
	assert.Equalf(t, OK, s.Status().Code, "status = %v, want OK", s.Status().Code)
}

// TestDiscardMetadata_ConcurrentStatusesDoNotCross is the gate for the hazard
// this option creates: with no copy, finish reads a block that belongs to a
// pooled block, and releasing that block one line too early is invisible in a
// single-threaded test — the bytes are still there because nothing has reused
// them yet.
//
// Many concurrent calls on one connection make the pool actually recycle, so a
// block released before it is read gets overwritten by another call's and
// one RPC reports another's message.
func TestDiscardMetadata_ConcurrentStatusesDoNotCross(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo the caller's marker back as the status message, so every call has
		// a distinct one and a crossed block is visible as a mismatch.
		marker := r.Header.Get("x-marker")
		srvBeginResponse(w)
		srvFinish(w, PermissionDenied, "denied-for-"+marker)
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx := t.Context()
	const workers, perWorker = 8, 25
	errs := make(chan error, workers*perWorker)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				marker := fmt.Sprintf("%d-%d", w, i)
				md, err := AppendMetadata(nil, "x-marker", []byte(marker))
				if err != nil {
					errs <- err
					return
				}
				_, err = cc.Invoke(ctx, "/t.S/M", []byte("q"), md)
				var st *Status
				if !errors.As(err, &st) {
					errs <- fmt.Errorf("%s: error = %T %v, want *Status", marker, err, err)
					continue
				}
				if want := "denied-for-" + marker; st.Message != want {
					errs <- fmt.Errorf("%s: got status message %q, want %q — a pooled header "+
						"block was read after it went back", marker, st.Message, want)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)

	n := 0
	for err := range errs {
		if n < 5 {
			assert.NoError(t, err, "a concurrent DiscardMetadata call read a recycled block")
		}
		n++
	}
	assert.LessOrEqualf(t, n, 5, "... and %d more", n-5)
}

// TestDiscardMetadata_DefaultStillPopulates is the no-regression half: a stream
// that did not ask for it still gets both blocks.
func TestDiscardMetadata_DefaultStillPopulates(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("x-served-by", "test")
		srvBeginResponse(w)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx := t.Context()

	s, err := cc.NewStream(ctx, "/t.S/M", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	if err := s.SendLast(ctx, []byte("q")); err != nil && !benignSendLastErr(err) {
		require.NoError(t, err, "SendLast")
	}
	hdr, err := s.Header(ctx)
	require.NoError(t, err, "Header")
	for {
		if _, err := s.Recv(ctx); err != nil {
			break
		}
	}

	v, ok := findField(hdr, "x-served-by")
	assert.Truef(t, ok && string(v) == "test",
		"x-served-by = %q (present=%v) — the default must still copy the header block", v, ok)
	assert.NotNil(t, s.Trailer(),
		"Trailer() is nil by default — the trailer block must still be copied")
}

// TestSendLast_AfterServerFinished pins the tolerance itself, deterministically.
//
// The two tests above race the server: they call SendLast on a stream the server
// may already have finished. On a fast machine the client usually wins and the
// call succeeds, which is why tolerating only the gRPC-level sentinel passed here
// for a long time and fataled on a loaded CI runner. This test waits for the
// handler to return first, so the peer's END_STREAM is on its way and the error —
// when there is one — comes from the transport, not from gRPC.
func TestSendLast_AfterServerFinished(t *testing.T) {
	done := make(chan struct{})
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srvBeginResponse(w)
		srvFinish(w, OK, "")
		close(done)
	}))
	defer srv.Close()
	cc := dialGRPC(t, srv, cfg)
	ctx := t.Context()
	s, err := cc.NewStream(ctx, "/t.S/M", nil)
	require.NoError(t, err, "NewStream")
	defer func() { _ = s.Close() }()
	<-done // the server has answered and finished; our stream is being retired
	// Give the client's reader the chance to process END_STREAM, so the send below
	// meets a stream the transport has already closed.
	for i := 0; i < 100; i++ {
		if _, err := s.Recv(ctx); err != nil {
			break
		}
	}

	sendErr := s.SendLast(ctx, []byte("q"))

	require.Truef(t, sendErr == nil || benignSendLastErr(sendErr),
		"SendLast after the server finished returned %v — a send that loses "+
			"this race is ordinary, and both layers have a sentinel for it: "+
			"grpc.ErrStreamClosed and conn.ErrStreamClosed", sendErr)
}
