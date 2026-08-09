package grpc

import (
	"errors"
	"net/http"
	"testing"
)

// DiscardMetadata skips copying the response header and trailer blocks out of
// conn's pooled slab. The risk it creates is precise: grpc-status and
// grpc-message are then read from the LIVE block, which the pool reclaims when
// the event handler returns. Get that wrong and a failing call reports a garbage
// diagnosis instead of the server's — quietly, because the shape of a Status is
// still a Status.
//
// So these test the outcome of failing calls, not just that Header() is nil.

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
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %T %v, want *Status", err, err)
	}
	if st.Code != PermissionDenied {
		t.Errorf("code = %v, want PermissionDenied", st.Code)
	}
	if st.Message != msg {
		t.Errorf("message = %q, want %q — grpc-message is read from the live block and "+
			"must be copied out of it before the slab returns", st.Message, msg)
	}
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
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %T %v, want *Status", err, err)
	}
	if st.Code != NotFound || st.Message != msg {
		t.Errorf("Trailers-Only gave (%v, %q), want (NotFound, %q)", st.Code, st.Message, msg)
	}
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
	if !errors.As(err, &st) {
		t.Fatalf("Invoke error = %T %v, want *Status", err, err)
	}
	if st.Code != Unavailable {
		t.Errorf("503 mapped to %v, want Unavailable", st.Code)
	}
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
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SendLast(ctx, []byte("q")); err != nil && !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendLast: %v", err)
	}
	// Drain to the end so the trailer block has been handled.
	for {
		if _, err := s.Recv(ctx); err != nil {
			break
		}
	}
	if h, _ := s.Header(ctx); h != nil {
		t.Errorf("Header() returned %d fields under DiscardMetadata, want nil", len(h))
	}
	if tr := s.Trailer(); tr != nil {
		t.Errorf("Trailer() returned %d fields under DiscardMetadata, want nil", len(tr))
	}
	if s.Status().Code != OK {
		t.Errorf("status = %v, want OK", s.Status().Code)
	}
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
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SendLast(ctx, []byte("q")); err != nil && !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("SendLast: %v", err)
	}
	hdr, err := s.Header(ctx)
	if err != nil {
		t.Fatalf("Header: %v", err)
	}
	if v, ok := findField(hdr, "x-served-by"); !ok || string(v) != "test" {
		t.Errorf("x-served-by = %q (present=%v) — the default must still copy the header block",
			v, ok)
	}
	for {
		if _, err := s.Recv(ctx); err != nil {
			break
		}
	}
	if s.Trailer() == nil {
		t.Error("Trailer() is nil by default — the trailer block must still be copied")
	}
}
