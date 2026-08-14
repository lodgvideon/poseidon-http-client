package grpc

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// BorrowMetadata takes the header and trailer copies from the stream's pooled
// arena instead of the heap. The saving is real and so is the hazard: the arena
// goes back to a pool the next RPC draws from, so anything that survives Close —
// a field the caller kept, a field header parked inside the pooled struct — is
// another call's memory. These tests are about the boundary, not the saving; the
// saving is gated in borrowmetadata_alloc_test.go.

// mdMap collects a block into a map so two calls can be compared without
// depending on the order net/http2 chose to send the fields in.
//
// "date" is dropped: net/http stamps it per response, so two calls a second
// apart disagree on it for reasons that have nothing to do with where the bytes
// were copied from.
func mdMap(fields []conn.HeaderField) map[string]string {
	m := make(map[string]string, len(fields))
	for _, f := range fields {
		if string(f.Name) == "date" {
			continue
		}
		m[string(f.Name)] = string(f.Value)
	}
	return m
}

// mdServer answers with a header block and a trailer block that both carry
// caller-checkable fields.
func mdServer(t *testing.T) *ClientConn {
	t.Helper()
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("x-served-by", "poseidon")
		w.Header().Set("x-request-id", "req-0123456789")
		srvBeginResponse(w)
		if err := srvWriteMessage(w, []byte("pong")); err != nil {
			t.Errorf("server write: %v", err)
		}
		w.Header().Set(http.TrailerPrefix+"x-trailer-note", "done-here")
		srvFinish(w, OK, "")
	}))
	t.Cleanup(srv.Close)
	return dialGRPC(t, srv, cfg)
}

// drain reads to the end of the stream so the trailer block has been folded in.
func drain(t *testing.T, s *Stream) {
	t.Helper()
	for {
		if _, err := s.Recv(t.Context()); err != nil {
			return
		}
	}
}

// TestBorrowMetadata_MatchesTheCopy is the correctness gate: borrowing changes
// where the bytes live and nothing else. A borrowed block that dropped a field,
// truncated a value or aliased the transport's block would show up here as a
// disagreement with the same call made the ordinary way.
func TestBorrowMetadata_MatchesTheCopy(t *testing.T) {
	cc := mdServer(t)
	ctx := t.Context()

	call := func(opts ...CallOption) (map[string]string, map[string]string) {
		t.Helper()
		s, err := cc.NewStream(ctx, "/t.S/M", nil, opts...)
		if err != nil {
			t.Fatalf("NewStream: %v", err)
		}
		defer func() { _ = s.Close() }()
		if err := s.SendLast(ctx, []byte("q")); err != nil && !benignSendLastErr(err) {
			t.Fatalf("SendLast: %v", err)
		}
		hdr, err := s.Header(ctx)
		if err != nil {
			t.Fatalf("Header: %v", err)
		}
		// mdMap copies every byte into strings, which is exactly what a caller
		// keeping borrowed metadata past Close has to do.
		h := mdMap(hdr)
		drain(t, s)
		return h, mdMap(s.Trailer())
	}

	wantHdr, wantTrl := call()
	gotHdr, gotTrl := call(BorrowMetadata())

	for k, want := range wantHdr {
		if got := gotHdr[k]; got != want {
			t.Errorf("borrowed header %q = %q, want %q", k, got, want)
		}
	}
	if len(gotHdr) != len(wantHdr) {
		t.Errorf("borrowed header has %d fields, the copy has %d: %v vs %v",
			len(gotHdr), len(wantHdr), gotHdr, wantHdr)
	}
	for k, want := range wantTrl {
		if got := gotTrl[k]; got != want {
			t.Errorf("borrowed trailer %q = %q, want %q", k, got, want)
		}
	}
	if len(gotTrl) != len(wantTrl) {
		t.Errorf("borrowed trailer has %d fields, the copy has %d: %v vs %v",
			len(gotTrl), len(wantTrl), gotTrl, wantTrl)
	}
	// The fields the server set must actually be among them, or the comparison
	// above could agree on two equally empty blocks.
	if gotHdr["x-served-by"] != "poseidon" || gotTrl["x-trailer-note"] != "done-here" {
		t.Errorf("the server's own fields are missing: header %v, trailer %v", gotHdr, gotTrl)
	}
}

// TestBorrowMetadata_CloseNilsTheView pins the courtesy the option documents.
// Trailer is a plain field read with no closed guard, so without this it would
// keep answering out of an arena the next RPC is writing into — the one shape
// this option can get wrong that the package can cheaply make visible instead.
func TestBorrowMetadata_CloseNilsTheView(t *testing.T) {
	cc := mdServer(t)
	ctx := t.Context()

	after := func(opts ...CallOption) ([]conn.HeaderField, Status) {
		t.Helper()
		s, err := cc.NewStream(ctx, "/t.S/M", nil, opts...)
		if err != nil {
			t.Fatalf("NewStream: %v", err)
		}
		if err := s.SendLast(ctx, []byte("q")); err != nil && !benignSendLastErr(err) {
			t.Fatalf("SendLast: %v", err)
		}
		drain(t, s)
		if s.Trailer() == nil {
			t.Fatal("Trailer() is nil BEFORE Close — the block was never copied")
		}
		_ = s.Close()
		return s.Trailer(), s.Status()
	}

	if tr, _ := after(BorrowMetadata()); tr != nil {
		t.Errorf("Trailer() returned %d fields after Close under BorrowMetadata, want nil — "+
			"those fields point into an arena another RPC now owns", len(tr))
	}
	// The default is the contrast that keeps the check honest: without the option
	// the copies are the caller's outright and Close must not take them away.
	if tr, _ := after(); tr == nil {
		t.Error("Trailer() went nil after Close WITHOUT BorrowMetadata — those copies are " +
			"heap memory the caller owns and Close has no business dropping them")
	}
	// Status is not metadata. Its Message is built with string(), which copies, so
	// it survives Close under either mode — and a caller reading the outcome of a
	// finished call is the ordinary thing to do.
	if _, st := after(BorrowMetadata()); st.Code != OK {
		t.Errorf("Status() after Close = %v, want OK — Status must not depend on the arena", st.Code)
	}
}

// TestBorrowMetadata_ClampsEachFieldToItsOwnBytes pins the three-index clamp on
// the arena path. Every Name and Value has its capacity cut to its own length, so
// a caller appending to one field's value reallocates instead of writing over the
// next field's bytes. Without the clamp the capacity of each slice would run to
// the end of the arena and the overwrite would be silent.
//
// Driven through copyFields directly rather than a server, because it needs two
// fields it knows to be adjacent in the arena.
func TestBorrowMetadata_ClampsEachFieldToItsOwnBytes(t *testing.T) {
	s := &Stream{borrowMD: true}
	s.acquireBufs()
	defer s.releaseBufs()

	got := s.copyFields([]conn.HeaderField{
		{Name: []byte("x-first"), Value: []byte("one")},
		{Name: []byte("x-second"), Value: []byte("two")},
	})
	if len(got) != 2 {
		t.Fatalf("copyFields returned %d fields, want 2", len(got))
	}
	_ = append(got[0].Value, "OVERRUN"...)
	_ = append(got[0].Name, "OVERRUN"...)
	if string(got[1].Name) != "x-second" || string(got[1].Value) != "two" {
		t.Errorf("appending to field 0 rewrote field 1: %q / %q — the three-index clamp is gone",
			got[1].Name, got[1].Value)
	}
	if string(got[0].Name) != "x-first" || string(got[0].Value) != "one" {
		t.Errorf("field 0 corrupted itself: %q / %q", got[0].Name, got[0].Value)
	}
}

// TestBorrowMetadata_ReleaseClearsTheFieldHeaders pins the rule the arena is
// allowed to exist under: nothing reachable from the pooled struct may outlive
// the RPC that filled it. The field headers are pointers at whatever the peer
// sent — an authorization value among it — so truncating alone would leave a
// struct sitting in the pool holding live references to them.
func TestBorrowMetadata_ReleaseClearsTheFieldHeaders(t *testing.T) {
	s := &Stream{borrowMD: true}
	s.acquireBufs()
	b := s.bufs

	s.header = s.copyFields([]conn.HeaderField{
		{Name: []byte("authorization"), Value: []byte("Bearer hunter2")},
	})
	if len(s.header) != 1 {
		t.Fatalf("copyFields returned %d fields, want 1", len(s.header))
	}
	s.releaseBufs()

	if len(b.mdFields) != 0 {
		t.Errorf("mdFields kept length %d after release, want 0", len(b.mdFields))
	}
	for i, f := range b.mdFields[:cap(b.mdFields)] {
		if f.Name != nil || f.Value != nil {
			t.Errorf("pooled mdFields[%d] still points at %q/%q — a parked struct is holding "+
				"the last RPC's metadata alive", i, f.Name, f.Value)
		}
	}
	if s.header != nil {
		t.Errorf("s.header survived release as %v, want nil", s.header)
	}
}

// TestBorrowMetadata_TrailersOnlyGivesTwoIndependentBlocks covers the shape
// where one block is both: Header and Trailer must each be populated, and
// mutating one must not change the other. Under borrowing they share the arena's
// bytes, which is what makes "independent" worth checking rather than assuming.
func TestBorrowMetadata_TrailersOnlyGivesTwoIndependentBlocks(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("x-served-by", "poseidon")
		w.Header().Set("grpc-status", "0")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cc := dialGRPC(t, srv, cfg)
	ctx := t.Context()

	// Repeated on one connection on purpose. This shape makes the trailer view by
	// appending the header view to the very slice it was carved from, and the two
	// halves of self-append behave differently: the first RPC grows the arena and
	// copies out of the old array, while a later one finds it already big enough
	// and writes in place. Only a warm arena reaches the second.
	for i := 0; i < 4; i++ {
		s, err := cc.NewStream(ctx, "/t.S/M", nil, BorrowMetadata())
		if err != nil {
			t.Fatalf("round %d: NewStream: %v", i, err)
		}
		if err := s.SendLast(ctx, []byte("q")); err != nil && !benignSendLastErr(err) {
			t.Fatalf("round %d: SendLast: %v", i, err)
		}
		drain(t, s)

		hdr, err := s.Header(ctx)
		if err != nil {
			t.Fatalf("round %d: Header: %v", i, err)
		}
		trl := s.Trailer()
		if len(hdr) == 0 || len(trl) == 0 {
			t.Fatalf("round %d: Trailers-Only gave header=%d trailer=%d fields, want both populated",
				i, len(hdr), len(trl))
		}
		if v, ok := findField(hdr, "x-served-by"); !ok || string(v) != "poseidon" {
			t.Errorf("round %d: the header view lost the block's fields: %q (present=%v)", i, v, ok)
		}
		if v, ok := findField(trl, "x-served-by"); !ok || string(v) != "poseidon" {
			t.Errorf("round %d: the trailer view lost the block's fields: %q (present=%v)", i, v, ok)
		}
		// The two are separate field slices, so overwriting one entry of the header
		// view must leave the trailer view intact.
		hdr[0] = conn.HeaderField{Name: []byte("clobbered"), Value: []byte("clobbered")}
		if string(trl[0].Name) == "clobbered" {
			t.Errorf("round %d: Header() and Trailer() share one field slice — mutating what "+
				"Header returned changed what Trailer returns", i)
		}
		_ = s.Close()
	}
}

// TestBorrowMetadata_DiscardWins pins the documented precedence. Both options set
// means nothing is copied at all, which is cheaper than borrowing, so the blocks
// are nil rather than arena-backed.
func TestBorrowMetadata_DiscardWins(t *testing.T) {
	cc := mdServer(t)
	ctx := t.Context()
	s, err := cc.NewStream(ctx, "/t.S/M", nil, BorrowMetadata(), DiscardMetadata())
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SendLast(ctx, []byte("q")); err != nil && !benignSendLastErr(err) {
		t.Fatalf("SendLast: %v", err)
	}
	drain(t, s)
	if h, _ := s.Header(ctx); h != nil {
		t.Errorf("Header() returned %d fields with both options set, want nil", len(h))
	}
	if tr := s.Trailer(); tr != nil {
		t.Errorf("Trailer() returned %d fields with both options set, want nil", len(tr))
	}
	if s.Status().Code != OK {
		t.Errorf("status = %v, want OK", s.Status().Code)
	}
}

// TestBorrowFields_CapsFieldCount mirrors the cap on the allocating path. The
// arena is pooled, so an uncapped block would park a peer-chosen amount of memory
// in the pool rather than merely in one Stream.
func TestBorrowFields_CapsFieldCount(t *testing.T) {
	src := make([]conn.HeaderField, maxMetadataFields+500)
	for i := range src {
		src[i] = conn.HeaderField{Name: []byte("k"), Value: []byte("v")}
	}
	s := &Stream{borrowMD: true}
	s.acquireBufs()
	defer s.releaseBufs()
	if got := s.copyFields(src); len(got) != maxMetadataFields {
		t.Fatalf("borrowFields kept %d fields, want the cap of %d", len(got), maxMetadataFields)
	}
}

// TestBorrowMetadata_ConcurrentCallsDoNotCross is the gate for the hazard the
// arena creates. One stream's arena returning to the pool while its metadata is
// still being read is invisible with a single call — the bytes are still there
// because nothing has reused them yet. Enough concurrent calls make the pool
// actually recycle, so an arena released too early comes back carrying another
// call's fields.
func TestBorrowMetadata_ConcurrentCallsDoNotCross(t *testing.T) {
	srv, cfg := startGRPCServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo the caller's marker into both blocks, so every call has distinct
		// metadata and a crossed arena shows up as a mismatch.
		marker := r.Header.Get("x-marker")
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("x-echo-marker", marker)
		srvBeginResponse(w)
		w.Header().Set(http.TrailerPrefix+"x-echo-trailer", marker)
		srvFinish(w, OK, "")
	}))
	defer srv.Close()

	cc := dialGRPC(t, srv, cfg)
	ctx := t.Context()

	const workers, perWorker = 8, 25
	errs := make(chan error, 2*workers*perWorker)
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
				s, err := cc.NewStream(ctx, "/t.S/M", md, BorrowMetadata())
				if err != nil {
					errs <- fmt.Errorf("%s: NewStream: %w", marker, err)
					return
				}
				if err := s.SendLast(ctx, []byte("q")); err != nil && !benignSendLastErr(err) {
					errs <- fmt.Errorf("%s: SendLast: %w", marker, err)
					_ = s.Close()
					return
				}
				hdr, herr := s.Header(ctx)
				if herr != nil {
					errs <- fmt.Errorf("%s: Header: %w", marker, herr)
					_ = s.Close()
					return
				}
				if v, ok := findField(hdr, "x-echo-marker"); !ok || string(v) != marker {
					errs <- fmt.Errorf("%s: header marker = %q (present=%v) — a pooled "+
						"metadata arena was read after it went back", marker, v, ok)
				}
				for {
					if _, rerr := s.Recv(ctx); rerr != nil {
						break
					}
				}
				if v, ok := findField(s.Trailer(), "x-echo-trailer"); !ok || string(v) != marker {
					errs <- fmt.Errorf("%s: trailer marker = %q (present=%v) — a pooled "+
						"metadata arena was read after it went back", marker, v, ok)
				}
				_ = s.Close()
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	n := 0
	for err := range errs {
		if n < 5 {
			t.Error(err)
		}
		n++
	}
	if n > 5 {
		t.Errorf("... and %d more", n-5)
	}
}
