package conn

import (
	"runtime/debug"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// TestOnData_PooledZeroAlloc proves the per-DATA-frame payload copy is served
// from dataBufPool (0 heap allocs/op) once the consumer returns the buffer,
// replacing the previous `append([]byte(nil), p...)` that allocated every
// frame. GC is disabled for the measurement so a pool eviction cannot evict the
// warmed buffer mid-run and report a spurious alloc.
func TestOnData_PooledZeroAlloc(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	payload := make([]byte, 4096)
	fh := frame.FrameHeader{Type: frame.FrameData, Length: uint32(len(payload)), StreamID: 1}

	// Warm the pool so the buffer reaches the payload size before measuring.
	if err := h.OnData(fh, payload, 0); err != nil {
		t.Fatalf("OnData warmup: %v", err)
	}
	if ev := <-s.events; ev.DataSlab != nil {
		dataBufPool.Put(ev.DataSlab)
	}

	allocs := testing.AllocsPerRun(200, func() {
		_ = h.OnData(fh, payload, 0)
		ev := <-s.events // drain (mirrors a prompt consumer)
		_ = ev.Data      // payload available to the caller
		if ev.DataSlab != nil {
			dataBufPool.Put(ev.DataSlab) // consumer returns the pooled buffer
		}
	})
	if allocs != 0 {
		t.Fatalf("OnData data-copy allocs/op = %v, want 0 (pooled)", allocs)
	}
}

// TestOnData_DataMatchesPayload confirms the pooled copy delivers the exact
// payload bytes (and is decoupled from the framer's reused read buffer).
func TestOnData_DataMatchesPayload(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	payload := []byte("hello-poseidon-data-frame")
	fh := frame.FrameHeader{Type: frame.FrameData, Length: uint32(len(payload)), StreamID: 1}
	if err := h.OnData(fh, payload, 0); err != nil {
		t.Fatalf("OnData: %v", err)
	}
	ev := <-s.events
	if string(ev.Data) != string(payload) {
		t.Fatalf("Data = %q, want %q", ev.Data, payload)
	}
	// Mutating the caller's source buffer must not affect the delivered copy.
	for i := range payload {
		payload[i] = 0
	}
	if string(ev.Data) != "hello-poseidon-data-frame" {
		t.Fatalf("Data aliased the source buffer: %q", ev.Data)
	}
	if ev.DataSlab != nil {
		dataBufPool.Put(ev.DataSlab)
	}
}

// TestOnData_DistinctBuffersWhileOutstanding locks the core pooling-safety
// invariant: while a delivered EventData's buffer has NOT been returned, a
// subsequent OnData must Get a DISTINCT buffer — the pool must never hand back
// an outstanding one and corrupt the first event's Data.
func TestOnData_DistinctBuffersWhileOutstanding(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)

	fh := frame.FrameHeader{Type: frame.FrameData, Length: 4, StreamID: 1}
	if err := h.OnData(fh, []byte("AAAA"), 0); err != nil {
		t.Fatalf("OnData A: %v", err)
	}
	ev1 := <-s.events // hold ev1; deliberately do NOT return its DataSlab yet

	if err := h.OnData(fh, []byte("BBBB"), 0); err != nil {
		t.Fatalf("OnData B: %v", err)
	}
	ev2 := <-s.events

	if string(ev1.Data) != "AAAA" {
		t.Fatalf("ev1.Data corrupted by the second OnData: %q (pool handed back an outstanding buffer)", ev1.Data)
	}
	if string(ev2.Data) != "BBBB" {
		t.Fatalf("ev2.Data = %q, want BBBB", ev2.Data)
	}
	if ev1.DataSlab != nil {
		dataBufPool.Put(ev1.DataSlab)
	}
	if ev2.DataSlab != nil {
		dataBufPool.Put(ev2.DataSlab)
	}
}
