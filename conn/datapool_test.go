package conn

import (
	"runtime/debug"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/stretchr/testify/require"
)

// TestOnData_PooledZeroAlloc proves the per-DATA-frame payload copy is served
// from dataBufPool (0 heap allocs/op) once the consumer returns the buffer,
// replacing the previous `append([]byte(nil), p...)` that allocated every
// frame. GC is disabled for the measurement so a pool eviction cannot evict the
// warmed buffer mid-run and report a spurious alloc.
//
// Nothing from testify may appear inside the measured closure: it reflects and
// allocates, and AllocsPerRun would count that instead of the path under test.
func TestOnData_PooledZeroAlloc(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)
	s.headersReceived = true // response HEADERS received; DATA is valid body
	payload := make([]byte, 4096)
	fh := frame.FrameHeader{Type: frame.FrameData, Length: uint32(len(payload)), StreamID: 1}
	// Warm the pool so the buffer reaches the payload size before measuring.
	require.NoErrorf(t, h.OnData(fh, payload, 0), "OnData warmup")
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

	require.Zerof(t, allocs, "OnData data-copy allocs/op = %v, want 0 (pooled)", allocs)
}

// TestOnData_DataMatchesPayload confirms the pooled copy delivers the exact
// payload bytes (and is decoupled from the framer's reused read buffer).
func TestOnData_DataMatchesPayload(t *testing.T) {
	m := newFakeStreamMap()
	h := newConnHandler(m, hpack.NewDecoder())
	s := m.addStream(1)
	s.headersReceived = true // response HEADERS received; DATA is valid body
	payload := []byte("hello-poseidon-data-frame")
	fh := frame.FrameHeader{Type: frame.FrameData, Length: uint32(len(payload)), StreamID: 1}

	require.NoErrorf(t, h.OnData(fh, payload, 0), "OnData")

	ev := <-s.events
	require.Equalf(t, string(payload), string(ev.Data), "Data = %q, want %q", ev.Data, payload)
	// Mutating the caller's source buffer must not affect the delivered copy.
	for i := range payload {
		payload[i] = 0
	}
	require.Equalf(t, "hello-poseidon-data-frame", string(ev.Data),
		"Data aliased the source buffer: %q", ev.Data)
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
	s.headersReceived = true // response HEADERS received; DATA is valid body
	fh := frame.FrameHeader{Type: frame.FrameData, Length: 4, StreamID: 1}

	require.NoErrorf(t, h.OnData(fh, []byte("AAAA"), 0), "OnData A")
	ev1 := <-s.events // hold ev1; deliberately do NOT return its DataSlab yet
	require.NoErrorf(t, h.OnData(fh, []byte("BBBB"), 0), "OnData B")
	ev2 := <-s.events

	require.Equalf(t, "AAAA", string(ev1.Data),
		"ev1.Data corrupted by the second OnData: %q (pool handed back an outstanding buffer)", ev1.Data)
	require.Equalf(t, "BBBB", string(ev2.Data), "ev2.Data = %q, want BBBB", ev2.Data)
	if ev1.DataSlab != nil {
		dataBufPool.Put(ev1.DataSlab)
	}
	if ev2.DataSlab != nil {
		dataBufPool.Put(ev2.DataSlab)
	}
}
