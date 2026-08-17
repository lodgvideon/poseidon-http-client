package http3

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// These tests exercise QPACK blocked streams (RFC 9204 §2.1.2 / §2.1.3, Q4): with
// SETTINGS_QPACK_BLOCKED_STREAMS > 0 a response field section may reference
// dynamic-table entries the server's encoder stream has not delivered yet — its
// Required Insert Count is ahead of our insert count — and the decode PARKS off
// qpackMu until the reader advances the insert count, rather than failing. The
// wait/wake is driven deterministically against the fake conn: the reader goroutine
// parks in Poll for a request-less connection, so readQPACKEncoder (which advances
// the insert count and broadcasts) is stepped directly, while a decode blocked in
// dispatchFrame parks on a real goroutine. The interop-level RIC-ahead scenario
// (a fault server sending a section before its encoder-stream insert) is a
// follow-up; these unit tests fully exercise the wait/wake, the ctx-bounded park,
// and the M-blocked bound.

// newBlockedClient builds a client with the dynamic table enabled (capacity 65536)
// and SETTINGS_QPACK_BLOCKED_STREAMS = maxBlocked over a bare fake conn. The reader
// goroutine parks in Poll, so the test owns when the encoder stream's inserts are
// applied (via readQPACKEncoder).
func newBlockedClient(t *testing.T, maxBlocked uint64) (*Client, *fakeConn) {
	t.Helper()
	conn := &fakeConn{}
	client, err := NewClientFake(conn, []Setting{
		{SettingQPACKMaxTableCapacity, 65536},
		{SettingQPACKBlockedStreams, maxBlocked},
	})
	require.NoError(t, err, "NewClientFake with the dynamic table and blocked streams enabled")
	t.Cleanup(func() { _ = client.Close() })
	return client, conn
}

// waitBlockedN spins until at least n decodes are registered as blocked on the
// insert count, so a test acts on a genuinely parked decode. Bounded so a stall
// (a blocked decode that never parked) fails loudly rather than hanging.
func waitBlockedN(t *testing.T, c *Client, n uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for c.qpackBlocked.Load() < n {
		require.Falsef(t, time.Now().After(deadline),
			"only %d of %d decodes blocked", c.qpackBlocked.Load(), n)
		time.Sleep(time.Millisecond)
	}
}

// dynSectionAt builds a response HEADERS field section that references the static
// :status 200 entry and the dynamic-table entry at absolute index abs, with
// Required Insert Count abs+1 and Base abs+1 (so the Base-relative index is 0). The
// encoded Required Insert Count (abs+2) resolves back to abs+1 for any table whose
// insert count is within its first wraparound window, which holds for every table
// these tests build (capacity 65536 → MaxEntries 2048). The section is decodable
// only once the insert count has reached abs+1, so a decode of it while the insert
// count is short blocks (RFC 9204 §2.1.3).
func dynSectionAt(abs uint64) []byte {
	ric := abs + 1
	s := hpack.EncodeInteger(nil, 8, 0x00, ric+1) // encoded Required Insert Count
	s = append(s, 0x00)                           // Base sign 0, delta 0 → Base = ric
	s = append(s, 0xD9)                           // Indexed Field Line, static, :status 200 (index 25)
	s = hpack.EncodeInteger(s, 6, 0x80, 0)        // Indexed Field Line, dynamic, Base-relative index 0 → abs
	return s
}

// TestConformance_RFC9204_Sec213_BlockedDecodeWaitsForInsert proves the core Q4
// behavior: a response HEADERS section whose Required Insert Count is ahead of the
// insert count parks the decode (it becomes a blocked stream) until the reader
// applies the encoder-stream insert that satisfies it, then the decode unblocks and
// succeeds (RFC 9204 §2.1.3). Run -race -count=5: the reader advances the table
// under qpackMu.Lock while the parked decode wakes and resolves under qpackMu.RLock.
func TestConformance_RFC9204_Sec213_BlockedDecodeWaitsForInsert(t *testing.T) {
	client, conn := newBlockedClient(t, 16)
	// Encoder stream: Set Capacity 65536, Insert "x-dyn: yes" (abs 0). NOT applied
	// yet, so the insert count is 0 and dynSectionAt(0) (Required Insert Count 1) is
	// blocked.
	chunk := appendSetCapacity(nil, 65536)
	chunk = appendInsertLiteral(chunk, "x-dyn", "yes")
	enc := &fakeStream{id: 7, conn: conn, directRead: true, recvChunks: [][]byte{chunk}, ready: make(chan struct{}, 1)}
	client.qpackEnc = enc
	var dec qpack.Decoder
	rb := respBuilder{dec: &dec, streamID: 4, ctx: context.Background()}
	done := make(chan error, 1)
	go func() { done <- client.dispatchFrame(&rb, FrameHeaders, dynSectionAt(0)) }()
	waitBlockedN(t, client, 1) // the decode has genuinely parked as a blocked stream

	// The reader delivers the promised insert: insert count 0 → 1, then broadcast.
	rerr := client.readQPACKEncoder()

	require.NoErrorf(t, rerr, "readQPACKEncoder: %v", rerr)
	select {
	case err := <-done:
		require.NoErrorf(t, err, "blocked decode did not resolve: %v", err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "blocked decode did not unblock after the insert arrived (lost wake?)")
	}
	require.NotNil(t, rb.resp, "the resolved decode produced no response at all")
	assert.Equalf(t, 200, rb.resp.Status, "resp = %+v, want status 200", rb.resp)
	assert.Truef(t, hasHeader(rb.resp.Headers, "x-dyn", "yes"),
		"resp = %+v, want x-dyn:yes resolved from the dynamic table", rb.resp)
	assert.True(t, rb.refDynamic, "refDynamic must be set for a section that referenced the dynamic table")
	assert.Equalf(t, uint64(0), client.qpackBlocked.Load(),
		"blocked-stream count = %d, want 0 after the decode resolved", client.qpackBlocked.Load())
}

// TestConformance_RFC9204_Sec213_BlockedDecodeCtxTimeout proves the no-hang
// guarantee: a decode blocked on a Required Insert Count the encoder never
// satisfies must fail on the request context's deadline, not wait forever (RFC 9204
// §2.1.3). No insert is ever applied, so the section stays blocked; the overall test
// timeout would catch a true hang.
func TestConformance_RFC9204_Sec213_BlockedDecodeCtxTimeout(t *testing.T) {
	client, _ := newBlockedClient(t, 16)
	// The promised insert never arrives (no encoder stream is applied), so the decode
	// blocks indefinitely on Required Insert Count 1 unless the ctx bounds it.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var dec qpack.Decoder
	rb := respBuilder{dec: &dec, streamID: 4, ctx: ctx}
	done := make(chan error, 1)

	go func() { done <- client.dispatchFrame(&rb, FrameHeaders, dynSectionAt(0)) }()

	select {
	case err := <-done:
		assert.ErrorIsf(t, err, context.DeadlineExceeded,
			"blocked decode err = %v, want context.DeadlineExceeded", err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "blocked decode hung: it did not return on the ctx deadline")
	}
	assert.Equalf(t, uint64(0), client.qpackBlocked.Load(),
		"blocked-stream count = %d, want 0 after the blocked decode gave up", client.qpackBlocked.Load())
}

// TestConformance_RFC9204_Sec212_BlockedStreamLimitEnforced proves the M-blocked
// bound (RFC 9204 §2.1.2): with SETTINGS_QPACK_BLOCKED_STREAMS = 2, two decodes may
// block at once, but a third blocked section exceeds the limit the encoder must
// respect, so it is a QPACK_DECOMPRESSION_FAILED connection error rather than a
// third park.
func TestConformance_RFC9204_Sec212_BlockedStreamLimitEnforced(t *testing.T) {
	const maxBlocked = 2
	client, conn := newBlockedClient(t, maxBlocked)
	// Two decodes block on Required Insert Count 1 (insert count 0, no encoder applied),
	// filling both blocked-stream slots. They wake and exit only when Close tears the
	// connection down at the end.
	var wg sync.WaitGroup
	for i := 0; i < maxBlocked; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var dec qpack.Decoder
			rb := respBuilder{dec: &dec, streamID: uint64(4 * (i + 1)), ctx: context.Background()}
			_ = client.dispatchFrame(&rb, FrameHeaders, dynSectionAt(0))
		}(i)
	}
	waitBlockedN(t, client, maxBlocked)

	// A third blocked section would push past the advertised limit.
	var dec qpack.Decoder
	rb := respBuilder{dec: &dec, streamID: 99, ctx: context.Background()}
	err := client.dispatchFrame(&rb, FrameHeaders, dynSectionAt(0))

	assert.ErrorIsf(t, err, ErrH3Control,
		"over-limit blocked section = %v, want ErrH3Control (QPACK_DECOMPRESSION_FAILED)", err)
	assert.Equalf(t, H3QpackDecompressionFailed, conn.closeCode,
		"close code = %#x, want QPACK_DECOMPRESSION_FAILED (%#x)", conn.closeCode, H3QpackDecompressionFailed)

	// The connError closed the connection; connCtx is cancelled (by the reader's fatal
	// and by Close), waking the two parked decodes so their goroutines exit — no hang.
	_ = client.Close()
	wg.Wait()
}

// TestConcurrent_QPACKBlockedStreams_UnderRace is the Q4 acid test: many decoders,
// each referencing a different dynamic entry (so heterogeneous Required Insert
// Counts), run concurrently while the reader delivers the encoder-stream inserts one
// at a time. Some decodes are already satisfiable, most block and must wake as the
// reader advances the insert count and broadcasts — a single advance can unblock any
// number of parked decodes. A lost wake would hang a decode; a missing lock, or a
// copy that outlives its RLock while an insert rewrites the arena, trips -race. Every
// decode must resolve to its OWN entry. Run -race -count=5. M is set well above the
// decoder count so this test exercises the wait/wake, not the M-blocked bound.
func TestConcurrent_QPACKBlockedStreams_UnderRace(t *testing.T) {
	client, conn := newBlockedClient(t, 256)
	const entries = 6
	// Encoder stream: chunk 0 sets a large capacity and inserts abs 0; each later chunk
	// inserts one more entry, applied one per reader pass so the insert count climbs
	// 1..entries. Each entry has a distinct name/value for cross-talk detection.
	chunks := make([][]byte, entries)
	first := appendSetCapacity(nil, 65536)
	first = appendInsertLiteral(first, "x-dyn-0", "v0")
	chunks[0] = first
	for i := 1; i < entries; i++ {
		chunks[i] = appendInsertLiteral(nil, "x-dyn-"+strconv.Itoa(i), "v"+strconv.Itoa(i))
	}
	enc := &fakeStream{id: 7, conn: conn, directRead: true, recvChunks: chunks, ready: make(chan struct{}, 1)}
	client.qpackEnc = enc
	const perEntry = 10
	errCh := make(chan error, entries*perEntry)

	var wg sync.WaitGroup
	// The reader: apply the inserts one at a time, advancing the insert count and
	// broadcasting, with a stagger so the decodes genuinely park and wake.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < entries; i++ {
			if err := client.readQPACKEncoder(); err != nil {
				assert.NoErrorf(t, err, "readQPACKEncoder: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	for abs := 0; abs < entries; abs++ {
		for k := 0; k < perEntry; k++ {
			wg.Add(1)
			go func(abs, k int) {
				defer wg.Done()
				var dec qpack.Decoder
				rb := respBuilder{dec: &dec, streamID: uint64(4 * (abs*perEntry + k + 1)), ctx: context.Background()}
				if err := client.dispatchFrame(&rb, FrameHeaders, dynSectionAt(uint64(abs))); err != nil {
					errCh <- fmt.Errorf("abs %d: dispatchFrame: %w", abs, err)
					return
				}
				name, val := "x-dyn-"+strconv.Itoa(abs), "v"+strconv.Itoa(abs)
				if rb.resp == nil || !hasHeader(rb.resp.Headers, name, val) {
					errCh <- fmt.Errorf("abs %d decoded wrong entry: %+v (QPACK cross-talk?)", abs, rb.resp)
				}
			}(abs, k)
		}
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		assert.NoError(t, err)
	}
	assert.Equalf(t, uint64(0), client.qpackBlocked.Load(),
		"blocked-stream count = %d, want 0 after every decode resolved", client.qpackBlocked.Load())
	assert.Equalf(t, uint64(entries), client.qpackDyn.InsertCount(),
		"final InsertCount = %d, want %d", client.qpackDyn.InsertCount(), entries)
}
