package http3

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// These tests exercise the live QPACK dynamic table (RFC 9204 Q2 wiring + Q3
// flip). The production client now advertises a non-zero
// SETTINGS_QPACK_MAX_TABLE_CAPACITY (dial.go), so a real server inserts entries on
// its encoder stream and references them from response field sections. The tests
// drive that path deterministically against the fake conn at a non-zero capacity:
// the reader goroutine parks in Poll for the request-less connection (an empty req
// stream), so serviceControl and dispatchFrame are stepped directly — the same
// pattern the control tests use — with no goroutine racing the manual steps. The
// concurrent path is covered by TestConcurrent_QPACKDynamicTable_UnderRace.

// appendSetCapacity builds a Set Dynamic Table Capacity encoder instruction
// (RFC 9204 §4.3.1): the 001 pattern with a 5-bit prefix integer.
func appendSetCapacity(dst []byte, capacity uint64) []byte {
	return hpack.EncodeInteger(dst, 5, 0x20, capacity)
}

// appendInsertLiteral builds an Insert With Literal Name encoder instruction
// (RFC 9204 §4.3.3) with raw (non-Huffman) name and value literals: the 01
// pattern with a 5-bit name length, then a 7-bit value length.
func appendInsertLiteral(dst []byte, name, value string) []byte {
	dst = hpack.EncodeInteger(dst, 5, 0x40, uint64(len(name))) // 01, H=0
	dst = append(dst, name...)
	dst = hpack.EncodeInteger(dst, 7, 0x00, uint64(len(value))) // H=0
	return append(dst, value...)
}

// dynRefSection builds a response HEADERS field section whose Required Insert
// Count is 1 and Base is 1, indexing the static :status 200 entry and the dynamic
// entry at absolute index 0. The encoded Required Insert Count (2) resolves to 1
// for any table whose insert count is in its first wraparound window
// (< MaxEntries), which holds for every table these tests build.
func dynRefSection() []byte {
	return []byte{
		0x02, // encoded Required Insert Count = 2 → Required Insert Count 1
		0x00, // Base sign 0, delta 0 → Base = 1
		0xD9, // Indexed Field Line, static, index 25 (:status 200)
		0x80, // Indexed Field Line, dynamic, Base-relative index 0 → absolute 0
	}
}

// qpackServerEncoderStream frames a server QPACK encoder stream: the 0x02
// stream-type varint followed by encoder instructions that set a capacity and
// insert a single "x-dyn: yes" entry.
func qpackServerEncoderStream(capacity uint64) []byte {
	instr := appendSetCapacity(nil, capacity)
	instr = appendInsertLiteral(instr, "x-dyn", "yes")
	return append(appendV(nil, StreamTypeQPACKEncoder), instr...)
}

// TestConformance_RFC9204_Sec43_EncoderInstructionsApplied checks that the reader
// applies the server's encoder-stream instructions to the shared dynamic table,
// publishes the advanced insert count, and emits an Insert Count Increment on our
// decoder stream (RFC 9204 §4.3, §4.4.3).
func TestConformance_RFC9204_Sec43_EncoderInstructionsApplied(t *testing.T) {
	enc := &fakeStream{id: 7, recvChunks: [][]byte{qpackServerEncoderStream(256)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{enc}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 256}})
	require.NoError(t, err, "NewClientFake with a non-zero advertised table capacity")

	// The reader parks in Poll for a request-less connection, so drive one service
	// pass directly (as the control tests do) to peel the encoder stream and apply
	// its instructions.
	serr := client.serviceControl()

	require.NoErrorf(t, serr, "serviceControl: %v", serr)
	assert.Equalf(t, uint64(1), client.qpackDyn.InsertCount(),
		"dynamic table InsertCount = %d, want 1", client.qpackDyn.InsertCount())
	assert.Equalf(t, uint64(1), client.insertCount.Load(),
		"published insertCount = %d, want 1: a decode reads the atomic, not the table", client.insertCount.Load())
	// The decoder stream opens with its type byte (0x03), then the ICI.
	sent := conn.clientQDec.sent
	require.GreaterOrEqualf(t, len(sent), 2,
		"decoder stream must open with its type byte then an ICI: %x", sent)
	require.Equalf(t, byte(StreamTypeQPACKDecoder), sent[0],
		"decoder stream must open with its type byte then an ICI: %x", sent)
	require.Equalf(t, byte(0x00), sent[1]&0xc0, // Insert Count Increment is 00xxxxxx (§4.4.3)
		"instruction %#x is not an Insert Count Increment", sent[1])
	inc, n, derr := hpack.DecodeInteger(sent[1:], 6)
	require.NoErrorf(t, derr, "Insert Count Increment does not decode: %x (%v)", sent[1:], derr)
	require.NotZerof(t, n, "Insert Count Increment does not decode: %x", sent[1:])
	assert.Equalf(t, uint64(1), inc, "Insert Count Increment = %d, want 1", inc)
}

// TestConformance_RFC9204_Sec441_SectionAcknowledgment checks that a HEADERS field
// section referencing a dynamic-table entry decodes correctly against the shared
// table and that a Section Acknowledgment for the stream is emitted on our decoder
// stream (RFC 9204 §2.1.4, §4.4.1).
func TestConformance_RFC9204_Sec441_SectionAcknowledgment(t *testing.T) {
	enc := &fakeStream{id: 7, recvChunks: [][]byte{qpackServerEncoderStream(256)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{enc}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 256}})
	require.NoError(t, err, "NewClientFake with a non-zero advertised table capacity")
	require.NoError(t, client.serviceControl(), "serviceControl over the server encoder stream")
	before := len(conn.clientQDec.sent) // type byte + the ICI from the insert
	var dec qpack.Decoder
	rb := respBuilder{dec: &dec, streamID: 4}

	derr := client.dispatchFrame(&rb, FrameHeaders, dynRefSection())

	require.NoErrorf(t, derr, "dispatchFrame: %v", derr)
	require.NotNil(t, rb.resp, "the decode produced no response at all")
	assert.Equalf(t, 200, rb.resp.Status, "resp = %+v, want status 200", rb.resp)
	require.Lenf(t, rb.resp.Headers, 1, "headers = %+v, want the dynamic-table x-dyn: yes", rb.resp.Headers)
	assert.Equalf(t, "x-dyn", string(rb.resp.Headers[0].Name),
		"headers = %+v, want the dynamic-table x-dyn: yes", rb.resp.Headers)
	assert.Equalf(t, "yes", string(rb.resp.Headers[0].Value),
		"headers = %+v, want the dynamic-table x-dyn: yes", rb.resp.Headers)
	assert.True(t, rb.refDynamic, "refDynamic must be set for a section that referenced the dynamic table")
	ack := conn.clientQDec.sent[before:]
	require.NotEmptyf(t, ack, "expected a Section Acknowledgment (1xxxxxxx), got %x", ack)
	require.NotZerof(t, ack[0]&0x80, // Section Acknowledgment is 1xxxxxxx (§4.4.1)
		"expected a Section Acknowledgment (1xxxxxxx), got %x", ack)
	id, n, aerr := hpack.DecodeInteger(ack, 7)
	require.NoErrorf(t, aerr, "Section Acknowledgment stream id = %d (n=%d,%v), want 4", id, n, aerr)
	assert.Equalf(t, len(ack), n, "Section Acknowledgment = %x: it must consume the whole instruction", ack)
	assert.Equalf(t, uint64(4), id, "Section Acknowledgment stream id = %d, want 4", id)
}

// TestConformance_RFC9204_Sec442_StreamCancellationOnAbort checks that aborting a
// stream that referenced the dynamic table emits a Stream Cancellation for that
// stream on our decoder stream (RFC 9204 §4.4.2), alongside the QUIC reset.
func TestConformance_RFC9204_Sec442_StreamCancellationOnAbort(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 256}})
	require.NoError(t, err, "NewClientFake with a non-zero advertised table capacity")
	before := len(conn.clientQDec.sent) // just the type byte 0x03
	stream := &fakeStream{id: 8, conn: conn, ready: make(chan struct{}, 1)}
	br := &BodyReader{c: client, stream: stream, rb: respBuilder{refDynamic: true, streamID: 8}}

	br.abort(context.Canceled)

	cancel := conn.clientQDec.sent[before:]
	require.NotEmptyf(t, cancel, "expected a Stream Cancellation (01xxxxxx), got %x", cancel)
	require.Equalf(t, byte(0x40), cancel[0]&0xc0, // Stream Cancellation is 01xxxxxx (§4.4.2)
		"expected a Stream Cancellation (01xxxxxx), got %x", cancel)
	id, n, derr := hpack.DecodeInteger(cancel, 6)
	require.NoErrorf(t, derr, "Stream Cancellation stream id = %d (n=%d,%v), want 8", id, n, derr)
	assert.Equalf(t, len(cancel), n, "Stream Cancellation = %x: it must consume the whole instruction", cancel)
	assert.Equalf(t, uint64(8), id, "Stream Cancellation stream id = %d, want 8", id)
	assert.True(t, stream.reset && stream.stopped,
		"abort must also reset the request stream (STOP_SENDING + RESET_STREAM)")
}

// TestConcurrent_QPACKDynamicTable_UnderRace is the QPACK acid test: the reader
// goroutine keeps inserting into the shared dynamic table under qpackMu.Lock while
// N decoders resolve dynamic references under qpackMu.RLock and copy the emitted
// fields. A missing lock, or a copy that outlives the RLock while an insert
// rewrites the arena, trips -race here. Run with -race -count=5.
func TestConcurrent_QPACKDynamicTable_UnderRace(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 65536}})
	require.NoError(t, err, "NewClientFake with a non-zero advertised table capacity")
	defer client.Close()
	// A server encoder stream fed one instruction chunk per Recv: chunk 0 sets a
	// large capacity and inserts the stable entry at absolute index 0; the rest add
	// fresh entries. The capacity is far larger than the working set, so index 0 is
	// never evicted and the insert count stays inside the first wraparound window.
	const inserts = 100
	chunks := make([][]byte, 0, inserts+1)
	first := appendSetCapacity(nil, 65536)
	first = appendInsertLiteral(first, "x-dyn", "yes")
	chunks = append(chunks, first)
	for i := 0; i < inserts; i++ {
		chunks = append(chunks, appendInsertLiteral(nil, "x-fill-"+strconv.Itoa(i), "v"))
	}
	enc := &fakeStream{id: 7, conn: conn, directRead: true, recvChunks: chunks, ready: make(chan struct{}, 1)}
	client.qpackEnc = enc
	// Establish index 0 before the concurrent phase so every decode resolves it.
	require.NoError(t, client.readQPACKEncoder(), "initial readQPACKEncoder")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the reader: apply the remaining inserts under qpackMu.Lock
		defer wg.Done()
		for i := 0; i < inserts; i++ {
			if err := client.readQPACKEncoder(); err != nil {
				assert.NoErrorf(t, err, "readQPACKEncoder: %v", err)
				return
			}
		}
	}()
	const readers = 8
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) { // a decoder: resolve the dynamic reference under qpackMu.RLock
			defer wg.Done()
			var dec qpack.Decoder
			for i := 0; i < 200; i++ {
				rb := respBuilder{dec: &dec, streamID: uint64(4 * (r + 1))}
				if err := client.dispatchFrame(&rb, FrameHeaders, dynRefSection()); err != nil {
					assert.NoErrorf(t, err, "dispatchFrame: %v", err)
					return
				}
				if rb.resp == nil || len(rb.resp.Headers) != 1 ||
					string(rb.resp.Headers[0].Value) != "yes" {
					assert.Failf(t, "dynamic decode returned the wrong entry",
						"dynamic decode returned wrong entry: %+v", rb.resp)
					return
				}
			}
		}(r)
	}
	wg.Wait()

	assert.Equalf(t, uint64(inserts+1), client.qpackDyn.InsertCount(),
		"final InsertCount = %d, want %d", client.qpackDyn.InsertCount(), inserts+1)
}

// --- Q3: the dynamic table is LIVE (dial.go advertises a non-zero capacity). ---

// hasHeader reports whether fields contains a name/value pair.
func hasHeader(fields []hpack.HeaderField, name, value string) bool {
	for _, f := range fields {
		if string(f.Name) == name && string(f.Value) == value {
			return true
		}
	}
	return false
}

// assertSectionAck checks that instr is exactly a Section Acknowledgment (RFC 9204
// §4.4.1, 1xxxxxxx) for streamID.
func assertSectionAck(t *testing.T, instr []byte, streamID uint64) {
	t.Helper()
	require.NotEmptyf(t, instr, "want a Section Acknowledgment (1xxxxxxx) for stream %d, got %x", streamID, instr)
	require.NotZerof(t, instr[0]&0x80,
		"want a Section Acknowledgment (1xxxxxxx) for stream %d, got %x", streamID, instr)
	id, n, err := hpack.DecodeInteger(instr, 7)
	require.NoErrorf(t, err, "Section Acknowledgment = %x (id=%d n=%d err=%v), want stream %d",
		instr, id, n, err, streamID)
	assert.Equalf(t, len(instr), n, "Section Acknowledgment = %x: it must consume the whole instruction", instr)
	assert.Equalf(t, streamID, id, "Section Acknowledgment = %x (id=%d), want stream %d", instr, id, streamID)
}

// newLiveClient builds a client with the dynamic table enabled over a bare fake
// conn (no request stream), for driving the decoder-instruction helpers directly.
// The reader goroutine parks in Poll, so the test owns every decoder-stream write.
func newLiveClient(t *testing.T) (*Client, *fakeConn) {
	t.Helper()
	conn := &fakeConn{}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 4096}})
	require.NoError(t, err, "NewClientFake with the dynamic table enabled")
	t.Cleanup(func() { _ = client.Close() })
	return client, conn
}

// TestConformance_RFC9204_Sec321_DynamicTableCapacityHonored drives a live
// insert→reference→decode→Section-Ack→ICI round-trip at capacity 4096: the server
// encoder stream inserts two entries, then response HEADERS reference them by every
// dynamic form — Base-relative indexed, Base-relative name reference, and post-Base
// indexed. Each section decodes against the shared table, and the decoder-stream
// instructions carry the right Known Received Count: one Insert Count Increment of
// +2 for the inserts, then a Section Acknowledgment per stream that does not
// re-count the already-acknowledged inserts (RFC 9204 §3.2.1, §2.1.4, §4.4.1,
// §4.4.3). Run -race -count=5.
func TestConformance_RFC9204_Sec321_DynamicTableCapacityHonored(t *testing.T) {
	const capacity = 4096
	// Encoder stream: Set Capacity 4096, insert "x-alpha: 1" (abs 0), "x-beta: 2" (abs 1).
	instr := appendSetCapacity(nil, capacity)
	instr = appendInsertLiteral(instr, "x-alpha", "1")
	instr = appendInsertLiteral(instr, "x-beta", "2")
	encStream := append(appendV(nil, StreamTypeQPACKEncoder), instr...)
	enc := &fakeStream{id: 7, recvChunks: [][]byte{encStream}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{enc}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, capacity}})
	require.NoError(t, err, "NewClientFake at capacity 4096")
	defer client.Close()

	serr := client.serviceControl()

	require.NoErrorf(t, serr, "serviceControl: %v", serr)
	assert.Equalf(t, uint64(2), client.qpackDyn.InsertCount(),
		"InsertCount = %d, want 2", client.qpackDyn.InsertCount())
	assert.Equalf(t, uint64(2), client.knownReceivedCount,
		"knownReceivedCount after inserts = %d, want 2", client.knownReceivedCount)
	// Decoder stream: type byte 0x03 then an Insert Count Increment of +2.
	sent := conn.clientQDec.sent
	require.Lenf(t, sent, 2, "decoder stream = %x, want [03, ICI]", sent)
	require.Equalf(t, byte(StreamTypeQPACKDecoder), sent[0], "decoder stream = %x, want [03, ICI]", sent)
	inc, n, ierr := hpack.DecodeInteger(sent[1:], 6)
	require.NoErrorf(t, ierr, "ICI = %x (inc=%d), want an Insert Count Increment of +2", sent[1:], inc)
	assert.Equalf(t, 1, n, "ICI = %x, want a single-octet Insert Count Increment", sent[1:])
	assert.Equalf(t, uint64(2), inc, "ICI = %x (inc=%d), want an Insert Count Increment of +2", sent[1:], inc)
	assert.Equalf(t, byte(0x00), sent[1]&0xc0, "ICI = %x is not an Insert Count Increment (00xxxxxx)", sent[1:])

	// Section A (stream 4): Base-relative indexed + name reference, RIC=2 Base=2.
	//   03         encoded RIC 3 → RIC 2
	//   00         Base sign 0 delta 0 → Base 2
	//   D9         Indexed static :status 200
	//   80         Indexed dynamic, Base-relative index 0 → abs 1 → "x-beta: 2"
	//   41 01 '9'  Literal, dynamic name reference index 1 → abs 0 name "x-alpha", value "9"
	sectionA := []byte{0x03, 0x00, 0xD9, 0x80, 0x41, 0x01, '9'}
	before := len(conn.clientQDec.sent)
	var decA qpack.Decoder
	rbA := respBuilder{dec: &decA, streamID: 4}

	aerr := client.dispatchFrame(&rbA, FrameHeaders, sectionA)

	require.NoErrorf(t, aerr, "dispatchFrame A: %v", aerr)
	require.NotNil(t, rbA.resp, "section A produced no response at all")
	assert.Equalf(t, 200, rbA.resp.Status, "A resp = %+v, want status 200", rbA.resp)
	assert.True(t, rbA.refDynamic, "A refDynamic must be set: the section referenced the dynamic table")
	assert.Truef(t, hasHeader(rbA.resp.Headers, "x-beta", "2"),
		"A headers = %+v, want x-beta:2 (indexed)", rbA.resp.Headers)
	assert.Truef(t, hasHeader(rbA.resp.Headers, "x-alpha", "9"),
		"A headers = %+v, want x-alpha:9 (name-ref)", rbA.resp.Headers)
	assertSectionAck(t, conn.clientQDec.sent[before:], 4)
	assert.Equalf(t, uint64(2), client.knownReceivedCount,
		"KRC after Section Ack A = %d, want 2 (RIC 2 already known — no re-count)", client.knownReceivedCount)

	// Section B (stream 8): post-Base indexed, RIC=2 Base=0.
	//   03  encoded RIC 3 → RIC 2
	//   81  Base sign 1 delta 1 → Base = 2-1-1 = 0
	//   D9  :status 200
	//   10  Post-Base index 0 → abs 0 → "x-alpha: 1"
	//   11  Post-Base index 1 → abs 1 → "x-beta: 2"
	sectionB := []byte{0x03, 0x81, 0xD9, 0x10, 0x11}
	before = len(conn.clientQDec.sent)
	var decB qpack.Decoder
	rbB := respBuilder{dec: &decB, streamID: 8}

	berr := client.dispatchFrame(&rbB, FrameHeaders, sectionB)

	require.NoErrorf(t, berr, "dispatchFrame B: %v", berr)
	require.NotNil(t, rbB.resp, "section B produced no response at all")
	assert.Truef(t, hasHeader(rbB.resp.Headers, "x-alpha", "1"),
		"B headers = %+v, want x-alpha:1 (post-Base)", rbB.resp.Headers)
	assert.Truef(t, hasHeader(rbB.resp.Headers, "x-beta", "2"),
		"B headers = %+v, want x-beta:2 (post-Base)", rbB.resp.Headers)
	assertSectionAck(t, conn.clientQDec.sent[before:], 8)
	assert.Equalf(t, uint64(2), client.knownReceivedCount, "final KRC = %d, want 2", client.knownReceivedCount)
}

// TestConformance_RFC9204_Sec214_KnownReceivedCountCoordination proves the two
// decoder instructions that advance the encoder's Known Received Count never
// double-count the same insert (RFC 9204 §2.1.4). An Insert Count Increment reports
// only inserts past the Known Received Count, and a Section Acknowledgment
// implicitly advances it to the section's Required Insert Count — so whichever
// reaches the encoder first, the total advance equals the actual insert count. A
// double-count would push the encoder's Known Received Count past the inserts it
// made, a QPACK_DECODER_STREAM_ERROR.
func TestConformance_RFC9204_Sec214_KnownReceivedCountCoordination(t *testing.T) {
	// Normal order: the reader's Insert Count Increment lands first; the Section
	// Acknowledgment for a section with the same Required Insert Count adds nothing.
	t.Run("ICI_then_SectionAck", func(t *testing.T) {
		client, conn := newLiveClient(t)
		before := len(conn.clientQDec.sent)

		client.qpackAckInserts(1)    // reader applied insert 0 → ICI +1, KRC 1
		client.qpackSectionAck(4, 1) // section RIC 1 → Section Ack, KRC unchanged

		assert.Equalf(t, uint64(1), client.knownReceivedCount, "KRC = %d, want 1", client.knownReceivedCount)
		out := conn.clientQDec.sent[before:]
		assert.Equalf(t, []byte{0x01, 0x84}, out, // ICI(+1)=01, Section Ack(4)=84
			"decoder stream = %x, want ICI(+1) then Section Ack(4)", out)
	})

	// Race order: the Section Acknowledgment lands first (KRC → 1), so the reader's
	// later Insert Count Increment for the same insert is SUPPRESSED — no double count.
	t.Run("SectionAck_then_ICI", func(t *testing.T) {
		client, conn := newLiveClient(t)
		before := len(conn.clientQDec.sent)

		client.qpackSectionAck(4, 1) // section RIC 1 first → Section Ack, KRC 1
		client.qpackAckInserts(1)    // insert 0 already known-received → no ICI

		assert.Equalf(t, uint64(1), client.knownReceivedCount, "KRC = %d, want 1", client.knownReceivedCount)
		out := conn.clientQDec.sent[before:]
		assert.Equalf(t, []byte{0x84}, out, // only the Section Ack; the ICI is suppressed
			"decoder stream = %x, want only Section Ack(4) — the redundant ICI must be suppressed", out)
	})
}

// TestConformance_RFC9204_Sec322_EvictionAndEvictedReference drives the table past
// its capacity so the oldest entry is evicted (RFC 9204 §3.2.2), then checks a
// reference to a still-live entry decodes while a reference to the evicted entry is
// rejected as QPACK_DECOMPRESSION_FAILED — eviction accounting stays consistent
// with the insert count.
func TestConformance_RFC9204_Sec322_EvictionAndEvictedReference(t *testing.T) {
	const capacity = 128 // each "x-fill-N: v" is 41 bytes → 3 fit; the 4th evicts abs 0
	instr := appendSetCapacity(nil, capacity)
	for i := 0; i < 4; i++ {
		instr = appendInsertLiteral(instr, "x-fill-"+strconv.Itoa(i), "v")
	}
	encStream := append(appendV(nil, StreamTypeQPACKEncoder), instr...)
	enc := &fakeStream{id: 7, recvChunks: [][]byte{encStream}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{enc}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, capacity}})
	require.NoError(t, err, "NewClientFake at capacity 128")
	defer client.Close()
	require.NoError(t, client.serviceControl(), "serviceControl over the server encoder stream")
	require.Equalf(t, uint64(4), client.qpackDyn.InsertCount(),
		"InsertCount = %d, want 4", client.qpackDyn.InsertCount())
	require.Equalf(t, 3, client.qpackDyn.Len(),
		"live entries = %d, want 3 (abs 0 evicted)", client.qpackDyn.Len())
	// A reference to a live entry (abs 1, "x-fill-1: v") decodes. RIC=4 Base=4;
	// Base-relative index Base-abs-1 = 2 → 0x82.
	valid := []byte{0x05, 0x00, 0xD9, 0x82}
	var decV qpack.Decoder
	rbV := respBuilder{dec: &decV, streamID: 4}
	// A reference to the EVICTED entry (abs 0) must fail. Base-relative index
	// Base-abs-1 = 3 → 0x83; abs 0 is below the eviction threshold.
	evicted := []byte{0x05, 0x00, 0xD9, 0x83}
	var decE qpack.Decoder
	rbE := respBuilder{dec: &decE, streamID: 8}

	liveErr := client.dispatchFrame(&rbV, FrameHeaders, valid)
	evictedErr := client.dispatchFrame(&rbE, FrameHeaders, evicted)

	require.NoErrorf(t, liveErr, "live-entry reference must decode, got %v", liveErr)
	require.NotNil(t, rbV.resp, "the live-entry decode produced no response at all")
	assert.Truef(t, hasHeader(rbV.resp.Headers, "x-fill-1", "v"),
		"live headers = %+v, want x-fill-1:v", rbV.resp.Headers)
	assert.ErrorIsf(t, evictedErr, ErrH3Control,
		"evicted-entry reference = %v, want ErrH3Control (QPACK_DECOMPRESSION_FAILED)", evictedErr)
	assert.Equalf(t, H3QpackDecompressionFailed, conn.closeCode,
		"close code = %#x, want QPACK_DECOMPRESSION_FAILED (%#x)", conn.closeCode, H3QpackDecompressionFailed)
}

// TestConformance_RFC9204_Sec451_RequiredInsertCountExceedsInserts proves that at
// SETTINGS_QPACK_BLOCKED_STREAMS=0 a section whose Required Insert Count exceeds the
// entries actually inserted is rejected immediately as QPACK_DECOMPRESSION_FAILED —
// the decoder never blocks waiting for the encoder stream to catch up, so a
// misbehaving server cannot hang a Do (RFC 9204 §4.5.1). The section here has RIC 2
// against an insert count of 1.
func TestConformance_RFC9204_Sec451_RequiredInsertCountExceedsInserts(t *testing.T) {
	instr := appendSetCapacity(nil, 4096)
	instr = appendInsertLiteral(instr, "x-solo", "1") // abs 0 → insert count 1
	encStream := append(appendV(nil, StreamTypeQPACKEncoder), instr...)
	enc := &fakeStream{id: 7, recvChunks: [][]byte{encStream}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{enc}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 4096}})
	require.NoError(t, err, "NewClientFake at capacity 4096")
	defer client.Close()
	require.NoError(t, client.serviceControl(), "serviceControl over the server encoder stream")
	// encoded RIC 3 → RIC 2, Base 2, then :status; the RIC gate trips before any
	// field line resolves.
	section := []byte{0x03, 0x00, 0xD9}
	var dec qpack.Decoder
	rb := respBuilder{dec: &dec, streamID: 4}

	derr := client.dispatchFrame(&rb, FrameHeaders, section)

	assert.ErrorIsf(t, derr, ErrH3Control,
		"RIC>insertCount = %v, want ErrH3Control (QPACK_DECOMPRESSION_FAILED), not a hang", derr)
	assert.Equalf(t, H3QpackDecompressionFailed, conn.closeCode,
		"close code = %#x, want QPACK_DECOMPRESSION_FAILED (%#x)", conn.closeCode, H3QpackDecompressionFailed)
}
