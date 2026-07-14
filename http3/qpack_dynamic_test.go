package http3

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// These tests exercise the QPACK dynamic-table plumbing (RFC 9204 Q2). The
// production client is INERT — dial.go advertises SETTINGS_QPACK_MAX_TABLE_
// CAPACITY = 0, so the server sends no encoder instructions and no field section
// references the dynamic table — so the plumbing is driven here with a non-zero
// capacity, exactly as Q3 will once it flips dial.go.

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
	if err != nil {
		t.Fatal(err)
	}
	// The reader parks in Poll for a request-less connection, so drive one service
	// pass directly (as the control tests do) to peel the encoder stream and apply
	// its instructions.
	if err := client.serviceControl(); err != nil {
		t.Fatalf("serviceControl: %v", err)
	}

	if got := client.qpackDyn.InsertCount(); got != 1 {
		t.Fatalf("dynamic table InsertCount = %d, want 1", got)
	}
	if got := client.insertCount.Load(); got != 1 {
		t.Fatalf("published insertCount = %d, want 1", got)
	}

	// The decoder stream opens with its type byte (0x03), then the ICI.
	sent := conn.clientQDec.sent
	if len(sent) < 2 || sent[0] != byte(StreamTypeQPACKDecoder) {
		t.Fatalf("decoder stream must open with its type byte then an ICI: %x", sent)
	}
	if sent[1]&0xc0 != 0x00 { // Insert Count Increment is 00xxxxxx (§4.4.3)
		t.Fatalf("instruction %#x is not an Insert Count Increment", sent[1])
	}
	inc, n, derr := hpack.DecodeInteger(sent[1:], 6)
	if derr != nil || n == 0 {
		t.Fatalf("Insert Count Increment does not decode: %x (%v)", sent[1:], derr)
	}
	if inc != 1 {
		t.Fatalf("Insert Count Increment = %d, want 1", inc)
	}
}

// TestConformance_RFC9204_Sec441_SectionAcknowledgment checks that a HEADERS field
// section referencing a dynamic-table entry decodes correctly against the shared
// table and that a Section Acknowledgment for the stream is emitted on our decoder
// stream (RFC 9204 §2.1.4, §4.4.1).
func TestConformance_RFC9204_Sec441_SectionAcknowledgment(t *testing.T) {
	enc := &fakeStream{id: 7, recvChunks: [][]byte{qpackServerEncoderStream(256)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{enc}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 256}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.serviceControl(); err != nil {
		t.Fatalf("serviceControl: %v", err)
	}
	before := len(conn.clientQDec.sent) // type byte + the ICI from the insert

	var dec qpack.Decoder
	rb := respBuilder{dec: &dec, streamID: 4}
	if err := client.dispatchFrame(&rb, FrameHeaders, dynRefSection()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	if rb.resp == nil || rb.resp.Status != 200 {
		t.Fatalf("resp = %+v, want status 200", rb.resp)
	}
	if len(rb.resp.Headers) != 1 || string(rb.resp.Headers[0].Name) != "x-dyn" ||
		string(rb.resp.Headers[0].Value) != "yes" {
		t.Fatalf("headers = %+v, want the dynamic-table x-dyn: yes", rb.resp.Headers)
	}
	if !rb.refDynamic {
		t.Fatal("refDynamic must be set for a section that referenced the dynamic table")
	}

	ack := conn.clientQDec.sent[before:]
	if len(ack) == 0 || ack[0]&0x80 == 0 { // Section Acknowledgment is 1xxxxxxx (§4.4.1)
		t.Fatalf("expected a Section Acknowledgment (1xxxxxxx), got %x", ack)
	}
	id, n, derr := hpack.DecodeInteger(ack, 7)
	if derr != nil || n != len(ack) || id != 4 {
		t.Fatalf("Section Acknowledgment stream id = %d (n=%d,%v), want 4", id, n, derr)
	}
}

// TestConformance_RFC9204_Sec442_StreamCancellationOnAbort checks that aborting a
// stream that referenced the dynamic table emits a Stream Cancellation for that
// stream on our decoder stream (RFC 9204 §4.4.2), alongside the QUIC reset.
func TestConformance_RFC9204_Sec442_StreamCancellationOnAbort(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 256}})
	if err != nil {
		t.Fatal(err)
	}
	before := len(conn.clientQDec.sent) // just the type byte 0x03

	stream := &fakeStream{id: 8, conn: conn, ready: make(chan struct{}, 1)}
	br := &BodyReader{c: client, stream: stream, rb: respBuilder{refDynamic: true, streamID: 8}}
	br.abort(context.Canceled)

	cancel := conn.clientQDec.sent[before:]
	if len(cancel) == 0 || cancel[0]&0xc0 != 0x40 { // Stream Cancellation is 01xxxxxx (§4.4.2)
		t.Fatalf("expected a Stream Cancellation (01xxxxxx), got %x", cancel)
	}
	id, n, derr := hpack.DecodeInteger(cancel, 6)
	if derr != nil || n != len(cancel) || id != 8 {
		t.Fatalf("Stream Cancellation stream id = %d (n=%d,%v), want 8", id, n, derr)
	}
	if !stream.reset || !stream.stopped {
		t.Fatal("abort must also reset the request stream (STOP_SENDING + RESET_STREAM)")
	}
}

// TestConcurrent_QPACKDynamicTable_UnderRace is the QPACK acid test: the reader
// goroutine keeps inserting into the shared dynamic table under qpackMu.Lock while
// N decoders resolve dynamic references under qpackMu.RLock and copy the emitted
// fields. A missing lock, or a copy that outlives the RLock while an insert
// rewrites the arena, trips -race here. Run with -race -count=5.
func TestConcurrent_QPACKDynamicTable_UnderRace(t *testing.T) {
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, 65536}})
	if err != nil {
		t.Fatal(err)
	}
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
	if err := client.readQPACKEncoder(); err != nil {
		t.Fatalf("initial readQPACKEncoder: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the reader: apply the remaining inserts under qpackMu.Lock
		defer wg.Done()
		for i := 0; i < inserts; i++ {
			if err := client.readQPACKEncoder(); err != nil {
				t.Errorf("readQPACKEncoder: %v", err)
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
					t.Errorf("dispatchFrame: %v", err)
					return
				}
				if rb.resp == nil || len(rb.resp.Headers) != 1 ||
					string(rb.resp.Headers[0].Value) != "yes" {
					t.Errorf("dynamic decode returned wrong entry: %+v", rb.resp)
					return
				}
			}
		}(r)
	}
	wg.Wait()

	if got := client.qpackDyn.InsertCount(); got != inserts+1 {
		t.Fatalf("final InsertCount = %d, want %d", got, inserts+1)
	}
}
