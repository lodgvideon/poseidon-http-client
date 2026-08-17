package http3

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// These tests exercise the ENCODE-side dynamic QPACK wiring (RFC 9204 §2.1, Q5):
// the client installs an encoder dynamic table once the server's SETTINGS reveal a
// non-zero SETTINGS_QPACK_MAX_TABLE_CAPACITY, inserts repeated request headers on
// its encoder stream, reads the server's decoder stream to advance its Known
// Received Count, and references acknowledged entries in later requests. The reader
// goroutine parks in Poll for the request-less connection, so serviceControl is
// stepped directly (as the control/decode tests do). Correctness is proved by
// rebuilding a mirror of the server's decode table from our encoder-stream bytes
// and decoding the request field sections against it — the CI interop against real
// servers is the ultimate wire gate.

// serverMirrorTable rebuilds the dynamic table a server maintains as our decoder by
// applying every instruction we have written to our encoder stream (after the type
// byte). The encoder stream is a cumulative log, so replaying it from scratch each
// call yields the table state the server would hold.
func serverMirrorTable(t *testing.T, conn *fakeConn, capacity uint64) *qpack.DynamicTable {
	t.Helper()
	sent := conn.clientQEnc.sent
	require.NotEmptyf(t, sent, "encoder stream must lead with its type byte (0x02): %x", sent)
	require.Equalf(t, byte(StreamTypeQPACKEncoder), sent[0],
		"encoder stream must lead with its type byte (0x02): %x", sent)
	dt := qpack.NewDynamicTable(capacity)
	n, err := dt.ParseEncoderInstructions(sent[1:])
	require.NoErrorf(t, err, "apply encoder stream: %v", err)
	require.Equalf(t, len(sent)-1, n, "encoder stream partially consumed: %d/%d bytes", n, len(sent)-1)
	return dt
}

// decodeRequestSection extracts the field section from a HEADERS frame, decodes it
// against the server-mirror table, checks it reproduces want, and returns the
// section's Required Insert Count.
func decodeRequestSection(t *testing.T, server *qpack.DynamicTable, frame []byte, want []header.Field) uint64 {
	t.Helper()
	var fr FrameReader
	fr.SetMaxFrameLen(1 << 20)
	fr.Feed(frame)
	typ, payload, err := fr.ReadFrame()
	require.NoErrorf(t, err, "expected a HEADERS frame: typ=%d err=%v", typ, err)
	require.Equalf(t, FrameHeaders, typ, "expected a HEADERS frame: typ=%d", typ)
	ric, rerr := qpack.RequiredInsertCount(payload, server)
	require.NoErrorf(t, rerr, "RequiredInsertCount: %v", rerr)
	var got []header.Field
	derr := qpack.NewDecoder().DecodeFieldSection(payload, server, func(n, v []byte) error {
		got = append(got, header.Field{Name: append([]byte(nil), n...), Value: append([]byte(nil), v...)})
		return nil
	})
	require.NoErrorf(t, derr, "DecodeFieldSection: %v", derr)
	require.Lenf(t, got, len(want), "decoded %d fields, want %d: %v", len(got), len(want), got)
	for i := range want {
		assert.Equalf(t, string(want[i].Name), string(got[i].Name),
			"field %d = {%s:%s}, want {%s:%s}", i, got[i].Name, got[i].Value, want[i].Name, want[i].Value)
		assert.Equalf(t, string(want[i].Value), string(got[i].Value),
			"field %d = {%s:%s}, want {%s:%s}", i, got[i].Name, got[i].Value, want[i].Name, want[i].Value)
	}
	return ric
}

// sampleRequest is a repeated request whose field list EncodeHeaders builds as
// :method / :scheme / :authority / :path then the regular headers.
func sampleRequest() (*Request, []header.Field) {
	req := &Request{
		Method:    "GET",
		Scheme:    "https",
		Authority: "example.com",
		Path:      "/",
		Headers:   []header.Field{{Name: []byte("user-agent"), Value: []byte("poseidon/1")}},
	}
	want := []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
		{Name: []byte("user-agent"), Value: []byte("poseidon/1")},
	}
	return req, want
}

// TestConformance_RFC9204_Sec21_EncodeSideDynamicTable drives the full encode-side
// round-trip through the client: the server advertises a capacity, the client
// installs its encoder table and emits Set Dynamic Table Capacity, the first
// request inserts and stays static, the server acknowledges via an Insert Count
// Increment, and the second request references the dynamic entries — every section
// decoding back to the identical headers against the server-mirror table.
func TestConformance_RFC9204_Sec21_EncodeSideDynamicTable(t *testing.T) {
	const serverCap = 4096
	ctrl := &fakeStream{id: 3, recvChunks: [][]byte{serverControl([]Setting{{SettingQPACKMaxTableCapacity, serverCap}})}}
	dec := &fakeStream{id: 11, recvChunks: [][]byte{appendV(nil, StreamTypeQPACKDecoder)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{ctrl, dec}}
	client, err := NewClientFake(conn, defaultSettings)
	require.NoError(t, err, "NewClientFake with the production settings")
	defer client.Close()

	// Pass 1: read the server SETTINGS → the encoder dynamic table is installed and
	// Set Dynamic Table Capacity goes out on our encoder stream.
	require.NoError(t, client.serviceControl(), "serviceControl (settings)")

	require.Truef(t, client.qpackEncoder.Load() != nil,
		"encoder must be enabled once the server advertises a non-zero capacity")
	server := serverMirrorTable(t, conn, serverCap)
	require.Equalf(t, uint64(serverCap), server.Capacity(),
		"Set Capacity did not reach the server: capacity=%d, want %d", server.Capacity(), serverCap)

	req, want := sampleRequest()

	// Request 1: nothing acknowledged → the section is static and the repeated
	// headers are inserted on the encoder stream.
	frame1, err1 := client.encodeRequestHeaders(req)

	require.NoErrorf(t, err1, "encodeRequestHeaders 1: %v", err1)
	server = serverMirrorTable(t, conn, serverCap)
	require.NotZero(t, server.InsertCount(),
		"request 1 must insert the repeated headers on the encoder stream")
	ric1 := decodeRequestSection(t, server, frame1, want)
	assert.Equalf(t, uint64(0), ric1,
		"request-1 Required Insert Count = %d, want 0 (static, nothing acknowledged)", ric1)

	// The server acknowledges the inserts with an Insert Count Increment on its
	// decoder stream; pass 2 advances our Known Received Count.
	dec.recvChunks = append(dec.recvChunks, qpack.AppendInsertCountIncrement(nil, server.InsertCount()))
	require.NoError(t, client.serviceControl(), "serviceControl (ack)")
	require.Equalf(t, server.InsertCount(), client.qpackEncoder.Load().KnownReceivedCount(),
		"Known Received Count = %d, want %d after the server's Insert Count Increment",
		client.qpackEncoder.Load().KnownReceivedCount(), server.InsertCount())

	// Request 2: the same headers now reference the dynamic table.
	frame2, err2 := client.encodeRequestHeaders(req)

	require.NoErrorf(t, err2, "encodeRequestHeaders 2: %v", err2)
	server = serverMirrorTable(t, conn, serverCap)
	ric2 := decodeRequestSection(t, server, frame2, want)
	assert.NotZero(t, ric2, "request 2 must reference the dynamic table (Required Insert Count > 0)")
	assert.Lessf(t, len(frame2), len(frame1),
		"dynamic request (%d bytes) not smaller than the static one (%d bytes)", len(frame2), len(frame1))
}

// TestConformance_RFC9204_Sec21_EncodeStaticFallbackServerCapZero proves the static
// fallback: a server advertising SETTINGS_QPACK_MAX_TABLE_CAPACITY=0 keeps the
// encoder static-only, so a request's field section is byte-identical to the
// static-table encoder and nothing beyond the type byte is written on the encoder
// stream.
func TestConformance_RFC9204_Sec21_EncodeStaticFallbackServerCapZero(t *testing.T) {
	ctrl := &fakeStream{id: 3, recvChunks: [][]byte{serverControl([]Setting{{SettingQPACKMaxTableCapacity, 0}})}}
	dec := &fakeStream{id: 11, recvChunks: [][]byte{appendV(nil, StreamTypeQPACKDecoder)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{ctrl, dec}}
	client, err := NewClientFake(conn, defaultSettings)
	require.NoError(t, err, "NewClientFake with the production settings")
	defer client.Close()
	require.NoError(t, client.serviceControl(), "serviceControl over a server capacity of 0")
	req, _ := sampleRequest()

	frame, ferr := client.encodeRequestHeaders(req)

	require.NoErrorf(t, ferr, "encodeRequestHeaders: %v", ferr)
	assert.Truef(t, client.qpackEncoder.Load() == nil, "a server capacity of 0 must keep the encoder static-only")
	var static qpack.Encoder
	wantFrame, werr := req.EncodeHeaders(&static, nil, client.maxFieldSection.Load())
	require.NoErrorf(t, werr, "static EncodeHeaders: %v", werr)
	assert.Equalf(t, wantFrame, frame, "static fallback frame differs:\n got %x\nwant %x", frame, wantFrame)
	assert.Equalf(t, []byte{byte(StreamTypeQPACKEncoder)}, conn.clientQEnc.sent,
		"encoder stream must carry only its type byte at capacity 0: %x", conn.clientQEnc.sent)
}

// TestConcurrent_QPACKEncoderDynamic_UnderRace is the encode-side acid test: N
// goroutines encode requests through the shared encoder dynamic table (inserting on
// first sight, referencing once acknowledged) while another goroutine applies Insert
// Count Increments through the same encMu-guarded path the reader uses. A missing
// lock on the encoder table, its Known Received Count, or the encoder-stream write
// buffer trips -race here. Run with -race -count=5.
func TestConcurrent_QPACKEncoderDynamic_UnderRace(t *testing.T) {
	const serverCap = 65536
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, defaultSettings)
	require.NoError(t, err, "NewClientFake with the production settings")
	defer client.Close()
	client.enableEncoderDynamic(serverCap) // as the server's SETTINGS would
	stop := make(chan struct{})
	var ackWG, encWG sync.WaitGroup

	// The acknowledger: mirror the reader's readQPACKDecoder — take encMu and apply
	// an Insert Count Increment covering the inserts made so far.
	ackWG.Add(1)
	go func() {
		defer ackWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			client.encMu.Lock()
			if enc := client.qpackEncoder.Load(); enc != nil {
				if ic, kr := enc.InsertCount(), enc.KnownReceivedCount(); ic > kr {
					_, _ = enc.ParseDecoderInstructions(qpack.AppendInsertCountIncrement(nil, ic-kr))
				}
			}
			client.encMu.Unlock()
		}
	}()
	const encoders = 8
	for g := 0; g < encoders; g++ {
		encWG.Add(1)
		go func(g int) {
			defer encWG.Done()
			// A per-goroutine header plus a shared one, so both fresh inserts and
			// references to already-inserted entries are exercised concurrently.
			req := &Request{
				Method: "GET", Scheme: "https", Authority: "example.com", Path: "/",
				Headers: []header.Field{
					{Name: []byte("cookie"), Value: []byte("shared=1")},
					{Name: []byte("x-worker"), Value: []byte(strconv.Itoa(g))},
				},
			}
			for i := 0; i < 200; i++ {
				frame, err := client.encodeRequestHeaders(req)
				if err != nil || len(frame) == 0 {
					assert.Failf(t, "encodeRequestHeaders produced no frame",
						"encodeRequestHeaders: frame=%d err=%v", len(frame), err)
					return
				}
			}
		}(g)
	}

	encWG.Wait() // let every encoder finish
	close(stop)  // then stop the acknowledger
	ackWG.Wait()
}

// TestConformance_RFC9114_Sec103_CredentialFieldsNeverIndexed closes the loop on
// the §10.3 MUST NOT: the qpack encoder honours HeaderField.Sensitive, but nothing
// on the request path set it, so on the default Do path — where the dynamic table
// is enabled by the SERVER's advertised capacity, not by caller opt-in — a bearer
// token or session cookie was inserted into the connection-wide compression context
// on its second use. Credential-bearing names are now sensitive by default.
func TestConformance_RFC9114_Sec103_CredentialFieldsNeverIndexed(t *testing.T) {
	enc, err := qpack.NewDynamicEncoder(4096, 4096)
	require.NoErrorf(t, err, "NewDynamicEncoder: %v", err)
	enc.DrainEncoderInstructions(nil) // drop Set Dynamic Table Capacity
	req := &Request{
		Method: "GET", Scheme: "https", Authority: "e", Path: "/",
		Headers: []header.Field{
			{Name: []byte("authorization"), Value: []byte("Bearer s3cr3t")},
			{Name: []byte("cookie"), Value: []byte("sid=deadbeef")},
			{Name: []byte("proxy-authorization"), Value: []byte("Basic zzz")},
			{Name: []byte("x-run-id"), Value: []byte("load-test-42")},
		},
	}

	for round := 0; round < 3; round++ {
		_, rerr := req.EncodeHeaders(enc, nil, ^uint64(0))
		require.NoErrorf(t, rerr, "round %d: EncodeHeaders: %v", round, rerr)
	}

	// The instruction stream is Huffman-coded, so scanning it for the plaintext
	// proves nothing. Count insertions instead. Exactly two fields may enter the
	// table: :authority (a static NAME match, so a new entry) and x-run-id. None of
	// the three credential fields may.
	assert.Equalf(t, uint64(2), enc.InsertCount(),
		"InsertCount = %d, want 2 (:authority + x-run-id) — a credential was indexed", enc.InsertCount())

	// Control: give the same three values ordinary names and they DO get inserted,
	// so the assertion above is about the names, not about an inert table.
	ctl, cerr := qpack.NewDynamicEncoder(4096, 4096)
	require.NoErrorf(t, cerr, "NewDynamicEncoder: %v", cerr)
	plain := *req
	plain.Headers = []header.Field{
		{Name: []byte("x-a"), Value: []byte("Bearer s3cr3t")},
		{Name: []byte("x-b"), Value: []byte("sid=deadbeef")},
		{Name: []byte("x-c"), Value: []byte("Basic zzz")},
		{Name: []byte("x-run-id"), Value: []byte("load-test-42")},
	}
	for round := 0; round < 3; round++ {
		_, perr := plain.EncodeHeaders(ctl, nil, ^uint64(0))
		require.NoErrorf(t, perr, "control round %d: %v", round, perr)
	}
	require.Equalf(t, uint64(5), ctl.InsertCount(),
		"control InsertCount = %d, want 5 — the fixture does not exercise insertion", ctl.InsertCount())
}
