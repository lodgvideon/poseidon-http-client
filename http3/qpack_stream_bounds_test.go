package http3

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// This file pins the memory bound on the two QPACK instruction streams (RFC 9204
// §4.2). Both carry an UNFRAMED, fully server-controlled byte sequence for the life
// of the connection, and both parsers retain a trailing partial instruction across
// service passes so it can be resumed. That retention is the attack surface: a
// server that declares a length it never delivers makes the client hold the partial
// instruction forever. The bound is on the RETAINED TAIL, not on the bytes received
// — a server legitimately bursts many whole instructions at once, and those are
// applied and dropped within the pass.

// qpackUniStream builds the bytes of a server-initiated QPACK instruction stream:
// the stream-type varint (RFC 9204 §4.2) followed by instruction bytes.
func qpackUniStream(typ uint64, instr ...byte) []byte {
	return append(AppendClientQPACKStream(nil, typ), instr...)
}

// driveServiceControl runs serviceControl once per chunk the fake server stream
// holds — the fake delivers one chunk per Recv, which is exactly the dribble the
// tests need — and returns the first error. A nil return means the client consumed
// every chunk without complaint.
func driveServiceControl(t *testing.T, c *Client, passes int) error {
	t.Helper()
	for i := 0; i < passes; i++ {
		if err := c.serviceControl(); err != nil {
			return err
		}
	}
	return nil
}

// TestConformance_RFC9204_Sec42_EncoderStreamDribbleBounded pins the bound on the
// server's QPACK encoder stream. An Insert With Literal Name declares its name
// literal's octet count as a prefix integer and the parser retains the instruction
// until every declared octet arrives (readInstrString → needMore). A server may
// declare up to the prefix integer's ceiling and then dribble, so without a cap on
// the retained tail qpackEncBuf grows for as long as the server keeps sending —
// remote unbounded memory growth on a connection that is otherwise idle.
//
// The declared length here (2 GiB) can never be a real instruction: an entry the
// encoder may insert is bounded by the table capacity we advertised (RFC 9204
// §3.2.2), so the client must refuse it rather than buffer toward it.
func TestConformance_RFC9204_Sec42_EncoderStreamDribbleBounded(t *testing.T) {
	// Insert With Literal Name (01Hxxxxx, 5-bit prefix, H=0) declaring a 2 GiB name.
	head := hpack.EncodeInteger(nil, 5, 0x40, 1<<31)
	chunks := [][]byte{qpackUniStream(StreamTypeQPACKEncoder, head...)}
	const (
		dribble = 4096
		passes  = 64 // 256 KiB of a literal that never ends
	)
	for i := 0; i < passes; i++ {
		chunks = append(chunks, make([]byte, dribble))
	}

	server := &fakeStream{id: 3, recvChunks: chunks}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, qpackDynamicTableCapacity}})
	if err != nil {
		t.Fatal(err)
	}

	serr := driveServiceControl(t, client, len(chunks))
	if serr != ErrH3Control {
		t.Fatalf("serviceControl = %v after the server dribbled %d bytes of a declared 2 GiB literal, "+
			"want ErrH3Control; the retained qpackEncBuf grew to %d bytes",
			serr, dribble*passes, len(client.qpackEncBuf))
	}
	// A well-formed instruction we refuse to buffer is not a decode failure — RFC 9204
	// §6 scopes QPACK_ENCODER_STREAM_ERROR to "failed to interpret an encoder
	// instruction". Refusing an unbounded resource commitment is H3_EXCESSIVE_LOAD
	// (RFC 9114 §8.1), the same code the control stream's frame cap already uses.
	if conn.closeCode != H3ExcessiveLoad {
		t.Fatalf("close code = %#x, want H3_EXCESSIVE_LOAD (%#x)", conn.closeCode, H3ExcessiveLoad)
	}
}

// TestConformance_RFC9204_Sec44_DecoderStreamTailIsSelfBounding is the counterpart
// to the encoder-stream cap, and it documents a hole that is NOT there. The decoder
// stream (RFC 9204 §4.4) carries only Section Acknowledgment, Stream Cancellation,
// and Insert Count Increment — each a single prefix integer with no length-prefixed
// literal to declare and then withhold. So the dribble that defeats the encoder
// stream cannot work here: a run of continuation octets becomes an integer overflow
// (a typed decoder-stream error) at the 6th octet rather than an ever-growing
// "need more". The retained tail is structurally incapable of passing 5 bytes,
// which is why readQPACKDecoder carries no cap: one could never fire.
//
// This test exists to keep that true. It fails the moment an instruction carrying a
// declared length is added to this stream — at which point the tail becomes
// unbounded exactly as the encoder stream's was, and a cap has to follow.
func TestConformance_RFC9204_Sec44_DecoderStreamTailIsSelfBounding(t *testing.T) {
	// The ceiling asserted below. It is a structural property of "every decoder
	// instruction is one prefix integer", not a tuned number.
	const maxDecoderTail = 5
	// A Section Acknowledgment (1xxxxxxx) whose prefix integer never terminates:
	// every octet sets the continuation bit.
	chunks := [][]byte{qpackUniStream(StreamTypeQPACKDecoder, 0xff)}
	const (
		dribble = 4096
		passes  = 64
	)
	for i := 0; i < passes; i++ {
		chunk := make([]byte, dribble)
		for j := range chunk {
			chunk[j] = 0xff // continuation bit set: the integer never ends
		}
		chunks = append(chunks, chunk)
	}

	// The decoder stream is only parsed once the encode-side dynamic table exists, so
	// the server's SETTINGS must enable it first (applyServerSettings → enableEncoderDynamic).
	control := &fakeStream{id: 3, recvChunks: [][]byte{
		serverControl([]Setting{{SettingQPACKMaxTableCapacity, qpackDynamicTableCapacity}}),
	}}
	server := &fakeStream{id: 7, recvChunks: chunks}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{control, server}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, qpackDynamicTableCapacity}})
	if err != nil {
		t.Fatal(err)
	}

	// Drive one pass per chunk, asserting after EVERY pass that the retained tail has
	// not grown — a cap-style check would only catch growth after the fact.
	var serr error
	for i := 0; i < len(chunks); i++ {
		if serr = client.serviceControl(); serr != nil {
			break
		}
		if got := len(client.qpackDecBuf); got > maxDecoderTail {
			t.Fatalf("pass %d: qpackDecBuf retained %d bytes, want <= %d: the decoder stream "+
				"now buffers a declared length and needs the cap the encoder stream has",
				i, got, maxDecoderTail)
		}
	}
	if serr != ErrH3Control {
		t.Fatalf("serviceControl = %v, want ErrH3Control: a never-ending prefix integer must be "+
			"a typed error, not an ever-growing buffer (tail %d bytes)", serr, len(client.qpackDecBuf))
	}
	if conn.closeCode != H3QpackDecoderStreamError {
		t.Fatalf("close code = %#x, want QPACK_DECODER_STREAM_ERROR (%#x): an integer the decoder "+
			"cannot interpret is a decode failure (RFC 9204 §6), not excessive load",
			conn.closeCode, H3QpackDecoderStreamError)
	}
}

// TestQPACKEncoderStream_WholeInstructionBurstAccepted is the counterweight to the
// cap: a server that sends MANY whole instructions in one burst — far more bytes
// than the cap — must be accepted, because those instructions are applied and
// dropped within the pass and never retained. This is the regression that a cap
// applied to received bytes rather than to the retained tail would cause, and it is
// the interop failure mode that matters: a cap that rejects a real server is worse
// than the hole it closes.
func TestQPACKEncoderStream_WholeInstructionBurstAccepted(t *testing.T) {
	// Set Dynamic Table Capacity, then many small whole Insert With Literal Name
	// instructions (4 bytes each: 1-octet name, 1-octet value), repeated well past
	// the retained-tail cap. The table evicts continuously at this capacity, which is
	// exactly what a real encoder stream does over a long-lived connection.
	instr := hpack.EncodeInteger(nil, 5, 0x20, qpackDynamicTableCapacity)
	const inserts = 30000 // ~120 KB of whole instructions in a single burst
	for i := 0; i < inserts; i++ {
		instr = hpack.EncodeInteger(instr, 5, 0x40, 1) // Insert With Literal Name, 1-octet name
		instr = append(instr, byte('a'+i%26))
		instr = hpack.EncodeInteger(instr, 7, 0x00, 1) // 1-octet value
		instr = append(instr, byte('a'+i%26))
	}
	if cap := qpackEncoderInstrCap(qpackDynamicTableCapacity); uint64(len(instr)) <= cap {
		t.Fatalf("burst of %d bytes does not exceed the cap %d; the test would not "+
			"distinguish a tail cap from a received-bytes cap", len(instr), cap)
	}

	server := &fakeStream{id: 3, recvChunks: [][]byte{qpackUniStream(StreamTypeQPACKEncoder, instr...)}}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, qpackDynamicTableCapacity}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.serviceControl(); err != nil {
		t.Fatalf("serviceControl on a %d-byte burst of WHOLE instructions = %v, want nil: "+
			"the cap must bound the retained partial instruction, not the bytes received",
			len(instr), err)
	}
	if got := len(client.qpackEncBuf); got != 0 {
		t.Fatalf("qpackEncBuf retained %d bytes after a burst of whole instructions, want 0", got)
	}
	if client.qpackDyn.InsertCount() == 0 {
		t.Fatal("no inserts applied: the burst was not actually parsed")
	}
}

// TestQPACKEncoderStream_MaximalLegitimateInsertAccepted pins the cap against the
// largest instruction a conforming server can actually send: an entry that exactly
// fills the table capacity we advertised. RFC 9204 §3.2.2 forbids inserting an
// entry larger than the capacity, and §3.2.1 charges every entry name+value+32, so
// name+value == capacity-32 is the ceiling. Dribbling it one byte at a time proves
// the cap admits it even when it is retained across every service pass.
func TestQPACKEncoderStream_MaximalLegitimateInsertAccepted(t *testing.T) {
	const nameLen = 8
	valLen := int(qpackDynamicTableCapacity) - 32 - nameLen // the largest insertable entry

	instr := hpack.EncodeInteger(nil, 5, 0x20, qpackDynamicTableCapacity) // Set Capacity
	instr = append(instr, hpack.EncodeInteger(nil, 5, 0x40, nameLen)...)  // Insert With Literal Name
	instr = append(instr, []byte("x-bigval")...)
	instr = append(instr, hpack.EncodeInteger(nil, 7, 0x00, uint64(valLen))...)
	for i := 0; i < valLen; i++ {
		instr = append(instr, 'v')
	}

	// One byte per chunk: the whole instruction sits in the retained tail until its
	// last byte lands, which is precisely the state the cap governs.
	chunks := [][]byte{qpackUniStream(StreamTypeQPACKEncoder)}
	for _, b := range instr {
		chunks = append(chunks, []byte{b})
	}
	server := &fakeStream{id: 3, recvChunks: chunks}
	conn := &fakeConn{req: &fakeStream{}, acceptQ: []quicStream{server}}
	client, err := NewClientFake(conn, []Setting{{SettingQPACKMaxTableCapacity, qpackDynamicTableCapacity}})
	if err != nil {
		t.Fatal(err)
	}
	if err := driveServiceControl(t, client, len(chunks)); err != nil {
		t.Fatalf("serviceControl while dribbling the largest insertable entry (%d-byte instruction) = %v, "+
			"want nil: the cap rejects an entry a conforming server may send", len(instr), err)
	}
	if client.qpackDyn.InsertCount() != 1 {
		t.Fatalf("InsertCount = %d, want 1: the maximal entry was not inserted", client.qpackDyn.InsertCount())
	}
}

// TestQPACKEncoderInstrCap_AdmitsEveryInsertOurSettingsInvite pins the cap to the
// capacity the client advertises rather than to a fixed number. http3.NewClient is
// public and takes the SETTINGS, so a caller advertising a larger table must not
// find the inserts it invited refused: the cap has to move with the advertisement.
func TestQPACKEncoderInstrCap_AdmitsEveryInsertOurSettingsInvite(t *testing.T) {
	for _, capacity := range []uint64{0, 32, 4096, 1 << 16, 1 << 20} {
		got := qpackEncoderInstrCap(capacity)
		// A conforming encoder's largest insert: name+value == capacity-32 decoded
		// (RFC 9204 §3.2.1/§3.2.2), at most 3.75x that on the wire if it Huffman-codes
		// bytes it should have sent raw (the longest RFC 7541 code is 30 bits).
		var largestLegit uint64
		if capacity > 32 {
			largestLegit = (capacity-32)*30/8 + qpackInstrOverhead
		}
		if got < largestLegit {
			t.Errorf("capacity %d: cap %d is below the largest instruction a conforming "+
				"encoder may send (%d): a real server would be refused", capacity, got, largestLegit)
		}
	}
	// The clamp keeps an absurd advertised capacity from overflowing the multiply
	// into a cap so small it refuses everything.
	if got := qpackEncoderInstrCap(^uint64(0)); got < qpackEncoderInstrCap(1<<20) {
		t.Errorf("cap for an absurd capacity = %d, want >= the cap for a sane one: the multiply overflowed", got)
	}
}
