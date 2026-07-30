package qpack

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// These tests exercise the ENCODE-side dynamic table (RFC 9204 §2.1, Q5): the
// client inserts repeated request-header entries into its own encoder dynamic
// table — the table the peer maintains as decoder — and references them once the
// peer acknowledges them. The golden proof is a round-trip: the encoder-stream
// Insert instructions and the request field sections are decoded back with the Q1
// decoder against a mirror of the peer's decode table, and must reproduce the
// original headers. A wrong Required Insert Count, Base, or index would fail the
// decode here exactly as it would fail a real server on the wire.

// TestConformance_RFC9204_Sec21_EncodeReferenceAfterAck drives the conservative
// insert-then-reference-after-ack strategy: the first request inserts and stays
// static; after the peer's Insert Count Increment the second request references the
// dynamic entries. Both round-trip to the identical headers.
func TestConformance_RFC9204_Sec21_EncodeReferenceAfterAck(t *testing.T) {
	const serverMax = 4096
	enc, err := NewDynamicEncoder(serverMax, serverMax)
	if err != nil {
		t.Fatalf("NewDynamicEncoder: %v", err)
	}
	// A mirror of the dynamic table the peer maintains as our decoder, driven only
	// by the encoder-stream instructions we produce.
	server := NewDynamicTable(serverMax)
	mustApply(t, server, enc.DrainEncoderInstructions(nil)) // Set Dynamic Table Capacity (§4.3.1)
	if server.Capacity() != serverMax {
		t.Fatalf("server table capacity = %d, want %d", server.Capacity(), serverMax)
	}

	fields := []hpack.HeaderField{
		hf(":method", "GET"),
		hf(":scheme", "https"),
		hf(":authority", "example.com"),
		hf(":path", "/"),
		hf("cookie", "sess=abc"),
	}

	// --- Request 1: nothing acknowledged, so the section is static-only and the two
	// non-static fields are inserted for future requests (RFC 9204 §2.1). ---
	sec1 := enc.EncodeFieldSection(nil, fields)
	if want := NewEncoder().EncodeFieldSection(nil, fields); !bytes.Equal(sec1, want) {
		t.Fatalf("request-1 section = %x, want byte-identical static output %x", sec1, want)
	}
	insts1 := enc.DrainEncoderInstructions(nil)
	if len(insts1) == 0 {
		t.Fatal("request 1 must emit Insert instructions for the repeated headers")
	}
	// The first insert is Insert With Name Reference to the static table (§4.3.2):
	// name ":authority" is static index 0, so the byte is 0xc0 (1T=11, index 0).
	if insts1[0]&0x80 == 0 || insts1[0]&0x40 == 0 || insts1[0] != 0xc0 {
		t.Fatalf("first Insert instruction = %#x, want Insert With Name Reference static idx 0 (0xc0)", insts1[0])
	}
	mustApply(t, server, insts1)
	if server.InsertCount() != 2 {
		t.Fatalf("server insert count = %d, want 2 (:authority, cookie)", server.InsertCount())
	}
	wantEntry(t, server, 0, ":authority", "example.com")
	wantEntry(t, server, 1, "cookie", "sess=abc")
	assertDecodeDyn(t, NewDecoder(), sec1, server, fields) // RIC 0 (static)

	// --- Peer acknowledges the two inserts with an Insert Count Increment (§4.4.3). ---
	if _, aerr := enc.ParseDecoderInstructions(AppendInsertCountIncrement(nil, 2)); aerr != nil {
		t.Fatalf("ParseDecoderInstructions(ICI+2): %v", aerr)
	}
	if enc.KnownReceivedCount() != 2 {
		t.Fatalf("Known Received Count = %d, want 2", enc.KnownReceivedCount())
	}

	// --- Request 2: the same headers now reference the dynamic table. ---
	sec2 := enc.EncodeFieldSection(nil, fields)
	if extra := enc.DrainEncoderInstructions(nil); len(extra) != 0 {
		t.Fatalf("request 2 must not insert again, got %x", extra)
	}
	ric, rerr := RequiredInsertCount(sec2, server)
	if rerr != nil || ric != 2 {
		t.Fatalf("request-2 Required Insert Count = %d (%v), want 2", ric, rerr)
	}
	assertDecodeDyn(t, NewDecoder(), sec2, server, fields) // dynamic references, must round-trip
	if len(sec2) >= len(sec1) {
		t.Fatalf("dynamic section (%d bytes) not smaller than static (%d bytes)", len(sec2), len(sec1))
	}
}

// TestConformance_RFC9204_Sec214_ReferenceOnlyAcknowledged proves the encoder
// references only entries at an absolute index below the Known Received Count: with
// a partial acknowledgment (only the first insert) the second entry stays static,
// so the Required Insert Count never runs ahead of what the peer has received.
func TestConformance_RFC9204_Sec214_ReferenceOnlyAcknowledged(t *testing.T) {
	const serverMax = 4096
	enc, err := NewDynamicEncoder(serverMax, serverMax)
	if err != nil {
		t.Fatal(err)
	}
	server := NewDynamicTable(serverMax)
	mustApply(t, server, enc.DrainEncoderInstructions(nil))
	fields := []hpack.HeaderField{
		hf(":authority", "example.com"),
		hf("cookie", "sess=abc"),
	}
	_ = enc.EncodeFieldSection(nil, fields) // inserts abs 0 and abs 1
	mustApply(t, server, enc.DrainEncoderInstructions(nil))
	// Acknowledge only the first insert.
	if _, err := enc.ParseDecoderInstructions(AppendInsertCountIncrement(nil, 1)); err != nil {
		t.Fatal(err)
	}
	sec := enc.EncodeFieldSection(nil, fields)
	ric, rerr := RequiredInsertCount(sec, server)
	if rerr != nil || ric != 1 {
		t.Fatalf("Required Insert Count = %d (%v), want 1 (only abs 0 acknowledged)", ric, rerr)
	}
	assertDecodeDyn(t, NewDecoder(), sec, server, fields)
}

// TestConformance_RFC9204_Sec44_ParseDecoderInstructions covers the encode-side
// consumption of the peer's decoder stream (§4.4): an Insert Count Increment
// advances the Known Received Count, a zero increment or one past the inserts made
// is a QPACK_DECODER_STREAM_ERROR, Section Acknowledgment / Stream Cancellation are
// consumed as no-ops, and a partial instruction is left for the next call.
// TestConformance_RFC9204_Sec411_Integer62Bit pins the §4.1.1 MUST that a QPACK
// implementation decode prefixed integers up to and including 62 bits long. The
// shared HPACK reader stops at 2^32-1, which is enough for HTTP/2 but false-rejects
// a conformant server here: a Section Acknowledgment or Stream Cancellation carries
// a QUIC stream ID, legal up to 2^62-1.
func TestConformance_RFC9204_Sec411_Integer62Bit(t *testing.T) {
	const max62 = uint64(1)<<62 - 1
	for _, prefix := range []uint8{3, 4, 5, 6, 7, 8} {
		t.Run(strconv.Itoa(int(prefix)), func(t *testing.T) {
			buf := hpack.EncodeInteger(nil, prefix, 0x00, max62)
			got, n, err := decodeInt(buf, prefix)
			if err != nil || got != max62 || n != len(buf) {
				t.Fatalf("decodeInt(%x, %d) = %d, %d, %v; want %d, %d, nil",
					buf, prefix, got, n, err, max62, len(buf))
			}
			// 2^62 is past the ceiling and must still be rejected.
			over := hpack.EncodeInteger(nil, prefix, 0x00, uint64(1)<<62)
			if _, _, err := decodeInt(over, prefix); !errors.Is(err, hpack.ErrIntegerOverflow) {
				t.Fatalf("decodeInt(2^62) err = %v, want ErrIntegerOverflow", err)
			}
		})
	}
}

func TestConformance_RFC9204_Sec44_ParseDecoderInstructions(t *testing.T) {
	newEnc := func(t *testing.T, inserts int) *Encoder {
		t.Helper()
		enc, err := NewDynamicEncoder(4096, 4096)
		if err != nil {
			t.Fatal(err)
		}
		enc.DrainEncoderInstructions(nil) // discard the Set Capacity instruction
		if inserts > 0 {
			fields := make([]hpack.HeaderField, inserts)
			for i := range fields {
				fields[i] = hf("x-h"+strconv.Itoa(i), "v")
			}
			enc.EncodeFieldSection(nil, fields)
			enc.DrainEncoderInstructions(nil)
			if enc.InsertCount() != uint64(inserts) {
				t.Fatalf("setup inserted %d, want %d (raise capacity)", enc.InsertCount(), inserts)
			}
		}
		return enc
	}
	t.Run("ICI_advances_known", func(t *testing.T) {
		enc := newEnc(t, 3)
		n, err := enc.ParseDecoderInstructions(AppendInsertCountIncrement(nil, 3))
		if err != nil || n == 0 || enc.KnownReceivedCount() != 3 {
			t.Fatalf("ICI+3: n=%d err=%v known=%d, want known 3", n, err, enc.KnownReceivedCount())
		}
	})
	t.Run("zero_increment_error", func(t *testing.T) {
		enc := newEnc(t, 1)
		if _, err := enc.ParseDecoderInstructions([]byte{0x00}); !errors.Is(err, ErrDecoderStream) {
			t.Fatalf("ICI 0: err=%v, want ErrDecoderStream", err)
		}
	})
	t.Run("increment_past_inserts_error", func(t *testing.T) {
		enc := newEnc(t, 1)
		if _, err := enc.ParseDecoderInstructions(AppendInsertCountIncrement(nil, 2)); !errors.Is(err, ErrDecoderStream) {
			t.Fatalf("ICI past inserts: err=%v, want ErrDecoderStream", err)
		}
	})
	t.Run("section_ack_and_cancel_noop", func(t *testing.T) {
		enc := newEnc(t, 2)
		buf := AppendSectionAcknowledgment(nil, 4)
		buf = AppendStreamCancellation(buf, 8)
		n, err := enc.ParseDecoderInstructions(buf)
		if err != nil || n != len(buf) || enc.KnownReceivedCount() != 0 {
			t.Fatalf("SectionAck+Cancel: n=%d/%d err=%v known=%d, want full consume with known 0",
				n, len(buf), err, enc.KnownReceivedCount())
		}
	})
	t.Run("partial_instruction_retained", func(t *testing.T) {
		enc := newEnc(t, 64)
		full := AppendInsertCountIncrement(nil, 64) // 6-bit prefix overflows → 2 bytes
		if len(full) < 2 {
			t.Fatalf("expected a multi-byte increment, got %x", full)
		}
		if n, err := enc.ParseDecoderInstructions(full[:len(full)-1]); err != nil || n != 0 {
			t.Fatalf("partial ICI: n=%d err=%v, want 0 consumed and no error", n, err)
		}
		if n, err := enc.ParseDecoderInstructions(full); err != nil || n != len(full) || enc.KnownReceivedCount() != 64 {
			t.Fatalf("resumed ICI: n=%d err=%v known=%d, want full consume with known 64", n, err, enc.KnownReceivedCount())
		}
	})
}

// TestConformance_RFC9204_Sec322_EncoderNeverEvicts confirms the encode side stays
// eviction-free: given more distinct headers than the table can hold, it inserts
// only those that fit and leaves the rest literal, so the peer's mirror never
// evicts (live entries equal the insert count) and its size stays within capacity.
func TestConformance_RFC9204_Sec322_EncoderNeverEvicts(t *testing.T) {
	const capacity = 256
	enc, err := NewDynamicEncoder(capacity, capacity)
	if err != nil {
		t.Fatal(err)
	}
	server := NewDynamicTable(capacity)
	mustApply(t, server, enc.DrainEncoderInstructions(nil))
	fields := make([]hpack.HeaderField, 50)
	for i := range fields {
		fields[i] = hf("x-key-"+strconv.Itoa(i), "value-"+strconv.Itoa(i))
	}
	sec := enc.EncodeFieldSection(nil, fields)
	mustApply(t, server, enc.DrainEncoderInstructions(nil)) // fails if any insert forced an eviction
	if enc.InsertCount() == 0 {
		t.Fatal("expected at least one header to fit and be inserted")
	}
	if server.Len() != int(server.InsertCount()) {
		t.Fatalf("eviction occurred: live=%d, inserted=%d", server.Len(), server.InsertCount())
	}
	if server.Size() > capacity {
		t.Fatalf("table size %d exceeds capacity %d", server.Size(), capacity)
	}
	assertDecodeDyn(t, NewDecoder(), sec, server, fields) // still round-trips (RIC 0, nothing acked)
}

// TestNewDynamicEncoder_RejectsTinyCapacity keeps the dynamic encoder off when the
// peer advertises a capacity too small to hold any entry (below 32, MaxEntries 0):
// the caller falls back to the static-only NewEncoder.
func TestNewDynamicEncoder_RejectsTinyCapacity(t *testing.T) {
	for _, cap := range []uint64{0, 16, 31} {
		if _, err := NewDynamicEncoder(cap, cap); !errors.Is(err, ErrEncoderStream) {
			t.Fatalf("NewDynamicEncoder(%d): err=%v, want ErrEncoderStream", cap, err)
		}
	}
}
