package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"testing"
	"time"
)

// fuzzFrameHandler records the frames ParseFrames dispatches and touches every
// aliased byte slice it is handed, so a parser that produced an out-of-bounds or
// mis-sized slice surfaces as a panic under the fuzzer rather than passing
// silently. It embeds nopFrameHandler so it satisfies the full FrameHandler
// interface with no space restriction (every frame type is dispatched).
type fuzzFrameHandler struct {
	nopFrameHandler
	sink int
}

func (h *fuzzFrameHandler) OnCrypto(_ uint64, data []byte) error {
	h.sink += len(data)
	for _, b := range data { // walk the aliased slice to exercise its bounds
		h.sink += int(b)
	}
	return nil
}

func (h *fuzzFrameHandler) OnStream(_, _ uint64, _ bool, data []byte) error {
	h.sink += len(data)
	for _, b := range data {
		h.sink += int(b)
	}
	return nil
}

func (h *fuzzFrameHandler) OnNewToken(token []byte) error { h.sink += len(token); return nil }

func (h *fuzzFrameHandler) OnConnectionClose(_ bool, _, _ uint64, reason []byte) error {
	h.sink += len(reason)
	return nil
}

func (h *fuzzFrameHandler) OnNewConnectionID(_, _ uint64, connID []byte, _ *[16]byte) error {
	h.sink += len(connID)
	return nil
}

// FuzzParseFrames feeds arbitrary bytes to the QUIC frame parser (RFC 9000 §12.4,
// §19). The parser must reject any malformed packet payload with an error and
// never panic, hang, or allocate unboundedly — a decrypted payload is fully
// attacker-controlled once the AEAD is opened, so this is the top adversarial
// surface.
func FuzzParseFrames(f *testing.F) {
	f.Add([]byte{})                 // empty payload
	f.Add([]byte{0x01})             // PING
	f.Add([]byte{0x00, 0x00, 0x00}) // a PADDING run
	f.Add(AppendStream(nil, 4, 0, true, []byte("hello")))
	f.Add(AppendStream(nil, 8, 100, false, nil))
	f.Add(AppendCrypto(nil, 0, []byte("crypto-bytes")))
	f.Add(appendPing(nil))
	f.Add(AppendAck(nil, 10, 3, 4, []AckRange{{Gap: 1, Length: 2}}))
	f.Add(appendPathResponse(nil, [8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	f.Add([]byte{0x40})                                                             // truncated 2-byte varint frame type
	f.Add([]byte{0x08, 0x40})                                                       // STREAM with a truncated stream-id varint
	f.Add([]byte{0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})             // ACK: huge largest, no more bytes
	f.Add([]byte{0x02, 0x00, 0x00, 0xc0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // ACK: huge range count

	f.Fuzz(func(_ *testing.T, payload []byte) {
		var h fuzzFrameHandler
		// Contract: returns (an error or nil) without panicking, hanging, or
		// unbounded allocation. Progress is guaranteed — every branch advances the
		// cursor or returns — so the loop inside ParseFrames terminates on len(payload).
		_ = ParseFrames(payload, &h)
	})
}

// FuzzParseHeader feeds arbitrary bytes and a fuzzed local-DCID length to the
// QUIC packet-header parser (RFC 9000 §17). An on-path attacker controls every
// header byte before decryption, so the parser must never panic on a truncated,
// oversized, or nonsensical header — only ever return ErrPacketEncoding.
func FuzzParseHeader(f *testing.F) {
	// A short-header packet with an 8-byte DCID (fixed bit set).
	f.Add([]byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 0xab}, 8)
	// A long-header shell (fixed bit set, v1).
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}, 0)
	// A Version Negotiation shell (version == 0).
	f.Add([]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0)
	f.Add([]byte{}, 0)
	f.Add([]byte{0x00}, 8)
	f.Add([]byte{0x80}, 0)  // long-header first byte, nothing else
	f.Add([]byte{0x40}, 21) // short header, out-of-range dcidLen
	f.Add([]byte{0x40}, -1) // short header, negative dcidLen

	f.Fuzz(func(_ *testing.T, pkt []byte, dcidLen int) {
		// Contract: returns (Header, error) without panicking for any input,
		// including a negative or absurdly large dcidLen (both are range-guarded).
		_, _ = ParseHeader(pkt, dcidLen)
	})
}

// fuzzClientInitial builds a genuine client Initial datagram for dcid: a real
// crypto/tls ClientHello sealed under the Initial keys derived from dcid
// (RFC 9001 §5.2) and padded to the 1200-byte floor (RFC 9000 §14.1). It mirrors
// listener_test.go's clientHelloInitial, which takes a *testing.T a fuzz seed
// cannot supply. The corpus needs the one input AcceptInitial is meant to accept
// so the fuzzer has a valid packet to mutate outward from — mutating from garbage
// alone would never reach the post-AEAD frame parser. RootCAs is left nil: only
// the ClientHello bytes are wanted, and no server reply is ever verified.
func fuzzClientInitial(tb testing.TB, dcid []byte) []byte {
	tb.Helper()
	tp := AppendTransportParams(nil, LocalTransportParams{
		InitialMaxData:                1 << 20,
		InitialMaxStreamDataBidiLocal: 1 << 20,
		InitialMaxStreamDataUni:       1 << 20,
		InitialMaxStreamsUni:          4,
	})
	hs := newClientHandshake(&tls.Config{ServerName: "example.com"}, tp)
	// crypto/tls runs the handshake on its own goroutine; close it here since only
	// the ClientHello is wanted (the sink copies it) and it never completes.
	defer func() { _ = hs.Close() }()
	if err := hs.Start(context.Background()); err != nil {
		tb.Fatalf("client handshake Start: %v", err)
	}
	sink := newMemSink()
	if err := hs.Pump(sink); err != nil {
		tb.Fatalf("client handshake Pump: %v", err)
	}
	clientKeys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(clientKeys)
	if err != nil {
		tb.Fatalf("NewSealer: %v", err)
	}
	pkt, err := buildInitialPacket(nil, sealer, dcid, nil, nil, 0, 4, 0,
		sink.crypto[tls.QUICEncryptionLevelInitial], InitialDatagramMinSize)
	if err != nil {
		tb.Fatalf("buildInitialPacket: %v", err)
	}
	return pkt
}

// FuzzAcceptInitial feeds arbitrary bytes to the server's first-contact path
// (RFC 9000 §17.2.2, RFC 9001 §5.2): long-header parsing, version, DCID/SCID
// lengths, token, Length field, packet-number decoding, Initial-key derivation,
// and the AEAD open, followed by CRYPTO reassembly of the ClientHello. These
// bytes are an unauthenticated UDP datagram from an arbitrary remote host,
// accepted before any handshake state exists — anyone on the internet can send
// any bytes here, which makes it the top adversarial surface in the package.
//
// Contract: returns an error without panicking, hanging, or allocating
// unboundedly. Garbage must not authenticate; if it does succeed, the state
// handed back must be self-consistent with the packet on the wire.
func FuzzAcceptInitial(f *testing.F) {
	f.Add(fuzzClientInitial(f, []byte{1, 2, 3, 4, 5, 6, 7, 8}))
	f.Add(fuzzClientInitial(f, nil))                                   // zero-length DCID
	f.Add(fuzzClientInitial(f, bytes.Repeat([]byte{9}, maxConnIDLen))) // 20-byte DCID
	f.Add(fuzzClientInitial(f, []byte{1, 2, 3, 4, 5, 6, 7, 8})[:100])  // far below the §14.1 floor
	f.Add([]byte{})
	// A long-header Initial shell: v1, empty DCID/SCID, empty token, Length 0.
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00})
	// A Version Negotiation shell (version == 0, §17.2.1).
	f.Add([]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	// Absurd CID lengths: §17.2 caps a v1 connection ID at 20 bytes.
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00, 0x01, 21, 1, 2, 3})
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00, 0x01, 255, 1, 2, 3})
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00, 0x01, 20, 1, 2, 3}) // DCID Len exceeds the datagram
	// A 2-byte token-length varint with only one byte behind it.
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x40})
	// Length claims 1024 bytes of packet number + payload; the datagram holds none.
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x44, 0x00})

	f.Fuzz(func(t *testing.T, datagram []byte) {
		ci, err := AcceptInitial(datagram)
		if err != nil {
			return
		}
		// Success means the datagram authenticated under Initial keys derived from
		// its own DCID, so the header must re-parse and the echoed IDs must be the
		// bytes that were on the wire — a server seeds its whole connection from
		// them (§7.3: origDCID is echoed as a transport parameter, SCID becomes the
		// reply address), so a mismatch is a state-confusion bug, not cosmetic.
		hdr, herr := ParseHeader(datagram, 0)
		if herr != nil {
			t.Fatalf("AcceptInitial accepted a datagram ParseHeader rejects: %v", herr)
		}
		if hdr.Type != PacketInitial {
			t.Fatalf("AcceptInitial accepted a non-Initial packet type %v", hdr.Type)
		}
		if !bytes.Equal(ci.DCID, hdr.DCID) {
			t.Fatalf("DCID %x does not echo the packet's %x", ci.DCID, hdr.DCID)
		}
		if !bytes.Equal(ci.SCID, hdr.SCID) {
			t.Fatalf("SCID %x does not echo the packet's %x", ci.SCID, hdr.SCID)
		}
		if !bytes.Equal(ci.Token, hdr.Token) {
			t.Fatalf("Token %x does not echo the packet's %x", ci.Token, hdr.Token)
		}
		// The reassembler is what bounds a hostile CRYPTO offset (§19.6).
		if len(ci.CryptoData) > maxInitialCrypto {
			t.Fatalf("CryptoData %d bytes exceeds the %d cap", len(ci.CryptoData), maxInitialCrypto)
		}
	})
}

// FuzzParseTransportParams feeds arbitrary bytes to the transport-parameter
// parser (RFC 9000 §18). The peer sends these inside its TLS handshake, so they
// are attacker-controlled on both the client and the server role, and the
// encoding is varint-dense — id, length, and most values are varints, which is
// the classic overflow and truncation surface.
//
// Contract: returns an error without panicking, hanging, or allocating
// unboundedly, and any TransportParams it returns successfully respects the
// bounds §18.2/§7.4 declare invalid.
func FuzzParseTransportParams(f *testing.F) {
	f.Add(AppendServerTransportParams(nil, ServerTransportParams{
		MaxStreamsBidi: 16, MaxStreamsUni: 4, MaxIdleTimeout: 30000,
	}, []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}))
	f.Add(AppendTransportParams(nil, LocalTransportParams{
		InitialMaxData:                1 << 20,
		InitialMaxStreamDataBidiLocal: 1 << 20,
		InitialMaxStreamDataUni:       1 << 20,
		InitialMaxStreamsUni:          4,
		SourceConnectionID:            []byte{1, 2, 3, 4},
	}))
	f.Add([]byte{})
	// Truncations of a valid encoding: a field cut mid-value and mid-identifier.
	full := AppendServerTransportParams(nil, ServerTransportParams{MaxStreamsBidi: 16, MaxStreamsUni: 4}, nil, nil)
	f.Add(full[:len(full)/2])
	f.Add(full[:1])
	// A duplicate identifier is a TRANSPORT_PARAMETER_ERROR (§7.4).
	f.Add(append(appendTPInt(nil, tpInitialMaxData, 100), appendTPInt(nil, tpInitialMaxData, 200)...))
	// An unknown (GREASE-shaped) identifier must be ignored, not rejected (§18.1).
	f.Add(appendTPInt(nil, 0x3a1a, 1))
	// Length 8 with one byte behind it: a field overrunning the buffer.
	f.Add([]byte{0x04, 0x08, 0x00})
	// max_idle_timeout at the top of the varint space: milliseconds scaled to a
	// nanosecond Duration is where an unchecked value overflows int64 (§18.2).
	f.Add(appendTPInt(nil, tpMaxIdleTimeout, 1<<62-1))
	// A stateless_reset_token that is not the required 16 bytes (§18.2).
	f.Add(appendTPBytes(nil, tpStatelessResetToken, []byte{1, 2, 3}))

	f.Fuzz(func(t *testing.T, raw []byte) {
		tp, err := ParseTransportParams(raw)
		if err != nil {
			return
		}
		// The parser is the only thing standing between these bytes and the send
		// path, which does arithmetic on every one of these values.
		if tp.AckDelayExponent > 20 {
			t.Fatalf("ack_delay_exponent %d accepted, §18.2 caps it at 20", tp.AckDelayExponent)
		}
		if tp.MaxAckDelay < 0 || tp.MaxAckDelay >= (1<<14)*time.Millisecond {
			t.Fatalf("max_ack_delay %v accepted, §18.2 caps it below 2^14 ms", tp.MaxAckDelay)
		}
		// A negative Duration here means milliseconds × 1e6 overflowed int64, which
		// would turn every idle-timeout deadline into one already in the past.
		if tp.MaxIdleTimeout < 0 {
			t.Fatalf("max_idle_timeout %v accepted, scaling overflowed int64", tp.MaxIdleTimeout)
		}
		if tp.InitialMaxStreamsBidi > maxStreamsLimit {
			t.Fatalf("initial_max_streams_bidi %d accepted, §4.6 caps it at 2^60", tp.InitialMaxStreamsBidi)
		}
		if tp.InitialMaxStreamsUni > maxStreamsLimit {
			t.Fatalf("initial_max_streams_uni %d accepted, §4.6 caps it at 2^60", tp.InitialMaxStreamsUni)
		}
	})
}
