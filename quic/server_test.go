package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"testing"
)

// TestAcceptInitial_RoundTrip builds a real client Initial with BuildInitialPacket
// (sealed with the client's Initial keys), then checks AcceptInitial derives the
// same keys from the DCID, decrypts it, and recovers the connection IDs and the
// ClientHello CRYPTO bytes through the PADDING that pads the datagram to 1200.
func TestAcceptInitial_RoundTrip(t *testing.T) {
	t.Parallel()
	dcid := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	scid := []byte{0xaa, 0xbb, 0xcc}
	clientHello := []byte("this-stands-in-for-a-TLS-ClientHello-flight")

	clientKeys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(clientKeys)
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	pkt, err := BuildInitialPacket(nil, sealer, dcid, scid, nil, 0, 4, 0, clientHello, InitialDatagramMinSize)
	if err != nil {
		t.Fatalf("BuildInitialPacket: %v", err)
	}

	ci, err := AcceptInitial(pkt)
	if err != nil {
		t.Fatalf("AcceptInitial: %v", err)
	}
	if !bytes.Equal(ci.DCID, dcid) {
		t.Errorf("DCID = %x, want %x", ci.DCID, dcid)
	}
	if !bytes.Equal(ci.SCID, scid) {
		t.Errorf("SCID = %x, want %x", ci.SCID, scid)
	}
	if len(ci.Token) != 0 {
		t.Errorf("Token = %x, want empty", ci.Token)
	}
	if !bytes.Equal(ci.CryptoData, clientHello) {
		t.Errorf("CryptoData = %q, want %q", ci.CryptoData, clientHello)
	}
}

// TestAcceptInitial_NotInitial rejects a non-Initial packet (a 1-RTT short header)
// with ErrNotInitial rather than trying to decrypt it.
func TestAcceptInitial_NotInitial(t *testing.T) {
	t.Parallel()
	// Short header: high bit clear (short form), fixed bit set.
	if _, err := AcceptInitial([]byte{0x40, 0x00, 0x00, 0x00, 0x00}); err != ErrNotInitial {
		t.Fatalf("AcceptInitial(short header) = %v, want ErrNotInitial", err)
	}
}

// TestAcceptInitial_Malformed surfaces the decode error for inputs that are not
// parseable packet headers.
func TestAcceptInitial_Malformed(t *testing.T) {
	t.Parallel()
	if _, err := AcceptInitial(nil); err == nil {
		t.Fatal("AcceptInitial(nil) = nil error, want a decode error")
	}
	if _, err := AcceptInitial([]byte{0xc0, 0x00, 0x00}); err == nil {
		t.Fatal("AcceptInitial(truncated long header) = nil error, want a decode error")
	}
}

// TestCryptoReassembler_OutOfOrderAndCap checks the reassembler stitches
// out-of-order CRYPTO frames by offset and rejects an offset past the cap.
func TestCryptoReassembler_OutOfOrderAndCap(t *testing.T) {
	t.Parallel()
	var c cryptoReassembler
	if err := c.OnCrypto(6, []byte("world")); err != nil {
		t.Fatalf("OnCrypto(6): %v", err)
	}
	if err := c.OnCrypto(0, []byte("hello ")); err != nil {
		t.Fatalf("OnCrypto(0): %v", err)
	}
	if got := string(c.assembled()); got != "hello world" {
		t.Errorf("assembled = %q, want %q", got, "hello world")
	}
	if err := c.OnCrypto(maxInitialCrypto, []byte{0x00}); err != ErrCryptoBufferExceeded {
		t.Errorf("OnCrypto past cap = %v, want ErrCryptoBufferExceeded", err)
	}
}

// TestSealPacket_RoundTrip seals a CRYPTO-carrying packet in each of the long
// (Initial, Handshake) and short (1-RTT) forms, then reparses the header and
// AEAD-opens it with the matching keys, recovering the packet number and the
// CRYPTO payload.
func TestSealPacket_RoundTrip(t *testing.T) {
	t.Parallel()
	dcid := []byte{0xaa, 0xbb, 0xcc}       // the client SCID = the server's reply DCID
	scid := []byte{0x01, 0x02, 0x03, 0x04} // the server's chosen SCID
	_, serverKeys := InitialKeys([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	sealer, err := NewSealer(serverKeys)
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	opener, err := NewOpener(serverKeys)
	if err != nil {
		t.Fatalf("NewOpener: %v", err)
	}
	crypto := []byte("server-handshake-flight-bytes")
	payload := AppendCrypto(nil, 0, crypto)

	cases := []struct {
		typ     PacketType
		dcidLen int // ParseHeader needs the local DCID length for a short header
	}{
		{PacketInitial, 0},
		{PacketHandshake, 0},
		{PacketShort, len(dcid)},
	}
	for _, tc := range cases {
		pkt, err := SealPacket(nil, sealer, tc.typ, dcid, scid, nil, 7, 4, payload)
		if err != nil {
			t.Fatalf("SealPacket(%v): %v", tc.typ, err)
		}
		hdr, err := ParseHeader(pkt, tc.dcidLen)
		if err != nil {
			t.Fatalf("ParseHeader(%v): %v", tc.typ, err)
		}
		if hdr.Type != tc.typ {
			t.Errorf("type = %v, want %v", hdr.Type, tc.typ)
		}
		pn, _, frames, err := opener.Open(pkt, hdr.PNOffset, 0)
		if err != nil {
			t.Fatalf("Open(%v): %v", tc.typ, err)
		}
		if pn != 7 {
			t.Errorf("%v: pn = %d, want 7", tc.typ, pn)
		}
		var cr cryptoReassembler
		if err := ParseFrames(frames, &cr); err != nil {
			t.Fatalf("ParseFrames(%v): %v", tc.typ, err)
		}
		if !bytes.Equal(cr.assembled(), crypto) {
			t.Errorf("%v: crypto = %q, want %q", tc.typ, cr.assembled(), crypto)
		}
	}
}

// TestSealPacket_PadsShortPayload checks that a payload too small for the header-
// protection sample is padded so the packet still seals and opens.
func TestSealPacket_PadsShortPayload(t *testing.T) {
	t.Parallel()
	_, keys := InitialKeys([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	sealer, _ := NewSealer(keys)
	opener, _ := NewOpener(keys)
	// One PING byte with a 1-byte packet number is under the 20-byte floor.
	pkt, err := SealPacket(nil, sealer, PacketHandshake, []byte{9, 9, 9, 9}, []byte{8, 8}, nil, 0, 1, []byte{0x01})
	if err != nil {
		t.Fatalf("SealPacket: %v", err)
	}
	hdr, err := ParseHeader(pkt, 0)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if _, _, _, err := opener.Open(pkt, hdr.PNOffset, 0); err != nil {
		t.Fatalf("Open padded packet: %v", err)
	}
}

// TestSealPacket_BadPNLen rejects an out-of-range packet-number length.
func TestSealPacket_BadPNLen(t *testing.T) {
	t.Parallel()
	_, keys := InitialKeys([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	sealer, _ := NewSealer(keys)
	if _, err := SealPacket(nil, sealer, PacketHandshake, []byte{1}, []byte{2}, nil, 0, 5, []byte{0x01}); err != ErrPacketEncoding {
		t.Fatalf("SealPacket(pnLen=5) = %v, want ErrPacketEncoding", err)
	}
}

// TestStartServerHandshake_FullHandshake drives a real client Conn against the
// server-role primitives over an in-memory datagram channel and completes the
// handshake on BOTH sides: the client's TLS handshake completes against the
// server's flight, and the server completes when fed the client's Finished,
// installing 1-RTT keys. End-to-end cover for AcceptInitial + StartServerHandshake
// + HandleClientHandshake + SealPacket + NewServerHandshake.
// setupServerConn runs a full handshake between a real client Conn and the
// server-role primitives, completes the server side, and wraps it in a server
// Conn via NewServerConn. It returns the connected client and server connections
// and the in-memory datagram channels between them.
func setupServerConn(t *testing.T) (*Conn, *Conn, chan []byte, chan []byte) {
	t.Helper()
	cert, pool := genServerCert(t)
	clientTP := concat(
		tpInt(tpInitialMaxData, 1<<20),
		tpInt(tpInitialMaxStreamDataBidiRemote, 1<<20),
		tpInt(tpInitialMaxStreamDataBidiLocal, 1<<20), // server's send limit on the request stream
		tpInt(tpInitialMaxStreamsBidi, 16),
	)
	serverSCID := []byte{0xab, 0xcd, 0xef}

	toServer := make(chan []byte, 16)
	fromServer := make(chan []byte, 16)
	clientPC := &chanPC{rx: fromServer, tx: toServer}

	client, err := NewConn(clientPC, &tls.Config{ServerName: "example.com", RootCAs: pool}, clientTP)
	if err != nil {
		t.Fatal(err)
	}
	// The server authenticates against the client's original DCID and advertises
	// its own source CID (RFC 9000 §7.3).
	serverTP := concat(clientTP,
		tpBytes(tpInitialSourceConnectionID, serverSCID),
		tpBytes(tpOriginalDestinationConnectionID, client.origDCID))

	var flight *ServerFlight
	serverErr := make(chan error, 1)
	go func() {
		dg := <-toServer // the client's Initial datagram
		ci, err := AcceptInitial(dg)
		if err != nil {
			serverErr <- err
			return
		}
		flight, err = StartServerHandshake(ci, &tls.Config{Certificates: []tls.Certificate{cert}}, serverTP, serverSCID)
		if err != nil {
			serverErr <- err
			return
		}
		for _, d := range flight.Datagrams {
			fromServer <- d
		}
		serverErr <- nil
	}()

	if err := client.Establish(context.Background()); err != nil {
		t.Fatalf("client Establish: %v", err)
	}
	if !client.handshakeComplete {
		t.Fatal("client handshake did not complete against StartServerHandshake")
	}
	if err := <-serverErr; err != nil { // publishes flight (happens-before)
		t.Fatalf("server: %v", err)
	}
	if flight.HandshakeSealer == nil || flight.HandshakeOpener == nil {
		t.Fatal("server did not install Handshake keys")
	}
	if len(flight.PeerTransportParams) == 0 {
		t.Fatal("server did not capture the client's transport parameters")
	}
	if _, err := ParseTransportParams(flight.PeerTransportParams); err != nil {
		t.Fatalf("captured client transport params do not parse: %v", err)
	}

	// The client sent its Handshake Finished during Establish; drain it and
	// complete the server side.
	var clientHS []byte
drain:
	for {
		select {
		case dg := <-toServer:
			clientHS = append(clientHS, extractHandshakeCrypto(t, dg, flight.HandshakeOpener)...)
		default:
			break drain
		}
	}
	if len(clientHS) == 0 {
		t.Fatal("captured no client Handshake CRYPTO to complete the server")
	}
	if err := flight.HandleClientHandshake(clientHS); err != nil {
		t.Fatalf("HandleClientHandshake: %v", err)
	}
	if !flight.Complete {
		t.Fatal("server handshake did not complete")
	}
	if flight.AppSealer == nil || flight.AppOpener == nil {
		t.Fatal("server did not install 1-RTT keys")
	}

	// Wrap the completed handshake into a connected server Conn.
	sc, err := NewServerConn(&chanPC{rx: toServer, tx: fromServer}, flight, client.origDCID, client.scid)
	if err != nil {
		t.Fatalf("NewServerConn: %v", err)
	}
	return client, sc, toServer, fromServer
}

// TestStartServerHandshake_FullHandshake checks the connected state of a server
// connection built from a completed handshake against a real client.
func TestStartServerHandshake_FullHandshake(t *testing.T) {
	client, sc, _, _ := setupServerConn(t)
	if !client.handshakeComplete {
		t.Fatal("client handshake did not complete")
	}
	if !sc.isServer {
		t.Error("NewServerConn: isServer = false")
	}
	if sc.oneRTTSealer == nil || sc.keys.OneRTT == nil {
		t.Error("NewServerConn: 1-RTT keys not installed")
	}
	if !sc.handshakeComplete {
		t.Error("NewServerConn: handshakeComplete = false")
	}
	if sc.connMax == 0 {
		t.Error("NewServerConn: connMax (peer InitialMaxData) not seeded")
	}
}

// TestServerConn_RequestResponseRoundTrip drives a full 1-RTT request/response
// between a real client Conn and a server Conn.
func TestServerConn_RequestResponseRoundTrip(t *testing.T) {
	client, sc, _, fromServer := setupServerConn(t)

	// Request: the client seals a STREAM frame on bidi stream 0 with its real 1-RTT
	// sealer; the server decrypts it, accepts the request stream, and reads it.
	req := AppendStream(nil, 0, 0, true, []byte("GET /"))
	pkt, err := SealPacket(nil, client.oneRTTSealer, PacketShort, sc.scid, nil, nil, 0, 4, req)
	if err != nil {
		t.Fatalf("seal client 1-RTT request: %v", err)
	}
	res, err := ProcessDatagram(pkt, len(sc.scid), &sc.keys, func(PacketType) uint64 { return 0 }, &connFrameHandler{c: sc, space: spaceApp})
	if err != nil {
		t.Fatalf("server ProcessDatagram: %v", err)
	}
	if res.Processed != 1 {
		t.Fatalf("server processed %d packets, want 1 (%+v)", res.Processed, res)
	}
	rs := sc.AcceptBidiStream()
	if rs == nil || rs.ID() != 0 {
		t.Fatalf("AcceptBidiStream = %v, want request stream 0", rs)
	}
	if got := string(rs.Recv()); got != "GET /" {
		t.Fatalf("server read request %q, want %q", got, "GET /")
	}

	// Response: the server writes on the request stream via its real send path; the
	// client opens its side, decrypts the response, and reads it.
	reqStream, err := client.OpenStream()
	if err != nil {
		t.Fatalf("client OpenStream: %v", err)
	}
	for drained := false; !drained; { // clear any leftover handshake datagrams
		select {
		case <-fromServer:
		default:
			drained = true
		}
	}
	if _, err := rs.Send([]byte("200 OK"), true); err != nil {
		t.Fatalf("server Send response: %v", err)
	}
	respDg := <-fromServer
	rres, err := ProcessDatagram(respDg, len(client.scid), &client.keys, func(PacketType) uint64 { return 0 }, &connFrameHandler{c: client, space: spaceApp})
	if err != nil {
		t.Fatalf("client ProcessDatagram(response): %v", err)
	}
	if rres.Processed != 1 {
		t.Fatalf("client processed %d response packets, want 1 (%+v)", rres.Processed, rres)
	}
	if got := string(reqStream.Recv()); got != "200 OK" {
		t.Fatalf("client read response %q, want %q", got, "200 OK")
	}
}

// TestServerConn_AcceptsClientStreams checks a server connection accepts a
// client-initiated bidirectional stream (a request) and a client-initiated
// unidirectional stream (control/QPACK), rejects its own send-only streams, and
// enforces the advertised bidi limit.
func TestServerConn_AcceptsClientStreams(t *testing.T) {
	t.Parallel()
	c := &Conn{
		isServer:            true,
		localMaxStreamsBidi: 3,
		localMaxStreamsUni:  3,
		peer:                TransportParams{InitialMaxStreamDataBidiLocal: 1 << 20},
		// connRecvMax left 0 disables receive flow control for this hand-built conn.
	}
	h := &connFrameHandler{c: c}

	// Client-initiated bidi stream 0 is accepted as a request.
	if err := h.OnStream(0, 0, false, []byte("request")); err != nil {
		t.Fatalf("OnStream(client bidi 0): %v", err)
	}
	if c.streams[0] == nil {
		t.Fatal("client bidi stream 0 was not created")
	}
	if c.streams[0].sendMax != 1<<20 {
		t.Errorf("sendMax = %d, want %d (client bidi_local)", c.streams[0].sendMax, 1<<20)
	}
	if got := c.AcceptBidiStream(); got == nil || got.ID() != 0 {
		t.Fatalf("AcceptBidiStream = %v, want stream 0", got)
	}
	if c.AcceptBidiStream() != nil {
		t.Fatal("AcceptBidiStream returned an unexpected second stream")
	}

	// Client-initiated uni stream 2 is accepted (control/QPACK).
	if err := h.OnStream(2, 0, false, []byte{0x00}); err != nil {
		t.Fatalf("OnStream(client uni 2): %v", err)
	}
	if got := c.AcceptUniStream(); got == nil || got.ID() != 2 {
		t.Fatalf("AcceptUniStream = %v, want stream 2", got)
	}

	// The server's own send-only stream (server uni, id 3) must reject inbound STREAM.
	if err := h.OnStream(3, 0, false, []byte{0x00}); err != ErrStreamState {
		t.Fatalf("OnStream(server uni 3) = %v, want ErrStreamState", err)
	}

	// A request stream past the advertised bidi limit is a STREAM_LIMIT_ERROR.
	if err := h.OnStream(12, 0, false, []byte("x")); err != ErrTooManyBidiStreams {
		t.Fatalf("OnStream(over-limit bidi 12) = %v, want ErrTooManyBidiStreams", err)
	}
}

// extractHandshakeCrypto walks the packets in a datagram, decrypts each Handshake
// packet with opener, and returns the reassembled CRYPTO stream (the client's
// Finished), ignoring Initial/1-RTT packets and non-CRYPTO frames.
func extractHandshakeCrypto(t *testing.T, datagram []byte, opener *Opener) []byte {
	t.Helper()
	var out []byte
	for off := 0; off < len(datagram); {
		hdr, err := ParseHeader(datagram[off:], 0)
		if err != nil || hdr.PacketLen <= 0 {
			break
		}
		if hdr.Type == PacketHandshake {
			if _, _, payload, err := opener.Open(datagram[off:off+hdr.PacketLen], hdr.PNOffset, 0); err == nil {
				var cr cryptoReassembler
				if err := ParseFrames(payload, &cr); err == nil {
					out = append(out, cr.assembled()...)
				}
			}
		}
		off += hdr.PacketLen
	}
	return out
}
