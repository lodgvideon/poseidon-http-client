package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAcceptInitial_RoundTrip builds a real client Initial with buildInitialPacket
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
	require.NoError(t, err, "NewSealer with the client Initial keys")
	pkt, err := buildInitialPacket(nil, sealer, dcid, scid, nil, 0, 4, 0, clientHello, InitialDatagramMinSize)
	require.NoError(t, err, "buildInitialPacket")

	ci, err := AcceptInitial(pkt)

	require.NoError(t, err, "AcceptInitial on a well-formed client Initial")
	assert.Truef(t, bytes.Equal(ci.DCID, dcid), "DCID = %x, want %x", ci.DCID, dcid)
	assert.Truef(t, bytes.Equal(ci.SCID, scid), "SCID = %x, want %x", ci.SCID, scid)
	assert.Emptyf(t, ci.Token, "Token = %x, want empty", ci.Token)
	assert.Truef(t, bytes.Equal(ci.CryptoData, clientHello),
		"CryptoData = %q, want %q", ci.CryptoData, clientHello)
}

// TestAcceptInitial_NotInitial rejects a non-Initial packet (a 1-RTT short header)
// with ErrNotInitial rather than trying to decrypt it.
func TestAcceptInitial_NotInitial(t *testing.T) {
	t.Parallel()
	// Short header: high bit clear (short form), fixed bit set.
	shortHeader := []byte{0x40, 0x00, 0x00, 0x00, 0x00}

	_, err := AcceptInitial(shortHeader)

	assert.Truef(t, err == ErrNotInitial,
		"AcceptInitial(short header) = %v, want ErrNotInitial", err)
}

// TestAcceptInitial_Malformed surfaces the decode error for inputs that are not
// parseable packet headers.
func TestAcceptInitial_Malformed(t *testing.T) {
	t.Parallel()
	_, errNil := AcceptInitial(nil)
	_, errTruncated := AcceptInitial([]byte{0xc0, 0x00, 0x00})

	assert.Error(t, errNil, "AcceptInitial(nil) = nil error, want a decode error")
	assert.Error(t, errTruncated,
		"AcceptInitial(truncated long header) = nil error, want a decode error")
}

// TestCryptoReassembler_OutOfOrderAndCap checks the reassembler stitches
// out-of-order CRYPTO frames by offset and rejects an offset past the cap.
func TestCryptoReassembler_OutOfOrderAndCap(t *testing.T) {
	t.Parallel()
	var c cryptoReassembler

	errHigh := c.OnCrypto(6, []byte("world")) // the later fragment arrives first
	errLow := c.OnCrypto(0, []byte("hello "))
	errPastCap := c.OnCrypto(maxInitialCrypto, []byte{0x00})

	require.NoError(t, errHigh, "OnCrypto(6)")
	require.NoError(t, errLow, "OnCrypto(0)")
	assert.Equalf(t, "hello world", string(c.assembled()),
		"assembled = %q, want %q", string(c.assembled()), "hello world")
	assert.Truef(t, errPastCap == ErrCryptoBufferExceeded,
		"OnCrypto past cap = %v, want ErrCryptoBufferExceeded", errPastCap)
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
	require.NoError(t, err, "NewSealer with the server Initial keys")
	opener, err := NewOpener(serverKeys)
	require.NoError(t, err, "NewOpener with the server Initial keys")
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

		require.NoErrorf(t, err, "SealPacket(%v)", tc.typ)
		hdr, err := ParseHeader(pkt, tc.dcidLen)
		require.NoErrorf(t, err, "ParseHeader(%v)", tc.typ)
		assert.Equalf(t, tc.typ, hdr.Type, "type = %v, want %v", hdr.Type, tc.typ)
		pn, _, frames, err := opener.Open(pkt, hdr.PNOffset, 0)
		require.NoErrorf(t, err, "Open(%v)", tc.typ)
		assert.EqualValuesf(t, 7, pn, "%v: pn = %d, want 7", tc.typ, pn)
		var cr cryptoReassembler
		require.NoErrorf(t, ParseFrames(frames, &cr), "ParseFrames(%v)", tc.typ)
		assert.Truef(t, bytes.Equal(cr.assembled(), crypto),
			"%v: crypto = %q, want %q", tc.typ, cr.assembled(), crypto)
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

	require.NoError(t, err, "SealPacket with a payload under the header-protection sample")
	hdr, err := ParseHeader(pkt, 0)
	require.NoError(t, err, "ParseHeader of the padded packet")
	_, _, _, err = opener.Open(pkt, hdr.PNOffset, 0)
	assert.NoError(t, err, "Open padded packet: the pad must keep the packet openable")
}

// TestSealPacket_BadPNLen rejects an out-of-range packet-number length.
func TestSealPacket_BadPNLen(t *testing.T) {
	t.Parallel()
	_, keys := InitialKeys([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	sealer, _ := NewSealer(keys)

	_, err := SealPacket(nil, sealer, PacketHandshake, []byte{1}, []byte{2}, nil, 0, 5, []byte{0x01})

	assert.Truef(t, err == ErrPacketEncoding,
		"SealPacket(pnLen=5) = %v, want ErrPacketEncoding", err)
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
	return setupServerConnWith(t, nil)
}

// setupServerConnWith is setupServerConn with a hook that may wrap the client's
// PacketConn before the handshake starts, so a test can inject datagram faults
// (see faultpc_test.go). A nil wrap is the plain, fault-free path.
func setupServerConnWith(t *testing.T, wrap func(PacketConn) PacketConn) (*Conn, *Conn, chan []byte, chan []byte) {
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
	var clientPC PacketConn = &chanPC{rx: fromServer, tx: toServer}
	if wrap != nil {
		clientPC = wrap(clientPC)
	}

	client, err := NewConn(clientPC, &tls.Config{ServerName: "example.com", RootCAs: pool}, clientTP)
	require.NoError(t, err, "NewConn for the client half of the handshake fixture")
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

	require.NoError(t, client.Establish(context.Background()), "client Establish")
	require.True(t, client.handshakeComplete,
		"client handshake did not complete against StartServerHandshake")
	require.NoError(t, <-serverErr, "server side of the handshake") // publishes flight (happens-before)
	require.True(t, flight.HandshakeSealer != nil && flight.HandshakeOpener != nil,
		"server did not install Handshake keys")
	require.NotEmpty(t, flight.PeerTransportParams,
		"server did not capture the client's transport parameters")
	_, err = ParseTransportParams(flight.PeerTransportParams)
	require.NoError(t, err, "captured client transport params do not parse")

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
	require.NotEmpty(t, clientHS, "captured no client Handshake CRYPTO to complete the server")
	require.NoError(t, flight.HandleClientHandshake(clientHS), "HandleClientHandshake")
	require.True(t, flight.Complete, "server handshake did not complete")
	require.True(t, flight.AppSealer != nil && flight.AppOpener != nil,
		"server did not install 1-RTT keys")

	// Wrap the completed handshake into a connected server Conn.
	sc, err := NewServerConn(&chanPC{rx: toServer, tx: fromServer}, flight, client.origDCID, client.scid)
	require.NoError(t, err, "NewServerConn")
	return client, sc, toServer, fromServer
}

// TestStartServerHandshake_FullHandshake checks the connected state of a server
// connection built from a completed handshake against a real client.
func TestStartServerHandshake_FullHandshake(t *testing.T) {
	client, sc, _, _ := setupServerConn(t)

	require.True(t, client.handshakeComplete, "client handshake did not complete")
	assert.True(t, sc.isServer, "NewServerConn: isServer = false")
	assert.True(t, sc.oneRTTSealer != nil && sc.keys.OneRTT != nil,
		"NewServerConn: 1-RTT keys not installed")
	assert.True(t, sc.handshakeComplete, "NewServerConn: handshakeComplete = false")
	assert.NotZero(t, sc.connMax, "NewServerConn: connMax (peer InitialMaxData) not seeded")
}

// TestServerConn_RequestResponseRoundTrip drives a full 1-RTT request/response
// between a real client Conn and a server Conn.
func TestServerConn_RequestResponseRoundTrip(t *testing.T) {
	client, sc, _, fromServer := setupServerConn(t)

	// Request: the client seals a STREAM frame on bidi stream 0 with its real 1-RTT
	// sealer; the server decrypts it, accepts the request stream, and reads it.
	req := AppendStream(nil, 0, 0, true, []byte("GET /"))
	pkt, err := SealPacket(nil, client.oneRTTSealer, PacketShort, sc.scid, nil, nil, 0, 4, req)
	require.NoError(t, err, "seal client 1-RTT request")
	res, err := processDatagram(pkt, len(sc.scid), &sc.keys, func(PacketType) uint64 { return 0 }, &connFrameHandler{c: sc, space: spaceApp})
	require.NoError(t, err, "server processDatagram")
	require.EqualValuesf(t, 1, res.Processed,
		"server processed %d packets, want 1 (%+v)", res.Processed, res)
	rs := sc.AcceptBidiStream()
	require.NotNil(t, rs, "AcceptBidiStream = nil, want request stream 0")
	require.EqualValuesf(t, 0, rs.ID(), "AcceptBidiStream = stream %d, want request stream 0", rs.ID())
	assert.Equal(t, "GET /", string(rs.Recv()), "the server must read back the request body")

	// Response: the server writes on the request stream via its real send path; the
	// client opens its side, decrypts the response, and reads it.
	reqStream, err := client.OpenStream()
	require.NoError(t, err, "client OpenStream")
	for drained := false; !drained; { // clear any leftover handshake datagrams
		select {
		case <-fromServer:
		default:
			drained = true
		}
	}
	_, err = rs.Send([]byte("200 OK"), true)
	require.NoError(t, err, "server Send response")
	respDg := <-fromServer
	rres, err := processDatagram(respDg, len(client.scid), &client.keys, func(PacketType) uint64 { return 0 }, &connFrameHandler{c: client, space: spaceApp})

	require.NoError(t, err, "client processDatagram(response)")
	require.EqualValuesf(t, 1, rres.Processed,
		"client processed %d response packets, want 1 (%+v)", rres.Processed, rres)
	assert.Equal(t, "200 OK", string(reqStream.Recv()),
		"the client must read back the response body the server sent")
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
	errBidi := h.OnStream(0, 0, false, []byte("request"))
	// Client-initiated uni stream 2 is accepted (control/QPACK).
	errUni := h.OnStream(2, 0, false, []byte{0x00})
	// The server's own send-only stream (server uni, id 3) must reject inbound STREAM.
	errOwnUni := h.OnStream(3, 0, false, []byte{0x00})
	// A request stream past the advertised bidi limit is a STREAM_LIMIT_ERROR.
	errOverLimit := h.OnStream(12, 0, false, []byte("x"))

	require.NoError(t, errBidi, "OnStream(client bidi 0)")
	require.NotNil(t, c.streams[0], "client bidi stream 0 was not created")
	assert.EqualValuesf(t, 1<<20, c.streams[0].sendMax,
		"sendMax = %d, want %d (client bidi_local)", c.streams[0].sendMax, 1<<20)
	gotBidi := c.AcceptBidiStream()
	require.NotNil(t, gotBidi, "AcceptBidiStream = nil, want stream 0")
	assert.EqualValuesf(t, 0, gotBidi.ID(), "AcceptBidiStream = stream %d, want stream 0", gotBidi.ID())
	assert.Truef(t, c.AcceptBidiStream() == nil,
		"AcceptBidiStream returned an unexpected second stream")
	require.NoError(t, errUni, "OnStream(client uni 2)")
	gotUni := c.AcceptUniStream()
	require.NotNil(t, gotUni, "AcceptUniStream = nil, want stream 2")
	assert.EqualValuesf(t, 2, gotUni.ID(), "AcceptUniStream = stream %d, want stream 2", gotUni.ID())
	assert.Truef(t, errOwnUni == ErrStreamState,
		"OnStream(server uni 3) = %v, want ErrStreamState", errOwnUni)
	assert.Truef(t, errOverLimit == ErrTooManyBidiStreams,
		"OnStream(over-limit bidi 12) = %v, want ErrTooManyBidiStreams", errOverLimit)
}

// TestStreamIDsFollowRole pins role-aware stream-ID assignment (RFC 9000 §2.1): a
// server opens unidirectional streams 3, 7, 11 … and bidirectional 1, 5, 9 …,
// while a client opens 2, 6, 10 … and 0, 4, 8 …. Opening under the other role's
// IDs makes the peer close the connection as a protocol violation.
func TestStreamIDsFollowRole(t *testing.T) {
	t.Parallel()
	limits := TransportParams{InitialMaxStreamsUni: 3, InitialMaxStreamsBidi: 3}

	server := &Conn{isServer: true, nextBidiStreamID: 1, peer: limits}
	client := &Conn{peer: limits} // isServer false, nextBidiStreamID 0

	serverUni := openN(t, server.OpenUniStream, 3)
	serverBidi := openN(t, server.OpenStream, 3)
	clientUni := openN(t, client.OpenUniStream, 3)
	clientBidi := openN(t, client.OpenStream, 3)

	assert.Equal(t, []uint64{3, 7, 11}, serverUni, "server unidirectional stream IDs (§2.1)")
	assert.Equal(t, []uint64{1, 5, 9}, serverBidi, "server bidirectional stream IDs (§2.1)")
	assert.Equal(t, []uint64{2, 6, 10}, clientUni, "client unidirectional stream IDs (§2.1)")
	assert.Equal(t, []uint64{0, 4, 8}, clientBidi, "client bidirectional stream IDs (§2.1)")
}

// openN opens n streams through open and returns their IDs in order.
func openN(t *testing.T, open func() (*Stream, error), n int) []uint64 {
	t.Helper()
	ids := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		s, err := open()
		require.NoErrorf(t, err, "opening stream %d of %d", i+1, n)
		ids = append(ids, s.ID())
	}
	return ids
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
