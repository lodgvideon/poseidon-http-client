// Command faultserver is a test-only HTTP/3 server that deliberately misbehaves,
// so the from-scratch client's negative paths can be exercised against a real
// QUIC stack. It is built on quic-go's raw QUIC API (not its http3.Server) so it
// can control exact frame bytes and stream error codes.
//
// It routes on the request :path (decoded with quic-go's qpack), one fault per
// path (RFC 9114 references):
//   - /data-before-headers → a DATA frame before any HEADERS (§4.1 →
//     H3_FRAME_UNEXPECTED)
//   - /trunc-headers        → a HEADERS frame whose declared length is never
//     satisfied before the clean FIN (§7.1 → H3_FRAME_ERROR)
//   - /settings-on-request  → a SETTINGS frame on a request stream (§7.2.4 →
//     H3_FRAME_UNEXPECTED)
//   - /stop-sending         → STOP_SENDING the request while the client is still
//     sending its body, then a valid 200 (§4.1: the client reads the response)
//   - anything else (incl. "/") → RESET_STREAM with H3_REQUEST_REJECTED (§8.1),
//     which the client surfaces as a retryable StreamResetError (§4.1.1)
//
// It lives in its own module so quic-go is not a dependency of the client. Run in
// the compose harness; env: CERT/KEY (default /certs/{cert,key}.pem),
// ADDR (default :443).
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"log"
	"os"

	"github.com/quic-go/qpack"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// H3 frame types (RFC 9114 §7.2) and the H3_REQUEST_REJECTED code (§8.1) the
// client classifies as retryable.
const (
	frameData         = 0x00
	frameHeaders      = 0x01
	frameSettings     = 0x04
	h3RequestRejected = 0x010b
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	cert, err := tls.LoadX509KeyPair(env("CERT", "/certs/cert.pem"), env("KEY", "/certs/key.pem"))
	if err != nil {
		log.Fatalf("faultserver: load cert: %v", err)
	}
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h3"},
		MinVersion:   tls.VersionTLS13,
	}
	addr := env("ADDR", ":443")
	ln, err := quic.ListenAddr(addr, tlsConf, &quic.Config{})
	if err != nil {
		log.Fatalf("faultserver: listen: %v", err)
	}
	log.Printf("faultserver: listening %s (path-routed faults)", addr)

	for {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			log.Fatalf("faultserver: accept: %v", err)
		}
		go handle(conn)
	}
}

func handle(conn *quic.Conn) {
	ctx := context.Background()

	// A conformant server opens its control stream and sends SETTINGS first
	// (RFC 9114 §6.2.1): stream type 0x00, then an empty SETTINGS frame
	// (type 0x04, length 0). This client does not strictly require it — it only
	// enforces SETTINGS-first once a control stream exists — but sending it keeps
	// the server realistic. OpenUniStreamSync needs the client's uni-stream
	// credit, which it grants (initial_max_streams_uni).
	if ctrl, err := conn.OpenUniStreamSync(ctx); err != nil {
		log.Printf("faultserver: open control stream: %v", err)
	} else {
		_, _ = ctrl.Write([]byte{0x00, frameSettings, 0x00})
	}
	// Drain the client's unidirectional streams (its control + QPACK streams) so
	// their flow control does not stall.
	go func() {
		for {
			s, err := conn.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, s) }()
		}
	}()

	for {
		s, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go serve(s)
	}
}

func serve(s *quic.Stream) {
	switch requestPath(s) {
	case "/data-before-headers":
		_, _ = s.Write(dataFrame([]byte("early")))
		_ = s.Close()
	case "/trunc-headers":
		// A HEADERS frame declaring 100 bytes, but only 2 sent before the FIN
		// (100 needs a 2-byte varint, so encode it — the client must decode that).
		hdr := quicvarint.Append([]byte{frameHeaders}, 100)
		_, _ = s.Write(append(hdr, 0xaa, 0xbb))
		_ = s.Close()
	case "/settings-on-request":
		_, _ = s.Write([]byte{frameSettings, 0x00}) // SETTINGS, length 0
		_ = s.Close()
	case "/stop-sending":
		// Abort reading the request (STOP_SENDING) while the client is still
		// sending its body, but still send a valid response so the client — whose
		// body send returns ErrStreamReset — reads the response anyway (§4.1).
		s.CancelRead(quic.StreamErrorCode(h3RequestRejected))
		_, _ = s.Write(headersFrame(qpack.HeaderField{Name: ":status", Value: "200"}))
		_ = s.Close()
	default:
		s.CancelWrite(quic.StreamErrorCode(h3RequestRejected))
		s.CancelRead(quic.StreamErrorCode(h3RequestRejected))
	}
}

// requestPath reads the request's HEADERS frame off the stream and returns its
// :path, or "" if it cannot be read (in which case the default reset applies).
func requestPath(s *quic.Stream) string {
	br := bufio.NewReader(s)
	typ, err := quicvarint.Read(br)
	if err != nil || typ != frameHeaders {
		return ""
	}
	length, err := quicvarint.Read(br)
	if err != nil || length > 64<<10 { // guard against a hostile field-section length
		return ""
	}
	field := make([]byte, length)
	if _, err := io.ReadFull(br, field); err != nil {
		return ""
	}
	next := qpack.NewDecoder().Decode(field)
	for {
		hf, err := next()
		if err != nil { // io.EOF when the field section is exhausted, or a decode error
			return ""
		}
		if hf.Name == ":path" {
			return hf.Value
		}
	}
}

// dataFrame encodes an HTTP/3 DATA frame (RFC 9114 §7.2.1).
func dataFrame(data []byte) []byte {
	out := quicvarint.Append([]byte{frameData}, uint64(len(data)))
	return append(out, data...)
}

// headersFrame QPACK-encodes the fields (static table only — the field-section
// prefix is written by the encoder) into an HTTP/3 HEADERS frame (§7.2.2).
func headersFrame(fields ...qpack.HeaderField) []byte {
	var buf bytes.Buffer
	enc := qpack.NewEncoder(&buf)
	for i := range fields {
		_ = enc.WriteField(fields[i])
	}
	_ = enc.Close()
	return append(quicvarint.Append([]byte{frameHeaders}, uint64(buf.Len())), buf.Bytes()...)
}
