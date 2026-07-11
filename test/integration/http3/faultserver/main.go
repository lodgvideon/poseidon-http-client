// Command faultserver is a test-only HTTP/3 server that deliberately misbehaves,
// so the from-scratch client's negative paths can be exercised against a real
// QUIC stack. It is built on quic-go's raw QUIC API (not its http3.Server) so it
// can control exact frame bytes and stream error codes.
//
// FAULT selects the misbehaviour (default "reset"):
//   - reset: accept each request stream and RESET_STREAM it with
//     H3_REQUEST_REJECTED (0x010b), so the client surfaces a retryable
//     StreamResetError (RFC 9114 §4.1.1, §8.1).
//
// It lives in its own module so quic-go is not a dependency of the client.
// Run in the compose harness; env: CERT/KEY (default /certs/{cert,key}.pem),
// ADDR (default :443), FAULT.
package main

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"os"

	"github.com/quic-go/quic-go"
)

// h3RequestRejected is HTTP/3 H3_REQUEST_REJECTED (RFC 9114 §8.1) — the client
// classifies a reset with this code as retryable.
const h3RequestRejected = 0x010b

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
	fault := env("FAULT", "reset")

	ln, err := quic.ListenAddr(env("ADDR", ":443"), tlsConf, &quic.Config{})
	if err != nil {
		log.Fatalf("faultserver: listen: %v", err)
	}
	log.Printf("faultserver: listening %s, fault=%q", env("ADDR", ":443"), fault)

	for {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			log.Fatalf("faultserver: accept: %v", err)
		}
		go handle(conn, fault)
	}
}

func handle(conn *quic.Conn, fault string) {
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
		_, _ = ctrl.Write([]byte{0x00, 0x04, 0x00})
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
		go serve(s, fault)
	}
}

func serve(s *quic.Stream, fault string) {
	switch fault {
	default: // "reset"
		// Abort the response with RESET_STREAM and stop reading the request with
		// STOP_SENDING, both carrying H3_REQUEST_REJECTED.
		s.CancelWrite(quic.StreamErrorCode(h3RequestRejected))
		s.CancelRead(quic.StreamErrorCode(h3RequestRejected))
	}
}
