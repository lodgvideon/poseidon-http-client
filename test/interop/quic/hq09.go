package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"os"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// hqALPN is the ALPN token the interop runner uses for HTTP/0.9 over QUIC. Its
// servers offer nothing else on every test case except "http3", so this is what
// the handshake must ask for.
const hqALPN = "hq-interop"

// udpRecvBuffer enlarges the kernel receive buffer so a server's send burst is
// held rather than dropped while the single reader goroutine drains it one
// datagram at a time. Best-effort, and the same size the library's own HTTP/3
// dialler asks for.
const udpRecvBuffer = 4 << 20

// hqTransportParams are the limits advertised on an HTTP/0.9 connection. HTTP/0.9
// has no control or QPACK streams, so unlike the HTTP/3 profile nothing here has
// to admit a server's mandatory unidirectional streams — but the allowance is
// kept, because a peer is free to open a GREASE unidirectional stream and being
// unable to accept it would close the connection.
var hqTransportParams = quic.LocalTransportParams{
	InitialMaxData:                quic.DefaultConnRecvWindow,
	InitialMaxStreamDataBidiLocal: quic.DefaultStreamRecvWindow,
	InitialMaxStreamDataUni:       quic.DefaultStreamRecvWindow,
	InitialMaxStreamsUni:          3,
	MaxIdleTimeout:                30000, // 30 s (RFC 9000 §10.1), in milliseconds
}

// hqConn is one established HTTP/0.9-over-QUIC connection together with the
// reader goroutine driving its receive path. The QUIC layer parks that goroutine
// in Poll, which is also what runs loss detection and probe timeouts, so nothing
// arrives — and no retransmission goes out — while it is not running.
type hqConn struct {
	conn   *quic.Conn
	cancel context.CancelFunc
	done   chan struct{}
}

// hqDial opens a QUIC connection to addr and completes the handshake. base
// supplies the key log; ServerName, ALPN and the TLS floor are set here.
//
// The connected *net.UDPConn is handed to the QUIC engine directly: quic.PacketConn
// is only Read/Write/Close, and the engine type-asserts SetReadDeadline on top of
// it to arm probe and idle timers, which *net.UDPConn already provides. No adapter
// is needed, and the offload wrappers the library's HTTP/3 dialler adds would buy
// nothing at simulator bandwidths.
func hqDial(ctx context.Context, u *url.URL, base *tls.Config) (*hqConn, error) {
	raddr, err := net.ResolveUDPAddr("udp", hostPort(u))
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", hostPort(u), err)
	}
	uc, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", raddr, err)
	}
	_ = uc.SetReadBuffer(udpRecvBuffer)

	cfg := base.Clone()
	cfg.ServerName = u.Hostname()
	cfg.NextProtos = []string{hqALPN}
	cfg.MinVersion = tls.VersionTLS13

	conn, err := quic.NewConn(uc, cfg, quic.AppendTransportParams(nil, hqTransportParams))
	if err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("new conn: %w", err)
	}
	if err := conn.Establish(ctx); err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("handshake: %w", err)
	}

	readCtx, cancel := context.WithCancel(context.Background())
	h := &hqConn{conn: conn, cancel: cancel, done: make(chan struct{})}
	go h.readLoop(readCtx)
	return h, nil
}

// readLoop drives the connection's receive path — inbound packets, ACKs, loss
// detection and probe timeouts — until the connection ends. Poll returning an
// error is terminal for the connection; a request blocked on the stream wakes
// with that error through the QUIC layer's own latch, so there is nothing to
// report here.
func (h *hqConn) readLoop(ctx context.Context) {
	defer close(h.done)
	for {
		if err := h.conn.Poll(ctx); err != nil {
			return
		}
	}
}

// close ends the connection with a NO_ERROR CONNECTION_CLOSE and waits for the
// reader goroutine, so the next connection in a multiconnect run does not start
// while the previous one is still reading. Idempotent.
func (h *hqConn) close() {
	_ = h.conn.Close()
	h.cancel()
	<-h.done
}

// get performs one HTTP/0.9 request and writes the response body to dir.
//
// The wire format has no headers and no status line (this is what the runner's
// hq-interop servers speak): open a client-initiated bidirectional stream, write
// "GET <path>\r\n", close the send side, and read the response body as raw bytes
// until the FIN.
func (h *hqConn) get(ctx context.Context, u *url.URL, dir string) (err error) {
	stream, err := h.conn.OpenStreamContext(ctx)
	if err != nil {
		return fmt.Errorf("open stream for %s: %w", u.Path, err)
	}
	// The FIN closes the send side, which is what tells an HTTP/0.9 server the
	// request line is complete. Send consumes a prefix and returns a nil error
	// when flow control blocks, carrying the FIN only on the frame holding the
	// last byte — so the loop retries with fin still set until everything is in.
	req := []byte("GET " + u.Path + "\r\n")
	for sent := 0; sent < len(req); {
		n, serr := stream.Send(req[sent:], true)
		if serr != nil {
			return fmt.Errorf("send request for %s: %w", u.Path, serr)
		}
		sent += n
		if sent < len(req) {
			if werr := stream.WaitSendable(ctx); werr != nil {
				return fmt.Errorf("send request for %s: %w", u.Path, werr)
			}
		}
	}

	f, err := os.Create(downloadPath(dir, u))
	if err != nil {
		return fmt.Errorf("create download for %s: %w", u.Path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	for {
		// State BEFORE the drain, and that order is load-bearing. "Finished"
		// means every byte has ARRIVED, not that every byte has been read
		// (quic.recvStream.complete counts bytes already taken via base), so a
		// datagram landing between a drain and the state read would make the
		// stream look finished with data still buffered — and the body would be
		// silently truncated. Reading the state first inverts that: if finished
		// was true when observed, the Recv below sees everything there will ever
		// be. Getting this backwards cost exactly one empty file out of 1999 in
		// the runner's multiplexing case.
		finished, reset, code := stream.RecvState()
		if reset {
			return fmt.Errorf("%s: peer reset the stream, code %d", u.Path, code)
		}
		if data := stream.Recv(); len(data) > 0 {
			if _, werr := f.Write(data); werr != nil {
				return fmt.Errorf("write download for %s: %w", u.Path, werr)
			}
		}
		if finished {
			return nil
		}
		// Level-triggered: the predicate above is re-read after every wake, so a
		// wake that races the check costs one extra loop and never a lost one.
		if werr := stream.WaitReadable(ctx); werr != nil {
			return fmt.Errorf("read %s: %w", u.Path, werr)
		}
	}
}

// hqDownloadAll fetches every URL over one connection, all requests in flight
// together. This serves "handshake", "transfer" and "retry" — and, through
// "transfer", the multiplexing case, whose 1999 concurrent requests are what
// forces the peer's stream limit to be reached and then raised.
func hqDownloadAll(ctx context.Context, j *job) error {
	urls, err := parseURLs(j.urls)
	if err != nil {
		return err
	}
	// One connection to the authority of the first URL. The runner issues every
	// URL of a test case against a single server name.
	h, err := hqDial(ctx, urls[0], j.tlsConfig)
	if err != nil {
		return err
	}
	defer h.close()
	return inParallel(urls, func(u *url.URL) error { return h.get(ctx, u, j.downloads) })
}

// hqMulticonnect fetches each URL over a connection of its own, sequentially,
// closing each before opening the next. This is the "multiconnect" case, which
// the runner uses for handshakeloss and handshakecorruption: 50 URLs, so 50
// separate handshakes, under 30% packet loss or corruption. What is under test is
// the handshake's own loss recovery, so the connections must not be reused.
func hqMulticonnect(ctx context.Context, j *job) error {
	urls, err := parseURLs(j.urls)
	if err != nil {
		return err
	}
	for i, u := range urls {
		h, err := hqDial(ctx, u, j.tlsConfig)
		if err != nil {
			return fmt.Errorf("connection %d/%d: %w", i+1, len(urls), err)
		}
		err = h.get(ctx, u, j.downloads)
		h.close()
		if err != nil {
			return fmt.Errorf("connection %d/%d: %w", i+1, len(urls), err)
		}
	}
	return nil
}
