package quic

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"net"
	"os"
	"sync"
	"time"
)

// This file holds the server-role listener: one UDP socket serving many QUIC
// connections. connPacketConn is the per-connection view of that socket, so an
// accepted Conn drives its ordinary Poll loop without knowing it shares the
// socket with other connections.

// connPacketConn is one connection's view of a listener's shared UDP socket: it
// reads the datagrams the listener routed to this connection and writes back to
// that connection's peer through the shared socket.
//
// It implements SetReadDeadline because a Conn drives its Poll loop with read
// deadlines (loss timers, idle timeout, cancellation) and pokes the deadline into
// the past to unblock a parked Read. Per the net.Conn contract that must be safe
// while a Read is blocked and take effect immediately, so a parked Read here
// re-evaluates whenever the deadline changes.
type connPacketConn struct {
	shared *net.UDPConn
	remote *net.UDPAddr
	in     chan []byte // datagrams the listener demuxed for this connection

	// onClose, if set, runs once when this view is closed. The listener uses it to
	// drop its route, so the routing table tracks live connections rather than
	// every connection ever served. A Conn closes its PacketConn on every terminal
	// path, so this fires for idle timeouts and peer closes too.
	onClose func()

	mu       sync.Mutex
	deadline time.Time
	changed  chan struct{} // replaced on every SetReadDeadline to wake a parked Read

	closeOnce sync.Once
	closed    chan struct{}
}

// newConnPacketConn builds a per-connection view of shared addressed to remote.
// depth bounds the inbound queue; a full queue drops datagrams like a kernel
// receive buffer would.
func newConnPacketConn(shared *net.UDPConn, remote *net.UDPAddr, depth int) *connPacketConn {
	return &connPacketConn{
		shared:  shared,
		remote:  remote,
		in:      make(chan []byte, depth),
		changed: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

// Write sends one datagram to this connection's peer over the shared socket.
func (c *connPacketConn) Write(p []byte) (int, error) {
	return c.shared.WriteToUDP(p, c.remote)
}

// Read returns the next datagram the listener routed to this connection. It
// honours the read deadline, returning os.ErrDeadlineExceeded (a net.Error whose
// Timeout reports true) when it expires, and net.ErrClosed after Close.
func (c *connPacketConn) Read(p []byte) (int, error) {
	for {
		c.mu.Lock()
		dl, changed := c.deadline, c.changed
		c.mu.Unlock()

		var timer *time.Timer
		var timeout <-chan time.Time
		if !dl.IsZero() {
			d := time.Until(dl)
			if d <= 0 {
				return 0, os.ErrDeadlineExceeded
			}
			timer = time.NewTimer(d)
			timeout = timer.C
		}

		select {
		case dg := <-c.in:
			stopTimer(timer)
			return copy(p, dg), nil
		case <-timeout:
			return 0, os.ErrDeadlineExceeded
		case <-c.closed:
			stopTimer(timer)
			return 0, net.ErrClosed
		case <-changed:
			stopTimer(timer) // the deadline moved; re-evaluate it
		}
	}
}

func stopTimer(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}

// SetReadDeadline sets the deadline for future and in-flight Reads; a zero time
// clears it. It wakes a parked Read so a deadline moved into the past unblocks it
// at once (RFC-independent net.Conn semantics the Conn's PTO path relies on).
func (c *connPacketConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	close(c.changed) // wake any parked Read to re-evaluate
	c.changed = make(chan struct{})
	c.mu.Unlock()
	return nil
}

// Close releases this connection's view. The shared socket stays open for the
// listener's other connections; only the listener closes it.
func (c *connPacketConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		if c.onClose != nil {
			c.onClose()
		}
	})
	return nil
}

// deliver hands a demuxed datagram to this connection. It never blocks: a full
// queue drops the datagram, as a kernel receive buffer would, because stalling
// the listener's demux loop would starve every other connection. QUIC loss
// recovery handles the drop.
func (c *connPacketConn) deliver(dg []byte) {
	select {
	case c.in <- dg:
	default:
	}
}

// serverCIDLen is the length of the Source Connection ID the listener issues. A
// fixed length keeps the demux simple: every short header this server receives
// carries a DCID of exactly this many bytes (RFC 9000 §5.1 lets an endpoint pick
// any length up to 20 for the CIDs it issues).
const serverCIDLen = 8

// listenerHandshakeTimeout bounds how long a half-open connection may take to
// send its Handshake flight before the listener abandons it.
const listenerHandshakeTimeout = 10 * time.Second

// listenerReadBuf bounds a received datagram. QUIC requires endpoints to handle
// at least 1200 bytes (RFC 9000 §14); this covers any path MTU we accept.
const listenerReadBuf = 2048

// Listener accepts server-role QUIC connections on a single UDP socket. It reads
// datagrams, routes them to the connection named by their Destination Connection
// ID, and drives the handshake for peers it has not seen. Accept hands back
// established connections; the caller drives each one with Poll, exactly as a
// client drives its own.
//
// Not yet implemented (see docs/QUIC_SERVER_DESIGN.md): Retry / address
// validation, so this listener answers every Initial; connection migration; and
// per-peer rate limiting. Each new Initial starts one goroutine for its
// handshake, which an unvalidated peer can use to make the server do TLS work —
// front it with a rate limiter before exposing it to the internet.
type Listener struct {
	sock *net.UDPConn
	tls  *tls.Config
	tp   ServerTransportParams

	mu       sync.Mutex
	conns    map[string]*connPacketConn // by the SCID we issued: routes post-handshake packets
	halfOpen map[string]struct{}        // client original DCIDs with a handshake already in flight
	halfPCs  map[string]*connPacketConn // by SCID: views whose handshake is still in flight

	accepted  chan *Conn
	closed    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// Listen starts a QUIC server on addr ("host:port"; port 0 picks one). cfg must
// carry the server's certificate(s); p is the limits the server grants a client.
// The per-connection transport parameters — which also carry the connection IDs
// a client authenticates — are built per handshake from p.
func Listen(addr string, cfg *tls.Config, p ServerTransportParams) (*Listener, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	sock, err := net.ListenUDP("udp", ua)
	if err != nil {
		return nil, err
	}
	l := &Listener{
		sock:     sock,
		tls:      cfg,
		tp:       p,
		conns:    map[string]*connPacketConn{},
		halfOpen: map[string]struct{}{},
		halfPCs:  map[string]*connPacketConn{},
		accepted: make(chan *Conn, 16),
		closed:   make(chan struct{}),
	}
	l.wg.Add(1)
	go func() { defer l.wg.Done(); l.serve() }()
	return l, nil
}

// Addr returns the address the listener is bound to.
func (l *Listener) Addr() net.Addr { return l.sock.LocalAddr() }

// Accept returns the next established connection. It blocks until one is ready,
// ctx is done, or the listener is closed (net.ErrClosed). The caller owns the
// returned Conn and must drive it with Poll.
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	select {
	case c := <-l.accepted:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close stops the listener and its demux loop, and abandons any handshake still
// in flight. Connections already handed out by Accept are untouched — they belong
// to the caller — so Close does not interrupt in-flight requests; it only stops
// new ones from arriving. It returns once the demux loop and every pending
// handshake have stopped.
func (l *Listener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		close(l.closed)
		err = l.sock.Close() // unblocks the demux loop's ReadFromUDP

		// Wake any handshake parked reading its half-open connection. Closing the
		// socket does not touch those views, so without this they would linger until
		// their own listenerHandshakeTimeout fired and Close would wait for them.
		// Collect first, then close outside the lock: a view's onClose takes l.mu.
		l.mu.Lock()
		pending := make([]*connPacketConn, 0, len(l.halfPCs))
		for _, pc := range l.halfPCs {
			pending = append(pending, pc)
		}
		l.mu.Unlock()
		for _, pc := range pending {
			_ = pc.Close()
		}

		l.wg.Wait()
	})
	return err
}

// serve is the demux loop: one goroutine reading every datagram on the socket.
// It does no crypto and never blocks on a connection, so a slow peer cannot stall
// the others.
func (l *Listener) serve() {
	buf := make([]byte, listenerReadBuf)
	for {
		n, remote, err := l.sock.ReadFromUDP(buf)
		if err != nil {
			return // the socket is closed: the listener is shutting down
		}
		// buf is reused on the next read, so hand the connection its own copy.
		dg := make([]byte, n)
		copy(dg, buf[:n])
		l.route(dg, remote)
	}
}

// route sends one datagram to the connection its Destination Connection ID names,
// or starts a handshake when an Initial arrives for a connection we do not know.
func (l *Listener) route(dg []byte, remote *net.UDPAddr) {
	hdr, err := ParseHeader(dg, serverCIDLen)
	if err != nil {
		return // malformed: drop it
	}
	l.mu.Lock()
	pc, known := l.conns[string(hdr.DCID)]
	l.mu.Unlock()
	if known {
		pc.deliver(dg)
		return
	}
	if hdr.Type != PacketInitial {
		return // a packet for a connection we do not have: drop it
	}
	// A first Initial. Dedup retransmissions of it by the client's chosen DCID,
	// so one client cannot start several handshakes for one connection.
	l.mu.Lock()
	if _, dup := l.halfOpen[string(hdr.DCID)]; dup {
		l.mu.Unlock()
		return
	}
	l.halfOpen[string(hdr.DCID)] = struct{}{}
	l.mu.Unlock()

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.handshake(dg, remote)
	}()
}

// handshake drives one inbound connection from its first Initial to an
// established Conn, then publishes it to Accept. Every failure path just drops
// the half-open connection: an unauthenticated peer gets no error reply.
func (l *Listener) handshake(initial []byte, remote *net.UDPAddr) {
	ci, err := AcceptInitial(initial)
	if err != nil {
		l.forgetHalfOpen(initial)
		return
	}
	defer l.dropHalfOpen(ci.DCID)

	scid := make([]byte, serverCIDLen)
	if _, err := rand.Read(scid); err != nil {
		return
	}
	// The transport parameters are per-connection: they carry this connection's
	// source CID and the client's original destination CID, which the client
	// authenticates (RFC 9000 §7.3).
	tp := AppendServerTransportParams(nil, l.tp, scid, ci.DCID)
	flight, err := StartServerHandshake(ci, l.tls, tp, scid)
	if err != nil {
		return
	}

	// Register the route before sending, so the client's reply — addressed to the
	// SCID we are about to advertise — is delivered rather than dropped. onClose
	// drops the route once the connection ends, so the table holds live
	// connections rather than every one ever served.
	pc := newConnPacketConn(l.sock, remote, 64)
	pc.onClose = func() { l.dropConn(scid) }
	l.mu.Lock()
	l.conns[string(scid)] = pc
	l.halfPCs[string(scid)] = pc // its handshake is in flight; Close must wake it
	l.mu.Unlock()
	defer l.dropHalfPC(scid)

	// Until the connection is accepted this goroutine owns the handshake and the
	// socket view. crypto/tls keeps a goroutine alive behind the handshake until it
	// is closed, so every abandonment below must release it — otherwise a peer that
	// never finishes leaks one per attempt. Once accepted, the Conn owns both and
	// releases them when it terminates.
	accepted := false
	defer func() {
		if !accepted {
			_ = flight.Handshake.Close()
			_ = pc.Close() // its onClose drops the route
		}
	}()

	for _, d := range flight.Datagrams {
		if _, err := l.sock.WriteToUDP(d, remote); err != nil {
			return
		}
	}

	// Read the client's Handshake flight (its Finished) and complete the handshake.
	if err := l.completeHandshake(pc, flight); err != nil {
		return
	}

	conn, err := NewServerConn(pc, flight, ci.DCID, ci.SCID)
	if err != nil {
		return
	}
	select {
	case l.accepted <- conn:
		accepted = true
	case <-l.closed:
	}
}

// completeHandshake feeds the client's Handshake-level CRYPTO to the server
// handshake until it finishes or the deadline passes.
func (l *Listener) completeHandshake(pc *connPacketConn, flight *ServerFlight) error {
	if err := pc.SetReadDeadline(time.Now().Add(listenerHandshakeTimeout)); err != nil {
		return err
	}
	defer func() { _ = pc.SetReadDeadline(time.Time{}) }()

	buf := make([]byte, listenerReadBuf)
	for !flight.Complete {
		n, err := pc.Read(buf)
		if err != nil {
			return err // timed out or closed
		}
		crypto := handshakeCrypto(buf[:n], flight.HandshakeOpener)
		if len(crypto) == 0 {
			continue // no Handshake CRYPTO in this datagram (an ACK, say)
		}
		if err := flight.HandleClientHandshake(crypto); err != nil {
			return err
		}
	}
	return nil
}

// handshakeCrypto walks the packets coalesced in one datagram, decrypts each
// Handshake packet with opener, and returns the CRYPTO bytes they carry. Packets
// at other levels and non-CRYPTO frames are ignored.
func handshakeCrypto(datagram []byte, opener *Opener) []byte {
	if opener == nil {
		return nil
	}
	var out []byte
	for off := 0; off < len(datagram); {
		hdr, err := ParseHeader(datagram[off:], serverCIDLen)
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

func (l *Listener) dropConn(scid []byte) {
	l.mu.Lock()
	delete(l.conns, string(scid))
	l.mu.Unlock()
}

// dropHalfPC forgets a view whose handshake has finished (either way): Close no
// longer needs to wake it.
func (l *Listener) dropHalfPC(scid []byte) {
	l.mu.Lock()
	delete(l.halfPCs, string(scid))
	l.mu.Unlock()
}

func (l *Listener) dropHalfOpen(dcid []byte) {
	l.mu.Lock()
	delete(l.halfOpen, string(dcid))
	l.mu.Unlock()
}

// forgetHalfOpen clears the half-open entry for a datagram whose Initial did not
// even parse, so a later well-formed Initial with the same DCID is not ignored.
func (l *Listener) forgetHalfOpen(initial []byte) {
	if hdr, err := ParseHeader(initial, serverCIDLen); err == nil {
		l.dropHalfOpen(hdr.DCID)
	}
}
