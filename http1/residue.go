package http1

import (
	"net"
	"syscall"
	"time"
)

// netConner is satisfied by *tls.Conn, and by any other wrapper that exposes the
// transport underneath it. Used instead of a *tls.Conn type assertion so this
// file needs no crypto/tls import and any equivalent wrapper works.
type netConner interface{ NetConn() net.Conn }

// initResidue caches what HasResidue needs so that the hot path allocates
// nothing: the syscall.RawConn of the real socket, and a control func already
// bound to this Conn's result fields. Obtaining either per call costs an
// allocation, which is the whole reason this check is affordable at all.
//
// Best effort by design: a Conn whose transport is not a syscall.Conn simply
// gets no kernel-queue check, and HasResidue degrades to its reader and TLS
// checks with the caller's ProbeIdle still behind it.
func (c *Conn) initResidue() {
	// Unwrap as far as the transports will go, not one level: a *tls.Conn over a
	// wrapper over a socket is two, and each level that hides the syscall.Conn
	// costs the FIONREAD layer entirely. Bounded so a wrapper that returns itself
	// cannot spin.
	raw := c.nc
	for i := 0; i < 8; i++ {
		w, ok := raw.(netConner)
		if !ok {
			break
		}
		under := w.NetConn()
		if under == nil || under == raw {
			break
		}
		raw, c.layered = under, true
	}
	sc, ok := raw.(syscall.Conn)
	if !ok {
		return
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return
	}
	c.rawCtl = rc
	c.ctlFn = func(fd uintptr) { c.pendN, c.pendOK = socketPending(fd) }
}

// HasResidue reports whether any octet the peer sent is still unread — in this
// Conn's reader, inside the TLS layer's own buffers, or in the kernel receive
// queue.
//
// This is the checkout-time half of RFC 9112 §6.3: "A client MUST NOT process,
// cache, or forward such extra data as a separate response, since such behavior
// would be vulnerable to cache poisoning." A server can send a well-framed
// response, let the client finish reading it, and only then append a second
// complete response. Nothing is in the reader when the message ends, so the
// completion-time check cannot see it; only asking again at checkout can.
//
// It exists because the obvious way to ask — ProbeIdle — costs a bounded ~1ms
// per call, so it was gated on an idle threshold, and that threshold was itself
// the vulnerability: a peer only had to land its extra response inside the reuse
// gap, which under the load this client is built for is every reuse. This check
// is ~0.5µs and allocation-free, so it runs unconditionally.
//
// The three layers are not redundant; each sees something the others cannot.
//
//  1. c.br.Buffered() — octets already pulled into this reader.
//  2. A read under a deadline in the past. On a plain socket that returns a
//     timeout without ever issuing a recv (which is exactly why ProbeIdle must
//     use a FUTURE deadline). On a *tls.Conn it does something different and
//     useful: crypto/tls checks no deadline of its own, so the read decrypts a
//     record already sitting in the TLS layer's input buffer and returns its
//     plaintext without touching the socket. That case is invisible to both the
//     other layers — the socket can read empty while a complete response waits
//     inside crypto/tls.
//  3. FIONREAD on the real socket, which sees what has arrived but not yet been
//     read by anyone.
//
// Layer 3 is definitive on a plain socket: any octet on an idle HTTP/1.1
// connection is unsolicited. On a TLS connection it is not — TLS 1.3 servers
// routinely send NewSessionTicket records on an idle connection, and KeyUpdate
// may arrive at any time. Treating those as poison would evict healthy
// connections against most origin servers, so a positive is resolved by one
// bounded read: application data means residue, a timeout means the
// post-handshake records were consumed and the connection is clean.
//
// Not a liveness check. FIONREAD reports 0 on a socket the peer has closed, so a
// FIN is invisible here; ProbeIdle remains the probe that sees one. A false
// verdict either way costs at most a redialed connection.
//
// One irreducible gap, shared with every checkout-time design (net/http's
// included): octets that arrive after this returns and before the next response
// is read are indistinguishable from an early reply to that request. HTTP/1.1
// carries no sequencing that could tell them apart. A partial TLS record parked
// in the TLS layer's buffer is likewise undetectable without blocking — and
// costs the attacker nothing they do not already have, since forging a record
// requires the session keys, i.e. being the server, which can simply answer the
// next request with whatever it likes.
//
// MUST only be called when no exchange is in flight on this Conn: it moves the
// read deadline and consumes from the reader.
func (c *Conn) HasResidue() bool {
	if c.closed.Load() {
		return false
	}
	if c.br.Buffered() > 0 {
		return true
	}

	// FIONREAD first, because it is the only layer that allocates nothing: a peek
	// that times out costs a *net.OpError every call, and on a plain socket that
	// peek has nothing to tell us anyway — bufio reads the kernel directly, so
	// c.br plus the receive queue is the complete picture. The common case (plain
	// socket, nothing pending) therefore ends here, at one ioctl and zero
	// allocations, which is what makes running this on every checkout affordable.
	pending, known := 0, false
	if c.rawCtl != nil {
		c.pendN, c.pendOK = 0, false
		if cerr := c.rawCtl.Control(c.ctlFn); cerr == nil && c.pendOK {
			pending, known = c.pendN, true
		}
	}
	if known && !c.layered {
		return pending > 0
	}

	// Everything below is the layered (TLS) case, plus the fallback for a
	// transport whose queue could not be read at all.
	if known && pending > 0 {
		// Bytes on the socket, but under TLS they are not necessarily application
		// data. One bounded read settles it and drains the post-handshake records
		// either way: plaintext means residue, a timeout means the connection was
		// clean and is now clean and quiet.
		_ = c.nc.SetReadDeadline(time.Now().Add(time.Millisecond))
		_, err := c.br.Peek(1)
		_ = c.nc.SetReadDeadline(time.Time{})
		return err == nil
	}

	if !known {
		// The receive queue could not be read at all — a transport that is not a
		// syscall.Conn and hides whatever is (this module's own bufferedConn from
		// a CONNECT over-read used to; a caller's custom Dialer still can), or a
		// platform with no FIONREAD here.
		//
		// This case MUST NOT use the past-deadline read below. On a plain socket
		// that returns a timeout without issuing a recv, so the verdict would be
		// "clean" no matter what the peer sent — a security guard failing OPEN,
		// silently, on whole classes of transport. A brief FUTURE deadline asks
		// the socket the real question. It costs ~1ms, which is why it is not the
		// primary path; being slow on an exotic transport is a price worth paying,
		// being blind on one is not.
		_ = c.nc.SetReadDeadline(time.Now().Add(time.Millisecond))
		_, err := c.br.Peek(1)
		_ = c.nc.SetReadDeadline(time.Time{})
		return err == nil
	}

	// The socket is empty and this is a layered transport, which does not settle
	// it: crypto/tls may already hold a complete record in its own input buffer.
	// A deadline in the PAST is what asks that question — crypto/tls checks no
	// deadline itself, so the read decrypts what it has buffered and returns the
	// plaintext without ever touching the socket, and returns a timeout the
	// moment it would have to. (This is the exact opposite of ProbeIdle's future
	// deadline, and of the fallback above, which are asking the socket a question
	// this one deliberately avoids.)
	_ = c.nc.SetReadDeadline(deadlineLongPast)
	_, err := c.br.Peek(1)
	_ = c.nc.SetReadDeadline(time.Time{})
	return err == nil
}
