package quic

// UDP generic receive offload (GRO) is the receive-side twin of GSO: the kernel
// coalesces several consecutive same-size UDP datagrams from one 5-tuple into a
// single buffer delivered by ONE recvmsg, and reports the segment size out of band
// so the reader can slice the buffer back into individual datagrams. It turns N
// recv syscalls into one for a server's response burst. The syscall itself is
// Linux-only and lives in gro_linux.go / gro_other.go; this file holds the
// platform-independent seam: the capability interface, the enlarged read buffer,
// and the split that feeds every coalesced datagram through the UNCHANGED
// recvDatagram — so GRO lowers only the syscall count, never the per-datagram
// frame/ack/crypto processing, and the blocking read stays unlocked exactly as
// before (docs/HTTP3_DESIGN.md §3.2).

const (
	// pollBufSize is the reused datagram buffer for a non-GRO transport: one
	// received QUIC datagram fits well within it. Kept at the pre-GRO value so the
	// fallback path's buffer semantics are unchanged.
	pollBufSize = 2048
	// groReadBuffer sizes the reused read buffer when the transport is GRO-capable:
	// one recvmsg may now return a whole coalesced burst, up to the 64 KiB the
	// kernel's 16-bit GSO length path allows, so the buffer must be able to hold it.
	groReadBuffer = 64 << 10
)

// groReader is the OPTIONAL batched-receive capability a PacketConn may implement
// on top of the required Read/Write/Close. When c.pc satisfies it, the receive
// path reads a whole coalesced burst with one ReadGRO and splits it into its
// datagrams; a PacketConn without it falls back to a per-datagram Read, so the
// interface assertion is the only coupling — the transport stays a plain
// PacketConn. ReadGRO returns n bytes read into buf and segSize: segSize == 0 or
// segSize >= n means buf[:n] is a single datagram; 0 < segSize < n means buf[:n]
// is several coalesced datagrams of segSize bytes, the last of which may be
// shorter. It MUST degrade gracefully (segSize 0, one datagram) on any platform or
// path that cannot offload.
type groReader interface {
	ReadGRO(buf []byte) (n, segSize int, err error)
}

// pollBufLen is the size of the reused Poll/drain read buffer: enlarged to hold a
// coalesced GRO burst when the transport supports batched receive, else the
// single-datagram size (preserving the non-GRO buffer semantics).
func (c *Conn) pollBufLen() int {
	// groCanCoalesce, not just the interface assertion. A transport may implement
	// ReadGRO unconditionally — http3's udpConn does, so the tree builds
	// everywhere — but off Linux RecvGRO is a plain Read that can never fill more
	// than one datagram. Sizing on the assertion alone handed every connection a
	// 64 KiB buffer instead of 2 KiB on Windows, macOS and the BSDs: 32x the
	// memory, for a coalescing that cannot happen. On a pooled load generator
	// that is the difference between ~2 MB and ~64 MB at a thousand connections.
	_, isGROReader := c.pc.(groReader)
	return pollBufLenFor(groCanCoalesce, isGROReader)
}

// pollBufLenFor is the buffer-size decision itself, taking the platform
// capability as an argument rather than reading the build-tagged constant.
//
// That split exists for the test, and it is not ceremony. groCanCoalesce is a
// compile-time constant, so on Linux — where it is true, and where every
// `runs-on:` in .github/workflows/ is ubuntu-24.04 — deleting the gate above is
// completely unobservable: a test that derives its own expectation from
// groCanCoalesce agrees with the mutated code just as happily as with the real
// one. The regression this guards, a 64 KiB buffer per connection on Windows,
// macOS and the BSDs, could not turn CI red (#839). Passing the capability in
// lets the off-Linux row be asserted from a Linux run.
func pollBufLenFor(canCoalesce, isGROReader bool) int {
	if canCoalesce && isGROReader {
		return groReadBuffer
	}
	return pollBufSize
}

// readPacket performs the unlocked blocking read of the receive path. On a
// GRO-capable transport it reads a whole coalesced burst in one syscall (returning
// segSize so recvGRO can split it); otherwise it is a plain single-datagram Read
// with segSize 0. It touches only c.pc and c.pollBuf — never c.mu — so the read
// stays unlocked exactly as the pre-GRO c.pc.Read did (docs/HTTP3_DESIGN.md §3.2).
func (c *Conn) readPacket() (n, segSize int, err error) {
	if gr, ok := c.pc.(groReader); ok {
		return gr.ReadGRO(c.pollBuf)
	}
	n, err = c.pc.Read(c.pollBuf)
	return n, 0, err
}

// recvGRO splits a possibly-coalesced read of n bytes in c.pollBuf into its
// constituent datagrams and feeds each through recvDatagram — the SAME
// per-datagram processor the single-read path uses, so GRO changes only the
// syscall count, not the frame/ack/crypto logic. segSize <= 0 or segSize >= n is
// the single-datagram case (the non-GRO fallback and a non-coalesced GRO read).
// The loop stops early once a datagram ends the connection (a received
// CONNECTION_CLOSE sets peerClose): the remaining segments belong to a connection
// that is now draining, and the caller surfaces peerClose. Assumes c.mu is held.
func (c *Conn) recvGRO(n, segSize int) error {
	if segSize <= 0 || segSize >= n {
		return c.recvDatagram(c.pollBuf[:n])
	}
	for off := 0; off < n; off += segSize {
		end := off + segSize
		if end > n {
			end = n // the last coalesced datagram may be shorter than segSize
		}
		if err := c.recvDatagram(c.pollBuf[off:end]); err != nil {
			return err
		}
		if c.peerClose != nil {
			//nolint:nilerr // Not the error path: recvDatagram returned nil here.
			// c.peerClose records a CONNECTION_CLOSE frame, which the caller
			// surfaces after the burst — returning it twice would double-report.
			return nil
		}
	}
	return nil
}
