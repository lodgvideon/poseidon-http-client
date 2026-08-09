package http3

import (
	"context"
	"crypto/tls"
	"net"
	"syscall"
	"time"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// udpSocketBuffer is the kernel receive-buffer size requested on the UDP socket
// so a server's send burst is buffered rather than dropped between reads.
const udpSocketBuffer = 4 << 20

// groControlLen sizes the quic.GROState ancillary-data buffer that receives the
// recvmsg control message carrying the UDP_GRO segment size. 128 bytes holds the
// single small control message the kernel attaches (only UDP_GRO is enabled) with
// ample headroom; it is allocated once and reused across reads on the reader
// goroutine.
const groControlLen = 128

// udpConn adapts a connected *net.UDPConn to quic.PacketConn. Read/Write operate
// on the connected socket, so datagrams only flow to and from the dialed server.
// SetReadDeadline lets the QUIC engine bound each read by the probe timeout
// (RFC 9002 §6.2) rather than a fixed interval. It also implements the optional
// batched-write (WriteGSO) and batched-receive (ReadGRO) capabilities the QUIC
// engine uses on Linux; the groTried and gro fields backing ReadGRO are touched
// only by the single QUIC reader goroutine, and gso only by the sender, so
// neither needs locking.
type udpConn struct {
	c *net.UDPConn
	// rc is the raw fd for the offloaded send and receive paths, nil when
	// SyscallConn is unavailable. Resolved once in newUDPConn and never written
	// again: ReadGRO runs on the QUIC reader goroutine and WriteGSO on the
	// sender, so a lazily-populated cache here would be an unsynchronised write
	// racing a read. Immutable after construction, both may read it freely.
	rc       syscall.RawConn
	groTried bool // UDP_GRO enable attempted once (best-effort); reader goroutine only
	// gro and gso hold the reused receive- and send-syscall state (quic.GROState,
	// quic.GSOState), which is what keeps the offloaded paths from allocating a
	// closure and its captures on every datagram.
	//
	// Two objects rather than one shared one, because they are driven from two
	// goroutines: gro from the QUIC reader, gso from the sender. They are
	// allocated in newUDPConn and never re-pointed, so neither goroutine writes a
	// field the other reads.
	gro *quic.GROState
	gso *quic.GSOState
}

// newUDPConn wraps uc, resolving the raw file descriptor up front so the field
// is immutable for the lifetime of the connection. A failure leaves rc nil,
// which makes both SendGSO and RecvGRO fall back to the unoffloaded path.
func newUDPConn(uc *net.UDPConn) *udpConn {
	rc, err := uc.SyscallConn()
	if err != nil {
		rc = nil
	}
	return &udpConn{
		c:   uc,
		rc:  rc,
		gro: quic.NewGROState(groControlLen),
		gso: quic.NewGSOState(),
	}
}

func (u *udpConn) Read(b []byte) (int, error)        { return u.c.Read(b) }
func (u *udpConn) Write(b []byte) (int, error)       { return u.c.Write(b) }
func (u *udpConn) Close() error                      { return u.c.Close() }
func (u *udpConn) SetReadDeadline(t time.Time) error { return u.c.SetReadDeadline(t) }

// ReadGRO reads one datagram, or one GRO-coalesced burst, from the connected UDP
// socket in a single recvmsg on Linux, returning the bytes read and the GRO
// segment size (0 for a single, non-coalesced datagram); it degrades to a plain
// Read with segSize 0 on non-Linux platforms or paths without offload support
// (quic.RecvGRO). Implementing it makes *udpConn a batched-receive PacketConn, so
// the QUIC receive path processes a server's response burst with one syscall
// instead of one per datagram — the recv-side twin of WriteGSO. UDP_GRO is enabled
// (best-effort) on the first call; the raw fd and control buffer are reused across
// reads, which are serialized on the QUIC reader goroutine.
func (u *udpConn) ReadGRO(buf []byte) (int, int, error) {
	if u.rc != nil && !u.groTried {
		u.groTried = true
		_ = quic.EnableGRO(u.rc) // best-effort; on failure segSize stays 0 (single datagram)
	}
	return quic.RecvGRO(u.gro, u.rc, u.c, buf)
}

// WriteGSO sends buf as consecutive UDP datagrams of segSize bytes (the last may be
// shorter) in one syscall via Linux UDP segmentation offload, degrading to a
// per-datagram loop on non-Linux platforms or paths without offload support
// (quic.SendGSO). Implementing it makes *udpConn a batched-write PacketConn, so the
// QUIC send path coalesces a multi-datagram STREAM burst — the #1 UDP throughput
// limiter — into a single sendmsg. On Linux the raw fd comes from SyscallConn; if
// that is unavailable the per-datagram fallback over u.c still delivers every
// datagram.
func (u *udpConn) WriteGSO(buf []byte, segSize int) (int, error) {
	// u.rc, not u.c.SyscallConn(): the latter re-resolved and allocated a fresh
	// syscall.RawConn on every send. It is resolved once at construction, so
	// reading it here does not race the reader goroutine's ReadGRO. A nil rc
	// degrades SendGSO to a per-datagram loop over u.c, exactly as before.
	return quic.SendGSO(u.gso, u.rc, u.c, buf, segSize)
}

// qpackDynamicTableCapacity is the SETTINGS_QPACK_MAX_TABLE_CAPACITY the client
// advertises (RFC 9204 §5): the maximum bytes the server's encoder may hold in
// the dynamic table WE maintain as its decoder. A non-zero value turns the
// dynamic table on, so a live server inserts entries on its encoder stream and
// references them from response field sections; the shared *qpack.DynamicTable is
// sized to this value in newClient. 4096 bytes (~64 small entries) is the common
// default across HTTP/3 stacks and bounds the memory one connection's table costs.
const qpackDynamicTableCapacity uint64 = 4096

// qpackBlockedStreams is the SETTINGS_QPACK_BLOCKED_STREAMS the client advertises
// (RFC 9204 §5, §2.1.2): the maximum number of request streams whose response
// field section the decoder will accept while it references dynamic-table entries
// the server's encoder stream has not delivered yet — a section whose Required
// Insert Count is ahead of our insert count. Each such stream parks (off qpackMu)
// until the encoder stream catches up (§2.1.3), so this bounds how many requests
// may be simultaneously head-of-line blocked on the encoder stream, and thus the
// memory those parked decodes tie up. 16 is a conservative small value: it is
// enough that a normal server pipelining a handful of dynamic references never
// stalls needlessly, yet small enough that a misbehaving encoder cannot make an
// unbounded number of streams block — accepting a section past the limit is a
// QPACK_DECOMPRESSION_FAILED (§2.1.2). Exceeding it is a decoder-detectable
// encoder violation, not a hang: a blocked decode is always ctx-bounded.
const qpackBlockedStreams uint64 = 16

// defaultSettings are the HTTP/3 SETTINGS a client advertises. The QPACK dynamic
// table is enabled with a non-zero capacity (RFC 9204 §5), so the server may use
// dynamic QPACK for response headers, and SETTINGS_QPACK_BLOCKED_STREAMS is
// non-zero (Q4), so the server's encoder may reference an entry from a response
// field section before its Insert Count Increment has told us we hold it: such a
// section's Required Insert Count is ahead of our insert count, and the decode
// parks until the encoder stream delivers the promised inserts (RFC 9204 §2.1.3)
// rather than failing. The wait is bounded by the request context, so a server
// that promises inserts it never sends fails the request on ctx timeout instead of
// hanging, and at most qpackBlockedStreams streams may block at once (§2.1.2).
// The third entry is a reserved "grease" identifier of the form 0x1f*N + 0x21
// (N=1): RFC 9114 §7.2.4.1 says endpoints SHOULD include at least one, so that a
// peer's ignore-unknown-setting path is exercised on every connection instead of
// ossifying around the identifiers in use today. The value is arbitrary and the
// peer MUST NOT ascribe it any meaning.
var defaultSettings = []Setting{
	{SettingQPACKMaxTableCapacity, qpackDynamicTableCapacity},
	{SettingQPACKBlockedStreams, qpackBlockedStreams},
	{greaseSettingID, 0x1f},
}

// greaseSettingID is the reserved setting identifier 0x1f*1 + 0x21 (RFC 9114
// §7.2.4.1). Any identifier congruent to 0x21 modulo 0x1f is reserved; a fixed one
// keeps the emitted SETTINGS deterministic for tests while still exercising the
// peer's ignore path.
const greaseSettingID uint64 = 0x1f*1 + 0x21

// localTransportParams are the limits the client advertises so the server has
// credit to send the response and open its control/QPACK streams.
var localTransportParams = quic.LocalTransportParams{
	InitialMaxData:                quic.DefaultConnRecvWindow,
	InitialMaxStreamDataBidiLocal: quic.DefaultStreamRecvWindow,
	InitialMaxStreamDataUni:       quic.DefaultStreamRecvWindow,
	InitialMaxStreamsUni:          3,
	MaxIdleTimeout:                30000, // 30 s idle timeout (RFC 9000 §10.1)
}

// Dial establishes an HTTP/3 connection to addr ("host:port") over UDP and
// returns a ready Client. tlsConfig must set ServerName; the ALPN token "h3" and
// TLS 1.3 minimum are set automatically. ctx bounds the handshake.
func Dial(ctx context.Context, addr string, tlsConfig *tls.Config, opts ...quic.ConnOption) (*Client, error) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	uc, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}
	// Enlarge the kernel receive buffer so a fast server burst is held rather
	// than dropped while the single-goroutine receive loop drains it one
	// datagram at a time (best-effort).
	_ = uc.SetReadBuffer(udpSocketBuffer)
	return dialConn(ctx, newUDPConn(uc), h3TLSConfig(tlsConfig), opts...)
}

// dialConn establishes a QUIC connection over pc, wires the client transport
// parameters, and returns a ready HTTP/3 Client. pc is closed on any failure.
func dialConn(ctx context.Context, pc quic.PacketConn, cfg *tls.Config, opts ...quic.ConnOption) (*Client, error) {
	tp := quic.AppendTransportParams(nil, localTransportParams)
	conn, err := quic.NewConn(pc, cfg, tp, opts...)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	if err := conn.Establish(ctx); err != nil {
		_ = pc.Close()
		return nil, err
	}
	client, err := newClient(connAdapter{conn}, defaultSettings)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	return client, nil
}

// h3TLSConfig returns a copy of base with the HTTP/3 ALPN token and a TLS 1.3
// minimum applied (RFC 9114 §3.1).
func h3TLSConfig(base *tls.Config) *tls.Config {
	cfg := base.Clone()
	if cfg == nil {
		cfg = &tls.Config{}
	}
	cfg.NextProtos = []string{"h3"}
	if cfg.MinVersion < tls.VersionTLS13 {
		cfg.MinVersion = tls.VersionTLS13
	}
	// Keep the ClientHello inside one Initial packet. Go 1.24 offers the
	// post-quantum X25519MLKEM768 key share by default (~1200 bytes), which
	// pushes the ClientHello past a single ~1200-byte Initial datagram — and
	// this client does not yet split a CRYPTO stream across Initial packets, so
	// the oversized datagram would be fragmented and dropped by the peer.
	if cfg.CurvePreferences == nil {
		cfg.CurvePreferences = []tls.CurveID{tls.X25519, tls.CurveP256}
	}
	return cfg
}
