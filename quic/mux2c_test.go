package quic

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// The PR 2c mandatory tests (docs/HTTP3_DESIGN.md §5): flushControl grants receive
// credit from the consuming Recv (INV-3); a PATH_RESPONSE so flushed keeps its
// 1200-byte padding (BREAK3); and a body larger than the initial congestion window
// completes only because the reader's end-of-burst cwnd broadcast wakes the sender
// parked on blockCong (INV-5). These catch the stalls a local build alone misses.

// TestMux2c_FlushControlGrantsCreditOnRecv proves INV-3: consuming received data
// emits the MAX_STREAM_DATA grant immediately from Recv (via flushControl), rather
// than deferring it to the parked reader's next flush — else a flow-control-blocked
// peer stalls until the idle timeout.
func TestMux2c_FlushControlGrantsCreditOnRecv(t *testing.T) {
	dcid := []byte("inv3test")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(keys)
	if err != nil {
		t.Fatal(err)
	}
	pc := &capturePC{}
	c := &Conn{
		pc: pc, dcid: dcid, oneRTTSealer: sealer,
		peer:        TransportParams{InitialMaxStreamsBidi: 1},
		connRecvMax: DefaultConnRecvWindow,
	}
	c.keys.OneRTT = opener
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}

	// Deliver just over half a per-stream window so consuming it crosses the grant
	// threshold and queues a MAX_STREAM_DATA.
	h := &connFrameHandler{c: c}
	n := int(DefaultStreamRecvWindow/2) + 1
	if err := h.OnStream(s.ID(), 0, false, make([]byte, n)); err != nil {
		t.Fatal(err)
	}
	before := len(pc.pkts)
	if got := len(s.Recv()); got != n {
		t.Fatalf("Recv = %d bytes, want %d", got, n)
	}
	if len(pc.pkts) == before {
		t.Fatal("Recv must emit the MAX_STREAM_DATA grant immediately via flushControl (INV-3)")
	}

	// The emitted datagram carries a MAX_STREAM_DATA raising this stream's limit.
	col := &ctrlCollector{}
	for _, pkt := range pc.pkts[before:] {
		hdr, herr := ParseHeader(pkt, len(c.dcid)) // our sent short header carries our DCID
		if herr != nil {
			t.Fatalf("ParseHeader: %v", herr)
		}
		_, _, payload, oerr := opener.Open(pkt, hdr.PNOffset, 0)
		if oerr != nil {
			t.Fatalf("Open: %v", oerr)
		}
		if perr := ParseFrames(payload, col); perr != nil {
			t.Fatalf("ParseFrames: %v", perr)
		}
	}
	if col.streamData[s.ID()] == 0 {
		t.Fatalf("flushed datagram carried no MAX_STREAM_DATA for stream %d", s.ID())
	}
}

// TestMux2c_FlushControlPathResponsePadded proves BREAK3: a PATH_RESPONSE flushed
// via flushControl (a Do-side Recv path) still goes out in a >=1200-byte datagram
// (RFC 9000 §8.2.2), not a bare sub-1200 packet.
func TestMux2c_FlushControlPathResponsePadded(t *testing.T) {
	dcid := []byte("pathtest")
	keys, _ := InitialKeys(dcid)
	sealer, err := NewSealer(keys)
	if err != nil {
		t.Fatal(err)
	}
	pc := &capturePC{}
	c := &Conn{pc: pc, dcid: dcid, oneRTTSealer: sealer}

	h := &connFrameHandler{c: c}
	var challenge [8]byte
	if err := h.OnPathChallenge(&challenge); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	err = c.flushControl()
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("flushControl: %v", err)
	}
	if len(pc.pkts) != 1 {
		t.Fatalf("flushControl wrote %d datagrams, want 1", len(pc.pkts))
	}
	if got := len(pc.pkts[0]); got < 1200 {
		t.Fatalf("PATH_RESPONSE datagram = %d bytes, want >= 1200 (RFC 9000 §8.2.2)", got)
	}
}

// TestMux2c_LargeBodyCompletesViaCwndWake proves INV-5 end to end: a body larger
// than kInitialWindow (12000) completes only if the reader's Poll cwnd broadcast
// wakes the sender parked on blockCong. A reader goroutine drives Poll against an
// in-memory peer that acknowledges every client packet (freeing the window); a
// sender loop sends a 16 KB body, parking on WaitSendable when congestion-blocked.
func TestMux2c_LargeBodyCompletesViaCwndWake(t *testing.T) {
	dcid := []byte("cwndtest")
	keys, _ := InitialKeys(dcid)
	clientSealer, err := NewSealer(keys)
	if err != nil {
		t.Fatal(err)
	}
	clientOpener, err := NewOpener(keys)
	if err != nil {
		t.Fatal(err)
	}

	peer := newAckPeer(t, keys, dcid)
	defer peer.close()

	c := &Conn{
		pc: peer, dcid: dcid, oneRTTSealer: clientSealer,
		now:                time.Now,
		handshakeComplete:  true,
		handshakeConfirmed: true,
		cwnd:               kInitialWindow,
		ssthresh:           ^uint64(0),
		peer: TransportParams{
			InitialMaxStreamsBidi:          1,
			InitialMaxData:                 1 << 20,
			InitialMaxStreamDataBidiRemote: 1 << 20,
		},
		connMax:      1 << 20,
		done:         make(chan struct{}),
		streamCredit: make(chan struct{}, 1),
	}
	c.keys.OneRTT = clientOpener

	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}

	connCtx, connCancel := context.WithCancel(context.Background())
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			if perr := c.Poll(connCtx); perr != nil {
				return
			}
		}
	}()

	body := make([]byte, 16*1024) // larger than kInitialWindow (12000)
	for i := range body {
		body[i] = byte(i)
	}

	sendCtx, sendCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer sendCancel()
	sent := 0
	for {
		n, serr := s.Send(body[sent:], true)
		if serr != nil {
			t.Fatalf("Send at %d/%d: %v", sent, len(body), serr)
		}
		sent += n
		if sent >= len(body) {
			break
		}
		if n == 0 {
			if werr := s.WaitSendable(sendCtx); werr != nil {
				t.Fatalf("body stalled at %d/%d bytes: %v (the cwnd broadcast did not wake the sender)", sent, len(body), werr)
			}
		}
	}
	if sent != len(body) {
		t.Fatalf("sent %d bytes, want %d", sent, len(body))
	}
	if !s.finSent {
		t.Fatal("FIN not sent after the whole body")
	}

	connCancel()
	peer.close()
	<-readerDone
}

// ackPeer is an in-memory QUIC peer for the cwnd test: it decrypts each client
// 1-RTT packet, tracks the largest packet number, and returns a cumulative ACK so
// the client's congestion window opens. It shares the client's 1-RTT keys.
type ackPeer struct {
	mu        sync.Mutex
	toClient  [][]byte // ACK datagrams awaiting the client's Read
	deadline  time.Time
	serverPN  uint64
	maxSeen   uint64
	haveSeen  bool
	closeOnce sync.Once

	sealer  *Sealer
	opener  *Opener
	dcidLen int // the client's Destination Connection ID length (for ParseHeader)

	rwake  chan struct{} // cap 1; kicked when toClient grows
	writes chan []byte    // client Write pushes datagrams here
	closed chan struct{}
}

func newAckPeer(t *testing.T, keys PacketKeys, dcid []byte) *ackPeer {
	t.Helper()
	sealer, err := NewSealer(keys)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewOpener(keys)
	if err != nil {
		t.Fatal(err)
	}
	p := &ackPeer{
		sealer:  sealer,
		opener:  opener,
		dcidLen: len(dcid),
		rwake:   make(chan struct{}, 1),
		writes:  make(chan []byte, 4096),
		closed:  make(chan struct{}),
	}
	go p.run()
	return p
}

func (p *ackPeer) run() {
	for {
		select {
		case <-p.closed:
			return
		case pkt := <-p.writes:
			hdr, err := ParseHeader(pkt, p.dcidLen)
			if err != nil || hdr.Type != PacketShort {
				continue // only 1-RTT client packets are acknowledged
			}
			pn, _, _, err := p.opener.Open(pkt, hdr.PNOffset, p.maxSeen)
			if err != nil {
				continue
			}
			if !p.haveSeen || pn > p.maxSeen {
				p.maxSeen, p.haveSeen = pn, true
			}
			ack, err := p.sealAck(p.maxSeen)
			if err != nil {
				continue
			}
			p.mu.Lock()
			p.toClient = append(p.toClient, ack)
			p.mu.Unlock()
			select {
			case p.rwake <- struct{}{}:
			default:
			}
		}
	}
}

// sealAck seals a server 1-RTT packet carrying a single ACK of [0, largest]. The
// datagram's Destination Connection ID is the client's SCID (zero-length here).
func (p *ackPeer) sealAck(largest uint64) ([]byte, error) {
	frames := AppendAck(nil, largest, 0, largest, nil)
	const minFrames = 20
	if len(frames) < minFrames {
		frames = append(frames, make([]byte, minFrames-len(frames))...)
	}
	pnLen := 4
	hdr, pnOff := AppendShortHeader(nil, nil, pnLen, false)
	pn := p.serverPN
	p.serverPN++
	for i := pnLen - 1; i >= 0; i-- {
		hdr = append(hdr, byte(pn>>(8*uint(i))))
	}
	return p.sealer.Seal(nil, hdr, pnOff, pnLen, pn, frames)
}

func (p *ackPeer) Write(b []byte) (int, error) {
	cp := append([]byte(nil), b...)
	select {
	case p.writes <- cp:
	case <-p.closed:
	}
	return len(b), nil
}

func (p *ackPeer) Read(b []byte) (int, error) {
	for {
		p.mu.Lock()
		if len(p.toClient) > 0 {
			pkt := p.toClient[0]
			p.toClient = p.toClient[1:]
			p.mu.Unlock()
			return copy(b, pkt), nil
		}
		dl := p.deadline
		p.mu.Unlock()

		if dl.IsZero() {
			select {
			case <-p.rwake:
			case <-p.closed:
				return 0, io.EOF
			}
			continue
		}
		d := time.Until(dl)
		if d <= 0 {
			return 0, timeoutError{}
		}
		timer := time.NewTimer(d)
		select {
		case <-p.rwake:
			timer.Stop()
		case <-timer.C:
			return 0, timeoutError{}
		case <-p.closed:
			timer.Stop()
			return 0, io.EOF
		}
	}
}

func (p *ackPeer) SetReadDeadline(t time.Time) error {
	p.mu.Lock()
	p.deadline = t
	p.mu.Unlock()
	return nil
}

func (p *ackPeer) Close() error { return p.close() }

func (p *ackPeer) close() error {
	p.closeOnce.Do(func() { close(p.closed) })
	return nil
}
