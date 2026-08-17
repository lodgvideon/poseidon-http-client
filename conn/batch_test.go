package conn

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// SendBatch exists for exactly one number: transport writes per request. Every
// other property it has — id ordering, credit accounting, per-entry errors — is
// a constraint on getting that number without breaking the connection, so the
// tests come in that order.

// batchFixture is a Conn with a real Framer over a buffer and no transport: the
// shape the wire-byte assertions need. It has no bufio writer, so flushWrite is
// a no-op and every emitted byte is already in buf.
func batchFixture() (*Conn, *bytes.Buffer) {
	var buf bytes.Buffer
	c := &Conn{
		streams: map[uint32]*Stream{},
		opts:    ConnOptions{}.defaulted(),
		nextID:  1,
		enc:     hpack.NewEncoder(),
	}
	c.fr = frame.NewFramer(&buf, nil) // writer first
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.peerConnSendWindow = 65535
	return c, &buf
}

// batchStream makes an unsent stream on c: no id yet, so SendBatch assigns one.
func batchStream(c *Conn, sendWindow int32) *Stream {
	s := newStream(0, 8, c, 65535)
	s.sendWindow = sendWindow
	return s
}

var batchFields = []header.Field{
	{Name: []byte(":method"), Value: []byte("POST")},
	{Name: []byte(":scheme"), Value: []byte("http")},
	{Name: []byte(":authority"), Value: []byte("batch.local")},
	{Name: []byte(":path"), Value: []byte("/Svc/Echo")},
}

// TestSendBatch_OneTransportWriteForNStreams is the headline gate and the
// reason the API exists: eight requests on eight streams must reach the socket
// in ONE write, where eight SendHeadersAndData calls cost eight.
//
// It is two-sided on purpose. A rise means the coalescing broke; a fall below 1
// would mean the batch never reached the wire at all, which a write-count
// assertion alone would happily call an improvement.
func TestSendBatch_OneTransportWriteForNStreams(t *testing.T) {
	const streams = 8

	p := newLoadGenPeer(t, 64)
	wc := &lgWriteCounter{}
	c := dialLoadGenPeer(t, p, wc)
	ctx := context.Background()
	body := make([]byte, 64)

	batch := make([]BatchEntry, streams)
	for i := range batch {
		ref, err := c.NewStream(ctx)
		require.NoErrorf(t, err, "NewStream %d", i)
		batch[i] = BatchEntry{
			Stream:    ref,
			Fields:    lgRequestFields("batch.local"),
			Body:      body,
			EndStream: true,
		}
	}
	wc.writes.Store(0)

	err := c.SendBatch(ctx, batch)

	require.NoError(t, err, "SendBatch")
	for i := range batch {
		require.NoErrorf(t, batch[i].Err, "entry %d", i)
	}
	assert.EqualValuesf(t, 1, wc.writes.Load(),
		"%d streams cost %d transport writes, want exactly 1", streams, wc.writes.Load())

	// Drain, so the assertion is about a batch the peer actually answered rather
	// than about bytes that never made sense to it.
	for i := range batch {
		for {
			ev, rerr := batch[i].Stream.Recv(ctx)
			require.NoErrorf(t, rerr, "stream %d recv", i)
			ev.Release()
			if ev.DataSlab != nil {
				dataBufPool.Put(ev.DataSlab)
			}
			if ev.EndStream {
				break
			}
		}
		assert.NoErrorf(t, batch[i].Stream.Close(), "stream %d close", i)
	}
}

// TestSendBatch_BeatsPerStreamSends is the control for the test above: the same
// eight requests sent one at a time must cost eight writes. Without it, a
// SendBatch that silently degraded to per-entry flushes would still pass a
// "writes == 1" assertion if the buffer happened to hold everything.
func TestSendBatch_BeatsPerStreamSends(t *testing.T) {
	const streams = 8

	p := newLoadGenPeer(t, 64)
	wc := &lgWriteCounter{}
	c := dialLoadGenPeer(t, p, wc)
	ctx := context.Background()
	body := make([]byte, 64)

	refs := make([]StreamRef, streams)
	for i := range refs {
		ref, err := c.NewStream(ctx)
		require.NoErrorf(t, err, "NewStream %d", i)
		refs[i] = ref
	}
	wc.writes.Store(0)

	for i := range refs {
		require.NoErrorf(t, refs[i].SendHeadersAndData(ctx, lgRequestFields("batch.local"), body, true),
			"SendHeadersAndData %d", i)
	}

	assert.EqualValuesf(t, streams, wc.writes.Load(),
		"%d individual sends cost %d writes, want %d — the comparison "+
			"SendBatch's gate rests on is not what it claims", streams, wc.writes.Load(), streams)

	for i := range refs {
		for {
			ev, err := refs[i].Recv(ctx)
			require.NoErrorf(t, err, "stream %d recv", i)
			ev.Release()
			if ev.DataSlab != nil {
				dataBufPool.Put(ev.DataSlab)
			}
			if ev.EndStream {
				break
			}
		}
		_ = refs[i].Close()
	}
}

// TestConformance_RFC9113_Sec511_BatchStreamIDsIncreaseInEmitOrder pins the
// ordering the write lock is held for. RFC 9113 §5.1.1: "The identifier of a
// newly established stream MUST be numerically greater than all streams that
// the initiating endpoint has opened or reserved."
//
// A batch is the one place where several ids are allocated inside a single lock
// hold, so an implementation that assigned them up front and then emitted in a
// different order — or that skipped an entry after assigning it an id — would
// put a lower id on the wire after a higher one. The peer answers that with
// PROTOCOL_ERROR.
func TestConformance_RFC9113_Sec511_BatchStreamIDsIncreaseInEmitOrder(t *testing.T) {
	c, buf := batchFixture()

	batch := make([]BatchEntry, 4)
	for i := range batch {
		batch[i] = BatchEntry{Stream: batchStream(c, 65535).ref(), Fields: batchFields, EndStream: true}
	}

	err := c.SendBatch(context.Background(), batch)

	require.NoError(t, err, "SendBatch")
	var ids []uint32
	for _, fh := range parseFrameHeaders(t, buf.Bytes()) {
		if fh.ftype == byte(frame.FrameHeaders) {
			ids = append(ids, fh.streamID)
		}
	}
	require.Lenf(t, ids, len(batch), "got %d HEADERS frames, want %d", len(ids), len(batch))
	for i := 1; i < len(ids); i++ {
		require.Greaterf(t, ids[i], ids[i-1], "stream ids reached the wire out of order: %v", ids)
	}
	assert.EqualValuesf(t, 1, ids[0], "ids = %v; want them to start at 1", ids)
	assert.EqualValuesf(t, 1+2*len(batch), c.nextID, "nextID = %d, want %d", c.nextID, 1+2*len(batch))
}

// TestSendBatch_RefusesAStaleLifetime pins the check that makes a generator's
// own stream table safe: a handle minted for a finished request must not write
// onto whatever request owns the pooled struct now.
func TestSendBatch_RefusesAStaleLifetime(t *testing.T) {
	c, buf := batchFixture()

	live := batchStream(c, 65535)
	stale := batchStream(c, 65535)
	staleRef := stale.ref()
	stale.gen.Add(1) // the recycle

	batch := []BatchEntry{
		{Stream: staleRef, Fields: batchFields, EndStream: true},
		{Stream: live.ref(), Fields: batchFields, EndStream: true},
	}

	err := c.SendBatch(context.Background(), batch)

	require.NoError(t, err, "SendBatch")
	assert.Truef(t, errors.Is(batch[0].Err, ErrStaleStream),
		"stale entry Err = %v, want ErrStaleStream", batch[0].Err)
	assert.NoErrorf(t, batch[1].Err,
		"live entry Err = %v; one bad entry must not fail its neighbours", batch[1].Err)
	assert.Lenf(t, parseFrameHeaders(t, buf.Bytes()), 1,
		"emitted %d frames, want 1 — the stale entry put bytes on the wire",
		len(parseFrameHeaders(t, buf.Bytes())))
}

// TestSendBatch_RefusesAClosedStreamMidBatch is the case the generation check
// alone provably cannot catch. A peer RST_STREAM sets s.closed but does NOT
// bump s.gen — markStreamDone only recycles a stream when !s.closed, so
// resetForPoolLocked's gen.Add never runs for a reset one. A batch holding the
// write lock across N streams widens that window from one frame to the whole
// batch, so the entry check reads s.closed as well.
func TestSendBatch_RefusesAClosedStreamMidBatch(t *testing.T) {
	c, buf := batchFixture()

	first := batchStream(c, 65535)
	reset := batchStream(c, 65535)
	last := batchStream(c, 65535)

	reset.mu.Lock()
	reset.closed = true // what endWithReset leaves behind, without the gen bump
	reset.mu.Unlock()

	batch := []BatchEntry{
		{Stream: first.ref(), Fields: batchFields, EndStream: true},
		{Stream: reset.ref(), Fields: batchFields, Body: []byte("payload"), EndStream: true},
		{Stream: last.ref(), Fields: batchFields, EndStream: true},
	}
	err := c.SendBatch(context.Background(), batch)

	require.NoError(t, err, "SendBatch")
	assert.Truef(t, errors.Is(batch[1].Err, ErrStreamClosed),
		"reset entry Err = %v, want ErrStreamClosed", batch[1].Err)
	assert.NoErrorf(t, batch[0].Err, "neighbour failed with the reset stream: %v", batch[0].Err)
	assert.NoErrorf(t, batch[2].Err, "neighbour failed with the reset stream: %v", batch[2].Err)

	// Counting the frames, not just looking for DATA. tryAcquireSendCreditsAll
	// reports ErrStreamClosed too, so an implementation that dropped the door
	// check would still return the right error for this entry — after emitting
	// its HEADERS onto a stream the peer had already reset. Only the frame count
	// tells those two apart, and §5.1 permits nothing but PRIORITY there.
	frames := parseFrameHeaders(t, buf.Bytes())
	require.Lenf(t, frames, 2,
		"emitted %d frames, want 2 (one HEADERS for each live stream): %+v", len(frames), frames)
	for _, fh := range frames {
		require.NotEqual(t, byte(frame.FrameData), fh.ftype,
			"DATA emitted on a stream the peer had reset (RFC 9113 §6.4)")
	}
	reset.mu.Lock()
	resetID := reset.id
	reset.mu.Unlock()
	assert.EqualValuesf(t, 0, resetID,
		"the reset stream was assigned id %d; an id that never reaches the wire "+
			"can never be used, because §5.1.1 requires every later id to exceed it", resetID)
	assert.EqualValuesf(t, 65535, c.peerConnSendWindow,
		"connection send window debited %d bytes for a stream that emitted nothing; "+
			"the connection half of a debit has no refund path", 65535-c.peerConnSendWindow)
}

// hookWriter runs fn on the first write and then behaves as a buffer. It is a
// synchronisation point INSIDE the write lock, which is the only way to reach
// the window the test below is about.
type hookWriter struct {
	buf   bytes.Buffer
	fn    func()
	fired bool
}

func (w *hookWriter) Write(p []byte) (int, error) {
	if !w.fired {
		w.fired = true
		w.fn()
	}
	return w.buf.Write(p)
}

// TestSendBatch_StaleGenerationDuringTheHeaderWriteIsRefused covers the re-read
// of (id, gen) that sits between an entry's door check and the bookkeeping that
// follows its frames.
//
// The door check releases s.mu before the id is assigned, the block is encoded
// and the HEADERS are written. A stream that completes inside that window is
// recycled and handed to another request, and the struct then belongs to THAT
// request. What makes the re-read load-bearing rather than defensive is what
// runs after the write lock is released: SendBatch half-closes every entry that
// reported success and asked for END_STREAM. Reporting success here would call
// endLocalAndRetire on a struct this batch no longer owns — marking a live
// stranger's request locally ended and retiring its slot. That is issue #370's
// shape exactly, and it is the failure a generator keeping its own stream table
// produces.
//
// The window sits inside the connection's write lock, so it cannot be owned from
// outside the way conn/datav_stalegen_test.go owns the credit-wait window. It
// has a synchronisation point of its own: the framer writes the HEADERS through
// a writer this test supplies, and that Write lands between the assignment and
// the re-read.
func TestSendBatch_StaleGenerationDuringTheHeaderWriteIsRefused(t *testing.T) {
	const stranger = 99 // the request that claims the struct next

	c := &Conn{
		streams: map[uint32]*Stream{},
		opts:    ConnOptions{}.defaulted(),
		nextID:  1,
		enc:     hpack.NewEncoder(),
	}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.peerConnSendWindow = 65535
	s := batchStream(c, 65535)

	hw := &hookWriter{fn: func() {
		s.mu.Lock()
		s.gen.Add(1)
		s.id = stranger
		s.mu.Unlock()
	}}
	c.fr = frame.NewFramer(hw, nil) // writer first

	batch := []BatchEntry{{Stream: s.ref(), Fields: batchFields, EndStream: true}}

	err := c.SendBatch(context.Background(), batch)

	require.NoError(t, err, "SendBatch")
	assert.Truef(t, errors.Is(batch[0].Err, ErrStaleStream),
		"recycled entry Err = %v, want ErrStaleStream", batch[0].Err)
	s.mu.Lock()
	localEnded := s.localEnded
	s.mu.Unlock()
	require.False(t, localEnded,
		"the batch half-closed the request that claimed the struct next; "+
			"a finished entry retired a stranger's stream")
}

// TestSendBatch_NoCreditEmitsHeadersAndReports pins the one entry outcome that
// is not a failure, and pins it precisely, because what it leaves behind is
// what a caller has to reason about.
//
// HEADERS are not flow-controlled, so they go out; the body does not, and
// END_STREAM does not ride anything. The alternative — bail before assigning
// the id — would burn a stream id that can never be used afterwards, since
// §5.1.1 requires every later id to exceed it.
func TestSendBatch_NoCreditEmitsHeadersAndReports(t *testing.T) {
	c, buf := batchFixture()
	c.peerConnSendWindow = 4 // less than the body below

	s := batchStream(c, 65535)
	batch := []BatchEntry{{
		Stream:    s.ref(),
		Fields:    batchFields,
		Body:      []byte("a body that does not fit"),
		EndStream: true,
	}}

	err := c.SendBatch(context.Background(), batch)

	require.NoError(t, err, "SendBatch")
	require.Truef(t, errors.Is(batch[0].Err, ErrNoCredit), "Err = %v, want ErrNoCredit", batch[0].Err)
	frames := parseFrameHeaders(t, buf.Bytes())
	require.Lenf(t, frames, 1, "emitted %+v, want exactly one HEADERS frame", frames)
	require.Equalf(t, byte(frame.FrameHeaders), frames[0].ftype,
		"emitted %+v, want exactly one HEADERS frame", frames)
	assert.Zero(t, frames[0].flags&byte(frame.FlagHeadersEndStream),
		"END_STREAM rode the HEADERS although the body was not sent")
	assert.EqualValuesf(t, 4, c.peerConnSendWindow,
		"connection window = %d, want 4 — a refused entry must not debit", c.peerConnSendWindow)
	s.mu.Lock()
	localEnded := s.localEnded
	s.mu.Unlock()
	assert.False(t, localEnded, "the stream was half-closed although END_STREAM never went out")
}

// TestSendBatch_BodyOnlyBeforeHeadersIsRefused: a stream with no HEADERS on the
// wire has no on-wire identity, so there is nothing to address DATA to.
func TestSendBatch_BodyOnlyBeforeHeadersIsRefused(t *testing.T) {
	c, buf := batchFixture()

	batch := []BatchEntry{{Stream: batchStream(c, 65535).ref(), Body: []byte("x"), EndStream: true}}

	err := c.SendBatch(context.Background(), batch)

	require.NoError(t, err, "SendBatch")
	assert.Truef(t, errors.Is(batch[0].Err, ErrStreamClosed),
		"Err = %v, want ErrStreamClosed", batch[0].Err)
	assert.Zerof(t, buf.Len(), "emitted %d bytes for a stream with no id", buf.Len())
}

// TestSendBatch_BodyOnlyContinuesAnOpenStream is the positive half: once
// HEADERS are out, a later batch may carry only the body — the shape a
// generator takes when it streams a request in pieces.
func TestSendBatch_BodyOnlyContinuesAnOpenStream(t *testing.T) {
	c, buf := batchFixture()
	s := batchStream(c, 65535)

	head := []BatchEntry{{Stream: s.ref(), Fields: batchFields}}
	herr := c.SendBatch(context.Background(), head)
	require.NoError(t, herr, "headers batch")
	require.NoError(t, head[0].Err, "headers batch entry")
	buf.Reset()

	body := []BatchEntry{{Stream: s.ref(), Body: []byte("payload"), EndStream: true}}
	err := c.SendBatch(context.Background(), body)

	require.NoError(t, err, "body batch")
	require.NoError(t, body[0].Err, "body batch entry")
	frames := parseFrameHeaders(t, buf.Bytes())
	require.Lenf(t, frames, 1, "emitted %+v, want one DATA frame carrying END_STREAM", frames)
	require.Equalf(t, byte(frame.FrameData), frames[0].ftype,
		"emitted %+v, want one DATA frame carrying END_STREAM", frames)
	require.NotZerof(t, frames[0].flags&byte(frame.FlagDataEndStream),
		"emitted %+v, want one DATA frame carrying END_STREAM", frames)
	assert.EqualValuesf(t, 1, frames[0].streamID,
		"DATA on stream %d, want the id the HEADERS opened (1)", frames[0].streamID)
}

// TestSendBatch_VectoredBodyMatchesJoined pins that BodyV produces exactly the
// frames the joined bytes would, which is what makes it a copy optimisation
// rather than a different wire format.
func TestSendBatch_VectoredBodyMatchesJoined(t *testing.T) {
	pieces := [][]byte{[]byte("aaa"), []byte("bbbb"), []byte("cc")}
	joined := []byte("aaabbbbcc")

	c1, buf1 := batchFixture()
	b1 := []BatchEntry{{Stream: batchStream(c1, 65535).ref(), Fields: batchFields, BodyV: pieces, EndStream: true}}
	c2, buf2 := batchFixture()
	b2 := []BatchEntry{{Stream: batchStream(c2, 65535).ref(), Fields: batchFields, Body: joined, EndStream: true}}

	verr := c1.SendBatch(context.Background(), b1)
	serr := c2.SendBatch(context.Background(), b2)

	require.NoError(t, verr, "vectored")
	require.NoError(t, b1[0].Err, "vectored entry")
	require.NoError(t, serr, "scalar")
	require.NoError(t, b2[0].Err, "scalar entry")
	assert.Equalf(t, buf2.Bytes(), buf1.Bytes(),
		"vectored body produced different wire bytes than the joined one:\n%x\n%x",
		buf1.Bytes(), buf2.Bytes())
}

// TestSendBatch_EndStreamHalfCloses pins the bookkeeping that runs after the
// write lock is released: an entry that sent END_STREAM must leave the stream
// half-closed, or a later send on it would be accepted and rejected by the peer.
func TestSendBatch_EndStreamHalfCloses(t *testing.T) {
	c, _ := batchFixture()
	s := batchStream(c, 65535)

	batch := []BatchEntry{{Stream: s.ref(), Fields: batchFields, Body: []byte("x"), EndStream: true}}
	ferr := c.SendBatch(context.Background(), batch)
	require.NoError(t, ferr, "SendBatch")
	require.NoError(t, batch[0].Err, "SendBatch entry")

	again := []BatchEntry{{Stream: s.ref(), Body: []byte("y"), EndStream: true}}
	err := c.SendBatch(context.Background(), again)

	require.NoError(t, err, "second SendBatch")
	s.mu.Lock()
	localEnded := s.localEnded
	s.mu.Unlock()
	assert.True(t, localEnded, "END_STREAM went out but the stream was not half-closed")
	assert.Truef(t, errors.Is(again[0].Err, ErrStreamClosed),
		"sending after END_STREAM = %v, want ErrStreamClosed", again[0].Err)
}

// TestSendBatch_ChargesPaddingOverhead: a padded DATA frame puts its data bytes
// plus the pad-length octet and the padding on the wire, and the peer debits all
// of it (RFC 9113 §6.9.1). A batch that charged only the data would drift the
// windows above the peer's and eventually overrun them.
func TestSendBatch_ChargesPaddingOverhead(t *testing.T) {
	c, buf := batchFixture()
	c.opts.Padding = PaddingStrategy{Min: 8, Max: 8}

	s := batchStream(c, 65535)
	body := make([]byte, 100)
	batch := []BatchEntry{{Stream: s.ref(), Fields: batchFields, Body: body, EndStream: true}}

	err := c.SendBatch(context.Background(), batch)

	require.NoError(t, err, "SendBatch")
	require.NoError(t, batch[0].Err, "SendBatch entry")
	const padOverhead = 1 + 8
	want := int32(65535 - (len(body) + padOverhead))
	assert.Equalf(t, want, c.peerConnSendWindow,
		"connection window = %d, want %d (body + pad octet + padding)", c.peerConnSendWindow, want)
	for _, fh := range parseFrameHeaders(t, buf.Bytes()) {
		if fh.ftype != byte(frame.FrameData) {
			continue
		}
		assert.EqualValuesf(t, len(body)+padOverhead, fh.length,
			"DATA length %d, want %d — the debit and the wire disagree",
			fh.length, len(body)+padOverhead)
	}
}

// TestSendBatch_ClosedConnIsBatchFatal: a closed connection fails the call, and
// every entry says so, so a caller resubmitting on a fresh connection knows
// none of them went out.
func TestSendBatch_ClosedConnIsBatchFatal(t *testing.T) {
	c, _ := batchFixture()
	c.closed.Store(true)

	batch := []BatchEntry{
		{Stream: batchStream(c, 65535).ref(), Fields: batchFields},
		{Stream: batchStream(c, 65535).ref(), Fields: batchFields},
	}

	err := c.SendBatch(context.Background(), batch)

	require.Truef(t, errors.Is(err, ErrConnClosed),
		"SendBatch on a closed conn = %v, want ErrConnClosed", err)
}

// TestSendBatch_CancelledContextEmitsNothing: the context is checked before any
// credit is spent, because a connection-level debit has no refund path.
func TestSendBatch_CancelledContextEmitsNothing(t *testing.T) {
	c, buf := batchFixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := []BatchEntry{{Stream: batchStream(c, 65535).ref(), Fields: batchFields, Body: []byte("x")}}

	err := c.SendBatch(ctx, batch)

	require.Truef(t, errors.Is(err, context.Canceled),
		"SendBatch with a cancelled context = %v, want context.Canceled", err)
	assert.Zerof(t, buf.Len(), "emitted %d bytes under a cancelled context", buf.Len())
	assert.EqualValues(t, 65535, c.peerConnSendWindow,
		"credit was spent under a cancelled context, and it cannot be refunded")
}

// TestSendBatch_ReleasesDeferringWriter is the #360 boundary. SendBatch must
// not defer into a group-commit convoy — that is the waiting mechanism whose
// extension to DATA cost p50 +81% — but it must still do the convoy
// bookkeeping, so a writer parked behind it is released by the flush that
// carried its bytes rather than waiting for some unrelated writer.
//
// The negative control for this is TestFlushNow_PlainFlushLeavesWriterParked:
// an implementation ending in flushWrite instead of flushNow leaves the parked
// writer hanging, and that test proves the fixture can tell the difference.
func TestSendBatch_ReleasesDeferringWriter(t *testing.T) {
	wb := bufio.NewWriterSize(&countingSink{}, defaultWriteBufferSize)
	c := &Conn{
		streams: map[uint32]*Stream{},
		opts:    ConnOptions{}.defaulted(),
		nextID:  1,
		enc:     hpack.NewEncoder(),
		wb:      wb,
	}
	c.fr = frame.NewFramer(wb, bytes.NewReader(nil))
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.peerConnSendWindow = 65535
	c.wbatch = newWriteBatcher(true, &c.wmu, wb, defaultWriteBufferSize/2)

	// One writer parked in commit, exactly as gcDeferFixture arranges it.
	c.wbatch.enter()
	parked := make(chan struct{})
	go func() {
		c.wmu.Lock()
		_, _ = wb.WriteString("deferred frame")
		_ = c.wbatch.commit()
		c.wmu.Unlock()
		close(parked)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.wmu.Lock()
		d := c.wbatch.deferring
		c.wmu.Unlock()
		if d == 1 {
			break
		}
		require.False(t, time.Now().After(deadline),
			"the writer never parked; the fixture is not testing deferral")
		time.Sleep(time.Millisecond)
	}

	// SendBatch runs off the test goroutine so a batch that DEFERS — which is
	// what routing the flush through commit would do, with a queued writer
	// present and nothing left to wake it — fails this test in five seconds
	// instead of hanging the package until the go test timeout.
	batch := []BatchEntry{{Stream: batchStream(c, 65535).ref(), Fields: batchFields, EndStream: true}}
	sent := make(chan error, 1)
	go func() { sent <- c.SendBatch(context.Background(), batch) }()
	select {
	case err := <-sent:
		require.NoError(t, err, "SendBatch")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "SendBatch never returned: it deferred into the convoy instead of "+
			"flushing, and is waiting for a writer that is waiting for it")
	}
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "SendBatch did not release the writer deferring behind it; "+
			"it ended in a plain flush instead of flushNow")
	}
}

// TestSendBatch_SplitsAtTheWriteBufferBoundary pins the sizing contract: one
// write holds only while the batch fits in ConnOptions.WriteBufferSize, and past
// that the buffered writer flushes as it fills. Documented rather than hidden,
// because it is the whole reason WriteBufferSize became an option.
func TestSendBatch_SplitsAtTheWriteBufferBoundary(t *testing.T) {
	const (
		entries  = 8
		bodySize = 4096
	)
	sink := &countingSink{}
	wb := bufio.NewWriterSize(sink, minWriteBufferSize)
	c := &Conn{
		streams: map[uint32]*Stream{},
		opts:    ConnOptions{WriteBufferSize: minWriteBufferSize}.defaulted(),
		nextID:  1,
		enc:     hpack.NewEncoder(),
		wb:      wb,
	}
	c.fr = frame.NewFramer(wb, bytes.NewReader(nil))
	c.fcOutCond = sync.NewCond(&c.fcOutMu)
	c.peerConnSendWindow = int32(entries * bodySize * 2)
	c.wbatch = newWriteBatcher(false, &c.wmu, wb, minWriteBufferSize/2)

	body := make([]byte, bodySize)
	batch := make([]BatchEntry, entries)
	for i := range batch {
		batch[i] = BatchEntry{
			Stream:    batchStream(c, 65535).ref(),
			Fields:    batchFields,
			Body:      body,
			EndStream: true,
		}
	}
	err := c.SendBatch(context.Background(), batch)

	require.NoError(t, err, "SendBatch")
	for i := range batch {
		require.NoErrorf(t, batch[i].Err, "entry %d", i)
	}
	// 8 x 4 KiB of body plus headers against a ~16 KiB buffer: several writes,
	// but far fewer than one per entry, which is the property that matters.
	got := sink.writes
	assert.GreaterOrEqualf(t, got, 2,
		"a batch %d bytes larger than the buffer cost %d writes; "+
			"it cannot have reached the wire", entries*bodySize, got)
	assert.Lessf(t, got, entries,
		"%d writes for %d entries — the batch is not coalescing at all", got, entries)
}

// TestSendBatch_EmptyIsANoOp guards the trivial edge so a caller draining an
// empty queue does not pay a lock acquisition or a flush.
func TestSendBatch_EmptyIsANoOp(t *testing.T) {
	c, buf := batchFixture()

	err := c.SendBatch(context.Background(), nil)

	require.NoErrorf(t, err, "SendBatch(nil) = %v, want nil", err)
	assert.Zerof(t, buf.Len(), "an empty batch emitted %d bytes", buf.Len())
}
