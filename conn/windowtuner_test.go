package conn

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/header"
)

// tunerFor builds a tuner and the Conn it publishes into, both bare, so the
// growth rule can be driven without a transport.
func tunerFor(t *testing.T, maxWindow uint32) (*recvWindowTuner, *Conn) {
	t.Helper()
	c := &Conn{}
	c.connRecvTarget.Store(connInitialRecvWindow)
	c.streamRecvTarget.Store(connInitialRecvWindow)
	return &recvWindowTuner{max: maxWindow, probeThreshold: minProbeBytes}, c
}

// sample drives one complete measurement: enough bytes to trigger a probe, then
// sampleBytes delivered while it is outstanding, then the ACK.
func sample(t *testing.T, tn *recvWindowTuner, c *Conn, sampleBytes uint32) {
	t.Helper()
	require.Truef(t, tn.onData(uint32(tn.probeThreshold)),
		"onData(%d) did not open a sample at threshold %d", tn.probeThreshold, tn.probeThreshold)
	if sampleBytes > 0 {
		tn.onData(sampleBytes)
	}
	tn.onAck(c)
}

// TestRecvWindowTuner_TargetsTwiceTheSample pins the growth rule: a round trip
// that delivered S bytes wants a window of 2S, because a window of exactly S is
// what the peer stalled against.
func TestRecvWindowTuner_TargetsTwiceTheSample(t *testing.T) {
	tn, c := tunerFor(t, 8<<20)
	const s = 256 << 10
	sample(t, tn, c, s)

	assert.Equalf(t, uint32(2*s), c.connRecvTarget.Load(),
		"connRecvTarget = %d, want %d", c.connRecvTarget.Load(), 2*s)
	assert.Equalf(t, uint32(2*s), c.streamRecvTarget.Load(),
		"streamRecvTarget = %d, want %d", c.streamRecvTarget.Load(), 2*s)
}

// TestRecvWindowTuner_ClampsToCeiling pins that the ceiling is a hard bound, not
// a target the estimate is allowed to overshoot.
func TestRecvWindowTuner_ClampsToCeiling(t *testing.T) {
	const ceiling = 128 << 10
	tn, c := tunerFor(t, ceiling)
	sample(t, tn, c, 4<<20) // asks for 8 MiB

	assert.Equalf(t, uint32(ceiling), c.connRecvTarget.Load(),
		"connRecvTarget = %d, want the ceiling %d", c.connRecvTarget.Load(), ceiling)
	assert.Equalf(t, uint32(ceiling), c.streamRecvTarget.Load(),
		"streamRecvTarget = %d, want the ceiling %d", c.streamRecvTarget.Load(), ceiling)
}

// TestRecvWindowTuner_NeverShrinks pins that a later, smaller sample cannot pull
// a window back down. WINDOW_UPDATE can only add, so a shrinking target would
// desynchronise our accounting from the peer's rather than reduce anything.
func TestRecvWindowTuner_NeverShrinks(t *testing.T) {
	tn, c := tunerFor(t, 8<<20)
	sample(t, tn, c, 512<<10)
	high := c.connRecvTarget.Load()
	require.Equalf(t, uint32(1<<20), high, "setup: connRecvTarget = %d, want %d", high, 1<<20)

	sample(t, tn, c, 1<<10) // a trickle

	assert.Equalf(t, high, c.connRecvTarget.Load(),
		"connRecvTarget = %d after a small sample, want it held at %d", c.connRecvTarget.Load(), high)
}

// TestRecvWindowTuner_BacksOffWhenNothingGrew pins that a sample which taught us
// nothing costs the next one more evidence, so a connection whose window is
// already big enough stops paying for PINGs.
func TestRecvWindowTuner_BacksOffWhenNothingGrew(t *testing.T) {
	tn, c := tunerFor(t, 8<<20)
	sample(t, tn, c, 512<<10)
	require.EqualValuesf(t, minProbeBytes, tn.probeThreshold,
		"after growth probeThreshold = %d, want it reset to %d", tn.probeThreshold, minProbeBytes)
	before := tn.probeThreshold

	sample(t, tn, c, 1<<10)

	assert.EqualValuesf(t, before*2, tn.probeThreshold,
		"after a sample that grew nothing probeThreshold = %d, want %d", tn.probeThreshold, before*2)
	// And it is bounded.
	for i := 0; i < 40; i++ {
		sample(t, tn, c, 1<<10)
	}
	assert.EqualValuesf(t, maxProbeBytes, tn.probeThreshold,
		"probeThreshold = %d, want it capped at %d", tn.probeThreshold, maxProbeBytes)
}

// TestRecvWindowTuner_UnsolicitedAckCannotGrowTheWindow is the peer-input case.
// A peer that forges the tuner's PING ACK must not be able to manufacture
// window: with no sample outstanding the ACK is inert, and one that lands
// mid-sample only truncates the count, which can withhold growth but never
// invent it.
func TestRecvWindowTuner_UnsolicitedAckCannotGrowTheWindow(t *testing.T) {
	tn, c := tunerFor(t, 8<<20)
	before := c.connRecvTarget.Load()

	for i := 0; i < 100; i++ {
		tn.onAck(c) // no sample outstanding
	}
	assert.Equalf(t, before, c.connRecvTarget.Load(),
		"connRecvTarget = %d after 100 unsolicited ACKs, want %d", c.connRecvTarget.Load(), before)

	// Mid-sample: the peer ACKs immediately, before any DATA has been counted.
	require.True(t, tn.onData(uint32(tn.probeThreshold)), "no sample opened")
	tn.onAck(c)

	assert.Equalf(t, before, c.connRecvTarget.Load(),
		"connRecvTarget = %d after an ACK with an empty sample, want %d", c.connRecvTarget.Load(), before)
}

// TestRecvWindowTuner_ProbeFailedReopensSampling pins that a PING the transport
// refused does not park the tuner: the next threshold's worth of data must be
// able to open a fresh sample.
func TestRecvWindowTuner_ProbeFailedReopensSampling(t *testing.T) {
	tn, c := tunerFor(t, 8<<20)
	require.True(t, tn.onData(uint32(tn.probeThreshold)), "no sample opened")

	tn.probeFailed()

	require.True(t, tn.onData(uint32(tn.probeThreshold)),
		"probeFailed left the tuner unable to sample again")
	tn.onData(256 << 10)
	tn.onAck(c)
	assert.Equalf(t, uint32(512<<10), c.connRecvTarget.Load(),
		"connRecvTarget = %d, want %d", c.connRecvTarget.Load(), 512<<10)
}

// TestRecvWindowCeiling pins where the default bound comes from: one stream's
// event budget, which is the memory the connection has already committed to
// buffering for it.
func TestRecvWindowCeiling(t *testing.T) {
	cases := []struct {
		name  string
		opts  ConnOptions
		floor uint32
		want  uint32
	}{
		{
			name:  "derived from the event budget",
			opts:  ConnOptions{StreamEventBuffer: 8, Settings: AdvertisedSettings{MaxFrameSize: 16384}},
			floor: 65535,
			want:  8 * 16384,
		},
		{
			name:  "explicit MaxRecvWindow wins",
			opts:  ConnOptions{StreamEventBuffer: 8, Settings: AdvertisedSettings{MaxFrameSize: 16384}, MaxRecvWindow: 1 << 20},
			floor: 65535,
			want:  1 << 20,
		},
		{
			name:  "never below the window already in effect",
			opts:  ConnOptions{StreamEventBuffer: 1, Settings: AdvertisedSettings{MaxFrameSize: 16384}},
			floor: 65535,
			want:  65535,
		},
		{
			name:  "clamped to the absolute ceiling",
			opts:  ConnOptions{StreamEventBuffer: 8, Settings: AdvertisedSettings{MaxFrameSize: 16384}, MaxRecvWindow: 1 << 30},
			floor: 65535,
			want:  maxAutoRecvWindow,
		},
		{
			name:  "a huge derived budget is clamped too",
			opts:  ConnOptions{StreamEventBuffer: 100000, Settings: AdvertisedSettings{MaxFrameSize: 16777215}},
			floor: 65535,
			want:  maxAutoRecvWindow,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := recvWindowCeiling(c.opts, c.floor)

			assert.Equalf(t, c.want, got, "recvWindowCeiling = %d, want %d", got, c.want)
		})
	}
}

// TestRefundIncrement pins that the target-based refund is a strict
// generalisation: with no target published, or with a target sitting where the
// window started, it returns exactly what was spent.
func TestRefundIncrement(t *testing.T) {
	cases := []struct {
		name   string
		target uint32
		window int32
		spent  uint32
		want   uint32
	}{
		{"no target published falls back to what was spent", 0, 33000, 32535, 32535},
		{"target at the starting window returns what was spent", 65535, 33000, 32535, 32535},
		{"a raised target tops the window up", 131072, 33000, 32535, 131072 - 33000},
		{"a window already at the target needs nothing", 65535, 65535, 0, 0},
		{"a window past the target needs nothing", 65535, 70000, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := refundIncrement(c.target, c.window, c.spent)

			assert.Equalf(t, c.want, got, "refundIncrement(%d, %d, %d) = %d, want %d",
				c.target, c.window, c.spent, got, c.want)
		})
	}
}

// TestNewRecvWindowTuner_OffByDefault pins that the feature is opt-in: without
// the option there is no tuner, and with no tuner nothing about the flow-control
// path changes.
func TestNewRecvWindowTuner_OffByDefault(t *testing.T) {
	opts := ConnOptions{AutoTuneRecvWindow: true}.defaulted()

	off := newRecvWindowTuner(ConnOptions{}.defaulted(), 65535)
	on := newRecvWindowTuner(opts, 65535)

	assert.Nilf(t, off, "newRecvWindowTuner returned %+v with AutoTuneRecvWindow unset, want nil", off)
	assert.NotNil(t, on, "newRecvWindowTuner returned nil with AutoTuneRecvWindow set")
}

// hasBDPPing reports whether a raw frame stream contains the tuner's PING: a
// non-ACK PING frame whose 8-byte payload is bdpPingPayload.
func hasBDPPing(t *testing.T, b []byte) bool {
	t.Helper()
	for len(b) >= 9 {
		length := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		ftype, flags := b[3], b[4]
		body := b[9 : 9+length]
		if ftype == 0x6 && flags&0x1 == 0 { // PING, not ACK
			if len(body) != 8 {
				t.Fatalf("PING payload = %d bytes, want 8", len(body))
			}
			if [8]byte(body) == bdpPingPayload {
				return true
			}
		}
		b = b[9+length:]
	}
	return false
}

// TestConn_AutoTuneRecvWindow_ProbesAndTopsUpTheWindow is the end-to-end claim,
// asserted on the wire rather than against a live peer: the connection writes a
// BDP PING once enough DATA has arrived, the ACK raises both targets, and the
// next refund tops both windows up to the new target instead of merely
// returning what was spent.
//
// It drives onDataReceived and deliverPingAck directly, which is what makes it
// deterministic. The same sequence against an httptest peer depends on how much
// data the server happened to have in flight when its ACK came back, and on a
// loaded machine that is routinely too little to move the target — a real
// property of loopback, where the bandwidth-delay product genuinely is tiny, and
// a useless thing to assert on.
func TestConn_AutoTuneRecvWindow_ProbesAndTopsUpTheWindow(t *testing.T) {
	var buf bytes.Buffer
	opts := ConnOptions{
		AutoTuneRecvWindow: true,
		MaxRecvWindow:      1 << 20,
	}.defaulted()
	c := &Conn{
		opts:           opts,
		fr:             frame.NewFramer(&buf, bytes.NewReader(nil)),
		streams:        map[uint32]*Stream{},
		readerDone:     make(chan struct{}),
		connRecvWindow: int32(connInitialRecvWindow),
	}
	c.connRecvTarget.Store(connInitialRecvWindow)
	c.streamRecvTarget.Store(opts.Settings.InitialWindowSize)
	c.tuner = newRecvWindowTuner(opts, opts.Settings.InitialWindowSize)
	s := newStream(1, 8, c, int32(opts.Settings.InitialWindowSize))
	c.streams[1] = s

	// Enough DATA to open a sample, but under the 32 KiB refund threshold so
	// this step emits the PING and nothing else.
	if err := c.onDataReceived(s, minProbeBytes); err != nil {
		t.Fatalf("onDataReceived: %v", err)
	}
	if !hasBDPPing(t, buf.Bytes()) {
		t.Fatalf("no BDP PING after %d bytes of DATA, want one", minProbeBytes)
	}

	// Deliver the sample: three refund-threshold chunks while the PING is
	// outstanding.
	const sampleBytes = 3 * recvWindowRefundThreshold
	for i := 0; i < 3; i++ {
		if err := c.onDataReceived(s, recvWindowRefundThreshold); err != nil {
			t.Fatalf("onDataReceived: %v", err)
		}
	}

	// The ACK closes the sample. The target is twice what one round trip
	// carried, which is under the 1 MiB ceiling this connection was given.
	c.deliverPingAck(bdpPingPayload)
	const wantTarget = 2 * sampleBytes
	if got := c.connRecvTarget.Load(); got != wantTarget {
		t.Fatalf("connRecvTarget = %d after the ACK, want %d", got, wantTarget)
	}
	if got := c.streamRecvTarget.Load(); got != wantTarget {
		t.Fatalf("streamRecvTarget = %d after the ACK, want %d", got, wantTarget)
	}

	// The peer only learns at the next refund, which must now top both windows
	// up to the target rather than return the 32 KiB that were spent.
	buf.Reset()
	if err := c.onDataReceived(s, recvWindowRefundThreshold); err != nil {
		t.Fatalf("onDataReceived: %v", err)
	}
	updates := parseWindowUpdates(t, buf.Bytes())
	if len(updates) != 2 {
		t.Fatalf("WINDOW_UPDATE count = %d, want 2 (stream + conn)", len(updates))
	}
	for _, u := range updates {
		if u.increment <= recvWindowRefundThreshold {
			t.Errorf("WINDOW_UPDATE(stream %d) increment = %d, want more than the %d spent: "+
				"the refund is still returning what was consumed rather than reaching the target",
				u.streamID, u.increment, recvWindowRefundThreshold)
		}
	}
	if got := c.connRecvWindow; got != wantTarget {
		t.Errorf("connRecvWindow = %d after the refund, want the target %d", got, wantTarget)
	}
	s.mu.Lock()
	streamWindow := s.recvWindow
	s.mu.Unlock()
	if streamWindow != wantTarget {
		t.Errorf("stream recvWindow = %d after the refund, want the target %d", streamWindow, wantTarget)
	}
}

// TestIntegration_AutoTuneRecvWindow_RealPeerCompletes is the smoke test the
// deterministic one cannot be: a real net/http2 peer has to accept the tuner's
// PING and keep serving. It asserts the body, not the window — how far the
// window moves on loopback is a property of loopback.
func TestIntegration_AutoTuneRecvWindow_RealPeerCompletes(t *testing.T) {
	const bodySize = 3 << 20
	body := make([]byte, bodySize)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand: %v", err)
	}
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-length", fmt.Sprintf("%d", len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// A large event buffer for the reason dialServerOpts documents, and it also
	// sets the derived ceiling: 512 x 16 KiB is 8 MiB.
	c, err := Dial(ctx, srv.Listener.Addr().String(), ConnOptions{
		Dialer:             &TLSDialer{Config: cfg},
		StreamEventBuffer:  512,
		AutoTuneRecvWindow: true,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/big")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	got := drainBody(ctx, t, s)
	if len(got) != len(body) {
		t.Fatalf("got %d bytes, want %d", len(got), len(body))
	}
	for i := range got {
		if got[i] != body[i] {
			t.Fatalf("body differs at byte %d", i)
		}
	}
	t.Logf("after %d bytes: connRecvTarget=%d streamRecvTarget=%d (started at %d)",
		bodySize, c.connRecvTarget.Load(), c.streamRecvTarget.Load(), connInitialRecvWindow)
}

// TestIntegration_AutoTuneRecvWindow_OffLeavesTheWindowAlone is the control for
// the test above: the same download with the option unset must leave both
// targets exactly where the protocol put them.
func TestIntegration_AutoTuneRecvWindow_OffLeavesTheWindowAlone(t *testing.T) {
	const bodySize = 512 << 10
	body := make([]byte, bodySize)
	srv, cfg := startH2TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-length", fmt.Sprintf("%d", len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	c := dialServerOpts(t, srv, cfg, 512)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := c.NewStream(ctx)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := s.SendHeaders(ctx, []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/big")},
	}, true); err != nil {
		t.Fatalf("SendHeaders: %v", err)
	}
	if got := drainBody(ctx, t, s); len(got) != len(body) {
		t.Fatalf("got %d bytes, want %d", len(got), len(body))
	}
	if got := c.connRecvTarget.Load(); got != connInitialRecvWindow {
		t.Errorf("connRecvTarget = %d with auto-tuning off, want it held at %d", got, connInitialRecvWindow)
	}
	if got := c.streamRecvTarget.Load(); got != c.opts.Settings.InitialWindowSize {
		t.Errorf("streamRecvTarget = %d with auto-tuning off, want it held at %d",
			got, c.opts.Settings.InitialWindowSize)
	}
}
