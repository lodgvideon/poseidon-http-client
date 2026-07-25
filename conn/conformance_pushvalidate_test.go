package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// Batch 8 — RFC 9113 §8.4 / §6.6 validation of the OPT-IN server-push accept
// path (EnablePush=true). A client MUST reset a promised stream whose promised
// request is not safe-and-cacheable, indicates request content, or names an
// :authority the server is not authoritative for; and a PUSH_PROMISE on an idle
// parent stream is a connection error. Push is disabled by default (a
// PUSH_PROMISE is then already a connection error), so these bind only the
// opt-in path.

// assertPromiseRejected drives a server that responds to the client's request,
// then sends one PUSH_PROMISE on stream 1 carrying `promise`. It asserts the
// client resets the promised stream (id 2) with PROTOCOL_ERROR and keeps the
// connection alive.
func assertPromiseRejected(t *testing.T, promise []hpack.HeaderField) {
	t.Helper()
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newPushProbe()
	finish, stop := newFinish()
	defer stop()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		drainFrames(srvFr, probe)
		enc := hpack.NewEncoder()
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}),
				EndHeaders:    true,
			})
		})
		<-asyncWrite(func() error { return srvFr.WritePushPromise(1, 2, enc.EncodeBlock(nil, promise), true, 0) })
		<-asyncWrite(func() error { return srvFr.WritePing(false, [8]byte{9}) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{Settings: AdvertisedSettings{}.defaulted(), StreamEventBuffer: 16, EnablePush: true})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	parent := openParentStream(ctx, t, c)
	_ = parent
	select {
	case <-probe.pingAck:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the PING ACK barrier")
	}

	found := false
	for _, code := range probe.rstCodes {
		if code == frame.ErrCodeProtocolError {
			found = true
		}
	}
	if !found {
		t.Errorf("promised stream not reset with PROTOCOL_ERROR; got RST codes %v", probe.rstCodes)
	}
	if probe.goAwayHit {
		t.Errorf("connection torn down (GOAWAY %v); a bad push is a stream refusal, not a connection error", probe.goAwayErr)
	}
	if !c.IsAlive() {
		t.Error("connection died on a bad PUSH_PROMISE; it must survive a stream-level refusal")
	}
	stop()
}

// TestConformance_RFC9113_Sec8_4_PushValidation_RejectsBadPromise pins the three
// promised-request refusals: a non-authoritative :authority, a method that is not
// safe-and-cacheable, and a promise that indicates request content.
func TestConformance_RFC9113_Sec8_4_PushValidation_RejectsBadPromise(t *testing.T) {
	base := func() []hpack.HeaderField {
		return []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":authority"), Value: []byte("example.com")},
			{Name: []byte(":path"), Value: []byte("/a.css")},
		}
	}
	t.Run("non-authoritative authority", func(t *testing.T) {
		p := base()
		p[2].Value = []byte("evil.example") // not the request's authority
		assertPromiseRejected(t, p)
	})
	t.Run("non-safe method", func(t *testing.T) {
		p := base()
		p[0].Value = []byte("POST") // not safe/cacheable
		assertPromiseRejected(t, p)
	})
	t.Run("indicates request content", func(t *testing.T) {
		p := append(base(), hpack.HeaderField{Name: []byte("content-length"), Value: []byte("5")})
		assertPromiseRejected(t, p)
	})
}

// TestConformance_RFC9113_Sec5_1_RefusedPushResponseDoesNotKillConn is the
// regression guard for a refusal × idle-stream interaction: refusing a promise
// still marks the promised id spent (advances lastPromisedID), so the server's
// in-flight pushed-response frames on that id are treated as a closed stream and
// discarded, NOT as an idle stream (which would tear the whole connection down,
// RFC 9113 §5.1). A regression left lastPromisedID unadvanced on the refusal
// paths, so a raced pushed response killed every sibling stream on the connection.
// Both refusal paths are covered — a malformed promised field set, and an §8.4
// validation failure — since notePromisedID runs before either.
func TestConformance_RFC9113_Sec5_1_RefusedPushResponseDoesNotKillConn(t *testing.T) {
	base := []hpack.HeaderField{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/a.css")},
	}
	t.Run("non-safe method (validation refusal)", func(t *testing.T) {
		p := append([]hpack.HeaderField(nil), base...)
		p[0].Value = []byte("POST")
		assertRefusedPushSurvives(t, p)
	})
	t.Run("malformed fields refusal", func(t *testing.T) {
		p := append(append([]hpack.HeaderField(nil), base...),
			hpack.HeaderField{Name: []byte("x-bad"), Value: []byte("v\r\ninjected")})
		assertRefusedPushSurvives(t, p)
	})
}

// assertRefusedPushSurvives drives a server that refuses the promise carrying
// `promise`, then races the pushed response onto the promised stream, and asserts
// the promise is reset with the connection kept alive.
func assertRefusedPushSurvives(t *testing.T, promise []hpack.HeaderField) {
	t.Helper()
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newPushProbe()
	finish, stop := newFinish()
	defer stop()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		if !awaitRequest(t, srvFr) {
			return
		}
		drainFrames(srvFr, probe)
		enc := hpack.NewEncoder()
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      1,
				BlockFragment: enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}),
				EndHeaders:    true,
			})
		})
		<-asyncWrite(func() error { return srvFr.WritePushPromise(1, 2, enc.EncodeBlock(nil, promise), true, 0) })
		// The server races the pushed response onto stream 2 before our RST lands.
		<-asyncWrite(func() error {
			return srvFr.WriteHeaders(frame.WriteHeadersParams{
				StreamID:      2,
				BlockFragment: enc.EncodeBlock(nil, []hpack.HeaderField{{Name: []byte(":status"), Value: []byte("200")}}),
				EndHeaders:    true,
			})
		})
		<-asyncWrite(func() error { return srvFr.WriteData(2, true, []byte("body")) })
		<-asyncWrite(func() error { return srvFr.WritePing(false, [8]byte{9}) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{Settings: AdvertisedSettings{}.defaulted(), StreamEventBuffer: 16, EnablePush: true})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	parent := openParentStream(ctx, t, c)
	_ = parent
	select {
	case <-probe.pingAck:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the PING ACK barrier (connection likely torn down by the pushed response)")
	}

	refused := false
	for _, code := range probe.rstCodes {
		if code == frame.ErrCodeProtocolError {
			refused = true
		}
	}
	if !refused {
		t.Errorf("push not refused with PROTOCOL_ERROR; RST codes %v", probe.rstCodes)
	}
	if probe.goAwayHit {
		t.Errorf("connection torn down (GOAWAY %v) by a pushed response on a refused promised stream", probe.goAwayErr)
	}
	if !c.IsAlive() {
		t.Error("connection died from an in-flight pushed response on a refused promised stream")
	}
	stop()
}

// TestConformance_RFC9113_Sec6_5_2_PushPromiseOnIdleParent_ConnError pins that a
// PUSH_PROMISE naming an idle parent stream (one the client never opened) is a
// connection error of type PROTOCOL_ERROR.
func TestConformance_RFC9113_Sec6_5_2_PushPromiseOnIdleParent_ConnError(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()
	probe := newFramingProbe()
	finish, release := newFinish()

	go pipeServer(t, srv, func(srvFr *frame.Framer) {
		drainFrames(srvFr, probe)
		enc := hpack.NewEncoder()
		block := enc.EncodeBlock(nil, []hpack.HeaderField{
			{Name: []byte(":method"), Value: []byte("GET")},
			{Name: []byte(":scheme"), Value: []byte("https")},
			{Name: []byte(":authority"), Value: []byte("example.com")},
			{Name: []byte(":path"), Value: []byte("/a.css")},
		})
		// Parent stream 1 was never opened by the client → idle.
		<-asyncWrite(func() error { return srvFr.WritePushPromise(1, 2, block, true, 0) })
		<-finish
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := NewClientConn(ctx, cli, ConnOptions{Settings: AdvertisedSettings{}.defaulted(), StreamEventBuffer: 16, EnablePush: true})
	if err != nil {
		t.Fatalf("NewClientConn: %v", err)
	}
	defer c.Close()

	if code := recvCode(t, "GOAWAY", probe.away); code != frame.ErrCodeProtocolError {
		t.Errorf("GOAWAY code = %v, want PROTOCOL_ERROR", code)
	}
	if aliveWithin(c, false, 2*time.Second) {
		t.Error("connection still alive after a PUSH_PROMISE on an idle parent stream")
	}
	release()
}
