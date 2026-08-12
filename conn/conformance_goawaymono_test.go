package conn

import (
	"sync"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/frame"
)

// TestConformance_RFC9113_Sec6_8_GoAwaySendPeerScopedAndMonotonic pins two rules
// for the GOAWAY frames a client SENDS. RFC 9113 §6.8: the last-stream-id is the
// highest stream the sender acted on — for a client that is a peer-initiated
// (even, pushed) stream, so with no push it is 0, not the client's own last
// request id. And "Endpoints MUST NOT increase the value they send in the last
// stream identifier": a later GOAWAY may not advertise a larger id than an
// earlier one.
func TestConformance_RFC9113_Sec6_8_GoAwaySendPeerScopedAndMonotonic(t *testing.T) {
	c := &Conn{}
	c.goAwaySentLast.Store(goAwayNoneSent)
	c.nextID = 7 // client opened streams 1,3,5 — must NOT leak into the sent value

	if got := c.goAwayLastStreamIDToSend(); got != 0 {
		t.Errorf("no-push GOAWAY last-stream-id = %d, want 0 (client acts on peer-initiated streams)", got)
	}
	// A push later raises lastPromisedID, but a GOAWAY already went out at 0 — the
	// next one must not increase past it.
	c.lastPromisedID = 6
	if got := c.goAwayLastStreamIDToSend(); got != 0 {
		t.Errorf("successive GOAWAY raised the last-stream-id to %d, want 0 (MUST NOT increase, §6.8)", got)
	}
}

// TestConformance_RFC9113_Sec6_8_GoAwaySendPushScoped pins that a client that DID
// process a pushed stream advertises that (even) id.
func TestConformance_RFC9113_Sec6_8_GoAwaySendPushScoped(t *testing.T) {
	c := &Conn{}
	c.goAwaySentLast.Store(goAwayNoneSent)
	c.lastPromisedID = 4
	if got := c.goAwayLastStreamIDToSend(); got != 4 {
		t.Errorf("GOAWAY last-stream-id = %d, want 4 (last pushed stream)", got)
	}
}

// TestConformance_RFC9113_Sec6_8_GoAwayReceivedClampsRaisedLastID pins the
// receive-side defense: a second GOAWAY that tries to RAISE the last-stream-id is
// clamped (§6.8 forbids the peer from raising it), while one that lowers it is
// honored.
func TestConformance_RFC9113_Sec6_8_GoAwayReceivedClampsRaisedLastID(t *testing.T) {
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)

	c.onGoAwayReceived(5, frame.ErrCodeNoError)
	if got := c.goAwayLastStreamID.Load(); got != 5 {
		t.Fatalf("first GOAWAY last-stream-id = %d, want 5", got)
	}
	c.onGoAwayReceived(9, frame.ErrCodeNoError) // a raise — must be clamped
	if got := c.goAwayLastStreamID.Load(); got != 5 {
		t.Errorf("a raised second GOAWAY set last-stream-id to %d, want 5 (clamped, §6.8)", got)
	}
	c.onGoAwayReceived(3, frame.ErrCodeNoError) // a lower — honored
	if got := c.goAwayLastStreamID.Load(); got != 3 {
		t.Errorf("a lowered GOAWAY set last-stream-id to %d, want 3", got)
	}
}

// TestConformance_RFC9113_Sec6_8_SecondGoAwayRefinesCodeWithoutRaisingLastID
// pins the asymmetry between the two fields of a repeated GOAWAY.
//
// §6.8 provides for exactly this sequence: an endpoint that wants a graceful
// shutdown sends NO_ERROR first and "MAY" follow it with a second GOAWAY
// carrying a real error code. So the REASON must track the newest frame, while
// the last-stream-id must never grow — the set of streams the peer claims to
// have processed can only shrink.
//
// Clamping both, which is the easy mistake, would leave a client reading
// NO_ERROR while the peer is telling it the connection died of a protocol
// error.
func TestConformance_RFC9113_Sec6_8_SecondGoAwayRefinesCodeWithoutRaisingLastID(t *testing.T) {
	c := &Conn{streams: map[uint32]*Stream{}}
	c.fcOutCond = sync.NewCond(&c.fcOutMu)

	c.onGoAwayReceived(5, frame.ErrCodeNoError)
	if got := frame.ErrCode(c.goAwayCode.Load()); got != frame.ErrCodeNoError {
		t.Fatalf("first GOAWAY code = %v, want NO_ERROR", got)
	}

	// The peer escalates: same shutdown, now with a diagnosis, and an id it is
	// not allowed to raise.
	c.onGoAwayReceived(9, frame.ErrCodeProtocolError)
	if got := frame.ErrCode(c.goAwayCode.Load()); got != frame.ErrCodeProtocolError {
		t.Errorf("second GOAWAY code = %v, want PROTOCOL_ERROR — the refined reason "+
			"must replace the initial NO_ERROR (§6.8)", got)
	}
	if got := c.goAwayLastStreamID.Load(); got != 5 {
		t.Errorf("last-stream-id = %d, want 5 — a second GOAWAY may refine the code "+
			"but MUST NOT raise the id", got)
	}
}
