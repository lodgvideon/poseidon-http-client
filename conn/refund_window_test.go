package conn

import (
	"bytes"
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/header"
)

// TestConn_SmallAdvertisedWindow_StillCompletes pins a download against every
// advertised receive window a caller may legally choose.
//
// AdvertisedSettings.InitialWindowSize is public, documented as configurable,
// and RFC 7540 §6.5.2 permits anything from 0 to 2^31-1. But the refund that
// replenishes a stream's window only fired once recvWindowRefundThreshold
// (32768) bytes had accumulated — a constant, chosen as half of the *default*
// 65535 window. Advertise anything below it and the peer sends exactly one
// window's worth, exhausts its credit, and waits for a WINDOW_UPDATE that can
// never arrive, because the refund counter can never reach a threshold larger
// than the window itself.
//
// Measured before the fix, downloading 200 KiB:
//
//	InitialWindowSize=65535  -> 204800/204800 bytes in 19ms
//	InitialWindowSize=32768  -> 204800/204800 bytes in  4ms
//	InitialWindowSize=16384  ->  16384/204800 bytes in  4s   context deadline exceeded
//	InitialWindowSize=8192   ->   8192/204800 bytes in  4s   context deadline exceeded
//
// The bytes received equal the window exactly, and the cliff sits exactly at the
// threshold. 16384 is not an exotic choice — it is the default MAX_FRAME_SIZE.
//
// 65535 and 32768 are the controls: they pass before the fix, so a failure here
// is the small-window path, not a broken harness.
func TestConn_SmallAdvertisedWindow_StillCompletes(t *testing.T) {
	const bodyLen = 200 << 10 // comfortably larger than any window under test
	body := bytes.Repeat([]byte("x"), bodyLen)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	for _, window := range []uint32{65535, 32768, 16384, 8192, 1024} {
		t.Run(windowName(window), func(t *testing.T) {
			d := &TLSDialer{Config: &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{"h2"},
			}}
			nc, err := d.Dial(t.Context(), srv.Listener.Addr().String())
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			opts := ConnOptions{}
			opts.Settings.InitialWindowSize = window
			// Size the event buffer to the whole download.
			//
			// The default is 8, and nothing in this client applies backpressure
			// from Recv to the reader goroutine: the reader refunds the window as
			// it processes each DATA frame, which lets the peer send more, which
			// the reader pushes as more events. The only limit on how far ahead
			// the reader gets is how fast this loop drains — and a full channel
			// is not a wait, it is a DROP: push() closes the stream and delivers
			// EventReset with EndStream set (conn/stream.go), so the loop below
			// exits early and the test reports a short read.
			//
			// That matters most at the small windows this test exists for: a
			// 1024-byte window means ~200 DATA events for a 200 KiB body, so an
			// 8-deep buffer needs the consumer to keep pace with the socket for
			// all 200. On a loaded CI runner under -race it does not, and the
			// failure surfaced as "the peer is waiting for a WINDOW_UPDATE" —
			// accusing the very bug this test pins, which was not what happened.
			// Event-buffer capacity is not what this test is about; make it a
			// non-factor rather than a coin flip.
			opts.StreamEventBuffer = 512
			c, err := NewClientConn(t.Context(), nc, opts)
			if err != nil {
				t.Fatalf("NewClientConn: %v", err)
			}
			defer func() { _ = c.Close() }()

			s, err := c.NewStream(t.Context())
			if err != nil {
				t.Fatalf("NewStream: %v", err)
			}
			if err := s.SendHeaders(t.Context(), getHeaders(), true); err != nil {
				t.Fatalf("SendHeaders: %v", err)
			}

			// Bound every Recv on its own, not the whole loop: the deadlock this
			// pins is a *stall* — a Recv that never returns because the window is
			// spent — so the clock must reset on each byte that does arrive.
			// A single loop-wide deadline instead measures total transfer time,
			// which under -race on a loaded runner is long enough to fire on a
			// download that is merely slow, not stalled (seen in CI: 49152 of
			// 204800 at exactly the 5s mark).
			var got int
			for {
				recvCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				ev, err := s.Recv(recvCtx)
				cancel()
				if err != nil {
					t.Fatalf("stalled after %d of %d bytes with InitialWindowSize=%d: %v — "+
						"the peer spent its window and is waiting for a WINDOW_UPDATE that a "+
						"refund threshold larger than the window can never send",
						got, bodyLen, window, err)
				}
				if ev.Type == EventData {
					got += len(ev.Data)
					GetDataBufPool().Put(ev.DataSlab)
				}
				// A reset ends the stream too, and would otherwise be
				// indistinguishable from a clean finish at the check below — the
				// short read would then be blamed on the refund threshold, which
				// is not what happened. Name the real cause instead.
				if ev.Type == EventReset {
					t.Fatalf("stream reset (code %v) after %d of %d bytes with "+
						"InitialWindowSize=%d — this is NOT the refund bug: the client "+
						"reset its own stream, which push() does when the event buffer "+
						"fills and this loop has fallen behind the reader",
						ev.RSTCode, got, bodyLen, window)
				}
				if ev.EndStream {
					break
				}
			}
			if got != bodyLen {
				t.Errorf("received %d of %d bytes with InitialWindowSize=%d; the peer is waiting for a "+
					"WINDOW_UPDATE that a refund threshold larger than the window can never send",
					got, bodyLen, window)
			}
		})
	}
}

func windowName(w uint32) string {
	switch w {
	case 65535:
		return "default_65535"
	case 32768:
		return "at_threshold_32768"
	default:
		return "below_threshold"
	}
}

func getHeaders() []header.Field {
	return []header.Field{
		{Name: []byte(":method"), Value: []byte("GET")},
		{Name: []byte(":scheme"), Value: []byte("https")},
		{Name: []byte(":authority"), Value: []byte("example.com")},
		{Name: []byte(":path"), Value: []byte("/")},
	}
}
