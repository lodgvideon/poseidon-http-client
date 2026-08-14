package quic

import (
	"context"
	"crypto/tls"
	"testing"
	"testing/synctest"
	"time"
)

// The whole-handshake bound (WithHandshakeTimeout), measured rather than
// inspected.
//
// These run inside a synctest bubble, so the durations below are exact fake time
// and cost microseconds of real time — a 30-second bound is asserted as an
// equality, not as "finished under a minute". Without the bubble the raising
// case could not be a unit test at all: proving that a bound EXCEEDS the
// 10-second default requires outliving 10 seconds, and the default is not
// something a test may shrink.
//
// Raising is the case that matters. A caller could already LOWER the bound
// before this option existed, by handing Establish a ctx with a nearer deadline
// — Establish takes the minimum of the two. What it could not do was raise the
// bound above the hardcoded 10 seconds, which is what a lossy path needs: the
// PTO ladder doubles per probe (RFC 9002 §6.2.1) and, before any RTT sample has
// shrunk the base, only four probes fit inside 10 seconds (0.67 + 1.33 + 2.67 +
// 5.33 s). So a test that merely shortens the bound would pass against the
// unfixed code and prove nothing.
//
// Only the elapsed time is asserted, deliberately. The identity of the returned
// error is not stable: at expiry the ctx timer and the transport read deadline
// come due at the same instant, and depending on which the runtime services
// first the caller sees either context.DeadlineExceeded or the transport's
// i/o timeout. Both satisfy isTimeout, which is the property the engine itself
// classifies on, so that is what is asserted. Measured across three runs of a
// five-bound sweep, elapsed was exactly the bound 15 times out of 15 while the
// error identity and the probe count both varied run to run.

// blackholeConn returns a client Conn whose datagrams all vanish and which never
// receives one — the shape that makes the handshake run its bound out. faultPC
// (faultpc_test.go) is reused for its read deadline, which is what keeps the
// engine on the probe-timeout path rather than the plain-Read path.
func blackholeConn(t *testing.T, opts ...ConnOption) *Conn {
	t.Helper()
	pc := &faultPC{
		rx:        make(chan []byte),              // never delivers
		tx:        make(chan []byte),              // never reached: every write is dropped
		dropWrite: func(int) bool { return true }, // total blackhole
	}
	tp := AppendTransportParams(nil, LocalTransportParams{InitialMaxData: 1 << 20})
	c, err := NewConn(pc, &tls.Config{ServerName: "example.com"}, tp, opts...)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	return c
}

// lossyHandshake stages a handshake against a real, live server peer across a
// path that is down for the first `outage` of fake time and works afterwards. It
// reports how long Establish took and how it ended.
//
// The outage is expressed in time rather than in a count of dropped datagrams,
// and that is load-bearing. A count-based drop range makes the test flaky:
// when the bound expires the engine can emit an extra probe or two before
// unwinding, so whether datagram number N+1 exists at all varies run to run, and
// a peer that starts its TLS handshake and never finishes it strands the
// synctest bubble. Against a clock, every write before the outage ends is
// dropped no matter how many there are, which is also the more honest model of
// a real path.
func lossyHandshake(t *testing.T, outage time.Duration, opts ...ConnOption) (time.Duration, error) {
	t.Helper()
	pathUp := time.Now().Add(outage)
	cert, pool := genServerCert(t)
	clientTP := concat(
		tpInt(tpInitialMaxData, 1<<20),
		tpInt(tpInitialMaxStreamDataBidiRemote, 1<<20),
		tpInt(tpInitialMaxStreamsBidi, 16),
	)
	serverSCID := []byte{0xab, 0xcd, 0xef}

	toServer := make(chan []byte, 16)
	fromServer := make(chan []byte, 16)
	pc := &faultPC{rx: fromServer, tx: toServer,
		dropWrite: func(int) bool { return time.Now().Before(pathUp) }}

	client, err := NewConn(pc, &tls.Config{ServerName: "example.com", RootCAs: pool}, clientTP, opts...)
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	serverTP := concat(clientTP,
		tpBytes(tpInitialSourceConnectionID, serverSCID),
		tpBytes(tpOriginalDestinationConnectionID, client.origDCID))

	// stop releases the peer when the client gave up before its first datagram
	// ever arrived; every goroutine must leave before the synctest bubble ends,
	// and in the abandoned case the peer is still parked on its first receive.
	stop := make(chan struct{})
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		var dg []byte
		select {
		case dg = <-toServer: // the first client datagram to survive the drop range
		case <-stop:
			return
		}
		ci, err := AcceptInitial(dg)
		if err != nil {
			return
		}
		flight, err := StartServerHandshake(ci, &tls.Config{Certificates: []tls.Certificate{cert}}, serverTP, serverSCID)
		if err != nil {
			return
		}
		for _, d := range flight.Datagrams {
			fromServer <- d
		}
		// Drive the peer's own handshake to completion. Its TLS goroutine is
		// parked on the client's Finished, and an unfinished one would keep the
		// bubble alive after the test returns.
		for !flight.Complete {
			var cdg []byte
			select {
			case cdg = <-toServer:
			case <-stop:
				return
			}
			crypto := handshakeCrypto(cdg, flight.HandshakeOpener)
			if len(crypto) == 0 {
				continue
			}
			if err := flight.HandleClientHandshake(crypto); err != nil {
				return
			}
		}
	}()

	start := time.Now()
	err = client.Establish(context.Background())
	elapsed := time.Since(start)
	close(stop)
	<-peerDone
	return elapsed, err
}

// TestConn_HandshakeTimeout_RescuesALiveHandshake is the measurement behind the
// change, and the reason the bound is absolute rather than idle-based.
//
// A 15-second outage outlasts the four probes that fit inside the 10-second
// default (0.67 + 1.33 + 2.67 + 5.33 = 9.99 s), so under the default the client
// gives up while the path is still down. The server is alive the whole time and
// answers the first probe that gets through, so with a bound large enough to
// reach it the same handshake completes. That is the defect in one line: the
// connection was abandoned while it was doing exactly what RFC 9002 §6.2.2.1
// tells it to do.
//
// It also settles absolute-versus-idle. Nothing arrives during the outage: no
// ACK, no CRYPTO, no packet at all. An idle bound resets on progress, and there
// is no progress here to reset on, so a 10-second idle bound would expire at the
// same instant a 10-second absolute bound does. Only a larger bound rescues this
// handshake, which is why the fix is a configurable absolute bound rather than
// quic-go's HandshakeIdleTimeout shape.
//
// The control matters as much as the experiment: shortenedOutage completes under
// the untouched default, which is what proves the peer really does answer and
// that the experiment's failure is the bound rather than a broken fixture.
func TestConn_HandshakeTimeout_RescuesALiveHandshake(t *testing.T) {
	const outage = 15 * time.Second
	const shortenedOutage = 3 * time.Second

	t.Run("control: a shorter outage completes under the default", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			elapsed, err := lossyHandshake(t, shortenedOutage)
			if err != nil {
				t.Fatalf("handshake failed after %v across a %v outage: %v — the peer must "+
					"answer here, or the experiment below fails for the wrong reason",
					elapsed, shortenedOutage, err)
			}
			if elapsed >= defaultHandshakeTimeout {
				t.Errorf("completed in %v, at or past the %v default — the control has to "+
					"land inside the default bound to be a control", elapsed, defaultHandshakeTimeout)
			}
		})
	})

	t.Run("default bound abandons it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			elapsed, err := lossyHandshake(t, outage)
			if err == nil {
				t.Fatalf("handshake completed in %v under the default bound; then this "+
					"test no longer stages the cut-off it exists to measure", elapsed)
			}
			if elapsed != defaultHandshakeTimeout {
				t.Errorf("gave up after %v, want exactly %v — it must be the bound that "+
					"ends this, not some other timer", elapsed, defaultHandshakeTimeout)
			}
		})
	})

	t.Run("raised bound completes the same handshake", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			elapsed, err := lossyHandshake(t, outage, WithHandshakeTimeout(60*time.Second))
			if err != nil {
				t.Fatalf("handshake failed after %v with a 60s bound: %v — the peer is "+
					"alive and answers once the path is back, so only the bound could "+
					"have stopped it", elapsed, err)
			}
			if elapsed <= defaultHandshakeTimeout {
				t.Errorf("completed in %v, inside the %v default — then the raised bound "+
					"bought nothing and this proves nothing", elapsed, defaultHandshakeTimeout)
			}
		})
	})
}

// TestConn_HandshakeTimeout_BoundIsHonoured pins that the whole-handshake bound
// is the option's value, that it can exceed the built-in default — the defect:
// the bound was hardcoded, so no caller could raise it for a lossy path — and
// that a nearer caller ctx still wins.
func TestConn_HandshakeTimeout_BoundIsHonoured(t *testing.T) {
	tests := []struct {
		name    string
		opt     time.Duration // 0 = pass no option at all
		ctxDL   time.Duration // 0 = context.Background()
		want    time.Duration
		explain string
	}{
		{
			name:    "raised above the default",
			opt:     30 * time.Second,
			want:    30 * time.Second,
			explain: "the option must be able to RAISE the bound; hardcoded, it capped at 10s",
		},
		{
			name:    "default when no option is passed",
			want:    defaultHandshakeTimeout,
			explain: "adding the option must not move the bound for existing callers",
		},
		{
			name:    "non-positive selects the default",
			opt:     -1,
			want:    defaultHandshakeTimeout,
			explain: "a zero field is what this package's bare &Conn{} literals carry",
		},
		{
			name:    "nearer caller ctx wins over a larger option",
			opt:     30 * time.Second,
			ctxDL:   4 * time.Second,
			want:    4 * time.Second,
			explain: "the two bounds compose as a minimum, so ctx can still tighten",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var opts []ConnOption
				if tc.opt != 0 {
					opts = append(opts, WithHandshakeTimeout(tc.opt))
				}
				c := blackholeConn(t, opts...)

				ctx := context.Background()
				if tc.ctxDL != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.ctxDL)
					defer cancel()
				}

				start := time.Now()
				err := c.Establish(ctx)
				elapsed := time.Since(start)

				if err == nil {
					t.Fatal("Establish succeeded against a blackhole; it can only end by " +
						"running its bound out, so the measurement below would be meaningless")
				}
				if !isTimeout(err) {
					t.Fatalf("Establish returned %v, which isTimeout rejects — the bound "+
						"expired, so the caller must see a timeout", err)
				}
				if elapsed != tc.want {
					t.Errorf("handshake gave up after %v, want exactly %v (%s)",
						elapsed, tc.want, tc.explain)
				}
			})
		})
	}
}
