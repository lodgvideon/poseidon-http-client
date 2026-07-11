//go:build ignore

// Command lossproxy is a test-only UDP relay between a QUIC client and an
// upstream server that injects network faults to exercise recovery (RFC 9002):
// in each direction it drops LOSS_PCT% of datagrams and reorders REORDER_PCT% of
// them. Reordering is a guaranteed swap, not a timed gamble: a selected datagram
// is held and the next same-direction datagram overtakes it (a flight-final
// datagram, with nothing behind it, is released by a short fallback timer so it
// is never lost). Each direction has its own seeded RNG and keeps drop/reorder
// counters that are logged on shutdown, so a run's log proves the fault was
// actually injected.
//
// It handles the interop suite's sequential single-connection tests: the current
// client address is tracked and upstream responses relayed back to it.
//
// Env: UPSTREAM=host:port (required), LOSS_PCT (default 10), REORDER_PCT
// (default 0), SEED (default 1). //go:build ignore keeps it out of package
// builds; run with `go run test/integration/http3/lossproxy.go`.
package main

import (
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 || n > 100 {
		log.Fatalf("lossproxy: %s=%q must be an integer in [0,100]: %v", name, v, err)
	}
	return n
}

// faulter injects loss and reorder into one direction's datagram stream.
type faulter struct {
	name       string
	lossPct    int
	reorderPct int

	mu      sync.Mutex // guards rng, held, heldGen
	rng     *rand.Rand // a custom Source is not concurrent-safe
	held    []byte     // a datagram waiting to be overtaken (reorder)
	heldGen uint64     // identifies the current held datagram for the fallback timer

	dropped   atomic.Int64
	reordered atomic.Int64
	forwarded atomic.Int64
}

func (f *faulter) roll(pct int) bool {
	if pct <= 0 {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rng.Intn(100) < pct
}

// forward applies loss then reorder to one datagram. send delivers a surviving
// datagram; it is called synchronously (the read buffer is still valid) for
// immediate datagrams and from a goroutine (with a private copy) for a held one.
func (f *faulter) forward(data []byte, send func([]byte)) {
	if f.roll(f.lossPct) {
		f.dropped.Add(1)
		return
	}
	// If a datagram is held for reorder, this one overtakes it: send this first,
	// then release the held one — a guaranteed swap.
	f.mu.Lock()
	h := f.held
	f.held = nil
	f.mu.Unlock()
	if h != nil {
		send(data) // synchronous, buffer still valid
		send(h)    // the held copy, now after — swapped
		f.forwarded.Add(2)
		return
	}
	if f.roll(f.reorderPct) {
		d := append([]byte(nil), data...) // copy: released later
		f.mu.Lock()
		f.held = d
		f.heldGen++
		gen := f.heldGen
		f.mu.Unlock()
		f.reordered.Add(1)
		// Fallback for a flight-final datagram (nothing overtakes it): release it
		// so it is never lost, unless a later datagram already did.
		go func() {
			time.Sleep(30 * time.Millisecond)
			f.mu.Lock()
			var out []byte
			if f.heldGen == gen && f.held != nil {
				out, f.held = f.held, nil
			}
			f.mu.Unlock()
			if out != nil {
				send(out)
				f.forwarded.Add(1)
			}
		}()
		return
	}
	send(data) // synchronous
	f.forwarded.Add(1)
}

func (f *faulter) logStats() {
	log.Printf("lossproxy %s: forwarded=%d dropped=%d reordered=%d",
		f.name, f.forwarded.Load(), f.dropped.Load(), f.reordered.Load())
}

func main() {
	loss := envInt("LOSS_PCT", 10)
	reorder := envInt("REORDER_PCT", 0)
	seed := int64(envInt("SEED", 1))

	upstream := os.Getenv("UPSTREAM")
	if upstream == "" {
		log.Fatal("lossproxy: UPSTREAM (host:port) required")
	}
	upAddr, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		log.Fatal(err)
	}
	down, err := net.ListenUDP("udp", &net.UDPAddr{Port: 443})
	if err != nil {
		log.Fatal(err)
	}
	up, err := net.DialUDP("udp", nil, upAddr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("lossproxy: :443 <-> %s, drop ~%d%%, reorder ~%d%% each way (seed %d)", upstream, loss, reorder, seed)

	// Independent RNGs so each direction's fault sequence is reproducible.
	toClient := &faulter{name: "up->client", lossPct: loss, reorderPct: reorder, rng: rand.New(rand.NewSource(seed))}
	toUp := &faulter{name: "client->up", lossPct: loss, reorderPct: reorder, rng: rand.New(rand.NewSource(seed + 1))}

	// `docker compose down` sends SIGTERM; log the injection counts before exiting
	// so the run's log proves the fault actually happened.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		toClient.logStats()
		toUp.logStats()
		os.Exit(0)
	}()

	// Log the running counts so they are captured even on a clean teardown (the
	// relay outlives each short test), proving the fault was injected.
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for range t.C {
			toClient.logStats()
			toUp.logStats()
		}
	}()

	var mu sync.Mutex
	var client *net.UDPAddr

	// Upstream -> client.
	go func() {
		b := make([]byte, 2048)
		for {
			n, err := up.Read(b)
			if err != nil {
				log.Printf("lossproxy: upstream read: %v", err)
				return
			}
			toClient.forward(b[:n], func(d []byte) {
				mu.Lock()
				c := client
				mu.Unlock()
				if c != nil {
					_, _ = down.WriteToUDP(d, c)
				}
			})
		}
	}()

	// Client -> upstream.
	buf := make([]byte, 2048)
	for {
		n, addr, err := down.ReadFromUDP(buf)
		if err != nil {
			log.Printf("lossproxy: client read: %v", err)
			continue
		}
		mu.Lock()
		client = addr
		mu.Unlock()
		toUp.forward(buf[:n], func(d []byte) { _, _ = up.Write(d) })
	}
}
