//go:build ignore

// Command lossproxy is a test-only UDP relay that forwards QUIC datagrams
// between a client and an upstream server, randomly dropping a percentage of
// datagrams in each direction to exercise loss recovery (RFC 9002). It handles
// the interop suite's sequential single-connection tests: the current client
// address is tracked and upstream responses are relayed back to it.
//
// The //go:build ignore tag keeps it out of package builds; it is run with
// `go run test/integration/http3/lossproxy.go`.
package main

import (
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
)

func main() {
	lossPct := 10
	if v := os.Getenv("LOSS_PCT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			lossPct = n
		}
	}
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
	log.Printf("lossproxy: :443 <-> %s, dropping ~%d%% each way", upstream, lossPct)

	drop := func() bool { return rand.Intn(100) < lossPct }

	var mu sync.Mutex
	var client *net.UDPAddr

	// Upstream -> client.
	go func() {
		b := make([]byte, 2048)
		for {
			n, err := up.Read(b)
			if err != nil {
				return
			}
			mu.Lock()
			c := client
			mu.Unlock()
			if c == nil || drop() {
				continue
			}
			_, _ = down.WriteToUDP(b[:n], c)
		}
	}()

	// Client -> upstream.
	buf := make([]byte, 2048)
	for {
		n, addr, err := down.ReadFromUDP(buf)
		if err != nil {
			log.Printf("lossproxy: read: %v", err)
			continue
		}
		mu.Lock()
		client = addr
		mu.Unlock()
		if drop() {
			continue
		}
		_, _ = up.Write(buf[:n])
	}
}
