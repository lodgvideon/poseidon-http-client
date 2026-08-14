// Command interop is the client endpoint of the poseidon Docker image built for
// quic-interop/quic-interop-runner.
//
// The runner starts the container with ROLE=client, a TESTCASE naming the
// scenario, and the list of absolute URLs to fetch (passed as argv by
// run_endpoint.sh, from the runner's $REQUESTS). It downloads every URL into
// /downloads, where the runner byte-compares the results against the server's
// source directory, and writes an NSS key log to $SSLKEYLOGFILE so the runner can
// decrypt the simulator's packet captures.
//
// Two wire protocols, chosen by TESTCASE. Everything except "http3" is HTTP/0.9
// over QUIC with ALPN "hq-interop" (hq09.go) — the runner's servers offer no
// other ALPN on those cases, so a client that speaks HTTP/3 everywhere fails
// every one of them at the handshake. "http3" is HTTP/3 with ALPN "h3" (h3.go).
//
// Exit codes are part of the runner's protocol (interop.py::_run_test):
//
//	0	every requested file was downloaded
//	127	this implementation does not implement TESTCASE — recorded UNSUPPORTED
//	1	anything else — recorded FAILED
//
// The 127 branch must be reached before any network I/O. Before running a single
// test the runner boots the simulator and this client alone, with no server, a
// random TESTCASE and an empty request list; an implementation that dials, hangs
// or exits non-127 there is marked "not compliant" and every pairing involving it
// is skipped. So the very first thing run does is the support-table lookup, which
// touches nothing but a map.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Exit codes the runner interprets. 127 is the protocol for "unsupported": the
// runner scans the combined container output for "exited with code 127". Every
// other non-zero code is a FAILED result, so an unsupported case must never fail
// its way out — it has to reach exitUnsupported deliberately.
const (
	exitOK          = 0
	exitFailed      = 1
	exitUnsupported = 127
)

// Container paths the runner provides. /downloads is a bind mount rather than an
// environment variable, so the runner never sets DOWNLOADS; it is honoured anyway
// so the binary is runnable by hand outside the runner.
const (
	defaultDownloadsDir = "/downloads"
	logFilePath         = "/logs/client.log"
	caCertPath          = "/certs/ca.pem"
)

// job is what a test case is handed: the TLS template each connection is cloned
// from (ServerName and ALPN are filled in per connection), the directory
// downloaded bodies go into, and the URLs to fetch.
type job struct {
	tlsConfig *tls.Config
	downloads string
	urls      []string
}

// testCase is one row of the support table. A supported case carries run; an
// unsupported one carries the reason it exits 127. Exactly one of the two is set.
type testCase struct {
	run    func(context.Context, *job) error
	reason string
}

// support is the single authority for what this client serves. Every TESTCASE
// string the runner can emit appears here exactly once, so "what do we implement,
// and why not the rest" is one table to read rather than a chain of conditionals
// spread through the program. main exits 127 both for a row carrying a reason and
// for any name absent from the map — the compliance check sends a random slug,
// which lands in the second case.
//
// A matrix cell and its TESTCASE are not the same string: the runner maps one to
// the other (TestCase.testname). "transfer" is what the client receives for
// multiplexing, blackhole, amplificationlimit, transferloss, transfercorruption,
// ipv6, rebind-port, rebind-addr, connectionmigration, goodput and crosstraffic;
// "multiconnect" is what it receives for handshakeloss and handshakecorruption;
// "handshake" also covers longrtt. Those cells therefore cannot be declined here
// even where the underlying feature is missing — connectionmigration in
// particular arrives as a plain "transfer" and will simply fail.
var support = map[string]testCase{
	// Supported.
	"handshake":    {run: hqDownloadAll},
	"transfer":     {run: hqDownloadAll},
	"retry":        {run: hqDownloadAll},
	"multiconnect": {run: hqMulticonnect},
	"http3":        {run: h3DownloadAll},

	// Declined, with the reason each is out of reach today.
	"chacha20": {reason: "the ClientHello must offer TLS_CHACHA20_POLY1305_SHA256 alone; " +
		"crypto/tls ignores Config.CipherSuites for TLS 1.3, so the offered suite list is not ours to restrict"},
	"keyupdate": {reason: "the client never initiates a key update — it stops at the AEAD " +
		"confidentiality limit and closes instead (quic/conn_seal.go)"},
	"resumption": {reason: "session resumption is an explicit non-goal: no session cache is " +
		"installed, so a ticket is never stored (quic/handshake.go)"},
	"zerortt": {reason: "0-RTT is an explicit non-goal, for the same reason resumption is " +
		"(quic/handshake.go)"},
	"ecn": {reason: "outgoing packets are not ECN-marked and ACK_ECN counts are parsed but " +
		"discarded (quic/frame.go)"},
	"v2": {reason: "QUIC v1 only: the version_information transport parameter is not sent, " +
		"so compatible version negotiation never starts"},
	"versionnegotiation": {reason: "the supported-version list is not configurable, so the " +
		"deliberately-unsupported version this case requires cannot be offered"},
}

func main() {
	os.Exit(run())
}

// run is main's body, returning the process exit code so every path leaves
// through one os.Exit and deferred cleanup still runs.
func run() int {
	name := os.Getenv("TESTCASE")
	tc, ok := support[name]
	if !ok || tc.reason != "" {
		// First statement with any effect, and it reads one map. This is the
		// compliance check's requirement: decide before dialing anything.
		reason := tc.reason
		if reason == "" {
			reason = "not a test case this client recognises"
		}
		fmt.Printf("unsupported test case %q: %s\n", name, reason)
		return exitUnsupported
	}

	closeLog := startLogging()
	defer closeLog()

	keyLog, err := openKeyLog()
	if err != nil {
		log.Printf("key log: %v", err)
		return exitFailed
	}
	if keyLog != nil {
		defer func() { _ = keyLog.Close() }()
	}
	if dir := os.Getenv("QLOGDIR"); dir != "" {
		// Read and reported rather than silently ignored: the runner pre-creates
		// the directory and will find it empty, and this line in the copied-out
		// log is the explanation. This stack has no qlog output yet.
		log.Printf("QLOGDIR=%s: qlog is not implemented, the directory stays empty", dir)
	}

	roots, err := trustPool()
	if err != nil {
		log.Printf("trust anchors: %v", err)
		return exitFailed
	}

	tlsConfig := &tls.Config{
		// The server's certificate chain is verified, hostname included. The
		// runner mounts its per-run CA read-only at /certs for the client as
		// well as the server (docker-compose.yml, the client service), and the
		// leaf it signs carries every host name the request URLs use, so there
		// is a real chain to check — see trustPool. MinVersion and NextProtos
		// are set per connection by the protocol that dials, as is ServerName,
		// which is the URL's host and so what the leaf's SAN is matched against.
		RootCAs: roots,
		// Pinned to X25519 to keep the ClientHello small, which is a workaround
		// for a library limitation rather than a preference.
		//
		// Go's default key-share list also offers X25519MLKEM768, whose ~1.2 KB
		// share makes the ClientHello about 1.4 KB. This stack puts the whole
		// ClientHello into a single Initial packet (quic.Conn.sendInitialFlight,
		// which pads UP to a floor of 1200 bytes but never splits), so the
		// datagram came out at 1522 bytes, was IP-fragmented, and the ns-3
		// simulator delivered only a fragment: quic-go's server logged "packet
		// length (1434 bytes) is smaller than the expected length (1504 bytes)"
		// and every handshake timed out. RFC 9000 §14 is explicit — "UDP
		// datagrams MUST NOT be fragmented at the IP layer" — so the real fix is
		// to coalesce or split a large ClientHello across Initial packets (§14.1
		// allows either); until the library does that, the interop client keeps
		// its ClientHello under the limit.
		CurvePreferences: []tls.CurveID{tls.X25519},
	}
	if keyLog != nil {
		// Assigned conditionally, not inline: a nil *os.File stored in the
		// io.Writer field is a NON-nil interface holding a nil pointer, and
		// crypto/tls would then call Write on it and fail the handshake with
		// os.ErrInvalid whenever SSLKEYLOGFILE is unset.
		tlsConfig.KeyLogWriter = keyLog
	}

	j := &job{
		tlsConfig: tlsConfig,
		downloads: envOr("DOWNLOADS", defaultDownloadsDir),
		urls:      requestedURLs(),
	}

	log.Printf("testcase=%s downloads=%s urls=%d", name, j.downloads, len(j.urls))
	if len(j.urls) == 0 {
		log.Printf("no URLs requested")
		return exitFailed
	}
	// No overall deadline: the runner bounds each test case itself (60 s, 300 s for
	// the loss cases) and tears the compose stack down. A second, self-imposed
	// deadline could only turn a slow-but-passing run into a spurious failure.
	if err := tc.run(context.Background(), j); err != nil {
		log.Printf("testcase %s failed: %v", name, err)
		return exitFailed
	}
	log.Printf("testcase %s: %d file(s) downloaded", name, len(j.urls))
	return exitOK
}

// requestedURLs returns the URLs to fetch. run_endpoint.sh passes the runner's
// $REQUESTS as argv, which is how the reference implementations receive them; the
// environment variable is read as a fallback so the binary also works when run
// directly.
func requestedURLs() []string {
	if args := os.Args[1:]; len(args) > 0 {
		return args
	}
	return strings.Fields(os.Getenv("REQUESTS"))
}

// envOr returns the environment variable or, when it is unset or empty, def.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// startLogging tees the log to /logs, which the runner copies out of the
// container after the run, while leaving it on stderr where docker compose picks
// it up. A container without a writable /logs (running the image by hand) keeps
// stderr alone rather than failing.
func startLogging() func() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	f, err := os.Create(logFilePath)
	if err != nil {
		log.Printf("no log file at %s (%v), logging to stderr only", logFilePath, err)
		return func() {}
	}
	log.SetOutput(io.MultiWriter(os.Stderr, f))
	return func() { _ = f.Close() }
}

// openKeyLog opens $SSLKEYLOGFILE for the TLS key log. The runner decrypts the
// simulator's packet captures with it, and several of its checks — multiplexing,
// amplificationlimit, rebind-port, rebind-addr and connectionmigration among them
// — report UNSUPPORTED when the file is missing or holds no
// SERVER_HANDSHAKE_TRAFFIC_SECRET line. A nil writer (variable unset) is a valid
// tls.Config.KeyLogWriter and disables the log.
func openKeyLog() (*os.File, error) {
	path := os.Getenv("SSLKEYLOGFILE")
	if path == "" {
		return nil, nil
	}
	//nolint:gosec // G304: the path is $SSLKEYLOGFILE, which the runner sets to a
	// literal /logs/keys.log in its own compose file; there is no untrusted input.
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
}

// trustPool returns the roots the server's certificate chain is verified against.
// A nil pool is not a failure and not a bypass: it is tls.Config.RootCAs' own
// spelling of "the host's system roots", which is what a by-hand run outside the
// runner wants. Verification is on down every path through here.
//
// Inside the runner the file is always there. certs.sh regenerates a chain per
// run and renames its root to ca.pem, docker-compose.yml mounts that directory
// read-only at /certs for the CLIENT service and not only for the server, and
// interop.py exports CERTS for every test case as well as for the compliance
// check. The leaf is signed with subjectAltName DNS:server, server4, server6 and
// server46 — precisely the hosts the runner puts in the request URLs (its
// urlprefix is https://server4:443/, overridden to server6 for the IPv6-only
// transfer and server46 for the dual-stack one), so hostname verification has a
// name to match rather than something to be waived. The amplification case signs
// a 9-certificate chain; the server presents the intermediates and ca.pem is
// still the anchor, so depth changes nothing here.
//
// quic-go's interop client sets InsecureSkipVerify instead. That establishes the
// harness tolerates skipping, not that it requires it — and an interop result is
// only worth having if the handshake it measures is the one a real caller gets.
func trustPool() (*x509.CertPool, error) {
	pem, err := os.ReadFile(caCertPath)
	if errors.Is(err, os.ErrNotExist) {
		log.Printf("no CA at %s; verifying against the system roots", caCertPath)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read CA %s: %w", caCertPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA %s: no PEM certificate in %d bytes", caCertPath, len(pem))
	}
	log.Printf("verifying the server chain against %s", caCertPath)
	return pool, nil
}

// downloadPath is where the body of u is written. The runner compares /downloads
// against the server's source directory and fails the test on any file it did not
// expect, so nothing else may be created there — no temporary or partial files
// under a different name.
func downloadPath(dir string, u *url.URL) string {
	return filepath.Join(dir, filepath.FromSlash(u.Path))
}

// parseURLs parses every request URL up front, so a malformed one fails before a
// connection is opened rather than halfway through a transfer.
func parseURLs(raw []string) ([]*url.URL, error) {
	out := make([]*url.URL, 0, len(raw))
	for _, r := range raw {
		u, err := url.Parse(r)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", r, err)
		}
		if u.Host == "" {
			return nil, fmt.Errorf("parse %q: no host", r)
		}
		out = append(out, u)
	}
	return out, nil
}

// hostPort returns u's authority with the QUIC default port filled in. The runner
// always spells the port out, so this only matters when the binary is driven by
// hand.
func hostPort(u *url.URL) string {
	if u.Port() != "" {
		return u.Host
	}
	return u.Host + ":443"
}

// inParallel runs fn over every URL concurrently and returns the first error.
// Concurrency is the point rather than a speed-up: the runner's "transfer" case
// requires several streams in flight on one connection, and its multiplexing
// variant requires enough of them to hit the peer's stream limit and resume when
// MAX_STREAMS raises it.
func inParallel(urls []*url.URL, fn func(*url.URL) error) error {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	for _, u := range urls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(u); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return first
}
