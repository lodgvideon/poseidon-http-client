package quic

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// connCloseCodes is a registration point, and a registration point nobody is
// forced to visit is a bug generator: a new connection-error sentinel that is
// never added there closes the connection carrying no transport error code, and
// the peer is told nothing about why. Nothing fails — the code is simply absent
// from the wire (#517).
//
// So the source is the input to the test. Every `Err…` sentinel declared in
// errors.go must be accounted for exactly once: either closeCodeFor maps it to a
// code, or it appears below with the reason it deliberately has none. Adding a
// sentinel without deciding fails this test with the name that needs a decision.

// sentinelsByName is the bridge from a name in the AST to the value, which Go
// cannot do at runtime for package-level variables. It must list every sentinel;
// the test fails on any name in errors.go that is missing here.
var sentinelsByName = map[string]error{
	"ErrFrameEncoding":          ErrFrameEncoding,
	"ErrPacketEncoding":         ErrPacketEncoding,
	"ErrCryptoKey":              ErrCryptoKey,
	"ErrCryptoSample":           ErrCryptoSample,
	"ErrCryptoDecrypt":          ErrCryptoDecrypt,
	"ErrCryptoSuite":            ErrCryptoSuite,
	"ErrTransportParameter":     ErrTransportParameter,
	"ErrIdleTimeout":            ErrIdleTimeout,
	"ErrNoProgress":             ErrNoProgress,
	"ErrConnClosed":             ErrConnClosed,
	"ErrStatelessReset":         ErrStatelessReset,
	"ErrVersionNegotiation":     ErrVersionNegotiation,
	"ErrStreamFinished":         ErrStreamFinished,
	"ErrStreamReset":            ErrStreamReset,
	"ErrStreamState":            ErrStreamState,
	"ErrTooManyStreams":         ErrTooManyStreams,
	"ErrFlowControl":            ErrFlowControl,
	"ErrFinalSize":              ErrFinalSize,
	"ErrTooManyUniStreams":      ErrTooManyUniStreams,
	"ErrTooManyBidiStreams":     ErrTooManyBidiStreams,
	"ErrServerBidiStream":       ErrServerBidiStream,
	"ErrNotEstablished":         ErrNotEstablished,
	"ErrConnectionIDLimit":      ErrConnectionIDLimit,
	"ErrCryptoBufferExceeded":   ErrCryptoBufferExceeded,
	"ErrProtocolViolation":      ErrProtocolViolation,
	"ErrAEADLimit":              ErrAEADLimit,
	"ErrHandshakeClosed":        ErrHandshakeClosed,
	"errNoClientHello":          errNoClientHello,
	"ErrNotInitial":             ErrNotInitial,
	"errServerFlightIncomplete": errServerFlightIncomplete,
}

// noCloseCode lists the sentinels that carry no transport error code, with why.
// A CONNECTION_CLOSE is only correct when the PEER violated the protocol and is
// still there to be told; these are local state, API misuse, or a connection
// that is already gone.
var noCloseCode = map[string]string{
	"ErrPacketEncoding":         "a malformed packet is discarded (§5.2), not a connection error — an off-path forger could otherwise close the connection",
	"ErrCryptoKey":              "local invariant: our own keys are the wrong size",
	"ErrCryptoSample":           "the packet is discarded (RFC 9001 §5.4.2); it may be someone else's traffic",
	"ErrCryptoDecrypt":          "authentication failure means the packet is discarded (RFC 9001 §5.3) — acting on it is exactly the forgery amplification §5.2 warns about",
	"ErrCryptoSuite":            "local: this build does not implement the negotiated suite",
	"ErrIdleTimeout":            "RFC 9000 §10.1 discards the state silently; no CONNECTION_CLOSE is sent",
	"ErrNoProgress":             "the peer has stopped answering — that is the whole reason we gave up — so a close would go nowhere, exactly as for ErrIdleTimeout. It is also our own decision to stop waiting, not a peer violation",
	"ErrConnClosed":             "the connection is already closed; there is nothing to send on",
	"ErrStatelessReset":         "RFC 9000 §10.3: the peer has already lost the state, so a close would go nowhere",
	"ErrVersionNegotiation":     "RFC 9000 §6.2: the attempt is abandoned, and 1-RTT keys to close with never existed",
	"ErrStreamFinished":         "local API misuse by our own caller, not peer behaviour",
	"ErrStreamReset":            "local send-side state after RESET_STREAM",
	"ErrTooManyStreams":         "our OpenStream hit the peer's advertised limit — we are the one being limited",
	"ErrNotEstablished":         "local API misuse: application data before the handshake completed",
	"ErrHandshakeClosed":        "the peer closed first; answering a close with a close is not a thing",
	"errNoClientHello":          "internal invariant failure in our own TLS stack",
	"ErrNotInitial":             "AcceptInitial rejected a datagram before any connection exists to close",
	"errServerFlightIncomplete": "local API misuse by the caller of NewServerConn; no connection exists yet",
}

// TestCloseCodeFor_EverySentinelIsClassified reads errors.go and requires each
// sentinel in it to be either mapped to a transport error code or explicitly
// recorded as having none.
func TestCloseCodeFor_EverySentinelIsClassified(t *testing.T) {
	names := sentinelNamesInSource(t, "errors.go")
	if len(names) == 0 {
		t.Fatal("no sentinels found in errors.go — the parser found nothing, so this " +
			"test would pass no matter what the source says")
	}
	t.Logf("%d sentinels declared in errors.go", len(names))

	for _, name := range names {
		sentinel, known := sentinelsByName[name]
		if !known {
			t.Errorf("%s is declared in errors.go but missing from sentinelsByName — add it, "+
				"then decide whether it needs a transport error code in connCloseCodes", name)
			continue
		}
		_, hasCode := closeCodeFor(sentinel)
		_, excused := noCloseCode[name]
		switch {
		case hasCode && excused:
			t.Errorf("%s is both in connCloseCodes and listed as having no code — one of "+
				"the two is wrong", name)
		case !hasCode && !excused:
			t.Errorf("%s has no transport error code and no recorded reason for not having "+
				"one. If the peer violated the protocol, add it to connCloseCodes; if it is "+
				"local state or API misuse, add it to noCloseCode with why. Leaving it "+
				"unclassified means a CONNECTION_CLOSE that carries no code.", name)
		}
	}

	// The bridge must not rot in the other direction either: a sentinel deleted
	// from errors.go should not linger here pretending to be covered.
	inSource := make(map[string]bool, len(names))
	for _, n := range names {
		inSource[n] = true
	}
	for name := range sentinelsByName {
		if !inSource[name] {
			t.Errorf("sentinelsByName lists %s, which errors.go no longer declares", name)
		}
	}
}

// TestCloseCodeFor_TableIsConsistent pins the properties of the table itself,
// which the source-driven test above cannot see: no sentinel registered twice,
// and no entry mapping to the zero code (NO_ERROR is not a violation).
func TestCloseCodeFor_TableIsConsistent(t *testing.T) {
	seen := make(map[error]bool, len(connCloseCodes))
	for _, e := range connCloseCodes {
		if seen[e.err] {
			t.Errorf("%v appears twice in connCloseCodes; the first entry wins and the "+
				"second is dead", e.err)
		}
		seen[e.err] = true
		if e.code == ErrCodeNoError {
			t.Errorf("%v maps to NO_ERROR, which says the peer did nothing wrong", e.err)
		}
	}
}

// sentinelNamesInSource parses a file in this package and returns the names of
// its package-level `var X = errors.New(...)` declarations. Type declarations
// like PeerClosedError are not sentinels and are skipped; so are the ErrCode
// constants, which are consts rather than vars.
func sentinelNamesInSource(t *testing.T, file string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	var names []string
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, id := range vs.Names {
				// Unexported sentinels count too. Whether a sentinel maps to a
				// CONNECTION_CLOSE code is a question about what the connection does
				// when it is raised, not about who can name it — #478 unexported two
				// of these and a capital-only prefix would have silently stopped
				// classifying them.
				if !strings.HasPrefix(id.Name, "Err") && !strings.HasPrefix(id.Name, "err") {
					continue
				}
				if i >= len(vs.Values) {
					continue
				}
				call, ok := vs.Values[i].(*ast.CallExpr)
				if !ok {
					continue
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "New" {
					names = append(names, id.Name)
				}
			}
		}
	}
	return names
}
