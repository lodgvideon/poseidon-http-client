package client

import (
	"crypto/tls"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// TestNewClient_ValidationErrorsCarryTheirSentinel covers every error return in
// NewClient's option validation and asserts each is classifiable with errors.Is.
//
// The sentinel differs per family and that is deliberate вЂ” Pool cross-field
// errors carry ErrInvalidPoolOptions, an unusable dialer carries
// ErrALPNProtocolMismatch, an undefined kind carries ErrInvalidTransportKind вЂ”
// so this table pins WHICH one each path returns rather than only that some
// error came back. The pre-existing tests for the first four rows asserted
// `err != nil` and nothing more, which is exactly how three of them came to
// wrap no sentinel at all (#713).
func TestNewClient_ValidationErrorsCarryTheirSentinel(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts ClientOptions
		want error
	}{
		{
			name: "AddrEmpty",
			opts: ClientOptions{ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}}},
			want: ErrInvalidOptions,
		},
		{
			name: "AddrWhitespace",
			opts: ClientOptions{Addr: "host: 80", ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}}},
			want: ErrInvalidOptions,
		},
		{
			name: "H3MissingTLSConfig",
			opts: ClientOptions{Addr: "h3.example:443", Transport: TransportH3},
			want: ErrInvalidOptions,
		},
		{
			name: "MissingDialer",
			opts: ClientOptions{Addr: "example:443"},
			want: ErrInvalidOptions,
		},
		{
			name: "DialerALPNMismatch",
			opts: ClientOptions{
				Addr:      "example:443",
				Transport: TransportH1SingleConn,
				ConnOpts:  conn.ConnOptions{Dialer: &conn.TLSDialer{}}, // asserts "h2"
			},
			want: ErrALPNProtocolMismatch,
		},
		{
			name: "PoolSetOnSingleConn",
			opts: ClientOptions{
				Addr:     "example:443",
				Pool:     &PoolOptions{},
				ConnOpts: conn.ConnOptions{Dialer: &fakeDialer{}},
			},
			want: ErrInvalidPoolOptions,
		},
		{
			name: "PoolMissingForTransportPool",
			opts: ClientOptions{
				Addr:      "example:443",
				Transport: TransportPool,
				ConnOpts:  conn.ConnOptions{Dialer: &fakeDialer{}},
			},
			want: ErrInvalidPoolOptions,
		},
		{
			name: "PoolMissingForH3Pool",
			opts: ClientOptions{
				Addr:      "h3.example:443",
				Transport: TransportH3Pool,
				TLSConfig: &tls.Config{ServerName: "h3.example"},
			},
			want: ErrInvalidPoolOptions,
		},
		{
			name: "PoolMissingForH1Pool",
			opts: ClientOptions{
				Addr:      "example:80",
				Transport: TransportH1Pool,
				ConnOpts:  conn.ConnOptions{Dialer: &conn.PlaintextDialer{}},
			},
			want: ErrInvalidPoolOptions,
		},
		{
			name: "ManagedMissingResolver",
			opts: ClientOptions{
				Transport: TransportManaged,
				ConnOpts:  conn.ConnOptions{Dialer: &fakeDialer{}},
			},
			want: ErrInvalidOptions,
		},
		{
			name: "ManagedWithAddr",
			opts: ClientOptions{
				Addr:      "example:443",
				Transport: TransportManaged,
				Resolver:  StaticResolver(Address{Host: "example", Port: 443}),
				ConnOpts:  conn.ConnOptions{Dialer: &fakeDialer{}},
			},
			want: ErrInvalidOptions,
		},
		{
			name: "UnknownTransportKind",
			opts: ClientOptions{
				Addr:      "example:443",
				Transport: TransportKind(99),
				ConnOpts:  conn.ConnOptions{Dialer: &fakeDialer{}},
			},
			want: ErrInvalidTransportKind,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClient(tc.opts)

			require.Errorf(t, err, "NewClient = %v, nil; want an error", c)
			require.ErrorIsf(t, err, tc.want,
				"NewClient error = %v\n  errors.Is(err, %v) = false; a caller classifying this error cannot tell it from a transport failure",
				err, tc.want)
		})
	}
}

// validationFuncs are the functions whose every error return is an option
// rejection reported to the caller of NewClient.
var validationFuncs = map[string]bool{
	"NewClient":                true,
	"validateTransportOptions": true,
	"validateDialerALPN":       true,
}

// TestNewClient_ValidationErrorsAlwaysWrap is the guard the three errors of
// #713 slipped past. The table above pins the paths that exist today; this pins
// the property, so a validation branch added tomorrow with a bare fmt.Errorf
// fails here instead of silently joining them. Same idea as quic's
// errors_table_test.go: make "remembered to wrap it" a test failure rather than
// something a reader has to notice.
func TestNewClient_ValidationErrorsAlwaysWrap(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "client.go", nil, 0)
	require.NoError(t, err, "parse client.go")

	checked := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !validationFuncs[fn.Name.Name] {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Errorf" {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "fmt" {
				return true
			}
			format, ok := call.Args[0].(*ast.BasicLit)
			if !ok || format.Kind != token.STRING {
				return true
			}
			checked++
			assert.Containsf(t, format.Value, "%w",
				"%s: fmt.Errorf in %s wraps no sentinel: %s\n  errors.Is cannot classify it; wrap ErrInvalidOptions (or the sentinel for its family)",
				fset.Position(call.Pos()), fn.Name.Name, format.Value)
			return true
		})
	}

	// Without this the test passes vacuously the day someone renames a function
	// or moves validation to another file.
	require.GreaterOrEqualf(t, checked, 8,
		"inspected only %d fmt.Errorf calls across %v; the validation code moved and this guard is no longer looking at it",
		checked, validationFuncs)
	t.Logf("inspected %d fmt.Errorf calls in NewClient's validation path", checked)
}
