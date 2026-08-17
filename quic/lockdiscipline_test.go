package quic

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// publicWrappers are the lock-taking public entry points on Conn/Stream. Each
// takes c.mu and delegates to an assume-held …Locked internal.
var publicWrappers = map[string]bool{
	"Poll":            true,
	"Send":            true,
	"Recv":            true,
	"OpenStream":      true,
	"OpenUniStream":   true,
	"AcceptUniStream": true,
	"CloseWithError":  true,
	"Reset":           true,
	"StopSending":     true,
}

// TestLockDiscipline_LockedNeverCallsPublicWrapper statically guards the c.mu
// discipline introduced in PR 2a (docs/HTTP3_DESIGN.md §5): a …Locked internal
// already runs with c.mu held, so calling any re-locking public wrapper would
// self-deadlock the single sync.Mutex. It parses every non-test file in the
// package and fails if a function whose name ends in "Locked" contains a method
// call to one of the public wrappers. This is the regression guard for PRs
// 2b–2d as much as for 2a; the three known reentrant up-calls (sealPacket and
// fail → CloseWithError, OnStopSending → Reset) are repointed to …Locked
// internals precisely so this test passes.
func TestLockDiscipline_LockedNeverCallsPublicWrapper(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err, "glob the package's source files")
	fset := token.NewFileSet()
	lockedFns := 0

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, file, nil, 0)
		require.NoErrorf(t, err, "parse %s", file)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasSuffix(fn.Name.Name, "Locked") {
				continue
			}
			lockedFns++
			enclosing := fn.Name.Name
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				assert.Falsef(t, publicWrappers[sel.Sel.Name],
					"%s: %s (a …Locked internal) calls public wrapper %s(...), which re-takes c.mu and self-deadlocks; call the %sLocked internal instead",
					fset.Position(call.Pos()), enclosing, sel.Sel.Name, sel.Sel.Name)
				return true
			})
		}
	}

	assert.NotZero(t, lockedFns,
		"found no …Locked functions to scan — the c.mu split is missing or the scan is broken")
}
