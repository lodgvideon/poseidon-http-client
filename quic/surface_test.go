package quic

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExportedSurfaceDoesNotLeakUnexportedTypes is the guard the #478 surface
// sweep needs and that the toolchain does not provide.
//
// Unexporting a type that an exported function still mentions produces an API
// nobody outside the package can name: they can hold the value through
// inference, but cannot declare a variable of it, write a wrapper signature, or
// implement an interface over it. It is the classic "exported func returns
// unexported type" defect.
//
// Neither existing gate catches it. Measured on this package with a probe file
// containing exactly that shape:
//
//	go build ./quic/        -> compiles, exit 0
//	golangci-lint run ./quic/ -> 0 issues
//
// The compiler permits it by design, and revive's exported rule as configured
// here (disableStutteringCheck, disableChecksOnConstants) does not report it. So
// the sweep's stated verification — "any miss is a compile error, not a silent
// regression" — does not hold, and this test is what makes it true.
func TestExportedSurfaceDoesNotLeakUnexportedTypes(t *testing.T) {
	fset := token.NewFileSet()
	files := parsePackageFiles(t, fset)

	// Package-level type names, so an unexported *parameter name* is never
	// mistaken for an unexported type.
	unexportedTypes := map[string]bool{}
	for _, f := range files {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				ts := spec.(*ast.TypeSpec)
				if !ts.Name.IsExported() {
					unexportedTypes[ts.Name.Name] = true
				}
			}
		}
	}

	type leak struct{ where, typ string }
	var leaks []leak

	record := func(where string, expr ast.Expr) {
		if expr == nil {
			return
		}
		ast.Inspect(expr, func(n ast.Node) bool {
			// Only bare identifiers name types in this package; a selector is
			// another package's type and cannot be this package's unexported one.
			if id, ok := n.(*ast.Ident); ok && unexportedTypes[id.Name] {
				leaks = append(leaks, leak{where, id.Name})
			}
			if _, ok := n.(*ast.SelectorExpr); ok {
				return false
			}
			return true
		})
	}

	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || !fd.Name.IsExported() {
				continue
			}
			where := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				// A method is only reachable if its receiver type is exported.
				recv := fd.Recv.List[0].Type
				if star, isStar := recv.(*ast.StarExpr); isStar {
					recv = star.X
				}
				if id, isID := recv.(*ast.Ident); !isID || !id.IsExported() {
					continue
				}
				where = exprName(fd.Recv.List[0].Type) + "." + fd.Name.Name
			}
			for _, p := range fd.Type.Params.List {
				record(where+" (parameter)", p.Type)
			}
			if fd.Type.Results != nil {
				for _, r := range fd.Type.Results.List {
					record(where+" (result)", r.Type)
				}
			}
		}
	}

	sort.Slice(leaks, func(i, j int) bool { return leaks[i].where < leaks[j].where })
	var b strings.Builder
	for _, l := range leaks {
		b.WriteString("\n  " + l.where + " mentions unexported type " + l.typ)
	}

	assert.Emptyf(t, leaks,
		"exported API names %d unexported type(s); a caller outside quic cannot "+
			"declare these, only hold them by inference:%s\n\nEither export the type or "+
			"unexport the function that mentions it.", len(leaks), b.String())
}

// parsePackageFiles parses the package's non-test sources.
func parsePackageFiles(t *testing.T, fset *token.FileSet) []*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err, "ReadDir of the package directory")
	out := make([]*ast.File, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.SkipObjectResolution)
		require.NoErrorf(t, err, "parse %s", name)
		out = append(out, f)
	}
	require.NotEmpty(t, out, "parsed no package files, so this test asserted nothing")
	return out
}

func exprName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.StarExpr:
		return "*" + exprName(v.X)
	case *ast.Ident:
		return v.Name
	default:
		return "?"
	}
}
