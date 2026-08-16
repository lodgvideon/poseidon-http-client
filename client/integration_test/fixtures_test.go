//go:build integration

package integration_test

import (
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// echoHeaderSummary renders the request headers as "name: value" joined by "; ", the
// shape Undertow already produces (test/integration/undertow-server/.../Main.java), so
// one assertion reads the same thing from every peer. Sorted, because Go's header map
// iterates in random order and a test comparing whole values would flake.
func echoHeaderSummary(h http.Header) string {
	parts := make([]string, 0, len(h))
	for name, vals := range h {
		for _, v := range vals {
			parts = append(parts, name+": "+v)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

// registerFixtures installs all test endpoint handlers on the given mux.
// All four servers (Go, nginx, Undertow, nghttpx) implement the same contract.
func registerFixtures(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "ok")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "hello from go-http")
	})

	// X-Echo-Headers is half of what CONTRACT.md has always specified for /echo. It
	// was implemented only by Undertow, so no cross-peer test could read a request
	// header back and the header channel was unverifiable here (#651).
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Echo-Headers", echoHeaderSummary(r.Header))
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	})

	mux.HandleFunc("/large", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("bytes"))
		if n <= 0 {
			n = 1048576 // 1 MiB
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(n))
		chunk := make([]byte, 4096)
		sent := 0
		for sent < n {
			sz := len(chunk)
			if n-sent < sz {
				sz = n - sent
			}
			_, _ = w.Write(chunk[:sz])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			sent += sz
		}
	})

	mux.HandleFunc("/status/", func(w http.ResponseWriter, r *http.Request) {
		code, _ := strconv.Atoi(r.URL.Path[len("/status/"):])
		if code == 0 {
			code = 200
		}
		w.WriteHeader(code)
		_, _ = io.WriteString(w, "status "+strconv.Itoa(code))
	})

	mux.HandleFunc("/delay", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		if ms <= 0 {
			ms = 1000
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "delayed "+strconv.Itoa(ms)+"ms")
	})

	mux.HandleFunc("/chunked", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		chunk := make([]byte, 1024)
		for i := 0; i < 100; i++ {
			_, _ = w.Write(chunk)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	})

	mux.HandleFunc("/gzip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Encoding", "gzip")
		// Pre-compressed 100KB body would go here.
		// For simplicity in Go reference, we send raw (server doesn't auto-gzip).
		_, _ = io.WriteString(w, "gzip response")
	})

	mux.HandleFunc("/trailers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Trailer", "X-Trailer-Foo")
		_, _ = io.WriteString(w, "trailers")
		w.Header().Set("X-Trailer-Foo", "bar")
	})

	mux.HandleFunc("/never", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Second)
	})
}
