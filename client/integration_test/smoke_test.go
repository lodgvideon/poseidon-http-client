//go:build integration

package integration_test

import (
	"testing"
)

// TestSmoke_GoHTTP_ReferenceServer verifies the in-process reference server
// boots and responds on /healthz. This is the minimal smoke test that must
// pass before any Docker-dependent tests run.
func TestSmoke_GoHTTP_ReferenceServer(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)
	if srv.H2CAddr == "" {
		t.Fatal("Go reference server has no address")
	}
	t.Logf("Go reference server: %s (h2c=%s, tls=%s)",
		srv.Kind, srv.H2CAddr, srv.TLSAddr)
}

// TestSmoke_AllServers_Ready checks which Docker servers passed healthcheck.
func TestSmoke_AllServers_Ready(t *testing.T) {
	for kind, srv := range allServers {
		status := "READY"
		if !srv.Ready {
			status = "SKIP (not ready)"
		}
		t.Logf("  %s: %s (h2c=%s, tls=%s)", kind, status, srv.H2CAddr, srv.TLSAddr)
	}
}
