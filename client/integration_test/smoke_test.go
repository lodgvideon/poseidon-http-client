//go:build integration

package integration_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSmoke_GoHTTP_ReferenceServer verifies the in-process reference server
// boots and responds on /healthz. This is the minimal smoke test that must
// pass before any Docker-dependent tests run.
func TestSmoke_GoHTTP_ReferenceServer(t *testing.T) {
	srv := requireServer(t, ServerGoHTTP)

	addr := srv.H2CAddr

	require.NotEmpty(t, addr, "Go reference server has no address")
	t.Logf("Go reference server: %s (h2c=%s, tls=%s)",
		srv.Kind, srv.H2CAddr, srv.TLSAddr)
}

// TestSmoke_AllServers_Ready checks which Docker servers passed healthcheck.
//
// It used to only log them. That made it the one test here that could not fail
// at all: every peer down, every address empty, and it still passed — while the
// rest of the suite reacts to a missing peer by SKIPPING it, so an absent
// implementation quietly shrinks the matrix instead of failing it. This is where
// that has to be caught, for the same reason TestMatrix_TLS_Healthz refuses to
// pass on an empty peer set: a cross-implementation suite that silently tests one
// implementation is worse than one that fails.
//
// Not-ready is asserted rather than skipped because the CI job brings the stack
// up with `docker compose up -d --wait`, which already gates on the same
// healthchecks — so a peer missing HERE means it died between compose and now,
// which is a fact worth a red run. Under POSEIDON_IT_SKIP_REMOTE the remote peers
// are never registered, so the loop sees only the in-process reference and the
// assertion is vacuously satisfied rather than special-cased.
func TestSmoke_AllServers_Ready(t *testing.T) {
	discovered := allServers

	var notReady, addressless []string
	for kind, srv := range discovered {
		t.Logf("  %s: ready=%t (h2c=%s, tls=%s)", kind, srv.Ready, srv.H2CAddr, srv.TLSAddr)
		if !srv.Ready {
			notReady = append(notReady, kind.String())
			continue
		}
		if srv.H2CAddr == "" && srv.TLSAddr == "" {
			addressless = append(addressless, kind.String())
		}
	}
	sort.Strings(notReady)
	sort.Strings(addressless)

	require.Contains(t, discovered, ServerGoHTTP,
		"the in-process Go reference is started unconditionally by TestMain, so its "+
			"absence means discovery itself is broken and every requireServer below is skipping")
	assert.Emptyf(t, notReady, "peers failed their healthcheck: %v — the rest of this suite "+
		"SKIPS a peer that is not ready, so this is the only place a shrunken matrix is "+
		"visible instead of silently passing on fewer implementations", notReady)
	assert.Emptyf(t, addressless, "peers reported ready with no address at all: %v — "+
		"waitReady dialled something, so a ready peer with neither an h2c nor a TLS "+
		"address means the discovery table and the healthcheck disagree", addressless)
}
