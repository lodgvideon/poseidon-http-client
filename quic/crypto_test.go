package quic

import (
	"bytes"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoErrorf(t, err, "bad hex %q: %v", s, err)
	return b
}

// TestConformance_RFC9001_AppA1_InitialKeys derives the client and server
// Initial packet keys from the Appendix A.1 example Destination Connection ID
// and checks each against the RFC's published test vectors. A match validates
// the whole chain: HKDF-Extract, Expand-Label of the traffic secrets, and
// Expand-Label of key/iv/hp.
func TestConformance_RFC9001_AppA1_InitialKeys(t *testing.T) {
	dcid := unhex(t, "8394c8f03e515708")

	client, server := InitialKeys(dcid)

	checks := []struct {
		name string
		got  []byte
		want string
	}{
		{"client key", client.Key, "1f369613dd76d5467730efcbe3b1a22d"},
		{"client iv", client.IV, "fa044b2f42a3fd3b46fb255c"},
		{"client hp", client.HP, "9f50449e04a0e810283a1e9933adedd2"},
		{"server key", server.Key, "cf3a5331653c364c88f0f379b6067e37"},
		{"server iv", server.IV, "0ac1493ca1905853b0bba03e"},
		{"server hp", server.HP, "c206b8d9b9f0f37644430b490eeaa314"},
	}
	for _, c := range checks {
		assert.Truef(t, bytes.Equal(c.got, unhex(t, c.want)), "%s = %x, want %s", c.name, c.got, c.want)
	}
}

// TestConformance_RFC9001_Sec51_ExpandLabel pins the intermediate
// client_initial_secret from Appendix A.1, exercising the HKDF-Expand-Label
// framing directly.
func TestConformance_RFC9001_Sec51_ExpandLabel(t *testing.T) {
	dcid := unhex(t, "8394c8f03e515708")
	prk, err := hkdf.Extract(sha256.New, dcid, quicV1InitialSalt)
	require.NoError(t, err)

	got := hkdfExpandLabel(sha256.New, prk, "client in", sha256.Size)

	want := "c00cf151ca5be075ed0ebfb5c80323c42d6b7db67881289af4008f1f6c357aea"
	assert.Truef(t, bytes.Equal(got, unhex(t, want)), "client_initial_secret = %x, want %s", got, want)
}
