package http1_test

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/http1"
)

// TestHasResidue_TLSBufferedRecordWithDrainedSocket pins the state the layered
// branch of HasResidue exists for, which no other test produced: the kernel
// receive queue empty, this Conn's bufio empty, and a decrypted record still
// sitting in crypto/tls's own input buffer.
//
// A server that simply writes leaves its bytes on the socket, so FIONREAD
// reports them and HasResidue answers from the branch above —
// TestHasResidue_TLSUnsolicitedResponse never reaches this one, despite its
// comment. The state is built here by reading a single byte straight from the
// *tls.Conn before wrapping it: crypto/tls pulls the whole record off the
// socket, decrypts it, returns one byte and keeps the rest.
//
// What this does NOT pin is the past deadline that branch uses, and the
// distinction is worth stating because the comment there invites the wrong
// reading. crypto/tls returns buffered plaintext without consulting a deadline
// at all, so a future deadline finds this record too — verified by mutation.
// The past deadline is what makes the NEGATIVE answer instant instead of costing
// a full probe window on every checkout of a layered connection. Latency, not
// correctness.
func TestHasResidue_TLSBufferedRecordWithDrainedSocket(t *testing.T) {
	cert := selfSignedCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	require.NoError(t, err, "tls.Listen")
	defer ln.Close()

	const payload = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nPWNED"

	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer nc.Close()
		_, _ = nc.Write([]byte(payload))
		buf := make([]byte, 1)
		_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = nc.Read(buf)
	}()

	tc, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	require.NoError(t, err, "tls.Dial")
	defer tc.Close()
	require.NoError(t, tc.Handshake(), "Handshake")
	// Let the session tickets a TLS 1.3 server emits after the handshake arrive
	// along with the payload, so the single read below consumes them too and
	// leaves nothing on the socket to be found by the cheaper check.
	time.Sleep(100 * time.Millisecond)
	one := make([]byte, 1)
	_ = tc.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := tc.Read(one)
	require.Truef(t, err == nil && n == 1,
		"priming read from the TLS conn: n=%d err=%v — without it the residue "+
			"is on the socket rather than inside crypto/tls, and this test would be "+
			"exercising the branch above", n, err)
	_ = tc.SetReadDeadline(time.Time{})

	c := http1.NewConn(tc)

	assert.Truef(t, c.HasResidue(),
		"HasResidue() = false with %d bytes of an unsolicited response decrypted "+
			"and held inside crypto/tls while the socket is drained.\n"+
			"The checkout-time guard has failed open: the connection goes back to the "+
			"pool and the next request parses the peer's leftovers as its own response "+
			"(RFC 9112 §6.3).", len(payload)-1)
}

// TestHasResidue_TLSDrainedRecordIsClean is the control. Same setup, but the
// whole record is consumed before the Conn is built, so crypto/tls holds nothing
// and the socket is empty. A HasResidue that answered "true" unconditionally —
// or a probe that mistook its own timeout error for data — would pass the test
// above and fail this one.
func TestHasResidue_TLSDrainedRecordIsClean(t *testing.T) {
	cert := selfSignedCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	require.NoError(t, err, "tls.Listen")
	defer ln.Close()

	const payload = "HTTP/1.1 204 No Content\r\n\r\n"

	go func() {
		nc, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer nc.Close()
		_, _ = nc.Write([]byte(payload))
		buf := make([]byte, 1)
		_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = nc.Read(buf)
	}()

	tc, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	require.NoError(t, err, "tls.Dial")
	defer tc.Close()
	require.NoError(t, tc.Handshake(), "Handshake")
	time.Sleep(100 * time.Millisecond)
	// Consume the whole payload, so nothing is left in either buffer.
	got := make([]byte, 0, len(payload))
	tmp := make([]byte, 64)
	_ = tc.SetReadDeadline(time.Now().Add(3 * time.Second))
	for len(got) < len(payload) {
		n, rerr := tc.Read(tmp)
		got = append(got, tmp[:n]...)
		require.NoError(t, rerr, "draining the payload")
	}
	_ = tc.SetReadDeadline(time.Time{})

	c := http1.NewConn(tc)

	assert.False(t, c.HasResidue(),
		"HasResidue() = true on a TLS connection whose payload was fully consumed: "+
			"a false positive here evicts a healthy connection on every checkout")
}
