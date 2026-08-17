package http3

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSendRequest_BodyRidesHeadersWithoutCopy pins that eliminating the DATA
// body copy produces byte-identical stream content and the same FIN placement
// as the old AppendData(nil, body) path.
//
// The old path streamed [HEADERS] then [DATA-header ++ body]; the new one
// streams [HEADERS ++ DATA-header] then [body]. The QUIC stream delivers the
// concatenation either way, so the reassembled bytes and the FIN offset must
// match — only the datagram boundaries move, and those do not reach the peer's
// stream.
func TestSendRequest_BodyRidesHeadersWithoutCopy(t *testing.T) {
	for _, n := range []int{0, 1, 512, 11 * 1024, 256 * 1024} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			body := make([]byte, n)
			for i := range body {
				body[i] = byte(i)
			}
			hdr := []byte{0x01, 0x02, 0x03} // stand-in for the encoded HEADERS frame
			// New path (production): DATA header appended to the HEADERS buffer.
			newStream := &fakeStream{id: 4, conn: &fakeConn{}}
			c := &Client{}
			frameNew := append([]byte(nil), hdr...)
			// Old path, reconstructed: HEADERS, then AppendData(nil, body).
			want := append([]byte(nil), hdr...)
			if n > 0 {
				want = AppendData(want, body)
			}

			err := c.sendRequest(context.Background(), newStream,
				&Request{Method: "POST", Scheme: "https", Path: "/", Body: body}, frameNew)

			require.NoErrorf(t, err, "n=%d sendRequest", n)
			assert.Truef(t, bytes.Equal(newStream.sent, want),
				"n=%d: stream bytes differ (got %d, want %d) — the peer reassembles one byte "+
					"stream, so dropping the copy must not move a single byte of it",
				n, len(newStream.sent), len(want))
			assert.Truef(t, newStream.finSent,
				"n=%d: FIN never latched — the server would wait forever for the request to end", n)
		})
	}
}
