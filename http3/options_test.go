package http3

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWithMaxResponseBytes_IsWhatTheReadPathEnforces walks one fixture past two
// caps. The paired rows are the point: a single small-cap row proves only that
// something rejected the response, and would pass against a client that read the
// cap from anywhere at all — including a hard-coded constant. The default row
// accepts the identical bytes, so the only thing that changed is the option.
//
// Before #712 the limit was a package var, so the value the read path used could
// not be set from outside the package at all.
func TestWithMaxResponseBytes_IsWhatTheReadPathEnforces(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 4000)
	for _, tc := range []struct {
		name    string
		opts    []Option
		wantErr error
	}{
		{"a cap below the response refuses it", []Option{WithMaxResponseBytes(1024)}, ErrResponseTooLarge},
		{"the same response under the default is accepted", nil, nil},
		{"a zero option selects the default rather than refusing everything", []Option{WithMaxResponseBytes(0)}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := AppendHeaders(nil, encodeSection(hf(":status", "200")))
			data := AppendData(nil, payload)
			conn := &fakeConn{req: &fakeStream{recvChunks: [][]byte{append(headers, data...)}, fin: true}}
			client, err := NewClientFakeWithOptions(conn, nil, tc.opts...)
			require.NoError(t, err, "NewClientFakeWithOptions")

			_, body, doErr := client.Do(context.Background(),
				&Request{Method: "GET", Scheme: "https", Authority: "example.com", Path: "/"})

			if tc.wantErr == nil {
				require.NoErrorf(t, doErr, "Do = %v, want nil for a %d-byte body", doErr, len(payload))
				assert.Truef(t, bytes.Equal(body, payload),
					"body = %d bytes, want the %d the fixture sent", len(body), len(payload))
				return
			}
			assert.ErrorIsf(t, doErr, tc.wantErr,
				"Do = %v, want %v: the configured cap is below this response and must be "+
					"the value the read path enforces", doErr, tc.wantErr)
		})
	}
}

// TestWithConnOptions_Accumulate pins that the QUIC options Dial used to take
// directly still all arrive when they are assembled in more than one place. The
// option appends rather than replaces, so a caller merging a base set with a
// per-call one does not silently lose the first.
func TestWithConnOptions_Accumulate(t *testing.T) {
	cfg := apply([]Option{
		WithConnOptions(nil, nil),
		WithConnOptions(nil),
	})

	assert.Lenf(t, cfg.connOpts, 3,
		"connOpts = %d, want 3: two WithConnOptions calls must accumulate, not replace",
		len(cfg.connOpts))
}

// TestApply_SkipsNilOptions covers the branch a caller hits when it builds its
// option slice conditionally and leaves a hole in it.
func TestApply_SkipsNilOptions(t *testing.T) {
	cfg := apply([]Option{nil, WithMaxResponseBytes(77), nil})

	assert.EqualValuesf(t, 77, cfg.maxResponseBytes,
		"maxResponseBytes = %d, want 77: a nil option must be skipped, not panic or "+
			"discard the options around it", cfg.maxResponseBytes)
}
