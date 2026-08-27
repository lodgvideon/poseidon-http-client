package client

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lodgvideon/poseidon-http-client/quic"
)

// TestH3Options_TranslatesEveryH3Knob is a decision table over the two HTTP/3
// settings ClientOptions carries. It exists because h3Options is the single place
// they are translated for http3.Dial, and a knob added to ClientOptions without a
// line here is silently ignored — the client would accept the configuration and
// dial with the default.
//
// The option values themselves are opaque across the package boundary (http3's
// config type is unexported, which is the point of functional options), so the
// count is what is observable here. The value actually reaching the read path is
// pinned inside http3 by TestWithMaxResponseBytes_IsWhatTheReadPathEnforces.
func TestH3Options_TranslatesEveryH3Knob(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts ClientOptions
		want int
	}{
		{"nothing set", ClientOptions{}, 0},
		{"conn options only", ClientOptions{H3ConnOptions: []quic.ConnOption{nil, nil}}, 1},
		{"response cap only", ClientOptions{H3MaxResponseBytes: 1024}, 1},
		{"both", ClientOptions{H3ConnOptions: []quic.ConnOption{nil}, H3MaxResponseBytes: 1024}, 2},
		// Zero is "use the default", not "cap everything at zero", so it must not
		// produce an option — otherwise the default path stops being the default.
		{"a zero cap is not a setting", ClientOptions{H3MaxResponseBytes: 0}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := h3Options(tc.opts)

			assert.Lenf(t, got, tc.want,
				"h3Options = %d options, want %d — a ClientOptions knob with no line in "+
					"h3Options is accepted and then ignored at dial time", len(got), tc.want)
		})
	}
}

// TestMakeH3DialFn_DefaultPathIsUnchanged pins the branch that keeps the common
// case free: with nothing configured, the dial function is the plain h3DialFn
// rather than a closure wrapping it. Cheap to keep, and it is what makes "the
// default path and the tests that pin it are unchanged" a checked claim rather
// than a comment.
func TestMakeH3DialFn_DefaultPathIsUnchanged(t *testing.T) {
	plain := reflect.ValueOf(makeH3DialFn(nil)).Pointer()
	configured := reflect.ValueOf(makeH3DialFn(h3Options(ClientOptions{H3MaxResponseBytes: 1024}))).Pointer()

	assert.Equalf(t, reflect.ValueOf(h3DialFn).Pointer(), plain,
		"makeH3DialFn(nil) returned a wrapper; with no options it must be h3DialFn itself")
	assert.NotEqualf(t, plain, configured,
		"makeH3DialFn returned the unconfigured dial function for a client that set "+
			"H3MaxResponseBytes; the setting would never reach http3.Dial")
}
