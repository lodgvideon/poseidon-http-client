package pool

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCloseReason_StringIsStableAndTotal pins the labels. They are documented as
// handy for metric labels and log fields, which makes them an interface: a
// dashboard keys on these strings, so a rename is a silent break. The unknown
// arm matters too — a reason added without a label would otherwise surface as
// an empty string and merge with everything else in a group-by.
func TestCloseReason_StringIsStableAndTotal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		reason CloseReason
		want   string
	}{
		{CloseIdle, "idle"},
		{CloseDead, "dead"},
		{CloseGoAway, "goaway"},
		{CloseManual, "manual"},
		{CloseNotReusable, "not-reusable"},
		{CloseReason(42), "unknown"},
		{CloseReason(-1), "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()

			got := tc.reason.String()

			assert.Equalf(t, tc.want, got,
				"CloseReason(%d).String() = %q, want %q — these labels are what a dashboard "+
					"groups connection churn by", int(tc.reason), got, tc.want)
		})
	}
}

// TestDialError_WrapsAndUnwraps covers the property the failover classifier rests
// on. IsDialOnlyErr finds a *DialError with errors.As, and a caller distinguishes
// the underlying cause with errors.Is — both need Unwrap to keep the chain, and
// the message needs the address or a log line cannot say which backend refused.
func TestDialError_WrapsAndUnwraps(t *testing.T) {
	t.Parallel()
	cause := errors.New("connection refused")
	de := &DialError{Addr: "10.0.0.7:443", Err: cause}

	wrapped := fmt.Errorf("acquire: %w", de)

	assert.Contains(t, de.Error(), "10.0.0.7:443",
		"DialError.Error must name the address; without it a dial failure cannot be "+
			"attributed to a backend")
	assert.Contains(t, de.Error(), "connection refused",
		"DialError.Error must carry the underlying cause")
	assert.ErrorIsf(t, wrapped, cause,
		"errors.Is could not reach the cause through DialError; %v", wrapped)

	var got *DialError
	require.Truef(t, errors.As(wrapped, &got),
		"errors.As could not find the *DialError through a wrap; the managed pool's address "+
			"failover is exactly this lookup")
	assert.Equal(t, "10.0.0.7:443", got.Addr, "errors.As must yield the original DialError")
}

// TestNops_SatisfyTheInterfacesAndDoNothing pins the substitutes the pool
// constructors install for a nil Observer or Recorder. They are what replaced
// the per-call-site nil checks, so "does not panic" IS the contract; the
// compile-time assertions below are the other half.
func TestNops_SatisfyTheInterfacesAndDoNothing(t *testing.T) {
	t.Parallel()
	var obs Observer = NopObserver{}
	var rec Recorder = NopRecorder{}

	assert.NotPanics(t, func() {
		obs.OnDial(DialEvent{Addr: "a:1", Duration: time.Millisecond})
		obs.OnConnClose(ConnCloseEvent{Addr: "a:1", Reason: CloseIdle})
		obs.OnResolverUpdate(ResolverUpdateEvent{Total: 2})
		rec.DialAttempted()
		rec.DialFailed()
		rec.ConnClosed()
		rec.GoAwayReceived()
		rec.ObserveDial(time.Millisecond)
		rec.ObserveAcquire(time.Millisecond)
	}, "a nop must absorb every event; a pool built without observability calls these on "+
		"its dial and close paths with no nil check of its own")
}

var (
	_ Observer = NopObserver{}
	_ Recorder = NopRecorder{}
)

// TestDNSResolver_BuildsAResolver covers the exported constructor. It asserts
// only what can be asserted without a network: the resolver's behaviour is
// covered in resolver_test.go through the internal lookup seam, and reaching
// real DNS from a unit test would make the suite depend on a nameserver.
func TestDNSResolver_BuildsAResolver(t *testing.T) {
	t.Parallel()

	r := DNSResolver("svc.invalid", 8443, DNSOptions{TTL: time.Minute})

	require.NotNil(t, r, "DNSResolver returned nil; every managed constructor takes one of these")
}

// TestStaticResolver_WatchDeliversTheSetOnce pins what a Watch-capable resolver
// owes the managed pool: one delivery of the current set, then a closed channel.
// The pool falls back to polling when it sees ErrWatchUnsupported, so a static
// resolver returning that sentinel instead would turn a fixed address list into
// a ticker for no reason.
func TestStaticResolver_WatchDeliversTheSetOnce(t *testing.T) {
	t.Parallel()
	addrs := []Address{{Host: "10.0.0.1", Port: 443}, {Host: "10.0.0.2", Port: 443}}
	r := StaticResolver(addrs...)

	ch, err := r.Watch(t.Context())

	require.NoError(t, err, "a static set is watchable; it just never changes")
	got, ok := <-ch
	require.True(t, ok, "Watch closed its channel without delivering the initial set")
	assert.Equal(t, addrs, got, "Watch must deliver the set the resolver was built with")
	_, ok = <-ch
	assert.False(t, ok,
		"Watch must close after the only update a static set will ever have; leaving it open "+
			"parks the pool's watch goroutine forever")
}
