package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// fakeInvoker is the shape the interface exists to make possible: a client
// substitute with no socket behind it.
type fakeInvoker struct {
	gotMethod string
	gotReq    []byte
	gotMD     []conn.HeaderField
	gotOpts   int
	reply     []byte
	err       error
}

func (f *fakeInvoker) Invoke(_ context.Context, method string, req []byte, md []conn.HeaderField, opts ...CallOption) ([]byte, error) {
	f.gotMethod, f.gotReq, f.gotMD, f.gotOpts = method, req, md, len(opts)
	return f.reply, f.err
}

func (f *fakeInvoker) NewStream(context.Context, string, []conn.HeaderField, ...CallOption) (*Stream, error) {
	return nil, errors.New("fakeInvoker: streaming not supported")
}

var _ Invoker = (*fakeInvoker)(nil)

// callThrough is the shape generated code takes: it knows an Invoker and a
// method name, and nothing about connections.
func callThrough(ctx context.Context, cc Invoker, req []byte, opts ...CallOption) ([]byte, error) {
	return cc.Invoke(ctx, "/helloworld.Greeter/SayHello", req, nil, opts...)
}

// TestInvoker_AcceptsASubstitute is the property the interface is for: code
// written against Invoker runs against something that is not a connection.
func TestInvoker_AcceptsASubstitute(t *testing.T) {
	f := &fakeInvoker{reply: []byte("world")}

	got, err := callThrough(context.Background(), f, []byte("hello"),
		WithMetadata([]conn.HeaderField{{Name: []byte("x-tenant"), Value: []byte("acme")}}))

	require.NoError(t, err, "callThrough")
	assert.Equalf(t, "world", string(got), "reply = %q, want \"world\"", got)
	assert.Equalf(t, "/helloworld.Greeter/SayHello", f.gotMethod, "method = %q", f.gotMethod)
	assert.Equalf(t, "hello", string(f.gotReq), "request = %q", f.gotReq)
	// The option tail reaches the substitute intact — a wrapper that dropped it
	// would silently discard metadata, deadlines and message-size overrides.
	assert.Equalf(t, 1, f.gotOpts, "substitute received %d options, want 1", f.gotOpts)
}

// TestInvoker_ErrorsPropagate pins that a substitute's failure is the caller's
// failure, unmodified — a wrapper is a seam, not a translation layer.
func TestInvoker_ErrorsPropagate(t *testing.T) {
	sentinel := errors.New("substitute refused")
	f := &fakeInvoker{err: sentinel}

	_, err := callThrough(context.Background(), f, nil)

	require.ErrorIsf(t, err, sentinel, "error = %v, want the substitute's own", err)
}

// TestInvoker_RealConnSatisfiesItAndWorks is the other half: the interface is
// worthless if the real connection only satisfies it on paper. This drives an
// actual RPC through the interface rather than through *ClientConn.
func TestInvoker_RealConnSatisfiesItAndWorks(t *testing.T) {
	cc := dialMockPeer(t, newMockGRPCPeer(t), nil)
	var inv Invoker = cc // the assignment is the assertion under test

	got, invokeErr := inv.Invoke(context.Background(), "/bench.Svc/Echo", []byte("hello"), nil)
	s, streamErr := inv.NewStream(context.Background(), "/bench.Svc/Echo", nil)

	require.NoError(t, invokeErr, "Invoke through Invoker")
	assert.Equalf(t, "hello", string(got), "echo = %q, want \"hello\"", got)
	require.NoError(t, streamErr, "NewStream through Invoker")
	closeErr := s.Close()
	require.Truef(t, closeErr == nil || errors.Is(closeErr, ErrStreamClosed), "Close: %v", closeErr)
}

// middlewareInvoker shows the wrapper case the issue names — per-call metadata
// added by something between the generated client and the connection.
type middlewareInvoker struct {
	next  Invoker
	extra []conn.HeaderField
}

func (m middlewareInvoker) Invoke(ctx context.Context, method string, req []byte, md []conn.HeaderField, opts ...CallOption) ([]byte, error) {
	return m.next.Invoke(ctx, method, req, md, append(opts, WithMetadata(m.extra))...)
}

func (m middlewareInvoker) NewStream(ctx context.Context, method string, md []conn.HeaderField, opts ...CallOption) (*Stream, error) {
	return m.next.NewStream(ctx, method, md, append(opts, WithMetadata(m.extra))...)
}

// TestInvoker_WrapsForMiddleware pins that the seam composes: a wrapper can add
// to the option tail without the layer above knowing, which is why WithMetadata
// (#432) and this interface land together.
func TestInvoker_WrapsForMiddleware(t *testing.T) {
	f := &fakeInvoker{reply: []byte("ok")}
	mw := middlewareInvoker{next: f, extra: []conn.HeaderField{{Name: []byte("x-auth"), Value: []byte("t")}}}
	// One option from the caller, one added by the wrapper: the connection must
	// see both, or a wrapper is silently dropping what the layer above asked for.
	caller := WithMetadata([]conn.HeaderField{{Name: []byte("x-tenant"), Value: []byte("acme")}})

	_, err := callThrough(context.Background(), mw, []byte("q"), caller)

	require.NoError(t, err, "through middleware")
	assert.Equalf(t, 2, f.gotOpts,
		"connection saw %d options, want 2 (caller's plus the wrapper's)", f.gotOpts)
}
