package http3

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lodgvideon/poseidon-http-client/header"
	"github.com/lodgvideon/poseidon-http-client/qpack"
)

// The encode-side encoder pointer is published atomically so the static-only path
// — every request on a connection to a server advertising QPACK capacity 0 — needs
// no lock. That makes one race reachable that was not before: a Do can load the
// pointer while the reader goroutine is installing the encoder from the server's
// SETTINGS.
//
// TestConcurrent_QPACKEncoderDynamic_UnderRace does not cover it: it installs the
// encoder before starting any goroutine, so the pointer is already non-nil for
// every load it makes.
//
// Losing that race means encoding static-only while a dynamic encoder exists. The
// claim being pinned here is that this is safe: static-only output is legal on its
// own, and it emits no encoder instructions, so it cannot perturb the ordered
// instruction stream the server's decoder table is rebuilt from.

// TestConcurrent_QPACKEncoderInstall_UnderRace encodes on N goroutines across the
// installation of the encoder dynamic table, and requires every frame produced —
// before, during, and after — to decode back to the identical headers against the
// table the server would hold. Run with -race -count=5.
func TestConcurrent_QPACKEncoderInstall_UnderRace(t *testing.T) {
	const serverCap = 65536
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, defaultSettings)
	require.NoError(t, err, "NewClientFake over the fake transport")
	defer client.Close()

	req, want := sampleRequest()

	// Two barriers make the two ends deterministic rather than timing-dependent:
	// encodedOnce guarantees at least one frame is produced with the encoder still
	// nil, and installed guarantees at least one is produced after it exists. The
	// band between them is the race itself.
	encodedOnce := make(chan struct{})
	installed := make(chan struct{})

	var (
		mu     sync.Mutex
		frames [][]byte
		once   sync.Once
	)
	collect := func(f []byte) {
		mu.Lock()
		frames = append(frames, f)
		mu.Unlock()
	}

	const encoders = 8
	var encWG sync.WaitGroup
	for g := 0; g < encoders; g++ {
		encWG.Add(1)
		go func() {
			defer encWG.Done()
			for {
				frame, eerr := client.encodeRequestHeaders(req)
				if !assert.NoError(t, eerr, "encodeRequestHeaders while the install races") {
					return
				}
				collect(frame)
				once.Do(func() { close(encodedOnce) })
				select {
				case <-installed:
					// One more encode after the install is fully visible, then stop.
					frame, eerr = client.encodeRequestHeaders(req)
					if !assert.NoError(t, eerr, "encodeRequestHeaders after install") {
						return
					}
					collect(frame)
					return
				default:
				}
			}
		}()
	}

	<-encodedOnce
	client.enableEncoderDynamic(serverCap) // as the reader would, from the server's SETTINGS
	close(installed)
	encWG.Wait()

	require.NotNil(t, client.qpackEncoder.Load(),
		"encoder was never installed: the race under test never happened")
	require.GreaterOrEqualf(t, len(frames), encoders,
		"collected %d frames from %d goroutines, want at least one each", len(frames), encoders)

	// Rebuilding the server's table is itself the ordering check: serverMirrorTable
	// fails unless our encoder stream parses whole, from the type byte on, with no
	// byte left over. That is what a torn or reordered install would break.
	server := serverMirrorTable(t, conn, serverCap)
	require.NotZero(t, server.InsertCount(),
		"the server's table holds no entry: no encode ran after the install, "+
			"so this exercised only the static path and proves nothing about the race")
	for i, f := range frames {
		// Nothing acknowledges an insert in this fixture, so no section may reference
		// the dynamic table (RFC 9204 §2.1.3) — every frame is self-contained,
		// whichever side of the install it was encoded on, and must decode to the
		// same headers.
		ric := decodeRequestSection(t, server, f, want)
		require.Zerof(t, ric,
			"frame %d has Required Insert Count %d, want 0: nothing acknowledged an insert", i, ric)
	}
}

// TestQPACKEncoderInstall_StaticFrameIsSelfContained is the deterministic half of
// the race above: a frame encoded while the encoder was nil must decode identically
// whether or not a dynamic encoder is installed afterwards, and must have
// contributed nothing to the encoder stream. That is the property that makes losing
// the race a non-event.
func TestQPACKEncoderInstall_StaticFrameIsSelfContained(t *testing.T) {
	const serverCap = 4096
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, defaultSettings)
	require.NoError(t, err, "NewClientFake over the fake transport")
	defer client.Close()
	req, want := sampleRequest()

	frame, encErr := client.encodeRequestHeaders(req) // encoder still nil
	beforeInstall := len(conn.clientQEnc.sent)
	client.enableEncoderDynamic(serverCap)

	require.NoError(t, encErr, "encodeRequestHeaders on the static-only path")
	// The static encode wrote nothing to the encoder stream, so everything on it is
	// the install's own Set Dynamic Table Capacity.
	require.Equalf(t, 1, beforeInstall,
		"static encode put %d bytes on the encoder stream, want only the type byte", beforeInstall)
	server := serverMirrorTable(t, conn, serverCap)
	ric := decodeRequestSection(t, server, frame, want)
	assert.Zerof(t, ric,
		"static frame has Required Insert Count %d, want 0: it must reference no dynamic entry", ric)
}

// BenchmarkEncodeRequestHeaders_StaticOnly measures what the atomic pointer buys on
// the static-only path: the Unlocked arm is the production path, the Locked arm
// wraps the identical encode in encMu the way this function did before. Both arms
// do the same work; the only difference is the mutex. Against a server advertising
// QPACK capacity 0 this path serves every request for the connection's whole life.
//
// It is env-guarded for the same reason quic's send benchmarks are (see
// requireSendBench there): the request-header encode allocates today — 14 allocs/op,
// tracked separately — and the absolute zero-alloc bench-gate scans every Benchmark
// line emitted by ./http3, which IS one of the seven packages it benchmarks
// (.github/workflows/bench-gate.yml, scripts/bench-gate.sh). A skipped benchmark
// prints no B/op columns, so the gate ignores it. Run it with:
//
//	POSEIDON_BENCH_ENCODE=1 go test -run=^$ -bench=EncodeRequestHeaders -benchmem -cpu=8 ./http3
func BenchmarkEncodeRequestHeaders_StaticOnly(b *testing.B) {
	if os.Getenv("POSEIDON_BENCH_ENCODE") == "" {
		b.Skip("header-encode bench allocates; set POSEIDON_BENCH_ENCODE=1 to run (kept out of the zero-alloc bench-gate)")
	}
	conn := &fakeConn{req: &fakeStream{}}
	client, err := NewClientFake(conn, defaultSettings)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	req := &Request{
		Method: "GET", Scheme: "https", Authority: "example.com", Path: "/",
		Headers: []header.Field{{Name: []byte("user-agent"), Value: []byte("poseidon/1")}},
	}

	// lockedEncode is the pre-change shape of the static path, kept here rather than
	// in production code so the benchmark can measure the difference the change
	// makes without either arm being hypothetical.
	lockedEncode := func() ([]byte, error) {
		client.encMu.Lock()
		defer client.encMu.Unlock()
		var static qpack.Encoder
		return req.EncodeHeaders(&static, nil, client.maxFieldSection.Load())
	}

	b.Run("Unlocked", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := client.encodeRequestHeaders(req); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("Locked", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := lockedEncode(); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
