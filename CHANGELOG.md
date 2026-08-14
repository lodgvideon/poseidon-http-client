# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed (breaking)

- **`conn.Conn.NewStream` now returns a `*conn.GoAwayError` after a peer GOAWAY,
  instead of the bare `conn.ErrGoAway` sentinel.** The peer's error code was
  taken by `onGoAwayReceived` and dropped at its own signature, and nothing
  anywhere recorded it, so a graceful drain (`NO_ERROR`), a demand to back off
  (`ENHANCE_YOUR_CALM`) and the peer rejecting this client outright
  (`PROTOCOL_ERROR`) all arrived as one value. Those call for opposite responses
  from a load generator — redial elsewhere, slow down, or stop and report — and
  RFC 9113 §6.8 puts the code in the frame precisely so the receiver can tell
  them apart. The new error carries `Code` and `LastStreamID`.

  **Migration:** `errors.Is(err, conn.ErrGoAway)` is unaffected — `GoAwayError`
  reports itself as that sentinel, which is why the retry classifier needed no
  change. Only a direct comparison, `err == conn.ErrGoAway`, stops matching;
  switch it to `errors.Is`. Use `errors.As` to read the code.

### Fixed

- **An HTTP/1.1 request that timed out did not fail with
  `context.DeadlineExceeded`, so the retry layer replayed it.** `Request.Timeout`
  promises "the request fails with `context.DeadlineExceeded`" and
  `docs/CLIENT_GUIDE.md` repeats it, including the consequence that such a
  request is *never* retried. Over HTTP/2 that held. Over HTTP/1.1 it did not:
  `http1.Exchange.ReadResponse` installs the context's deadline on the socket and
  a peer that accepts the request and then goes silent trips it, so the failure
  came back as the socket's own `os.ErrDeadlineExceeded` — which does not match
  `context.DeadlineExceeded`. `client`'s `isHardStop` reads exactly that, so it
  missed the case, and a user `IsRetryable` written the ordinary way ("a
  `net.Error` whose `Timeout()` is true is transient") replayed a request that
  had already spent its entire budget, against the same silent peer. A read
  deadline on an `http1.Conn` can only have come from the exchange's own context
  — `ReadResponse` installs one on every entry, the watchdog installs one in the
  past on cancellation — so a timeout is now reported as that context's error,
  keeping the socket error as a second cause for logs. Cancelling a read reports
  `context.Canceled` for the same reason, where it used to say `i/o timeout`.
  Pinned end to end against a stalled peer by
  `client/integration_test/toxiproxy_test.go`.

- **`conn.ConnError.Last` was never assigned, so every connection error printed
  `last=0`.** Its doc comment ("0 if originated locally") described the reverse
  of the truth: a GOAWAY *received* from the peer builds no `ConnError` at all,
  and the only GOAWAY one ever accompanies is the local diagnosis the reader
  loop sends. It is now set to the id actually advertised, on that path, and the
  comment says so.

- **A PUSH_PROMISE reaching either streaming read path leaked its header slab
  back to the garbage collector instead of the pool.** A PUSH_PROMISE is
  delivered on the *parent* stream, which is the stream `responseBodyReader` and
  `StreamResponse.Recv` are draining, so it lands there whenever a `PushHandler`
  is installed. `Client.Do`'s buffered path dispatches the promise and `grpc`'s
  receive loop drops it with an explicit `putHeaderSlab`; these two switches had
  no arm for it at all, so the event fell straight out while every neighbouring
  skip arm — `EventHeaders`, `EventInterimHeaders` — recycled. Found by
  `exhaustive`, which is the whole argument for enabling it: nothing failed, the
  event was simply ignored. Pinned by `client/pushpromise_slab_test.go`, which
  asserts on the slab rather than on the bytes the caller reads, and which fails
  on the pre-fix code.

### Changed

- **An HTTP/3 response body no longer costs several times its own size to
  receive.** The buffered `Do` path allocated 75 KB to deliver a 16 KiB body and
  276 KB to deliver a 64 KiB one, arriving at QUIC packet granularity. Two things
  caused it, and neither could be fixed alone. `FrameReader.ReadFrame` re-sliced
  its buffer past each frame it handed out, which put the consumed capacity out
  of reach, so every response climbed a fresh `append` growth ladder from
  nothing; and the body then *adopted* the reader's first DATA payload rather
  than copying it, which made that array caller-owned and therefore impossible to
  reuse. The reader now tracks the consumed prefix as an offset and reclaims it
  when its tail runs out, its backing array is recycled between requests, and the
  body is copied into one exact-sized allocation. A response costs two
  allocations at roughly its own size, whatever the body size and however the
  bytes were chopped up on the wire:

  | body | delivered in | before | after |
  |---|---|---:|---:|
  | 4 KiB | 1200-byte bursts | 8,913 B / 4 allocs | 4,179 B / 2 allocs |
  | 16 KiB | 1200-byte bursts | 75,346 B / 9 allocs | 16,596 B / 2 allocs |
  | 64 KiB | 1200-byte bursts | 276,050 B / 13 allocs | 66,428 B / 2 allocs |
  | 64 KiB | one burst | 73,810 B / 2 allocs | 65,932 B / 2 allocs |
  | 64 KiB, 8 DATA frames | 1200-byte bursts | 379,091 B / 44 allocs | 174,249 B / 6 allocs |

  A 256-byte body pays 25 bytes more than before (394 vs 369), the copy it now
  makes with nothing large enough to amortize it. `make bench-alloc` reports all
  of these, and carries the unpooled reader as its own case so the trade stays
  visible: the copy alone, without reuse, is a regression, which is why neither
  half of this could ship without the other.

  Nothing in the public surface moves. A buffered response already owned its
  bytes — headers and trailers were copied at the QPACK decode — and the body
  now does too. The payload `FrameReader.ReadFrame` returns is still documented
  as valid only until the next `Feed` or `ReadFrame`; that lifetime is now
  enforced by reuse rather than merely stated, so a caller that held one past it
  and got away with it will not any more. `BodyReader` (`DoStream`) is unchanged:
  it hands payloads that alias its reader out to its caller, so its buffer stays
  per-request rather than being recycled.

- **Formatting is now gated.** golangci-lint v2 moved formatters into their own
  top-level `formatters:` section, and `.golangci.yml` never grew one — so
  neither `make lint` nor the CI lint job ran `gofmt` at all, and neither ever
  would have. 32 files had drifted, 8 of them production: misaligned
  struct-field comments in `client/metrics.go` and `client/hooks.go`, trailing
  blank lines in `hpack/static_table.go`, over-long one-line method bodies in
  `quic/conn.go` and `http3/stream_body.go`. All 32 are reformatted here; the
  change is whitespace only.

  The gate only holds on an LF tree, because gofmt emits LF unconditionally and
  a CRLF checkout reports every file as unformatted. A `.gitattributes` pins
  `* text=auto eol=lf`, which also covers the parts of the tree CI only ever
  runs on Linux or inside a container — `scripts/*.sh`, the Python gates, the
  Dockerfiles and nginx/Caddy configs under `test/integration/` — where a CRLF
  shebang is a container-only failure no local run reproduces.

- **`depguard` enforces the from-scratch claim.** The premise of this module is
  that HTTP/1.1, HTTP/2, HPACK, QUIC, HTTP/3 and QPACK are implemented from the
  RFCs rather than wrapped around existing libraries, and that premise lived
  only in prose. Production code may no longer import `net/http`,
  `golang.org/x/net/http2` or `github.com/quic-go/quic-go`; test code still may,
  since those are exactly the reference peers the conformance and interop suites
  are written against. No production file imported any of them when the rule
  went in — it is a ratchet, not a cleanup.

- **Nine more zero-finding ratchets.** `durationcheck` (the RFC 9002 PTO/RTT/
  pacing code does more `time.Duration` arithmetic than the rest of the tree,
  and a unit error there surfaces as a stall rather than a compile failure),
  `gocheckcompilerdirectives` (a misspelled `//go:noinline` is a plain comment,
  and the alloc benchmarks depend on inlining decisions), `mirror`, `bodyclose`,
  `usetesting`, `nilnesserr`, `asasalint`, `reassign`, `exptostd`, `bidichk`.
  Each reported zero findings across the root module, `contrib/`, and all three
  build tags when it was enabled.

  `fieldalignment` was measured and deliberately left off; the reasoning is
  recorded next to the `disable:` entry in `.golangci.yml`.

- **`//nolint` directives now have to earn their place** — `nolintlint` with
  `require-explanation`, `require-specific` and `allow-unused: false`. The tree
  had **67 directives; 46 of them suppressed nothing.** 30 bare
  `//nolint:gosec` on `InsecureSkipVerify` in examples and tests were dead twice
  over — G402 is excluded globally and `gosec` does not run on `_test.go` at all
  — and another 23, these ones *with* explanations, had outlived what they
  silenced: `//nolint:gocyclo` on functions since simplified below the
  complexity floor, `//nolint:errcheck` in files the exclusion rules already
  cover. All 46 are deleted. The 21 that remain each suppress a real finding and
  each say why.

  The one directive that turned out to be load-bearing is
  `unsafeStringToBytes`: stripping it surfaced gosec G103, and it is back with
  the audit G103 asks for — every caller passes a `*Request` field that outlives
  the header block, and every consumer only reads.

- **Six linters with small standing findings**, each fixed or annotated rather
  than baselined: `wastedassign` (three dead initializers, in `conn`, `http1`
  and `quic`), `recvcheck` (`rttStats.lossDelay` unified onto the pointer
  receiver; `grpc.Status` keeps its split receiver, which is deliberate and now
  documented), `nilerr` (five intentional drop-the-error sites, including the
  one RFC 9001 §5.2 requires — a packet failing authentication must be discarded
  silently, since surfacing that error would let an off-path attacker kill a
  connection with one forged datagram), `fatcontext`, `makezero` (production
  clean, fixture builders excluded), `nolintlint`.

- **`exhaustive` guards the enum dispatch.** This module is nine enum-shaped
  wire vocabularies — frame types, settings IDs, error codes, packet types,
  stream events — and adding a member while missing a switch is both the
  characteristic defect here and a silent one: the new value falls through and
  the code does nothing, which is what the PUSH_PROMISE slab leak above was.
  Ten production switches were reviewed; one was a bug, one is a deferred
  finding, and the other eight now carry an `//exhaustive:ignore` that states
  why handling a subset is correct. Tests are excluded — a table test switching
  on the three codes its scenario produces should not have to spell out the
  other twelve.

  The deferred one is `TLSHandshake.Pump`: Go 1.26 adds `tls.QUICErrorEvent`,
  which carries a fatal handshake error, and the loop drops it — turning a
  failed handshake into a `Pump` that reports success. It cannot be fixed here,
  because naming the constant does not compile under this module's declared
  `go 1.25.0`; the reasoning and the trigger are recorded at the switch.

## [v0.12.0] — 2026-08-10

### Added

- **`loadgen -transport h3`** (#449). The many-connections-times-small-requests
  regime is only reachable over HTTP/3, which puts one UDP socket behind each
  connection, and the load generator spoke HTTP/2 only — so the syscall-floor
  question in #361 had no instrument. `ServerName` is set explicitly on its TLS
  config: without it the ClientHello carries no SNI and a server that selects its
  certificate by name closes the connection during the handshake, which surfaces
  as `quic: connection closed during handshake` rather than as a missing field.
  Certificate verification is now **on by default** for both transports, with
  `-insecure` as the opt-out — it used to be disabled unconditionally, so a
  loadgen aimed at a real endpoint silently accepted any certificate.

- **A congestion-control measurement harness** (#447). `lossproxy` gained
  `DELAY_MS`, a fixed one-way delay implemented as a strict FIFO queue per
  direction — not a timer per datagram, because equal timers fire in
  goroutine-schedule order, which is reordering, and would corrupt the arm that
  is supposed to have none. `TestInterop_CCGoodput` measures an **upload**
  deliberately: congestion control governs the sender, so a download would
  measure the server's controller. `scripts/cc-matrix.sh` runs both arms over the
  loss × RTT cells and proves per row that the configured loss was injected.

- **A receive-path allocation benchmark for HTTP/3** (#446), behind the
  `allocbench` build tag with a `make bench-alloc` target. The repository had no
  instrument for this — `client/bench_h3_alloc_test.go` substitutes a fake
  `h3Client` and never runs `http3.Client` at all — which is why the evidence in
  #342 came from an out-of-tree driver and why one of the three sites that
  profile named had gone stale without anything noticing. The tag is required:
  `bench-gate` is an absolute zero-alloc gate whose scope includes `./http3`.

- **`conn.H1TLSDialer` — HTTP/1.1 over TLS** (#334). The HTTP/1.1 transports
  documented a dialer requirement no exported dialer met: "a TLS dialer with
  `NextProtos` containing only `http/1.1`". `TLSDialer` asserts h2, `FlexDialer`
  offers h2 alongside http/1.1 (so any h2-capable server picks h2), and
  `PlaintextDialer` does no TLS — leaving every HTTPS + HTTP/1.1 caller to write
  their own `conn.Dialer`. `H1TLSDialer` offers only the `http/1.1` ALPN token
  and asserts the peer did not select something else (`ErrALPNNotHTTP11`; a peer
  that negotiates no ALPN at all is accepted, since that implies HTTP/1.1).

- **`conn.ConnOptions.AutoTuneRecvWindow` + `MaxRecvWindow` — bandwidth-delay-
  product tuning of the HTTP/2 receive windows** (opt-in, default off). RFC 9113
  §6.5.2 fixes both receive windows at 65535 bytes until an endpoint raises
  them, and this client never did: the connection was limited to one window per
  round trip — about 6.5 MB/s in total at 10 ms RTT, however fast the link or the
  CPU. The ceiling is invisible on loopback, which is why no benchmark here had
  ever shown it.

  With the option on, the connection measures how much a peer delivers in one
  round trip — a PING, and the DATA that arrives before its ACK — and raises both
  windows to twice that. The sample is bytes-per-round-trip by construction, so
  there is no clock and no smoothed RTT estimate to get wrong; the peer's ACK is
  the clock. Windows only grow, and probing backs off exponentially once a sample
  stops moving the target, so a connection whose window is already large enough
  stops spending PINGs.

  The ceiling is derived rather than guessed: a window never exceeds
  `StreamEventBuffer × Settings.MaxFrameSize`, the memory the connection has
  already committed to buffering for one stream, so tuning cannot make the
  event-channel overflow reset any likelier than the configured buffer already
  does. `MaxRecvWindow` overrides it; 64 MiB is the absolute clamp.

  It is opt-in for the reason `GroupCommit` is: this changes flow-control
  behaviour, and a v1 library should not do that to existing callers silently.
  Set it on any connection that streams bulk data over a real network —
  `client.ClientOptions.ConnOpts` and `grpc.Options.Conn` both reach it.

  With the option off, nothing changes: the refund arithmetic reduces exactly to
  the previous "give back what was spent", which `TestRefundIncrement` pins.

- **`grpc.Stream.SendLast`** — writes the final message and half-closes the
  request side in the same DATA frame. `Send` followed by `CloseSend` needs two,
  and the second carries no payload but still costs its own flush: over TLS a
  separate record with its own header and AEAD tag, and — since Go enables
  `TCP_NODELAY` — usually a separate segment. `Invoke` now uses it, taking a
  unary RPC from three transport writes to two (measured by
  `TestUnaryTransportWriteCount`). Prefer it wherever the last message is known
  in advance; `Send` + `CloseSend` remains correct and is still the right pair
  for a bidirectional call that learns it is done only afterwards.

- **`conn.ALPNAsserter`** — an optional `AssertsALPN() string` on a dialer,
  declaring the one protocol it ever returns. `TLSDialer` and `ProxyTLSDialer`
  answer `"h2"`, `H1TLSDialer` `"http/1.1"`, `FlexDialer` `""` (no assertion).
  `client.NewClient` uses it to reject a transport/dialer pairing that can only
  fail. Custom dialers may implement it to get the same check.

### Changed

- **The per-entry table overhead is named once per package** (#445).
  RFC 7541 §4.1 and RFC 9204 §3.2.1 both add 32 octets per dynamic-table entry,
  and each package spelled the literal three times — in `hpack` as the two halves
  of one running total, where applying it on insert but not on eviction inflates
  the table until it wedges or underflows a `uint32` past zero. `hpack` and
  `qpack` keep **separate** constants on purpose, as do `http1` and `http3`:
  those numbers are frozen by four different specs and by interop with the peer,
  so a shared definition would make the wrong thing happen automatically.

- **The 1xx cap is pinned across protocols** (#445). `conn`, `http1` and `http3`
  each declare `maxInterimResponses = 100` and must agree, because `client.Do` is
  one API over all three stacks — raising one alone makes the same application
  against the same origin succeed or fail depending on which protocol ALPN
  picked. They cannot share a definition without creating import edges the
  layering deliberately avoids, so each package now carries a tripwire test whose
  failure message names the other two.

- **The UDP_GRO control message is parsed without allocating.**
  `parseGROSegmentSize` handed the recvmsg ancillary data to
  `syscall.ParseSocketControlMessage`, which appends every message it finds to a
  slice — one heap allocation on exactly the reads UDP_GRO exists to produce.
  Measured at 1.00 allocations per coalesced recvmsg, and 0 when no control
  message is attached. The buffer is now walked in place: 0 either way.

  The offsets still come from the stdlib's exported `CmsgLen`/`CmsgSpace`, so the
  alignment rules are not hard-coded per GOARCH, and every field read out of the
  kernel's buffer is bounds-checked first.

  This is a site #348 does not name; it was found while fixing the ones it does.
  Because the parse now reads a length the kernel wrote and indexes on it,
  `TestParseGROSegmentSize_MatchesStdlib` diffs it against the implementation it
  replaced across a table of layouts, `TestParseGROSegmentSize_MalformedYieldsZero`
  covers the buffers a kernel would never produce, and `FuzzParseGROSegmentSize`
  keeps the stdlib as an oracle on arbitrary bytes — 6.8M executions clean.

- **The QUIC send path recycles the RFC 9000 §13.3 retransmit copies** (#347).
  Every STREAM chunk is copied out of the reusable frame scratch and retained so
  the frame can be re-sent at its original offset if the packet is lost. That copy
  was the last per-datagram heap allocation on the send path; it now comes from a
  per-connection free list that acknowledgements refill.

  Recycling it is sound only because ownership of a payload is **linear**: it is
  reachable from exactly one of a `sentPacket` or the retransmit queue, never
  both. Every exit from the sent-packet map either hands the frame to the queue
  and deletes the packet in the same step (`detectLost`, `queueOldestProbe`,
  `requeueInitialForRetry`) or is an acknowledgement, which hands it to nobody —
  so `sentSpace.ack` is the one moment a payload is dead and the only place it is
  released. Releasing at `detectLost` instead would hand back a buffer the queue
  still points at, and the lost packet would be retransmitted at its original
  offset carrying another frame's bytes, which the peer accepts as valid data.
  `TestRetransBuf_QueuedPayloadSurvivesRecycling` arranges that exact interleaving.

  Both ends run under `c.mu`, so the free list needs no synchronisation and,
  unlike a `sync.Pool`, boxes nothing on the way in. It is bounded at 64 buffers
  (~77 KiB per connection); CRYPTO-sized payloads are dropped rather than parked.

  On an acknowledged connection: **2 allocations / 208 B per datagram → 1 / 160 B**.
  The remaining one is the sent-packet map churning in the test harness, not the
  send path. `BenchmarkQUICSend` is unchanged at 1 alloc because it never
  acknowledges anything — with nothing to recycle there is nothing to save, which
  is why `TestSendPath_AllocsPerDatagramSteadyState` was added alongside it.

- **`grpc` pools the two per-RPC buffers** (#436). `sendBuf` and the decoder's
  reassembly buffer both started nil on every call, so each was regrown from zero even
  though the identical call a microsecond earlier on the same connection had one of
  exactly the right size. They are now drawn from a pool and returned by `Close`.

  | | before | after |
  |---|---|---|
  | `Invoke` | 13 allocs | **10** |
  | `InvokeInto` | 12 allocs | **9** |
  | `BenchmarkGRPC_Unary_8B` | 13 allocs/op | **10** |
  | `BenchmarkGRPC_ServerStream_64x1KB` | 81 allocs/op | **77** |

  Three per unary call rather than the two a buffer count suggests, because each
  buffer was being grown more than once on its way up.

  **The `Stream` struct itself is deliberately not pooled**, which is a departure from
  what the issue proposes. Pooling it would mean re-arming `closed` for the next owner,
  and a caller holding a `Stream` from a finished RPC would then pass every guard and
  operate on the next call's stream — the same shape as #370 one layer down, which cost
  an API change to close. Leaving the struct alone keeps `closed` a permanent latch, so
  a stale reference is refused forever and can never reach buffers the pool has since
  handed on. That is worth one allocation per call, and the remaining nine are named in
  the issue rather than hidden.

  Buffers above 64 KiB are dropped instead of pooled, so a single outlier response
  cannot park its memory for the life of the process.

  Last of the prerequisites tracked by #437.

- **`grpc.ClientConn.InvokeInto` — the unary buffer-reusing form** (#435). `Invoke`
  handed back the slice it got from `Stream.Recv`, so it inherited that per-call
  allocation and offered no way to reuse a caller's buffer — the response is the most
  obviously reusable object in a unary call for a load generator.

  `InvokeInto(ctx, method, req, dst, md, opts...)` appends the response into `dst[:0]`.
  Semantics are `Invoke`'s exactly: the `SendLast` fast path with its benign-half-close
  tolerance, `io.EOF` turned into an `Internal` status for a method that answered with
  nothing, and the drain to the terminal event that catches a unary method sending two
  messages. `Invoke` is now `InvokeInto(..., nil, ...)`.

  Measured: **13.0 → 12.0 allocations per unary call.** One, which is what this issue
  removes; the remaining twelve are the `Stream` struct, the decoder buffer, `sendBuf`
  and the header/trailer clones, and belong to #436.

  The drain deliberately uses `Recv`, not `RecvInto(resp)`. Handing it the response
  buffer would let a second message overwrite the answer about to be returned — the
  reuse would corrupt precisely what it optimises.

  The two allocation gates now sit behind `//go:build !race`. The race detector
  allocates as it instruments, and both gates turn on a difference of a single
  allocation, which that noise swamps: the new one failed four runs out of five under
  `-race` while passing every run without it.

  Fourth of the prerequisites tracked by #437.

- **`grpc.Stream.RecvInto` — receive without the per-message allocation** (#434).
  `Recv` returns a fresh copy of every message. Copying is the right default — the
  decoder reuses its buffer across messages, so handing out an alias would break the
  next call — but it was unconditional, putting a floor of one allocation and one copy
  per message on every caller, including one that already owns a large enough buffer.

  `RecvInto(ctx, dst)` appends into `dst[:0]` and returns the result, in the same
  idiom `AppendMessage` already uses on the send side:

  ```go
  for {
      buf, err = s.RecvInto(ctx, buf[:0])
      if errors.Is(err, io.EOF) { break }
      if err != nil { return err }
      use(buf)
  }
  ```

  Measured on a 64-message stream: **Recv 1.00 allocations per message, RecvInto 0.00.**

  On error `RecvInto` returns `dst[:0]` rather than nil. The terminal iteration of
  every stream is an error, so returning nil there would hand the caller's buffer to
  the garbage collector once per stream — exactly the allocation the method removes.
  `Recv` is now `RecvInto(ctx, nil)` and so still returns nil, keeping its own contract
  unchanged, which `TestRecv_ContractUnchanged` pins.

  Third of the prerequisites tracked by #437.

- **`grpc.Invoker` — the method set code above this package consumes** (#433).
  `ClientConn` is concrete and is one HTTP/2 connection, so anything built on it was
  welded to that connection. `Invoker` names `Invoke` and `NewStream` so a client can
  be pointed at a pool or round-robin (this package ships none by design), at a
  wrapper adding per-call auth, retry or metrics, or at a double so it can be
  unit-tested without a socket.

  Pure addition: no behaviour change, no signature change, no new dependency. The
  `var _ Invoker = (*ClientConn)(nil)` assertion lives in the package rather than a
  test, so a signature drift breaks the build rather than a test run.

  It is a seam for substituting the *connection*, not an abstraction over transports:
  `NewStream` still returns the concrete `*Stream`, which owns the send/receive
  lifetime contract. Abstracting that is a larger question and deliberately out of
  scope.

  Second of the prerequisites tracked by #437.

- **`grpc.WithMetadata` — request metadata as a `CallOption`** (#432). `Invoke` and
  `NewStream` took metadata as a positional argument, separate from the variadic
  option tail. That is fine by hand and awkward for anything generating call sites,
  which wants one uniform `(ctx, in, opts ...CallOption)` shape — and `CallOption`
  is a closed interface, so only this package can add one.

  Option-supplied metadata is sent in addition to whatever arrived positionally,
  positional first, and several `WithMetadata` options accumulate rather than
  overwriting each other. Both existing signatures keep working unchanged.

  The two sources are treated identically where it matters: the syntax check that
  is the last gate before the wire now runs after the options are resolved and
  covers both, and the never-indexed default for credential fields applies to
  both — otherwise the option would have been a way around either. Neither source
  is copied to combine them, so the header block is still built without
  allocating.

  First of the prerequisites tracked by #437.

- **BREAKING: streams are handled through `conn.StreamRef`, closing the
  use-after-recycle hole** (#370). `Conn.NewStream` and `Conn.LookupStream` now
  return a `StreamRef`, and `Recv`, `SendData`, `SendHeaders`,
  `SendHeadersWithPriority` and `Close` live on it rather than on `*Stream`.
  Method names and signatures are unchanged, so most call sites only change
  where they declared the type; `client` and `grpc` needed no test changes at
  all.

  `conn.Stream` structs are pooled. `allocStream` re-armed every per-lifetime
  guard for the new owner, so a pointer retained past `Close` passed all of
  them: a stale `Recv` returned the *next* request's events, and a stale
  `SendData` wrote DATA on someone else's stream. It could not be fixed inside
  the methods — the receiver IS the struct — so the caller has to present the
  lifetime it believes it holds. A `StreamRef` is that presentation, and every
  method refuses with the new `ErrStaleStream` once the struct has been
  recycled.

  Three things the obvious version of this fix gets wrong, all of them pinned by
  tests:

  - **The generation is bumped at release, not at re-allocation.**
    `resetForPoolLocked` clears `closed` and `localEnded` and nils the writer, so
    a stale send against a recycled-but-unclaimed struct would otherwise pass
    every gate and nil-deref. Bumping there also makes the regression tests
    independent of a `sync.Pool` draw — the test that pinned this before had a
    `t.Skip` for exactly that, meaning the guard could ship never having run.
  - **The check is under `s.mu`, the same section as the operation.** Checking a
    lifetime outside the lock leaves a window against the recycle it is meant to
    exclude, which for `Close` means RST_STREAM(CANCEL) on a stranger's live
    stream — deterministic, no race needed.
  - **A door check does not cover `SendData`.** `writeData` releases `s.mu` and
    can park on peer credit indefinitely; the wake loop re-read only `s.closed`,
    which is *guaranteed false* for every pooled stream because `markStreamDone`
    pools only when it is false. The lifetime is now re-validated on wake and
    again — together with the stream id, in one `s.mu` section — before each
    frame reaches the wire.

  `ErrStaleStream` is deliberately NOT wrapped around `ErrStreamClosed`: three
  shipped call sites treat that error as benign-and-continue, and laundering a
  use-after-recycle through them would turn a caller bug into a silent successful
  half-close, a hang, or a wrong result.

- **BREAKING: `quic.RecvGRO` and `quic.SendGSO` take a state handle** (#348).
  Their signatures are now `RecvGRO(*GROState, syscall.RawConn, io.Reader, []byte)`
  and `SendGSO(*GSOState, syscall.RawConn, io.Writer, []byte, int)`; `RecvGRO`'s
  trailing `oob` parameter is gone, absorbed into `GROState`. A caller builds one
  state per connection per direction with `NewGROState(oobLen)` / `NewGSOState()`
  and passes it on every call. A nil state takes the unoffloaded fallback, so the
  degraded path is still one line.

  `RawConn.Read` and `RawConn.Write` take a func through an interface, so the
  syscall closure escapes and drags every variable it captures to the heap with
  it. That cost four heap objects on **every** recvmsg and four on every offloaded
  sendmsg — the send side also rebuilt its whole UDP_SEGMENT control message per
  call, though only two of its bytes ever change. Hoisting the state onto an
  object that lives as long as the connection, and taking the closure as a method
  value exactly once in the constructor, makes both hot paths allocate nothing:

  ```
  RecvGRO   4 heap objects/call -> 0    (3 once, in NewGROState)
  SendGSO   4 heap objects/call -> 0    (3 once, in NewGSOState)
  ```

  Verified by escape analysis under Linux build tags, which is also how the
  figures above are counted. #348 reports the receive side; the send side has the
  same defect and is fixed here too.

  The two directions get separate state objects on purpose: `http3`'s `udpConn`
  drives `ReadGRO` from the QUIC reader goroutine and `WriteGSO` from the sender,
  and that file already documents the split as the reason not to cache lazily.

  `quic/gro_realsocket_linux_test.go` is new, and is the first test in the package
  to run `syscall.SendmsgN` and `syscall.Recvmsg` at all — every other GRO/GSO test
  drives a fake `PacketConn`, so the code that builds the UDP_SEGMENT control
  message and parses the UDP_GRO one back out had no runtime coverage.

- **The QUIC send path no longer allocates a slice per packet** (#347). Every
  packet carried its retransmittable frames in a `[]retransFrame`, and every send
  site in the package builds a packet around exactly one frame — the loss path
  re-seals each queued frame into a packet of its own — so that slice was
  allocated per datagram and never held more than one thing. `sentPacket` now
  holds the frame by value and the plumbing passes `*retransFrame`.

  `BenchmarkQUICSend`: 112 B / 2 allocs → 48 B / 1 alloc per datagram.
  `BenchmarkQUICSend_Stream16KiB`: 18988 B / 30 allocs → 18027 B / 15 allocs, and
  the same for the GSO variant. `datagrams/op` and `writes/op` are unchanged —
  this moves allocations, not packets.

  The one remaining allocation is the retransmit copy of the chunk, which RFC
  9000 §13.3 requires be available until the packet is acknowledged.
  `TestSendPath_AllocsPerDatagram` gates the count: the send benchmarks that
  measure it are behind `POSEIDON_BENCH_SEND` and excluded from the zero-alloc
  bench-gate, which left it ungated until now.

- **`grpc` builds its request header block without allocating.** `buildHeaders`
  cost 17 allocations per RPC — 22 with a deadline — because every constant
  header name and value was a `[]byte("...")` conversion that escaped into the
  header slice. `client/` had already solved this with shared name/value byte
  slices and a pooled backing array; `grpc/` never inherited the fix. The
  per-connection scheme, authority and user-agent bytes now live on the
  `ClientConn`, and a pooled scratch carries the field slice plus the two values
  that genuinely vary (the method path and the rendered `grpc-timeout`, which
  `appendTimeout` now writes in place). `BenchmarkGRPC_BuildHeaders`: 269 ns /
  640 B / 17 allocs → 38.5 ns / 0 B / 0 allocs; a small unary RPC drops from 30
  to 13 allocations end to end. No API change.

- **The HTTP/2 and HTTP/3 pools now report a failed dial as `*DialError`**
  (#359). Two consumers classify on that type and both silently did the wrong
  thing with the bare dialer error. `managedPool`'s acquire loop moves to the
  next address only for a dial-only failure, so multi-address failover — a
  documented feature — never fired on a first dial failure for either
  transport; the HTTP/1.1 pool has always wrapped and always failed over.
  `builtinShouldRetry` keys on the same type, so a `Retryer`-wrapped client
  over a pooled H2/H3 transport did not retry a dial failure either, although
  nothing had been sent.

  That second effect is a behaviour change, not only a bug fix: a client
  against a downed host goes from failing fast to `MaxAttempts` with backoff.
  It matches every other transport — the single-conn H2, H3 and H1 paths all
  wrapped already — and `DialError.Unwrap` keeps `errors.Is`/`errors.As`
  working on the cause, so callers inspecting the underlying error are
  unaffected.

- **`grpc.Stream.CloseSend` no longer reports a peer-closed stream as a
  failure** (#337). It returns nil when the peer has already reset the stream,
  because half-closing one is then a no-op and failing made callers discard a
  response the server had already sent (see Fixed, below). Code of the shape
  `if err := s.CloseSend(ctx); err != nil { abort() }` will now proceed in that
  case. Nothing is lost: the reset still decides the call through `Recv` and
  `Status`, which is where an RPC's outcome has always been determined. `Send`
  is unchanged and still strict.

- **A `Response.BodyReader` for a response that ended on its HEADERS frame now
  reports `io.EOF` immediately.** A 204, a 304, a HEAD or any status-only reply
  read one event too many before, which meant blocking until the context
  expired, or reporting a trailing `RST_STREAM` as a `*StreamResetError`.
  `DoStream`'s `StreamResponse` always carried the flag over; the buffered
  `Do` + `BodyMode: BodyStream` reader was the sibling that did not.

### Fixed

- **Pool selection no longer costs a channel operation per connection per
  request** (#448, #449, #450). `Alive`/`IsAlive` were a channel select and
  `pickLeastLoaded` visited every connection on every request, so dispatching one
  request over 10,000 connections meant 10,000 channel selects. A CPU profile at
  4,000 connections put `runtime.chanrecv` at 19.6% of all CPU.

  Both predicates now read an atomic mirroring "the reader has exited"; the
  channel remains what `Close` waits on. The flag is published **before** the
  channel closes, so a goroutine woken by it can never then observe the
  connection as alive — early is safe, late is the bug — and every path that
  retires a reader goes through one place, so a future raw `close` cannot
  silently leave the pool handing out a dead connection. The scan now returns at
  the first idle connection, starting from a rotating cursor so consecutive
  requests spread instead of piling onto whichever connection sorts first.

  **10,000 connections: 3,217 → 28,500 req/s (8.9x); 4,000: 12,500 → 35,700.**
  Syscall entry never appeared in any profile, which is what closed the io_uring
  question (#363).

- **The first DATA payload is adopted as the HTTP/3 response body** (#446)
  instead of being copied into a second buffer that grows to the same size. Two
  `FrameReader` properties make it sound and both are now pinned by tests: the
  payload's capacity equals its length, so appending a second frame must
  allocate rather than write into the reader's buffered frames; and `Feed` only
  appends at the tail while `ReadFrame` only slides forward, so a payload already
  handed out is never rewritten. A compacting `Feed` would break the second
  silently. 64 KiB body: 341,588 → 276,052 B/op at packet granularity, and
  139,346 → 73,809 when the response arrives in one burst.

- **A conformance gap in HPACK Huffman padding** (#445). RFC 7541 §5.2 states two
  padding rules; the existing test isolates the first and deliberately stays
  under 7 bits so the second cannot fire, so "padding strictly longer than 7 bits
  MUST be treated as a decoding error" had no test at all — moving the decoder's
  limit from 7 to 8 left the whole suite green and `0xff` decoded to the empty
  string instead of failing. Found by mutation-testing a dead flag:
  `hufFSMEntry.padOK` was assigned and never read, leaving the rule written twice
  with only one copy load-bearing.

- **A padding violation could not be classified, so no GOAWAY reached the**
  **peer** (#402). `frame.ErrInvalidPadding` was exported and documented, and
  never returned: `StripPadding` reports with `internal/bytesx`'s own sentinel,
  which nothing outside the module can name or match with `errors.Is`. That made
  the exported value dead API and — worse — made `conn`'s `mapFrameError` case
  for oversized padding unreachable, so a PADDED frame whose pad length reached
  the payload fell to the untyped default arm: the connection tore down with no
  `GOAWAY(PROTOCOL_ERROR)`, which RFC 9113 §6.1 requires. The three dispatch
  sites now wrap with `%w: %w`, so both identities match.

- **`Warmup` did not pre-dial** (#399). On a managed client it opened nothing
  at all; on the HTTP/2 and HTTP/3 pools it opened exactly one connection
  regardless of `n`. Two independent causes. `managedPool.warmup` iterated
  `mp.subPools`, which is populated lazily on the first acquire, so on a fresh
  pool it was empty and warmup returned early — the state a warmup exists to
  serve. And the base-pool warmup was a loop of acquire+release: these pools
  multiplex, so `pickLeastLoaded` handed back the connection just released and
  the loop never triggered a second dial. The managed pools now build their
  sub-pools from the resolved address set, and the H2/H3 pools warm up through
  a new actor message that starts the dials directly, honouring
  `MaxConnsPerHost` and dial backoff. `h1Pool` was already correct and is
  unchanged — its hold-then-release fix does not port, because it checks
  connections out exclusively and these do not. Behaviour now matches what
  `docs/CLIENT_GUIDE.md` has always documented.

- **A PUSH_PROMISE could be announced to a request that never asked for it**
  (#377, remaining scope). `handlePushPromiseBlock` resolved the parent stream
  by id and then reserved the promised stream, took a header slab from the pool
  and copied every field before delivering. A `*Stream` is pooled, so in that
  window the application can finish the parent request, `Close` it, and the next
  `NewStream` can claim the struct — and the ungated `push` then handed the
  promise to whichever request owns it now. Delivery is now gated on the parent
  still carrying the id it was looked up under (`pushIfID`, the delivery-path
  counterpart of `endWithReset`'s gate), and a promise that cannot be announced
  refuses the promised stream rather than leaving it reserved and unreachable
  (RFC 9113 §8.4.2 sanctions REFUSED_STREAM for declining a promise).

- **HTTP/1.1 now serves `DoStream` and `Do(BodyMode=BodyStream)`** (#322).
  Both returned `ErrStreamingUnsupported`, on the documented claim that
  `h1Exchange` "buffers whole responses and has no incremental path". That was
  never true of the code: `h1Exchange.Recv` has always read one chunk per call
  through `http1.Exchange.ReadBodyChunk`, handed it over in a pooled slab, and
  marked the last one `EndStream` — the same `Recv` -> `conn.StreamEvent`
  surface HTTP/2 and HTTP/3 present. Only `beginRespStream`'s dispatch rejected
  it. So this is real incremental streaming, not the buffered stand-in the issue
  asked for. Connection release is unchanged: `h1Exchange` owns it behind a
  `sync.Once`, and the release the streaming caller holds is the no-op the H1
  transports already returned.

- **A stream error delivered its reset without a stream-id gate** (#377),
  so a pooled `*Stream` recycled between the reader's lookup and its delivery
  handed the dead lifetime's `EventReset` to the NEXT request — a reset that
  request was never sent. The delivery now goes through `endWithReset`, which
  re-checks the id under `s.mu`, the same gate the peer-RST and GOAWAY teardown
  paths already had. Two latent defects go with it: the old path left a cleanly
  reset stream looking open, so the application's later `Close` sent a **second**
  RST carrying CANCEL instead of the error that actually killed the stream; and
  a writer parked on flow-control credit slept through the reset, because
  `acquireSendCredits` re-checks `s.closed` only on a condition-variable wake
  and nothing on this path broadcast one.

- **Every QUIC connection carried a 64 KiB receive buffer on platforms that**
  **cannot coalesce** (found while working #348). `pollBufLen` sized the buffer
  on whether the transport implements `ReadGRO`, and http3's `udpConn`
  implements it unconditionally so the tree builds everywhere — but off Linux
  `RecvGRO` is a plain single-datagram `Read`. Windows, macOS and the BSDs
  therefore got **32x the memory** (64 KiB instead of 2 KiB) per connection for
  a coalescing that cannot happen; on a pooled load generator that is ~2 MB
  versus ~64 MB at a thousand connections. Sizing is now gated on a
  platform constant as well as the interface.
- **`udpConn.WriteGSO` re-resolved the raw file descriptor on every send**,
  calling `SyscallConn()` per datagram burst where the read side had cached it
  since it was written.

- **Concurrent `Read` and `Close` on `Response.BodyReader` raced, and the**
  **abort did not work** (#392). `Close` returned the pooled DATA slab while an
  in-flight `Read` still aliased it through its surplus buffer, so a later
  request could be handed the same slab and overwrite bytes the caller was
  about to copy out — reproduced under `-race`. `closeOnce` guarded
  Close-against-Close only. Separately, `Close` did not wake a `Read` parked in
  `Recv`: the abort returned promptly and the reader goroutine hung until the
  caller's own deadline fired. Both matter because `BodyReader` is an
  `io.ReadCloser`, and closing one from another goroutine to abort a slow read
  is exactly what that interface's convention invites. The reader's fields are
  now serialised, and `Close` cancels a private context that releases the
  parked read.

- **The ACK tracker re-sorted its whole range set on every received packet**
  (rest of #345). `receive` appended the new packet number and called
  `sort.Slice`, whose reflect-based swapper allocates — **88 B/op and 3 allocs
  on every packet the peer sent** — to re-sort a slice that was already sorted
  apart from the one element just appended. Because the ranges are kept
  disjoint and non-adjacent, a single packet number can touch at most the range
  directly above it and the one directly below, so it is now an ordered insert
  with four cases. **0 B/op, 0 allocs**; 860 -> 21.6 ns in the permanently
  gapped worst case and 71.7 -> 4.8 ns for contiguous arrivals. Verified
  against the previous implementation kept verbatim as a differential oracle.

- **The QUIC packet-crypto path allocated on every packet** (part of #345).
  The AEAD nonce and the header-protection mask were built on the stack and
  returned by value, then handed to interface calls (`cipher.AEAD.Seal/Open`,
  `headerProtector.headerMask`), so both escaped: **32 B and 2 allocs per
  packet sealed**, plus the mask again per packet opened. Both now write
  through per-Sealer/Opener scratch. The per-packet frame handler escaped the
  same way through `ParseFrames`' interface parameter and is now a reset-per-
  packet field on `Conn`. Seal and header-open are both **0 B/op, 0 allocs**.
  `quic` is inside the enforced zero-alloc bench-gate, but the package had no
  packet-crypto benchmark at all — only send-path ones that self-skip — so the
  gate had nothing to fail on; `BenchmarkSealPacket` and `BenchmarkOpenPacket`
  close that. The ChaCha20 header protector still allocates a cipher per packet
  (no re-key API for a per-packet nonce); AES-GCM is the default suite.

- **A re-added address was blackholed forever under `DrainLazy`.**
  `subPoolState.draining` was set when the resolver dropped an address and
  never cleared anywhere. Under `DrainHard` and `DrainGraceful` the sub-pool
  leaves the registry so the stale flag goes with it, but `DrainLazy`'s
  `beginDrain` is a deliberate no-op — the entry stayed, and both
  `snapshotActive` and `getOrCreateSubPool` exclude a draining sub-pool. A
  resolver flap (remove then re-add, the ordinary DNS case) therefore removed
  the address permanently; with a single resolved address that is a permanent
  `ErrNoAddresses`. `applySet` now revives a returning address, and the drain
  watcher drops through `dropIfDraining`, which re-checks registration
  identity and the draining flag under the same lock as the delete so a revive
  cannot land in a check-then-act window and get a live address's connections
  torn down. Fixed in all three managed pools.

- **BREAKING: `hpack.HeaderField.Sensitive bool` is replaced by**
  **`Indexing hpack.IndexingMode`** (#332), adding RFC 7541 §6.2.2 literal
  without indexing, which the encoder could not emit. A caller previously had
  two choices: insert the field into the dynamic table, or mark it
  never-indexed (§6.2.3). A header whose value varies per request therefore
  evicted an entry it could never match again — in this client, `grpc-timeout`,
  whose value is the remaining deadline at 8-digit precision and so differs on
  essentially every RPC. Using `Sensitive` to avoid the insertion abuses a
  signal §7.1.3 reserves for values whose exposure to intermediaries matters.
  The three modes are now `IndexIncremental` (the zero value, previous
  default), `IndexWithout`, and `IndexNever` (previously `Sensitive: true`);
  `HeaderField.Sensitive()` remains as a method for reading. `grpc-timeout` is
  now sent with `IndexWithout`. The decoder already accepted incoming §6.2.2
  and now reports it as `IndexWithout` instead of losing the distinction.
  Migration: `Sensitive: true` becomes `Indexing: IndexNever`, and a
  `f.Sensitive` read becomes `f.Sensitive()`. Encoder stays 0 B/op.

- **The HTTP/1.1 request head was written one segment at a time** (#356).
  `WriteRequest` built the request line and every header field as a separate
  `net.Buffers` segment for a `writev`, but writev is void through TLS —
  crypto/tls has no vectored write, so `net.Buffers.WriteTo` falls back to one
  `tls.Conn.Write` per segment, each its own TLS record and syscall: seven for
  an ordinary head, where net/http sends one. The HTTP/2 stack solved this by
  wrapping its transport in a `bufio.Writer`; the HTTP/1.1 path had no
  equivalent. The head is now assembled into a reusable per-connection buffer
  and written once. Measured, seven-segment head: TLS **7 syscalls -> 1**,
  end-to-end HeadTLS 38.3 -> 6.1 us (-84%); plaintext stays at one syscall
  (writev today) and drops 21 -> 11 allocs by shedding the per-line buffers, so
  neither transport regresses. The single write is also atomic where
  net.Buffers could put a good prefix of a rejected request on the wire.

- **The HTTP/3 send path copied the whole request body to prepend a**
  **<=9-byte DATA frame header** (#347, part 1). `sendRequest` materialised the
  body into a fresh contiguous DATA buffer via `AppendData`, and that copy
  bought nothing: the QUIC layer takes its own retransmit copy of every chunk
  regardless, so the body was copied twice before the wire. Measured at
  ~`len(body)` B/op per request (12,296 -> 16 at an 11 KiB body). The DATA
  header now rides the request-owned HEADERS buffer and the body streams
  directly. Deliberately not the issue's two-write form — a lone 9-byte send
  would flush a GSO batch by itself, one extra datagram and syscall per
  request, the wrong trade on a stack whose ceiling is the syscall rate; riding
  the header on the HEADERS datagram keeps the datagram count unchanged. Wire
  bytes and FIN placement are byte-identical to before, pinned across body
  sizes from 0 to 256 KiB.

- **`frame.Framer.WritePing` allocated 8 B on every call**, on a path the
  inbound-PING echo reaches for every PING the peer sends (RFC 7540 §6.7). The
  `[8]byte` argument is a by-value array on the stack, and passing `data[:]`
  through `writeFrame`'s `io.Writer` forced it to the heap. It escaped the
  bench-gate's zero-allocation guarantee only because no PING benchmark was
  checked in. `WritePing` now copies into the Framer's heap-resident scratch
  first, as `WriteWindowUpdate` already does; a benchmark for it and for
  `WriteWindowUpdate` are added so the gate catches a regression.

- **Closing a stream did not cancel a send blocked on flow-control credit.**
  `acquireSendCredits` already bails with `ErrStreamClosed` when it sees the
  stream closed — but only on wake, and it is parked in `cond.Wait()`. That
  check and the broadcast that makes it observable were written together for
  the peer `RST_STREAM` path (RFC 9113 §6.4). `Stream.Close` sets the same flag
  and inherited neither, so a `SendData` blocked on an exhausted send window
  slept through the `Close` meant to abandon it and woke only when its own
  context expired — on the one API documented as how a client abandons a
  request early. With a long-lived request context that is a stuck goroutine.
  `Close` now wakes send waiters, as the peer-reset path does.

- **Recycling a stream allocated a fresh event channel every time, in the**
  **path whose purpose is to avoid allocating** (#341). At gRPC's default of
  272 slots that is ~24 KiB per RPC — the largest single allocation the gRPC
  client made — and the plain HTTP/2 path paid 1.1 KiB per request for the
  same thing. Measured end to end on a unary call: **35.4 KB/op down to 9.2
  KB/op**, 107 allocations down to 101.

  The replacement was justified as orphaning "a stale reference held by a
  goroutine from the previous stream lifetime". That reasoning does not hold:
  no writer in this package captures the channel — `push`, `deliverEnd`,
  `endWithReset` and `shutdownStreams` all read the field at send time, so a
  late writer lands in the new channel whether it was replaced or not. The
  orphaning prevented nothing.

  It was load-bearing for something else, undocumented: `shutdownStreams`
  closes the channel when the connection reader dies, and a closed channel
  survives the drain, so a struct pooled without repair hands the next request
  a stream whose first `Recv` reports `ErrStreamClosed` before anything is
  sent and whose first delivery panics the reader goroutine. The channel is
  now replaced only when that actually happened, recorded at the close. The
  two signal channels follow the same rule, using the flags that already guard
  their closes.

- **Pool evictions observed through `Stats()` were never counted, and a**
  **peer GOAWAY could be recorded as local idleness** (#359). `evictDeadSilent`
  closed a connection without incrementing `ConnsClosed`: silent was meant to
  mean "no user callback", and had come to mean "no record" as well. `Stats()`
  is reachable from the public `Client.PoolStats()`, and scraping is exactly
  what causes such an eviction to be noticed there — so a connection killed out
  of band and first seen by a metrics read was closed with the counter staying
  at zero forever. It is counted now; the hook stays suppressed, because firing
  a lifecycle callback from inside a metrics read is a different thing and
  remains wrong. Applies to the HTTP/2 and HTTP/1.1 pools; HTTP/3 already
  counted.

  The HTTP/2 maintenance tick also ran idle eviction before dead eviction, so a
  connection the peer had GOAWAY'd — which is very often also idle, having
  stopped taking new streams — was reaped as `CloseIdle` and never incremented
  `GoAwaysReceived`. A rolling restart looked like ordinary idling. Dead now
  sweeps first, which for HTTP/2 is a pair of atomic flag reads and costs
  nothing. The HTTP/1.1 pool deliberately keeps the opposite order: its dead
  sweep runs a bounded socket probe per connection on the actor goroutine, so
  probing connections the idle sweep would have discarded for free would stall
  every acquire and release — and HTTP/1.1 has no GOAWAY to attribute.

- **A request queued on the HTTP/2 pool could wait forever after the pool**
  **lost its last connection** (#359). `serveWaiters` can only hand out
  capacity that exists, and eviction routinely removes the last conn — a peer
  GOAWAY and its drain is the ordinary trigger. The dial decision lived only in
  `handleAcquire`, so `{no live conns, no in-flight dials, queued waiters}` was
  terminal: release, dial-done and tick all passed through it without acting.
  Measured on the default options, that state survived ~30 health-check ticks
  against a server that was still dialable, and the waiter was served only when
  an unrelated request happened to arrive — so a pool whose every worker is
  parked deadlocks rather than running slowly. The H1 and H3 pools have had the
  equivalent rescue since they shipped.

  A failed dial now also refuses **every** queued waiter once the pool is in
  the state that makes a new request fast-refuse, instead of only the first.
  Leaving the rest queued was a priority inversion: measured, a fresh acquire
  was refused with `ErrDialBackoff` in 0 ms while two earlier waiters stayed
  parked past the end of the backoff window and left only at `Close`.

  The tick path serves waiters before considering a dial, which is HTTP/2
  specific: per-conn capacity is dynamic here, so a peer that raised
  SETTINGS_MAX_CONCURRENT_STREAMS can make existing connections able to serve
  waiters that had nothing to wait for a moment ago. Dialing first would open a
  connection the pool did not need. The rescue is deliberately absent from the
  stats path: it only removes conns that were already not live, so it cannot
  create the terminal state, and a metrics scrape must not open connections.

- **A chunked response could be destroyed by the frame that carries no**
  **bytes** (#344). conn sheds a stream whose event channel overflows: it
  resets and hands the caller an `EventReset`. For a flushing server whose
  chunks exactly fill the channel, the event that overflows is the terminal
  zero-length DATA frame — every byte of the body already delivered, and the
  response lost to a marker. A terminal `EventData` with no payload is now
  reported out of band instead, the clean-completion sibling of the existing
  reset signal. A trailer block also ends the stream but carries fields the
  caller must receive, so it is deliberately not covered by that.

  `client` also sizes the per-stream event channel now, instead of inheriting
  conn's floor of 8 — `grpc` has had a computed default since it shipped and
  the client never got the equivalent. It is a byte budget divided by the
  advertised frame size (1 MiB, clamped to 16..64 slots, so 64 by default),
  not a flat count: every queued DATA event pins a pooled buffer of up to one
  frame, so a caller who raised `MaxFrameSize` for throughput would otherwise
  multiply what one stream can retain without asking. The reporter measured
  0/11,944 failures at 64.

  Neither change makes shedding impossible, and no finite size could: a
  consumer that falls more than a channel behind a flushing server still loses
  the stream, and nothing distinguishes "momentarily descheduled" from
  "genuinely slower than the peer". Only refunding flow-control window on
  consumption rather than on receipt would, which is a much larger change —
  conn refunds at receipt, so HTTP/2 backpressure never throttles a peer to
  its consumer's pace and this channel is the only bound there is.

- **`Stream.Recv` no longer registers on a stream that has been Closed**
  (#354). The reader registration that keeps a recycle off the struct protects
  a goroutine parked INSIDE `Recv`. It says nothing about the gap between two
  calls, which is the ordinary shape of a read loop — client's response-body
  reader issues one `Recv` per `Read` and loops outside. A `Close` landing in
  that gap pools the struct, and the next call registered on it: a reader from
  a finished request inflated the `recvActive` of whatever request claimed the
  struct next and deferred that request's recycle behind itself. It also parked
  on the orphaned channel until its own context expired, turning a finished
  stream into a full context's worth of waiting. `Recv` now returns
  `ErrStreamClosed` at once.

  This narrows the window rather than closing it, and the limit is documented
  in place with a test that asserts the current behaviour. If the struct is
  re-allocated before a stale reference re-enters `Recv`, `allocStream` re-arms
  the flag and the guard admits it — measured, it then receives the next
  request's events. Closing that needs the caller to present the lifetime it
  believes it holds, which `Recv` cannot infer from a receiver that is the
  struct itself, and the send side has the same hole. "Callers must not retain
  a `*Stream` past `Close`" remains a real obligation.

- **A non-200 gRPC response carrying `grpc-status: 0` was reported as a
  successful call** (#352). `grpc/stream.go` prefers a `grpc-status` found in a
  non-200 header block over the HTTP-status mapping table, deliberately: the
  table is defined for use "only for clients that received a response that did
  not include grpc-status. If grpc-status was provided, it must be used", and
  grpc-java's HTTP-error path puts one there. But that rule settles whose
  diagnosis wins, not whether a diagnosis may contradict the transport it
  arrived on. The gRPC protocol fixes `:status 200` for every conforming
  response, so a non-200 carrying OK is impossible by construction — and
  honouring it reported a call the client had already classified as failed as a
  success. On that path the body has already been dropped, so what a
  `NewStream` caller received was `io.EOF` with zero messages: an empty success
  it cannot tell from a real one, on a value the peer chose. `Invoke` was
  insulated only by accident, through its no-message check.

  The Trailers-Only shape had the same hole by a different route — it reaches
  `finish` before the non-200 check runs at all — and `"00"` parses to OK as
  surely as `"0"`, so the guard is on the parsed code rather than the text. A
  non-OK `grpc-status` on a non-200 still wins over the table, and a 200
  carrying OK is untouched; both are pinned as over-rejection guards.

- **A complete response could be discarded in favour of a reset that arrived
  after it** (#350). `Stream.Recv` selected over the event channel, the reset
  signal and the context with no priority. `signalReset` is reachable only from
  the full-channel fallback — every caller tries a send into `s.events` first —
  so a ready reset signal means, by construction, that undelivered events are
  queued behind it. Go picks uniformly among ready cases, which made each
  `Recv` an independent coin flip and an N-event response reach its consumer
  with probability 2^-N: measured at 49.5% for a single `Recv` and ~99.6% loss
  over a full drain loop. For a server that answers in full and then sends
  `RST_STREAM(NO_ERROR)` to stop an upload it does not need, that is the very
  discard RFC 9113 §8.1 forbids — the clause `conn` already pins on the ordered
  path. The reset case now delivers anything already buffered first. Draining
  before the select would fix the same thing and cost more: `ctx.Done()` would
  then lose to a channel that never empties during a fast download.

  The guard on `close(resetSignal)` was a CAS on `resetCode` from 0 to the
  code, which is not idempotent when the code is itself 0 — it succeeds, leaves
  the field at 0, and admits the next caller, closing the channel twice and
  panicking the reader. NO_ERROR is 0, and NO_ERROR is exactly what §8.1 has a
  server send after a complete response, so the one value that broke the
  contract was the common one. The guard is now its own flag.

- **A complete HTTP/2 response was discarded when the server ended the upload
  with `RST_STREAM(NO_ERROR)`** (#337). RFC 9113 §8.1 lets a server that has
  already written a complete response tell the client to stop sending the
  request body, "by sending a RST_STREAM with an error code of NO_ERROR after
  sending a complete response" — and it is a hard requirement that "Clients MUST
  NOT discard responses as a result of receiving such a RST_STREAM". Both
  `net/http2` and `grpc-go` do this for any handler that does not drain the
  body, which is the common case for a unary RPC or an ignored POST payload.
  `conn` closes the stream on that reset (correctly, per §5.1) and enqueues the
  reset event *after* the intact response — it has pinned the clause since
  `TestConformance_RFC9113_Sec8_1_CompleteResponseNotDiscardedByTrailingRSTNoError`.
  The two layers above threw the response away anyway: `grpc.ClientConn.Invoke`
  returned on the first `Send`/`CloseSend` error and `client.sendRequest`
  returned on the first body-write error, so both surfaced
  `conn: stream already closed` on a request the server had answered, with the
  response sitting unread in the stream's event buffer. Reproduced at ~9% under
  CPU load; it had already failed CI three times, each time in a different test.
  A send-side failure is no longer decisive for the outcome: when the upload
  fails with `conn.ErrStreamClosed` both paths now consult the receive side.
  This is what `http3.Client` already did with the QUIC equivalent
  (`STOP_SENDING` → `quic.ErrStreamReset`), so the HTTP/2 stack was the odd one
  out. `Send` stays strict — a message that never reached the peer is still an
  error.

  The guarantee is bounded by what the stream's event buffer can hold while the
  upload is parked and nobody is draining it: a response that overruns
  `ConnOptions.StreamEventBuffer` is still lost, because `conn` resets its own
  stream to shed it. That failure is reported as `conn.ErrStreamClosed` rather
  than as the synthesised `RST_STREAM(REFUSED_STREAM)` `conn` uses internally —
  deliberately, since the retry layer reads REFUSED_STREAM as "the server did
  not process this request" and would otherwise replay a request the server had
  already answered (measured at 3 executions of one `DELETE` instead of 1).

- **A stream could be recycled out from under a goroutine sitting in
  `Stream.Recv`.** `conn` pools `*Stream` and settles who returns it with a
  two-party handshake: `Close` marks the application side done, the reader
  goroutine's `markStreamDone` marks the connection side done, and whichever
  finishes second calls `recycleStream` — which rewrites every field, including
  `s.events` and `s.resetSignal`, and hands the struct back to the pool. The
  premise is that once both parties are done nobody holds a reference. A third
  party does: the application's own reader. `Recv` blocks on those two channels,
  so it must read the fields outside the mutex, and a `Close` racing an
  in-flight `Recv` is not a caller keeping a stream past `Close` — it is one
  goroutine cancelling another's read, which is ordinary. CI reported it as a
  `DATA RACE` at `conn/stream.go:243`/`:268` against `:482`/`:487`; #329 fixed
  *which* party recycles, not this.

  The reader is now a participant too: `Recv` registers under the stream mutex,
  `recycleStream` defers when a reader is registered, and the last reader out
  performs the deferred recycle. The field reset moved under the mutex and
  `pool.Put` now happens only after unlocking, so the struct's next owner can
  never find its mutex held by the previous one. A reader that blocks
  indefinitely postpones pooling, which costs a pool hit rather than
  correctness. `TestStream_RecycleUnderInFlightRecv_NoRace` reproduces it
  deterministically — it parks a reader in the select and then drives the
  recycle, where the existing stress test could only hope to hit the window —
  and asserts both halves: that the recycle is *deferred* while the reader is
  inside, and that it is not *lost* when the reader leaves.

  `recycleStream` no longer takes the destination pool as a parameter. The
  deferred path cannot honour one — it runs long after the caller that wanted
  the recycle is gone — so a parameter would have been obeyed on one path and
  silently ignored on the other, losing the recycle outright for any writer
  that is not the `*Conn` the stream belongs to. Both call sites passed exactly
  that `*Conn`'s pool, so reading it from `s.w` loses nothing.

  Separately, `TestStream_CloseDuringTerminalDelivery_NoRace` shared one 60 s
  context across all 60 iterations, so it parked on the first stream its own
  `Close` had cancelled and stayed there until the deadline: the test cost a
  full minute and 59 of its 60 iterations asserted nothing. Close and drain now
  race each other off the main goroutine and the drain is released as soon as
  `Close` returns — all 60 iterations run, in about three seconds.

- **HTTP/1.1 over TLS silently negotiated HTTP/2 and failed every request**
  (#334). `conn.TLSDialer` rewrote a caller's explicit `Config.NextProtos`,
  prepending `"h2"` — so the natural-looking `TLSDialer{Config: &tls.Config{
  NextProtos: []string{"http/1.1"}}}` on `TransportH1Pool` offered
  `["h2","http/1.1"]`, the server picked h2, and the dial *succeeded* because the
  dialer's own assertion is h2. The HTTP/1.1 codec then wrote request lines into
  a connection the peer framed as HTTP/2: every exchange failed with
  `http1: read status line: EOF` on the client and `bogus greeting` on the
  server, neither message mentioning ALPN. In a benchmark harness this failed
  100% of HTTP/1.1 requests while still producing plausible-looking allocation
  and CPU numbers, because the driver was faithfully measuring the cost of
  failing. Three changes, each closing the gap at a different depth:

  - `TLSDialer.Dial` (and `ProxyTLSDialer.Dial`) now returns
    `conn.ErrALPNConflict` — before any network I/O — when `Config.NextProtos` is
    non-empty and excludes `"h2"`, instead of overriding it. The empty-list case
    still defaults to `["h2"]`, so no working configuration changes behaviour.
  - `client.NewClient` rejects a dialer whose `AssertsALPN` the transport cannot
    use — an H1 transport with `TLSDialer`, an H2 transport with `H1TLSDialer` —
    with the new `client.ErrALPNProtocolMismatch`.
  - The HTTP/1.1 transports (`TransportH1SingleConn`, `TransportH1Pool`,
    `TransportH1Managed`) re-check the negotiated protocol on every dial and
    refuse a connection that came back as anything other than `http/1.1`,
    reporting it as a dial failure with the same typed error. This is the
    backstop for a custom dialer that makes no assertion for `NewClient` to
    check.

  `README.md`, `examples/http1` and `docs/CLIENT_GUIDE.md` carried the broken
  `TLSDialer` + `NextProtos: ["http/1.1"]` recipe and now use `H1TLSDialer`.

- **HTTP/1.1 allocated 16 KiB of scratch memory per request** (#331). `h1Exchange`
  carried an inline 16 KiB array for `ReadBodyChunk`, and every H1 transport
  heap-allocates one exchange per request — so the buffer meant to avoid a small
  per-`Recv` allocation was paid, in full, per request instead. At 200 RPS it was
  69.5% of all bytes the client allocated. `Recv` now reads straight into a
  buffer from `conn`'s shared DATA-payload pool and hands ownership to the caller
  via `StreamEvent.DataSlab`, the contract HTTP/2 has always used; that also
  removes the per-chunk `make([]byte, n)` copy `Recv` made out of the scratch
  array (a further 15% of allocated bytes). Measured client-side on a 2 KiB
  response, against a peer that allocates nothing per request:
  **23.1 KB → 2.6 KB allocated per request** (`-race`: 23.4 KB → 7.2 KB), with
  the allocation count unchanged. Affects `TransportH1SingleConn`,
  `TransportH1Pool` and `TransportH1Managed`; no API change.

- **HTTP/1.1 spawned a context watchdog per blocking I/O call** (follow-up to
  #331). `armCancel` and `armDeadline` each allocated two channels, two closures
  and a goroutine, and one request/response arms them several times over — the
  head write, each body write, the response read and every body chunk. They were
  the two remaining H1 sites in the reported heap profile (8 MB + 4 MB). The
  watchdog is now one goroutine per `http1.Conn`, started lazily on the first
  cancellable context and re-armed around each call under the serialization
  `Conn` already documents; arming allocates nothing. Both invariants the
  per-call form documented are preserved: the release still blocks until the
  watchdog can no longer install a deadline (an unbuffered rendezvous in place
  of `<-exited`), so a cancellation racing the return cannot latch a past
  deadline on a connection about to be pooled; and a context carrying BOTH a
  deadline and a cancel — what `client.Do` builds from `Request.Timeout` — still
  arms. `Conn.Close` retires the goroutine. Measured on the same 2 KiB response:
  **4.2 KB → 3.2 KB allocated per request, 66 → 51 allocations**. No API change.

  Those two figures, and the 4.3 KB one first published for the entry above, were
  taken with an in-process `net/http` peer inside the measurement — `AllocedBytesPerOp`
  is a process-wide counter, so the server's own per-request allocations were being
  attributed to the client. Re-measured against a peer that allocates nothing per
  request, the two changes together take HTTP/1.1 from **23.1 KB to 1.6 KB per
  request (45 → 29 allocations)**; the figures quoted per-entry above are each
  correct for the harness in use when that entry was written.

## [v0.11.0] — 2026-07-31

### Added

- **`grpc` — a gRPC-over-HTTP/2 client.** Note this is not a `client.Transport`:
  the package has its own entry point and does not appear under `client.Do`.
  `grpc.Dial` returns
  a `ClientConn` that multiplexes calls over one HTTP/2 connection, the same way
  gRPC itself does, so there is no pool to configure. All four call shapes work:
  unary (`Invoke`), server-streaming, client-streaming, and bidirectional — the
  send and receive halves of a `Stream` are independently usable from two
  goroutines. Covers length-prefixed message framing, `grpc-status` /
  `grpc-message` trailers, the Trailers-Only response shape, the
  HTTP-status-to-gRPC-code and RST_STREAM-to-gRPC-code mapping tables,
  `grpc-timeout` from the context deadline, and `-bin` binary metadata.
  No protobuf dependency: messages cross the API as `[]byte`. Credentials
  (`authorization`, `proxy-authorization`, `cookie`) are marked sensitive so they
  never enter the connection's HPACK dynamic table. Documented in
  [docs/GRPC_GUIDE.md](docs/GRPC_GUIDE.md), with `examples/grpc`.

  The package sits on `conn`, not on `client`: `client.DoStream` writes the
  whole request body before it returns, so client-streaming and bidirectional
  calls cannot be expressed through it.

  No pooling, resolver, retry, hooks or metrics yet — a `ClientConn` is one
  connection, and everything above it is the caller's. HTTP/2 only.

### Fixed

- A pre-existing data race in `conn` stream recycling: a teardown that made a
  stream both-ended while the reader still touched it could let a concurrent
  `Close` recycle the stream under the reader. Terminal-event delivery and
  stream end now happen atomically under the stream mutex (#329).

## [v1.0.0] — 2026-07-15

The first stable release: HTTP/1.1, HTTP/2, and HTTP/3 through one client API.
Every protocol stack — QUIC, HTTP/3, QPACK, HTTP/2 framing, HPACK, HTTP/1.1 — is
written from scratch in this module: no third-party protocol code, no cgo.

### Added

- **HTTP/3 as a first-class transport in `client.Client`.** The same `Do` /
  `DoStream` API drives HTTP/3 over QUIC: transports `TransportH3`,
  `TransportH3Pool`, `TransportH3Managed`, and the constructors `NewH3Client`,
  `NewH3PoolClient`, `NewManagedH3Client`. Concurrent in-flight requests over one
  QUIC connection, connection pooling, service discovery, streaming, retries,
  hooks, and metrics now work over HTTP/3, not only HTTP/2.
- **HTTP/1.1 parity** — `TransportH1Pool` and `TransportH1Managed` join
  `TransportH1SingleConn`, with `NewH1Client`, `NewH1PoolClient` and
  `NewManagedH1Client`. HTTP/1.1 has no multiplexing, so the pool is an
  exclusive-checkout pool: one exchange per connection, `MaxConnsPerHost` *is* the
  request concurrency (`MaxStreamsPerConn` does not apply), and a request waits
  for a free connection at the cap. Before this, HTTP/1.1 load could not be
  generated at all — a single connection meant strictly serial requests (#219).
- **Brotli and zstd response decoding** — the client now decodes `gzip`,
  `deflate`, `br` and `zstd`, and advertises all four in `accept-encoding` (it
  previously advertised only `gzip` while already decoding `deflate`). Pooled
  readers, a decompression-bomb guard, and an 8 MiB zstd window cap (#223).
- **Request-body compression** — `Request.CompressBody` compresses the body with
  any of the four codings and sets `content-encoding` itself. The zero value sends
  the body unchanged at zero allocations. A manually-set `content-encoding` still
  means "already encoded" (RFC 9110 §8.4) and is left alone; setting both returns
  `ErrConflictingContentEncoding` (#225).
- **QUIC server role** — `quic.Listen` / `Listener.Accept` and
  `Conn.AcceptBidiStream` / `AcceptUniStream` expose a server-role QUIC endpoint
  that reuses the client's connection, crypto and stream machinery. It is a QUIC
  server role, not an HTTP/3 server. Its immediate value is giving the client a
  genuine peer to be tested against (#205, #207, #217–#222).
- **Dynamic QPACK (RFC 9204), both directions** — encode and decode with the
  dynamic table, encoder/decoder instruction streams, known-received-count, and
  blocked-streams handling (#193–#196, #202).
- **ChaCha20-Poly1305 packet protection (RFC 9001 §5.3 / §5.4.4)** — handshakes
  complete against ChaCha20-only servers; verified byte-for-byte against the
  RFC 9001 §A.5 test vector and on the wire (#204).
- **BBR congestion control (opt-in)** via
  `ClientOptions.H3ConnOptions = []quic.ConnOption{quic.WithCongestionControl(quic.CCBBR)}`;
  NewReno stays the default (#203, #206).
- **Batched UDP I/O on Linux** — GSO send, GRO receive, and bounded ACK deferral
  (#199–#201).
- A documentation site (Hugo → GitHub Pages) in English, 中文, Español, Русский,
  and 日本語, plus runnable `examples/http1`, `examples/http2`, `examples/http3`.

### Changed

- **Minimum Go version is now 1.25** — a supported release that receives security
  backports.
- **Four direct dependencies**, all crypto or compression primitives taken
  deliberately rather than hand-rolled: `golang.org/x/net`, `golang.org/x/crypto`
  (ChaCha20-Poly1305), `github.com/andybalholm/brotli`, and
  `github.com/klauspost/compress` (zstd). Every protocol stack remains written
  from scratch here — still no `quic-go`, `nghttp2`, `net/http`, or cgo. Rolling
  our own Poly1305 or Brotli would be a security liability with no upside.
- `Content-Encoding` matching is now case-insensitive (RFC 9110 §8.4.1). A
  response labelled `Content-Encoding: GZIP` was previously handed back still
  compressed.
- README rewritten; per-protocol guides and an accurate support matrix replace the
  earlier single-request/HTTP-2-only description.
- **Removed `frame.Framer.SetMaxHeaderListSize`.** It was exported and documented
  as the read-side cap on a header block, but nothing ever read the value. The
  cap that does the work is `hpack.Decoder.SetMaxHeaderListSize`, wired from
  `conn`, plus the handler's compressed-byte ceiling.

### Fixed

- **A small advertised receive window deadlocked every HTTP/2 download.**
  `conn.AdvertisedSettings.InitialWindowSize` is public and RFC 7540 §6.5.2
  permits 0 to 2^31-1, but the WINDOW_UPDATE that replenishes a stream's window
  only fired once 32768 bytes had accumulated — a constant chosen as half of the
  *default* 65535 window. Advertise less than that and the peer spent its whole
  window, the refund counter never reached a threshold larger than the window
  feeding it, and the transfer stopped dead: at 16384 (the default
  MAX_FRAME_SIZE, a natural choice) a 200 KiB download delivered exactly 16384
  bytes and then hung. The threshold is now half the advertised window, which is
  what the constant was always meant to express.
- **An HTTP/2 pool never recovered from a server restart.** `conn.IsAlive`
  reported a connection healthy after its peer had vanished — a crash, a
  restart, an RST — because it asked only whether `Close` had been called or a
  GOAWAY received. The reader had already noticed and exited; nothing turned
  that into death, since `readerLoop` returns without closing the connection and
  the only listener on its exit is a keepalive loop that does not run unless
  `KeepaliveInterval` is set (it is zero by default). So the pool kept handing
  out the corpse: measured with a 50ms health check, every request after a
  restart failed on the same socket pair, forever — the health check asks the
  same question, and the resulting `context.DeadlineExceeded` is not retryable.
  `IsAlive` now treats the reader's exit as the connection's death, which is
  what `http3.Client.Alive` already did. HTTP/1.1 was never affected: its pool
  evicts on the exchange error rather than on a liveness probe. Graceful GOAWAY
  worked throughout, which is why this survived — the tested way of losing a
  peer was the one that worked.
- **A GOAWAY'd connection was evicted while its streams were still draining.**
  `IsAlive` goes false the moment a GOAWAY arrives, but RFC 7540 §6.8 requires
  streams at or below the GOAWAY's last-stream-id to be allowed to finish. Three
  pool sites — the health tick (`evictDead`), the `Client.PoolStats` path
  (`evictDeadSilent`), and release — closed the connection anyway, turning a
  graceful server drain into `RST(INTERNAL_ERROR)` on every in-flight request
  (measured: 4 of 4 died). The eviction now waits for the last stream, matching
  the `active == 0` guard the idle path already had; the same guard makes
  `GoAwaysReceived` count connections rather than draining streams.
- **Every closed QUIC connection leaked a goroutine.** `crypto/tls` runs a
  `QUICConn`'s handshake on its own goroutine, which exits only when the
  handshake finishes or `Close` cancels it — so a connection torn down before
  that, including any client connection, left one behind. Teardown now closes the
  handshake on every terminal path. The fix shipped in v0.10.0 alongside the
  server-role work but went unrecorded here; it affects clients, not only
  servers.
- **A race in the HTTP/2 and HTTP/3 connection pools leaked stream slots.**
  `replyAcquire` selected between delivering its reply and the caller's cancelled
  context; because the reply channel is buffered, both cases could be ready at
  once and Go chose at random. When the send won, the response — carrying a
  connection whose in-flight count had already been incremented — was stranded in
  a channel nobody read, so the slot was never returned. The trigger was an
  ordinary per-request timeout, and the leak was monotonic: a pool would starve
  permanently. Found while building the HTTP/1.1 pool, whose cap of one made it
  fire in half of all runs instead of hiding behind large caps. Waiter pruning
  also dropped queued requests without replying. Both fixed, with regression tests
  that fail on the previous code (#220).

### Security

- **CI security scanning** — `govulncheck` (Go vulnerability database, reachable
  symbols) and CodeQL, plus Dependabot for Go modules and GitHub Actions.
- **Suite-aware AEAD usage limits (RFC 9001 §6.6)** — ChaCha20-Poly1305 uses its
  mandated 2^36 integrity limit rather than AES-GCM's 2^52.
- **Bounded HTTP/1.1 response reads.** A peer that never sends a line terminator
  could make the client read until it ran out of memory. Status lines, header
  lines, and chunk-size lines are now read within the existing 16 KiB connection
  buffer; the header block is capped at 8 MiB (the same limit HTTP/2 advertises
  as `MaxHeaderListSize`), and interim 1xx responses and trailers are bounded.
  Over-limit responses fail with the new `http1.ErrResponseTooLarge`, mirroring
  `http3.ErrResponseTooLarge`. Header names are lowercased as ASCII per
  RFC 7230 §3.2.6: `strings.ToLower` re-encodes each invalid byte as U+FFFD,
  which both corrupted the name and let a peer retain three bytes per byte sent.
- **Server push no longer corrupts the request-stream gate.** Registering a
  pushed stream never took a `MAX_CONCURRENT_STREAMS` slot, but closing one
  released a slot — so every pushed stream that closed freed a slot belonging to
  a real request. Measured: three concurrent request streams against a peer gate
  of two, and `Shutdown` returning immediately with a request still open. The two
  limits are directional and independent (RFC 7540 §5.1.2 / §6.5.2), so they are
  now two counters, each releasing to the one that issued the slot. The push
  registry is bounded by the client's own advertised
  `SETTINGS_MAX_CONCURRENT_STREAMS` — which §6.5.2 already defines as the streams
  the peer may create — with over-cap promises refused per §6.6. A promised
  stream ID is now validated (even, non-zero, increasing; §5.1.1): previously a
  PUSH_PROMISE for ID 1 overwrote the client's own stream 1. Push is opt-in
  (`EnablePush`), off by default.
- **HTTP/2 understands 1xx informational responses.** A server sending
  `103 Early Hints` before its real response — Cloudflare, Fastly and Shopify all
  do — made the client report the 103 as the final answer: `Status` was 103, the
  real 200 header block arrived as *trailers*, and `Do` returned no error. On the
  `BodyStream` path the body came back empty, also with no error. The cause was
  RFC 7540 §8.1: trailers are a header block after a **final (non-informational)**
  status, and the code treated the first block as final whatever it held.
  Informational blocks now surface as `conn.EventInterimHeaders` and are capped at
  100 per stream (matching HTTP/1.1 and HTTP/3); `Client.Do` and `DoStream` report
  the final status, as they already did on the other two protocols. §8.1's other
  half is enforced too: a trailer block without END_STREAM is malformed
  (§8.1.2.6) and is now a stream error, which also bounds the trailer-block flood
  a peer could otherwise sustain.
- **QUIC streams release consumed bytes.** A receive stream's buffer was only
  ever appended to: reading advanced a cursor but never freed the bytes behind
  it, so a stream retained every byte it had ever received. Receive flow control
  did not bound this — its window is keyed on the read cursor, so it slides
  forward as the application consumes, bounding bytes *in flight* rather than
  bytes *retained*. `DoStream` therefore did not stream: an 8 MiB response
  retained 8 MiB after being fully consumed, 32x the 256 KiB window. Fixed by
  releasing the consumed prefix. Also ~3x faster on the receive path, since the
  growing buffer had been forcing `append` to recopy.
- **Bounded HPACK dynamic-table entry ring.** The HTTP/2 dynamic table grew its
  entry ring by one slot per header and never reused an evicted entry's slot, so
  a peer could drive the ring's size with its header *count* rather than the
  4096-octet table size that is supposed to bound it. Empty-name/empty-value
  literals cost three bytes on the wire and bought a slot each: 3 MB sent
  retained 16 MB, with no ceiling. The ring now grows only when every slot is
  live, matching QPACK. Compaction allocates a fresh ring rather than aliasing
  the old one — with a ring that can now wrap, the aliasing would have silently
  swapped one header's value for another's.
- **`AdvertisedSettings.HeaderTableSize` above 4096 no longer breaks the
  connection.** The value was wired into the HPACK encoder but not the decoder,
  which stayed at hpack's 4096 default — so a peer that used exactly the table
  size we offered was rejected with a decoding error (RFC 7541 §6.3). Go's
  HTTP/2 server never triggered it: its encoder clamps to its own 4096 limit and
  never announces a larger table.
- Added `SECURITY.md` with a private disclosure process.

### Tested

- **Soak / endurance** — over one million requests on one managed HTTP/3
  connection with no goroutine or heap growth. A second soak drives the *pooled*
  transport (471k requests plus 157k deliberately-abandoned acquires) — the
  single-connection soak never touched the pool's acquire/release path, which is
  how the slot leak above survived undetected.
- **Fuzzed wire parsers** for QUIC, HTTP/3, and QPACK (#198).
  connection with no goroutine or heap growth.
- **Fuzzed wire parsers** for QUIC, HTTP/3, QPACK (#198), and HTTP/1.1.
- HTTP/3 interop extended with a ChaCha20-only server and a 1 MiB BBR transfer,
  across Caddy, nginx, and aioquic.

### Removed

- Internal working documents (design specs, review notes, investigations) and
  stray build artifacts pruned from the repository.

## [v0.9.0] — 2026-07-11

### Added

- **HTTP/3 client — a from-scratch, zero-dependency QUIC + HTTP/3 stack.** A
  second protocol alongside the HTTP/2 client, implemented in pure Go from the
  RFCs with **no `quic-go`, no `net/http`, no `golang.org/x/net`** — the same
  zero-dependency, fine-grained-control philosophy as the HTTP/2 codec. Public
  entry point: `http3.Dial(ctx, addr, tlsConfig) → *Client`, then `Client.Do`.
  The layers, each conformance-tested (see
  [docs/RFC_COVERAGE.md](docs/RFC_COVERAGE.md); design in
  [docs/HTTP3_DESIGN.md](docs/HTTP3_DESIGN.md)):

  - **QUIC transport (RFC 9000)** — connection establishment, stream
    multiplexing, bidirectional flow control (`MAX_DATA` / `MAX_STREAM_DATA` /
    `MAX_STREAMS`), connection-ID issuance and rotation (`NEW_CONNECTION_ID` /
    `RETIRE_CONNECTION_ID`), ACK ranges, transport parameters, varint framing,
    `CONNECTION_CLOSE`, and Retry (`quic/`).
  - **Loss recovery + congestion control (RFC 9002)** — ACK-based loss
    detection, PTO / probe timeouts, RTT estimation, and a congestion controller
    with retransmission (`loss.go`, `cc.go`, `pto.go`, `rtt.go`).
  - **QUIC-TLS (RFC 9001)** — the TLS 1.3 handshake over CRYPTO streams, 1-RTT
    key derivation, key update (Key Phase 0→1), and AEAD packet + header
    protection (`crypto*.go`, `handshake.go`).
  - **QPACK (RFC 9204)** — a static-table encoder and decoder. The client
    advertises `SETTINGS_QPACK_MAX_TABLE_CAPACITY = 0` (static-only, no
    dynamic-table state to synchronise); a server that references the dynamic
    table anyway is rejected cleanly as `QPACK_DECOMPRESSION_FAILED` rather than
    mis-decoded (`qpack/`).
  - **HTTP/3 (RFC 9114)** — the control stream + `SETTINGS`, request/response
    mapping (`HEADERS` / `DATA` / trailers / 1xx interim responses), strict frame
    and message-order validation, typed HTTP/3 and QUIC error codes,
    `RESET_STREAM` / `STOP_SENDING` handling with retryable classification
    (`*StreamResetError.Retryable()`), and `GOAWAY` drain (`http3/`).

### Hardened

- **Receive-path resource bounds** (#162–166) — the decode path is bounded
  against an adversarial or buggy peer: bounded ACK / loss-tracking memory,
  capped stream and CRYPTO reassembly buffers, a `NEW_CONNECTION_ID`
  retire-flood bound, HTTP/3 response per-frame and cumulative size caps
  (`ErrResponseTooLarge`), and eviction of reset streams from the routing map.

### Tested

- **Conformance suites** for RFC 9000, 9001, 9002, 9114, and 9204, gate-tracked
  by `conformance-gate` (see [docs/RFC_COVERAGE.md](docs/RFC_COVERAGE.md)).
- **Whole-stack interop**, run in CI, against **three independent HTTP/3 server
  implementations** over real UDP — Caddy (`quic-go`, Go), nginx (its own C QUIC
  stack), and aioquic (pure Python) — plus a fault-injecting server (malformed
  frames, `RESET_STREAM`, `STOP_SENDING`, dynamic-QPACK) and a loss/reorder relay
  (`test/integration/http3`).

### Known limitations

- **ChaCha20-Poly1305 header protection is deferred** — AES-GCM cipher suites
  only (RFC 9001 §5.1). A ChaCha20-only server is a documented graceful failure.
- **QPACK is static-only** — no dynamic-table *encode*; the decoder honours a
  capacity of 0.
- **`Client.Do` is blocking and sequential** — the engine is single-goroutine
  and not safe for concurrent use, so the public API exposes no concurrent
  in-flight requests (QUIC multiplexing exists at the transport).
- A few rare RFC 9000 conformance items remain deferred (documented in `docs/`).

## [v0.8.0] — 2026-07-08

### Added

- **`ConnOptions.GroupCommit` — opt-in write batching (group-commit).** When a
  HEADERS writer finds another writer queued on the write lock, it defers its
  flush so the next holder batches both frames into a single `tls.Conn.Write` —
  fewer TLS record encrypts + socket writes under high per-connection stream
  concurrency. It is **synchronous** (the deferring writer waits for the convoy
  flush and returns its error, so per-frame write-error semantics are preserved
  — no async contract change), **adaptive** (a strict no-op with no contention;
  a lone request is never delayed), and **off by default**. Measured on one TLS
  h2 conn at 64 concurrent streams: 1.73 frames/flush, ≈−42% write syscalls,
  ≈+26% req/s; the win survives poseidon's dense-multiplexing pool (≈+30–35% at
  1–8 conns). Motivated by a review of ozontech/framer; see
  [docs/WRITE_BATCHING_INVESTIGATION.md](docs/WRITE_BATCHING_INVESTIGATION.md).
  Note: framer's `net.Buffers`/writev mechanism gives zero benefit through
  `crypto/tls.Conn`; plaintext frame coalescing is the only TLS-viable lever.

## [v0.7.1] — 2026-07-07

### Performance

- **Buffered connection writer** — the transport writer is now wrapped in a
  reused 16 KiB `bufio.Writer`, so a DATA frame's 9-byte header and its payload
  coalesce into a single flush = **1 write syscall per frame instead of 2**.
  A target-load CPU profile showed the client is syscall-bound (~50% CPU in the
  socket write); this halves the DATA-frame write syscalls (measured: a
  7-DATA-frame body dropped from ~16 write syscalls to 8) with no per-frame
  allocation. The buffer is flushed under `wmu` before every lock release and
  before every flow-control (`acquireSendCredits`) wait, so no buffered frame is
  ever stranded behind a WINDOW_UPDATE the peer hasn't been asked for.
  Regression guard: `TestWriteBuffer_MultiChunkUpload_NoDeadlock`.

## [v0.7.0] — 2026-06-23

### Changed

- **BREAKING:** `Request.WantBody bool` and `Request.StreamBody bool` are
  replaced by a single `Request.BodyMode` (`BodyMode` enum). The zero value
  `BodyDiscard` drops the body (same as the old `WantBody:false` /
  `StreamBody:false` default); `BodyBuffer` accumulates it into `Response.Body`
  (old `WantBody:true`); `BodyStream` returns `Response.BodyReader` for
  incremental reads (old `StreamBody:true`). This removes the invalid-but-
  representable `WantBody:true, StreamBody:true` state and the "StreamBody wins"
  precedence rule — matching the `IdempotencyMode` enum introduced in v0.6.0.

  Migration: `WantBody: true` → `BodyMode: client.BodyBuffer`;
  `StreamBody: true` → `BodyMode: client.BodyStream`; leave the field unset for
  the discard default. The `GET`/`POST`/`NewRequest` helpers set `BodyBuffer`
  for you, so code using those is unaffected.

### Docs

- Retired `docs/USAGE.md` (superseded by `docs/CLIENT_GUIDE.md`, the single
  canonical guide). It is now a redirect stub; README links to CLIENT_GUIDE.md.
  Keeping one guide avoids the drift that left USAGE.md with stale examples.

## [v0.6.0] — 2026-06-23

### Added

- **Opt-in ergonomic layer** (`client/sugar.go`): `client.H(name, value)`
  header helper (lowercases names per RFC 7540 §8.1.2) + a `client.HeaderField`
  alias so callers need not import `conn` for the common type; `NewRequest` /
  `GET` / `POST` constructors that default `WantBody=true` (the obvious path now
  captures the body instead of silently dropping it); `Request.WithHeaders`
  chaining; `Response.Header` / `HeaderString` (allocation-free reads);
  `Response.CopyBody` / `Response.Clone` / `StreamEvent.DataCopy` to safely
  retain bytes past the next `Recv`/`Reset`/`Close`; and
  `Client.Stream(ctx, req, fn)`, which pumps a streaming response and **always**
  `Close`s it so callers cannot leak the pooled stream slot.
- **Focused constructors** + functional options: `NewSingleConnClient`,
  `NewPoolClient`, `NewManagedClient` encode a valid transport + required-field
  combination in their signatures (invalid combinations are unrepresentable);
  options `WithHooks`, `WithPushHandler`, `WithDefaultScheme`, `WithRateLimit`,
  `WithMaxResponseBodySize`, `WithMaxDecompressedSize`, `WithDialBackoff`,
  `WithSelector`, `WithDrainMode`, `WithConnOptions`. `NewClient(ClientOptions{})`
  remains for full control.
- **`Client.Retryer(opts)`** — discoverable method form of `NewRetryer(c, opts)`.
- **Status-class metrics** — `Counters.Responses2xx` / `Counters.ResponsesNon2xx`
  split `RequestsSucceeded` by status class, so a load generator can measure
  real success rate rather than "a response arrived".
- **`EventNone`** — names the zero value of `EventType`.
- **Debug-gated leak detector** (`-tags poseidondebug`) — reports a
  `StreamResponse` or `Response.BodyReader` garbage-collected without `Close()`.
  Zero cost in normal builds; CI compile-checks the tag and `make test-debug`
  runs its finalizer tests.
- **`docs/CLIENT_GUIDE.md`** — full client usage guide with verified examples.
- **Examples** — 24 compile-tested godoc `Example` functions covering every
  feature (`client/example_*_test.go`, rendered on pkg.go.dev), plus a runnable
  `examples/loadgen` load generator (pooled client, rate limit, hooks, worker
  pool, metrics snapshot).

### Changed

- **BREAKING:** `Request.Idempotent *bool` → `Request.Idempotency`
  (`IdempotencyMode` enum). The zero value `IdempotencyAuto` classifies by HTTP
  method (same as the old `nil`); `ForceIdempotent` / `ForceNotIdempotent`
  override (same as the old `&true` / `&false`). Removes the
  addr-of-a-local-`*bool` dance.

### Performance

- **Zero-allocation receive hot path** — the per-DATA-frame copy buffer is now
  pooled (`conn.dataBufPool`); steady-state receive allocates nothing per frame.
  Ownership transfers to the client via `StreamEvent.DataSlab` and is returned
  on consume.
- **Buffered transport reader** — `NewClientConn` wraps the transport reader in
  a 16 KiB buffer, collapsing the per-frame 2× `ReadFull` into far fewer
  `read(2)` syscalls when frames arrive together (notably h2c). Strictly
  no-op-or-better; smaller win over TLS where the record layer already buffers.
- Realistic warm HPACK allocation benchmarks added; steady-state encode/decode
  is **0 alloc/op** even under Huffman-forcing custom-header traffic.

## [v0.5.1] — 2026-06-21

### Fixed

- **Pool reply-channel poisoning race** — the global `replyPool` recycled a
  buffered reply channel even when the pool actor could still send a late
  reply (caller abandoned via `ctx`/`closedCh` after the actor took the
  request). The recycled channel was then handed to a different `Pool`,
  which read the stale reply — surfacing as a spurious `ErrPoolClosed` or a
  cross-pool conn (`stream reset by peer`) under concurrent load. The reply
  channel is now recycled only when the actor can no longer send on it.
  Regression test: `TestPool_ReplyChannelNotPoisonedUnderAbandonment`.

- **HTTP/1.1 request trailers no longer corrupt the connection** — sending a
  request with `Trailers`/`TrailerFunc` over an H1.1 transport (explicit
  `TransportH1SingleConn` or an ALPN-negotiated H1.1 connection) previously
  re-invoked `WriteRequest`, emitting a second request line onto the live
  connection. The H1.1 transport now rejects such requests up front with the
  new `ErrTrailersUnsupportedH1` and discards the connection.
  Test: `TestNewClient_H1SingleConn_TrailersRejected`.

### Changed

- All packages now at ≥ 90% statement coverage (spec acceptance bar).

## [v0.5.0] — 2026-06-21

### Added

- **HTTP/1.1 fallback** (`http1/` package) — zero-dependency HTTP/1.1 wire
  protocol from scratch (no `net/http`). Uses `net.Buffers` (writev) for
  scatter-gather writes. Both `Content-Length` and `Transfer-Encoding: chunked`
  for request/response bodies. Automatic 1xx skip. Keep-alive from `Connection:`
  header. `HEAD`/`204`/`304` no-body fast paths. Chunk-extension stripping.

- **`TransportH1SingleConn`** — explicit H1.1 single-connection transport.
  Serializes exchanges via in-flight mutex. Dial backoff, keep-alive reuse.

- **`TransportALPN`** — ALPN-aware transport. Dials once with `conn.FlexDialer`
  (offers `h2` + `http/1.1`) and permanently routes to H2 or H1.1 based on
  negotiated protocol.

- **`conn.FlexDialer`** — TLS dialer prepending `h2` and `http/1.1` to
  `NextProtos`; returns `ErrALPNFailed` if neither is negotiated.

- **`conn.NegotiatedProtocol`** — returns ALPN protocol string from `*tls.Conn`;
  `""` for plain-TCP connections (H2C).

- **`protoStream` interface** — protocol-agnostic abstraction over
  `*conn.Stream` (H2) and `*h1Exchange` (H1.1), enabling `Client.sendRequest`
  and `drainResponse` to drive either protocol uniformly.

- **`docs/USAGE.md`** — 21-section usage guide covering all client features.

## [v0.4.0] — 2026-06-20

### Added

- **CONTINUATION write path** (RFC 7540 §6.2/§6.10) — `writeHeadersWithPriority`
  splits HPACK blocks exceeding `SETTINGS_MAX_FRAME_SIZE` into one HEADERS
  frame (END_HEADERS=0) plus N CONTINUATION frames; padding and priority only
  on the HEADERS frame; the final CONTINUATION sets END_HEADERS=1. Zero
  additional allocations. Applies to both request and trailer HEADERS.
  3 unit conformance tests + 1 integration test (50-header ~50 KiB block).
  (`15a5425`, `e24be99`)

- **Retry layer** — `client.NewRetryer(c, RetryOptions)` wraps `*Client` with
  an automatic retry loop for idempotent requests. Built-in retry on
  `REFUSED_STREAM` (RFC §8.1.4), GOAWAY, and dial errors. `IsRetryable`
  callback for caller-defined 5xx/etc. policy. `Request.Idempotent *bool`
  overrides method-based classification. Truncated-exponential backoff with
  ±25% jitter, configurable `MaxAttempts`. `DoStream` retries only before
  the first HEADERS frame arrives. 16 tests. (`7fe1552`)

- **Docker integration test infrastructure** — OpenResty (nginx + Lua) and
  Undertow (Java) servers in Docker Compose; `gen-certs.sh` auto-generates
  TLS certs; `client/integration_test/matrix_test.go` cross-server test suite
  (healthz, root, status codes, echo POST, connection reuse, concurrency,
  chunked body, large body, delay, context cancel, headers, metrics). CI
  `docker-it` workflow runs the full matrix on every PR. (`7dcac0d`, `33af9c1`)

- **Request validation** — `validateRequest` rejects missing Method, empty
  Path, BodyReader + Body conflict, Trailers + method conflict early (returns
  `ErrInvalidRequest`), before any conn is touched. (`7fe1552`)

### Fixed

- **`FramesReceived` Stats race** — counter was incremented in the read loop
  after `ReadFrame` returned, after events were dispatched. Test reading
  `Stats()` right after `Recv` could observe `FramesReceived = 0`. Fix:
  `bumpFramesReceived()` added to `connOps` interface, called at the start
  of each `On*` handler so the counter is visible as soon as the frame is
  dispatched. (`b5488dd`)

- **Undertow `/status/:code` 500** — `PathHandler` strips the `/status`
  prefix; `getRelativePath()` returns `/301` (not `/status/301`).
  `substring("/status/".length())` on a 4-char string → `StringIndexOutOfBoundsException`.
  Fixed to `substring(1)`. (`b5488dd`)

- **nginx `/echo` returns empty body** — `echo_duplicate 1 $request_body`
  requires the echo module to buffer the body first; under OpenResty with
  HTTP/2 the variable stays empty. Replaced with a Lua block that calls
  `ngx.req.read_body()` before printing. (`e45e524`)

- **Silent consumer hang on stream event-channel overflow** — when the
  8-slot `Stream.events` channel filled (fast server, slow consumer), `push`
  silently dropped the event and sent `RST_STREAM(REFUSED_STREAM)`. Consumer
  blocked in `Recv` forever. Fixed by non-blocking send + RST + close channel.
  (`2389f55`)

- **Flow-control hang on large body** — `TestIntegration_Client_StreamBody_Large`
  raced: server filled the default 8-slot event buffer before the consumer
  goroutine started, triggering the overflow path above. Fixed by using
  `StreamEventBuffer: 128` for that test. (`ebbfa4a`)

- **55 lint issues** — golangci-lint v2.5 clean: removed dead code
  (`encIdentity`, `pooledZlibReader`, `drainResponse` in push_test), fixed
  unchecked `Close()` returns in proxy.go, added doc comments, removed
  redundant `int32()` cast, extracted `handleHeadersEvent`/`handleDataEvent`
  to reduce `drainResponse` cyclomatic complexity, added `gosec` to
  test-file exclusion (eliminates G104 false-positives). (`ebbfa4a`)

- **Dockerfile.undertow baked-in certs** — `COPY fixtures/certs /app/certs`
  ran at image build time (before `gen-certs.sh`), causing CI failures.
  Removed; docker-compose already mounts `./fixtures/certs:/app/certs:ro`.
  (`1960ca2`)

- **Security — P0/P1 pre-production audit** — closed all P0 and P1 findings
  from the pre-production security review. (`e705532`)

### Performance

- **WriteHeaders single syscall** — coalesced the previously split header
  write into one `Write` call, removing a system-call boundary. −15% latency
  on `BenchmarkClient_DoParallel`. (`f32a062`)

- **−2 allocs/op on request path** — removed buildHeaders closure escape
  (−1 alloc) and replaced `*sentRequest` return with multi-value return
  (−1 alloc). (`9618fa7`, `f0b769a`)

### Diff

26 commits since v0.3.0. 19 files changed in PRs #31 and #32.

---

## [v0.3.0] — 2026-06-15

### Added

- **Automatic response body decompression** (gzip/deflate) — `Request.DisableDecompression`
  opt-out, auto-injected `accept-encoding: gzip` (preserved when caller
  supplies one), `decompressFully` for batch path, `decompressingReader`
  for streaming path, `gzipReaderPool` for reader reuse.
  `Response.BytesReceived` = wire bytes; `Response.Body` = decompressed.
  10 tests. (`a3338da`)

- **Priority hints** (RFC 7540 §5.3) — `Request.Priority *frame.Priority`
  embeds a 5-byte priority block (E + StreamDep + Weight) in the first
  HEADERS frame. `Stream.SendHeadersWithPriority` carries the PRIORITY
  flag. Backward compatible: nil priority → original SendHeaders
  behavior. 4 frame tests + 3 client tests. (`6dd5148`)

- **Graceful shutdown** (RFC 7540 §6.8) — `Conn.Shutdown(gracefulTimeout)`
  sends GOAWAY(lastClientStreamID, NO_ERROR), marks the conn as
  draining (NewStream returns `ErrConnDraining`), then waits up to
  the timeout for in-flight streams to drain. `Client.Shutdown(timeout)`
  proxies the request down to the underlying *conn.Conn (single-conn
  transport). Pool transports close all conns in parallel.
  `markStreamDone` closes a wake-up channel when inflight hits zero.
  4 conn tests + 3 client tests. (`9a5c1f8`)

- **Client.Warmup(n)** — pre-dial up to n conns in the background to
  avoid TLS handshake + HTTP/2 setup on the first request. n is
  capped at MaxConnsPerHost (1 for single-conn). Pool transport
  fan-outs across the live set. Idempotent. 4 tests.
  (`24be6f8`)

- **Client-side rate limiting** (token-bucket) —
  `ClientOptions.RateLimitPerSecond` (float) gates Do/DoStream via
  an internal token bucket. Take respects ctx cancellation. 5 tests
  (4 unit + 1 integration). (`0fb9dd5`)

- **Per-request timeout** — `Request.Timeout time.Duration` derives
  a sub-context from the parent ctx with the given deadline. When
  the timeout fires the request fails with
  `context.DeadlineExceeded` and the in-flight stream is reset.
  Applies to both Do and DoStream. Zero = use parent ctx. 4 tests.
  (`0fb9dd5`)

- **ClientOptions.RateLimitBurst** — separate burst capacity
  decoupled from steady-state RPS. Zero (default) falls back to
  `RateLimitPerSecond` for backward compatibility. (`809533c`)

### Fixed

- **TestWarmup_Pool_CappedByMaxConns** asserted `ActiveConns <= 2`
  only, which is also satisfied by a no-op warmup. Test now wires
  `countingDialer` and asserts `dialCount >= 1` (warmup actually
  ran) plus `dialCount <= MaxConnsPerHost` (cap honored). (`462c179`)

- **`rateLimiter` doc comment claimed "lock-free on the hot path"**
  while `Take` actually takes `rl.mu.Lock()` on the first line.
  Replaced with "goroutine-safe (sync.Mutex + Cond)". No code change.
  (`462c179`)

- **Goroutine leak in `singleConn.warmup`** — the 30s background
  dial context was created and `defer cancel()`-ed inside the
  goroutine, with no external handle. `Warmup(1) → Close()` left
  the goroutine alive for the full timeout. Fix caches
  `warmupCancel` on `singleConn`; `close()` and `shutdown()` call
  it. Repeated `warmup()` calls reuse the in-flight context. (`462c179`)

- **`TestClient_RateLimit_BlocksExcess` and warmup tests** had
  magic-number deadlines (`400ms`, `2s`, `3s`) chosen "by feel".
  Replaced with derived values: `expectedMin = (need-burst)/rps - slack`
  and `maxConns * dialPerBudget + slack`. Tweak parameters, not
  wall-clock guesses. (`809533c`)

### Docs

- **Self-review action plan** — post-sprint audit found 3 fake-green
  / false-claim / leak defects in v0.3.0 sprint tests. 7 items
  tracked in `docs/SELF_REVIEW_2026-06-15.md`. (`878e8d4`)

- **Self-review close** — all 7 items resolved; validation table
  documents the gate (`make test-fast`, count=20 stress, lint
  baseline unchanged). (`a3b4938`)

## [v0.2.0] — 2026-06-14

### Added

- **HTTP/1.1 CONNECT proxy support** — `ProxyDialer` and `ProxyTLSDialer`
  for tunneling HTTP/2 traffic through an HTTP/1.1 CONNECT proxy.
  Includes proxy auth, custom headers, and TLS to proxy. (`56be170`)

- **Frame padding** (RFC 7540 §4.2) — `PaddingStrategy` struct with
  `ForHeaders()` and `ForData()` methods. `WriteDataPadded` /
  `WriteHeadersPadded` emit PADDED frames with random-length padding
  for traffic-fingerprint resistance. (`f5df543`)

- **Server Push — connection layer** (RFC 7540 §8.2) —
  `ConnOptions.EnablePush`, `EventPushPromise` stream event with
  `PushStreamID`, `OnPushPromise` handler registers pushed streams and
  decodes promised request headers. Peer PUSH_PROMISE rejected with
  PROTOCOL_ERROR when push is disabled. 2 conn-layer tests. (`76dc45d`)

- **Server Push — client callback API** (RFC 7540 §8.2) —
  `ClientOptions.PushHandler` = `func(ctx, promisedHeaders, *Response, error)`.
  Auto-sets `ConnOpts.EnablePush = true` when non-nil. `drainPushedStream`
  goroutine drains pushed response and invokes handler with ready
  `*Response`. `Conn.LookupStream()` public method. Nested pushes
  supported. 2 integration tests. (`cd9fcd0`)

- **ORIGIN frame + connection coalescing API** (RFC 8336) —
  `FrameOrigin` (type 0x0c) with TLV parsing, stream-0 enforcement,
  malformed-frame detection. `Conn.Origins()` and `Conn.CanCoalesce(origin)`
  public API. 5 frame tests + 5 conn tests. (`e65cb3a`)

- **ALTSVC frame** (RFC 7838) — `FrameAltSvc` (type 0x0a)
  with TLV parsing, `AltSvcEntry` struct (Origin + AltValue),
  `Framer.WriteAltSvc`, `Conn.AltSvcEntries()`. Server-wide and
  per-stream variants. Empty payload clears all alt-svc entries.
  6 tests (3 roundtrip + 3 negative). (`a65c5a7`)

- **Extended CONNECT protocol** (RFC 8441) —
  `SettingEnableConnectProtocol` (0x8) setting ID.
  `Conn.ConnectProtocolSupported()` checks peer advertisement.
  `Request.Protocol` field emits `:protocol` pseudo-header in
  `buildHeaders` for WebSocket/extended CONNECT semantics. 6 tests
  (3 conn + 3 client). (`d360e12`)

### Changed

- **Code review fixes** — 3 BLOCKER + 11 WARNING + 8 INFO findings
  resolved: `parseStatus` alloc elimination, `sendRequest` extraction,
  slab leak fix, hash alloc reduction, `Unwrap` consistency,
  direct frame/hpack import removal from client package.

### Fixed

- **nil-panic** on `StreamBody` with nil `Response` (`941d6b4`)
- **Slab leak** in client request path (`8a3d261`)

### Diff

44 files changed, +4,109 / −283 lines.

---

## [v0.1.0] — 2026-05-12

Initial release. Full HTTP/2 + HPACK codec from scratch, no
`net/http` or `golang.org/x/net/http2` dependencies.

### Features

- **Phase A** — Frame layer + HPACK codec (RFC 7540 + 7541)
- **Phase B.1–B.2.6** — Connection layer: TLS+ALPN dial, SETTINGS
  handshake, multi-stream, bidirectional flow control, dynamic
  SETTINGS, MAX_CONCURRENT_STREAMS gate, GOAWAY drain, PING ACK
- **Phase C.1** — Public client API: `Client`, `Request`, `Response`,
  `Do`/`DoStream`
- **Phase C.2** — Per-host connection pool with lazy-grow, idle
  eviction, GOAWAY handling
- **Phase C.3** — Service discovery: managed pool with resolver-based
  address fan-out, round-robin / random / consistent-hash selectors
- **Phase C.4** — Metrics & hooks: `Counters`, lock-free `Histogram`,
  lifecycle `Hooks` (OnRequestStart/Complete, OnRetry, OnDial,
  OnConnClose, OnResolverUpdate)
- **Phase D.1** — Zero-alloc request path (33 allocs/op, down from 49)
- **Phase D.2** — Request/response body streaming (`StreamBody`,
  `BodyReader`, `ContentLength`)
- **Phase D.3** — H2C (plaintext HTTP/2) via `PlaintextDialer`
- **Phase D.4** — PING / keepalive (`Conn.Ping(ctx)`,
  `KeepaliveInterval`)
- **Phase D.5** — HTTP request trailers (`Request.Trailers` /
  `Request.TrailerFunc`, `StreamResponse.WaitTrailers`)

39 E2E + stress tests. Bench-gate enforced: frame + hpack = 0 B/op,
0 allocs/op.
