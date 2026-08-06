# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`conn.H1TLSDialer` — HTTP/1.1 over TLS** (#334). The HTTP/1.1 transports
  documented a dialer requirement no exported dialer met: "a TLS dialer with
  `NextProtos` containing only `http/1.1`". `TLSDialer` asserts h2, `FlexDialer`
  offers h2 alongside http/1.1 (so any h2-capable server picks h2), and
  `PlaintextDialer` does no TLS — leaving every HTTPS + HTTP/1.1 caller to write
  their own `conn.Dialer`. `H1TLSDialer` offers only the `http/1.1` ALPN token
  and asserts the peer did not select something else (`ErrALPNNotHTTP11`; a peer
  that negotiates no ALPN at all is accepted, since that implies HTTP/1.1).

- **`conn.ALPNAsserter`** — an optional `AssertsALPN() string` on a dialer,
  declaring the one protocol it ever returns. `TLSDialer` and `ProxyTLSDialer`
  answer `"h2"`, `H1TLSDialer` `"http/1.1"`, `FlexDialer` `""` (no assertion).
  `client.NewClient` uses it to reject a transport/dialer pairing that can only
  fail. Custom dialers may implement it to get the same check.

### Changed

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
