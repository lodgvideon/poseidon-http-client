# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **A connection-level `WINDOW_UPDATE` arriving during the SETTINGS exchange was
  discarded, capping every such connection at 65535 outbound bytes forever.**
  `handshakeSettings` reads frames through `settingsRecorder` until the peer's
  SETTINGS ACK, and that recorder's `OnWindowUpdate` returned `prefaceGuard()` —
  accepting the frame and throwing the increment away. The recorder exists to
  reject frames sent *before* the peer's SETTINGS; after that point "do not
  reject" had been implemented as "return nil", which is not "apply".

  nginx sends its connection-window raise immediately after its SETTINGS, i.e.
  inside exactly that interval, and grants the whole 2³¹−1 in one frame — so
  there is no later `WINDOW_UPDATE` to recover from a dropped one. Traced against
  the pinned fixture: the peer credited the connection at `.709745`, before the
  client's first HEADERS at `.709795`, and the client still wrote **exactly
  65535 bytes** — 21 whole 3006-byte bodies plus a 2409-byte remainder — then
  parked 8 streams in `acquireSendCredits` with their HEADERS sent and no DATA.
  Thirty concurrent POSTs finished 21 of 30 after a 10-second timeout; they now
  finish 30 of 30 in 5 ms, and 60 × 3006 (180360 bytes, 2.75× the old ceiling)
  in 7 ms.

  This was filed as a peer property of nginx and is not one (#701). The other
  peers in the matrix pass only because they extend credit as they read, long
  after the recorder is gone. `idBodySize` in the concurrency-identity
  integration test goes back to 3006, so 30 concurrent bodies total 90180 bytes
  and the nginx leg becomes an end-to-end guard on this: it fails against a
  build that drops the increment. Three comments that recorded the disproved
  mechanism as measured fact are corrected.

  Still open, deliberately not bundled: every other `settingsRecorder` handler
  swallows its frame the same way once the peer's SETTINGS has arrived — a
  GOAWAY in that window is dropped and the client proceeds as if the connection
  were healthy.

### Changed

- **`client` and `grpc` no longer import the HTTP/2 frame codec.**
  `conn/aliases.go` has said since it was written that it exists "so the client
  package can avoid importing frame and hpack directly, keeping conn as the single
  dependency surface", and that had drifted: `client` named `frame.Priority` in
  four places — including the **public** `Request.Priority` field — and `grpc`
  named `frame.ErrCode` and four of its constants. So a caller using HTTP/1.1 or
  HTTP/3 had to import the HTTP/2 frame package to spell a parameter type (#714).

  Five bindings are added to `conn/aliases.go` — `Priority`,
  `ErrCodeInternalError`, `ErrCodeCancel`, `ErrCodeEnhanceYourCalm`,
  `ErrCodeInadequateSecurity` — and the six production sites now use them.
  **Nothing breaks and no caller has to migrate:** a Go type alias is an identical
  type, so `client.Request{Priority: &frame.Priority{...}}` still compiles exactly
  as before, as does passing `conn.Priority`.

  A CI step keeps the edge from returning, modelled on the `hpack` one that has
  guarded the same shape at the layer below since #492. It checks **direct**
  imports for the same reason that one does: `client` still reaches `frame`
  transitively through `conn`, which is the layering working rather than a skip.
  The step was verified to discriminate — it passes on this tree and fails on
  `main`.

  This is the `conn` half of #714 only. The `http3/aliases.go` half is
  deliberately not here: it would add three exports to a package #479 is an open
  ticket about shrinking.

### Tests

- **A CI gate pins that the three protocol stacks agree on `maxInterimResponses`.**
  It is a peer-input bound spelled once per stack — every 1xx block is decoded and
  handed to the caller, so a stack whose bound drifts upward is the stack a hostile
  peer gets more work out of.

  #923 says "there is no test" and "change one and nothing fails"; both are false.
  Each package carries a `TestInterimCap_MatchesSiblings` tripwire, and a `go test
  -overlay` run with one declaration changed 100 → 101 reddens that package. Those
  tripwires stay: they run under a bare `go test ./...` where no CI script does, and
  they pin the **value** 100, which a script comparing the three to each other cannot.

  The hole that is real is a **coordinated single-package edit** — the constant plus
  that package's own tripwire — which diverges from the other two with a fully green
  suite. `scripts/interim-cap-gate.py` checks all six numbers at once and asserts the
  site count is exactly three, so a rename cannot make "all values agree" vacuously
  true. Proved to discriminate on four arms: control 0, one stack drifting 1, the
  coordinated edit 1, and a rename leaving two sites 1 (#923).

- **The two RFC 9204 stream-bound tests now assert the error code on the value the
  production path returned, not only on the fixture's latch.** `connError` builds its
  `*H3ConnError` on the calling goroutine, so that value pins the code regardless of
  what any other teardown does; `conn.closeCode` pins that the same code reached the
  wire. Neither subsumes the other, and `errors.Is(err, ErrH3Control)` pins neither —
  `H3ConnError.Is` matches every code. Verified by splitting them: a build that returns
  the wrong code but closes with the right one reddens only the new assertion, and the
  reverse reddens only the old one.

  This is the residue of #924, whose flake was fixed in #929. Re-measured on `a9c1048`:
  the 24x800 harness that produced **385 failures in 19,200 race-instrumented runs**
  before the fix now produces **0**. Both tests also gain the
  `docs/RFC_COVERAGE.md` rows the RFC trace policy requires and neither had.

- **A CI flake in `quic`'s PTO cancel tests was the instrumentation, not the
  timing.** `TestReadWithPTO_CancelUnblocksRead/200ms` failed on a loaded runner
  with *"the injection never happened (0 cancels)"* — its own control arm firing.
  The injecting goroutine did `cancel()` and then `cancels.Add(1)`, but `cancel()`
  is what unblocks `readWithPTO`, so the moment it ran the main goroutine was free
  to return from the read and reach the assertion before the counter was ever
  incremented. The test then reported a missing injection for an injection that
  had happened.

  Both tests count before they inject now, which is sound for what the counter
  means: nothing between the two statements can return early, so a counter of 1
  still implies the cancel follows.

  **Diagnosed by widening the window rather than by re-running**: one
  `runtime.Gosched()` between the two statements reproduces the exact CI message
  on all three subtests, and the same injection against the fixed order is green
  40/40. The binary hash check (`go test -c -race -trimpath`) confirmed the flake
  was not introduced by the branch it appeared on —
  `e956504e…` on both `origin/main` and the branch. The sibling
  `TestReadWithPTO_CtxCancelNoPTOSpin` carried the identical ordering and is fixed
  with it, before it could flake too.

- **The nightly fuzz matrix covers every target but one.** Eleven cells are
  added — `client` 4, `grpc` 3, `http1` 2, `hpack` 1, `quic` 1 — taking the
  matrix from 22 cells to 33. (#687's body says 19 targets are missing; that
  number predates its own first slice, which shipped in #704. The real figure at
  `4b42db8` was 12.)

  Each was fuzzed 30s locally before being listed, per the precedent #704 set,
  and **two of the twelve failed on first contact** — which is the staging
  argument in #687 working rather than failing:

  - `FuzzReadResponse_ValidRoundTrip` failed twice in a minute, on `0: \x00` and
    on `": 0`. Both were fixture defects: the target characterised "valid" with
    blocklists — `":\r\n \t"` for the name, `"\r\n"` for the value — against
    grammars RFC 9110 defines *positively*, so it reported correct rejections as
    parser bugs. A blocklist of a token grammar is wrong by construction: it can
    only enumerate the invalid characters someone thought of. Replaced with
    `isToken` (§5.6.2 `1*tchar`) and `isFieldValue` (§5.5 field-vchar / SP /
    HTAB) allowlists; 6.66M execs over 120s clean afterwards.
  - `FuzzReadResponse` fails in one second on `HTTP/1.0 000 0000`, and that one
    is **not** a fixture defect — the test and `parseStatusCode` hold two
    different rules about a final status below 200, and `ReadResponse` returns
    `(0, nil)` for it, colliding with the value it returns on every error path.
    That cell is deliberately held out of the matrix and tracked in #931; it is
    the only target the matrix does not name.

  No production code changed.

- **The `quic` allocation gates were never reached by CI.** The
  "Allocation gates (not under -race)" step listed `./conn/ ./client/ ./grpc/
  ./http1/` — `./quic/` was absent — and eight of quic's eleven `//go:build
  !race` tests missed the `-run` pattern as well, so both of the failure modes
  the step's own comment documents three times were live again, together, in the
  one package `bench-gate` cannot cover either: that gate scans `Benchmark` lines
  for `B/op`, and every quic benchmark that builds a `Conn` is env-skipped. So
  nothing was watching `buildACK`, `onSent`, `flush` or `retransCopy`.

  The package is listed and the pattern gains `Alloc` (which subsumes `Allocs`
  and `DoesNotAllocate`), `Scratch`, `Retrans` and `StaleRanges`. Coverage was
  verified by diffing the eleven `!race` names against the names `-v` actually
  prints — the check all three earlier failures skipped, now written into the
  comment.

  **The widened step was made to fail before being trusted**: with `retransCopy`
  mutated to bypass its free list, the command `main` runs today exits **0**,
  while the new one goes red on three tests, one of which
  (`TestRetransCopy_ReusesTheBufferItWasGiven`) is among the eight that no
  pattern reached. Found while re-deriving #508 (#928).

- **`http3`'s fake connection latches its close code in one critical section.**
  `fakeConn.CloseWithError` called `terminate()`, which takes and *releases*
  `c.mu`, and only then re-took the lock to check `c.closed` and record
  `closeCode`. In that gap a second teardown could run the whole latch, so the
  field reported whichever close acquired the lock second rather than which one
  terminated first. The comment beside it claimed the shape mirrors `quic.Conn`;
  it does not — `quic.Conn` does both under a single `c.mu`.

  34 assertions across 11 files read that field to decide which error code the
  code under test produced. Measured before the fix: 385 failures in 19,200
  race-instrumented runs of the two RFC 9204 bound tests, every one reporting
  `H3_INTERNAL_ERROR` where the typed QPACK code was expected.

  #924 attributed the flake to a different mechanism — the reader goroutine
  racing a hand-driven `serviceControl` loop over one chunk queue. It cannot:
  that reader parks on its first iteration for the fixture in question, and its
  only nil-returning wake path has no sender in the package. **No production
  code changes**; `quic.Conn` has always closed atomically (#924).

- **The GRO buffer-size gate can fail off Linux now, which is the only place its
  defect exists.** `TestPollBufLen_MatchesPlatformCoalescing` derived its own
  expectation from `groCanCoalesce` — the same compile-time constant `pollBufLen`
  branches on — so on Linux, where the constant is true and where every `runs-on:`
  in `.github/workflows/` is `ubuntu-24.04`, deleting the platform gate was
  completely unobservable. The regression it names, a 64 KiB receive buffer per
  connection on Windows, macOS and the BSDs where `RecvGRO` is a plain
  single-datagram read, could not turn CI red (#839).

  The decision moves into `pollBufLenFor(canCoalesce, isGROReader bool)`, which
  takes the platform capability as an argument, and the test is now a decision
  table over both booleans. The row that carries the property —
  `{canCoalesce: false, isGROReader: true}` — is asserted from a Linux run.
  `pollBufLen` keeps binding to it, so the method cannot drift away from the table.
  Verified: mutating the gate to `if isGROReader` fails the new row and passed
  before.

- **The ChaCha20 key-update test now covers the un-rotated header-protection
  key.** RFC 9001 §6 updates the AEAD key and IV on a key update and does not
  update the HP key, so a ratcheted generation keeps generation 0's — which is the
  whole reason `openerWithHP` exists. `TestChaCha20_KeyUpdateRoundTrip` drove one
  phase-1 packet, and on the packet that *causes* the update the header is
  unprotected by the current generation's key, before the phase is known. The next
  generation's HP key was therefore never used, and dropping the override survived
  this test while its AES twin caught it (#913).

  A second phase-1 packet now arrives after the update has committed, which is
  where the ratcheted opener is first used for real. Verified twice in each
  direction: `op.hp = hp` → `_ = hp` now fails this test alone, and passed it
  before.
- **The gRPC unary drain now has a test that fails when the drain changes, and
  `conn.go` states the right reason.**
  `TestInvokeInto_TwoMessageAnswerReturnsTheBufferEmpty` already drove
  `InvokeInto` — the first half of #803 was overtaken by a later rename and is
  stale — but nothing pinned the property the code comment names, so substituting
  the drain for `RecvInto(resp[:0])` passed 2 runs out of 2.

  Both explanations were wrong, in opposite directions. `conn.go` said a second
  message would overwrite "the answer this call is about to return"; it would not,
  because that path returns `resp[:0]` alongside the status. The test comment
  replied that the real difference is therefore only allocation; that is wrong
  too. The overwrite lands in the **caller's** `dst` array, which the caller still
  holds — so a loop reusing one buffer and inspecting it after an error reads the
  message the client has just decided not to trust.

  One assertion pins it, and it is measured rather than argued: under the
  substitution the buffer comes back holding the second message's `B`x64 where the
  first message's `A`x64 belongs, 2 runs out of 2. Both comments are rewritten to
  say that.

- **The tail of the #722 AAA + `testify` sweep: the last 31 hand-rolled
  assertions that sit outside a measured region.** Six files were partial
  conversions — each already imported `testify` and each still carried
  `t.Fatalf`/`t.Errorf` blocks: `conn/windowtuner_test.go` (22),
  `quic/crypto_chacha_test.go` (3), `client/options_validation_test.go` (2),
  `conn/sendflow_test.go` (2), `grpc/borrowmetadata_test.go` (1) and
  `quic/establish_latch_test.go` (1). Every failure message was carried over as
  `msgAndArgs` rather than dropped, since `testify` prints only
  expected-versus-actual and this suite's messages are what say why the property
  matters. Two of them stay `assert` on purpose and now say so in a comment: they
  run on an `httptest` handler goroutine, where `testing` forbids `FailNow`, so
  `require` there would `Goexit` the handler instead of failing the test. The
  four remaining hand-rolled calls in `qpack/dynamic_test.go` are inside
  `FuzzRequiredInsertCount` and `FuzzDynamicIndexResolution`, which the sweep
  leaves alone; the audit had also attributed one to
  `TestQPACK_RequiredInsertCount_Decode`, which is in fact already fully
  converted. Nothing weakened: each file's converted tests were scored against a
  source mutation twice before the conversion and twice after, and every verdict
  and killer set is identical across the pair — including one row that survives
  in both directions, `openerWithHP` dropping the un-rotated header-protection
  key, which is a pre-existing gap in the key-update suite rather than anything
  this change touched (#722).

- **Eleven `http1` and `frame` coverage gaps from the #722 sweep, eight closed by
  adding the missing case and three answered with a measurement instead.** The
  eight: the three write-side stream-0 guards `frame`'s test file grew for
  `WriteData` and never grew for `WritePriority`, `WritePushPromise` or
  `WriteContinuation`; the WINDOW_UPDATE increment mask, the one of this codec's
  three 31-bit payload masks whose doc comment claimed the parity and which
  nothing asserted, plus the frame's stream id, which no test looked at at all —
  writing every WINDOW_UPDATE on stream 0 refills the connection window while the
  blocked stream stays at zero; both read-side length guards at their boundary
  rather than comfortably past it, where a 7-octet GOAWAY indexes past a 7-octet
  slice on peer-chosen input; a 1-octet ALTSVC payload, which is the only thing
  between a peer's one-line frame and an index-out-of-range in the parser; the
  §6.10 continuity check's "or a frame on a different stream" half, which would
  otherwise splice two streams' field fragments into one HPACK block; the inbound
  padded PUSH_PROMISE trace detail, whose outbound twin was already pinned; the
  204 `Content-Length: 0` pooling exemption, whose absence let the reverted
  evict-on-presence behaviour — a connection burned per `generate_204` — come
  back for free; `WriteBody`'s condemn on a write that accepts every octet and
  reports an error anyway, the `(len(p), err)` return a `*tls.Conn` produces and
  the suite's fake conn cannot; three arms of `isConnectionManagedName` that had
  no ordinary-header control, so every two-character caller header could have
  been dropped from the wire silently; `HasResidue`'s reader-level check, the one
  layer that sees an over-read when the socket below it is genuinely clean; and
  `ProbeIdle`'s socket-level detection, asserted on the FIRST probe because the
  existing polling loop is answered by the buffered short-circuit from its second
  call onwards. Two fixtures were repaired rather than extended:
  `TestFramer_FrameTooLargeOnRead` lowered the read limit before writing, so the
  write failed and `ReadFrame` was never called (its property is now the boundary
  test), and the rule-1 bodyless table appended a second response to every row,
  which condemns a connection on its own — one row was decided entirely by that
  and has moved to the test named for it. The three measurements: `ProbeIdle`'s
  `Buffered()` short-circuit changes no verdict anywhere in its input partition
  (`peekUnder` is `Peek(1)`, which returns from the buffer), so it is a fast path
  that MASKS the socket check rather than a second detection; `WriteAltSvc`'s
  local stream-id mask emits byte-identical frames with and without it, because
  `WriteFrameHeader` masks every stream id already — what it really buys is a
  trace line that agrees with the wire, which is what now pins it; and
  `commitHeaderLine`'s `contentLen` writes are dead at any value, so the fix
  there was the comment that mis-attributed RFC 9112 §6.3 rule 3 to that site
  when `resolveContentLength`'s `respTE` early return is what implements it
  (#778, #779, #780, #781, #782, #799, #811, #820, #824, #830, #831).

- **The `client/integration_test` fault matrix was a diagonal, and three of its
  tests could not observe the property they are named for.** Every one of the ten
  Toxiproxy tests pinned one upstream constant, so a four-peer
  cross-implementation suite injected faults into nginx and nothing else; the
  proxy's upstream is now a parameter and all ten run as subtests against nginx,
  Undertow and nghttpx, over h2 and over HTTP/1.1. The in-process Go reference
  stays out and the reason is recorded in the peer table: Toxiproxy dials from
  inside the compose network and there is no route back to a host-side ephemeral
  port. That widening is what made the missing branch reachable — the HTTP/1.1
  mid-body test rests its claim on Content-Length reconciliation, but over TLS a
  cut socket surfaces as `io.ErrUnexpectedEOF`, so the RFC 9112 §6.3 rule 6 arm
  it names was never executed and deleting that arm's error left it green. A
  cleartext leg reaches a real `io.EOF`, and the new test requires the error to
  carry the declared and received lengths, which only rule 6 produces.
  `TestIT_GoHTTP_ConnectionReuse`, `TestIT_GoHTTP_MultipleRequests` and
  `TestMatrix_ConnectionReuse` counted statuses, which a transport dialling per
  request satisfies exactly as well as one reusing a connection; they count dials
  now. The two `RequestHeaders` tests asserted a status and never that the header
  arrived, and read it back off `/echo`'s `X-Echo-Headers`. Three fixtures had no
  consumer at all: `/trailers` gains `Request.WantTrailers` coverage in both
  directions on the one peer that emits a real trailer section, `/gzip` gains a
  cross-peer decode assertion that reads the same on every peer (a body longer
  than the octets received can only be a decompressed one) — the Go reference's
  own handler had to start emitting gzip rather than labelling plaintext as gzip
  — and `/never` gains the silent-peer case, which is a different state from
  `/delay`: the failure must be a `context.DeadlineExceeded` and the connection
  must survive it. Response sizes reach the 65535-byte connection window on every
  peer, and the bodyless 204/304 class is tested over HTTP/1.1, where the client
  has a rule 1 branch to get wrong; nghttpx is excluded there with the
  measurement, because Undertow answers `/status/204` with a body and nghttpx
  refuses to re-encode it (#892, #893, #894, #895, #896).

- **Fourteen coverage gaps in `trace`, `contrib/prometheus`, `internal/bufx` and
  `internal/bytesx`; eleven closed by adding the missing case, three refuted by
  measurement.** Every varint in `bytesx` was decoded from a buffer trimmed to
  exactly its own length, the opposite of the shape `quic` and `http3` produce —
  a frame type, then a length, then hundreds of bytes still to come — so all
  three multi-byte length guards could be weakened to equality tests and the
  one-byte form made to report `len(b)`, with the suite green: under the first,
  every multi-byte varint in every packet reads as incomplete and the parse
  stalls forever. `trace`'s renderer had no case where a detail-gated field held
  zero, so `last_stream=0` (a server refusing the connection without having
  processed a stream) and `incr=0` (a PROTOCOL_ERROR WINDOW_UPDATE) could both
  vanish from the log, and the nil-`Params` guard that stops a debug path
  panicking the connection it was installed to observe could be deleted
  unnoticed; the timestamp was cut off by the test helper before every
  assertion, leaving its resolution pinned by nothing; and the writer error both
  `Flush` and `Close` exist to return had no failing writer anywhere in the
  package, hiding a `Close` reordering that leaks one flusher goroutine per
  tracer. `contrib/prometheus` gained the degrade-rather-than-panic branch its
  scrape goroutine depends on, the top-of-window histogram boundary — bracketed
  from a distance at client buckets 8 and 40, so the term the bug drops was
  always zero — `WithConstLabels` on `HookMetrics` and on every `Collector`
  `Desc`, pinned through the duplicate-registration error a second client in one
  process actually hits, `statusLabel`'s `Err`-beats-`Status` precedence, all
  eighteen families at zero state rather than one gauge out of eighteen,
  `MetricsSnapshot`'s no-caching contract, and a concurrent-observation test
  with a single-goroutine control arm and a reported scrape count. `bufx` pinned
  `GetReadBuf`'s documented length-0 postcondition and the `PutReadBuf(nil)`
  guard, whose removal poisons the shared pool and panics an unrelated caller
  that merely drew the entry, plus the reserved-bit-only input that separates
  "clears the R bit" from "saturates at 0x7fffffff". Three sub-claims were
  refuted with measurements rather than closed with tests: `sync.Pool` retention
  at the `cap == min` boundary is unobservable, `WithDurationBuckets` has no
  data path to `Collector` at all, and the uint helpers' callers all pass
  exactly-width slices, so an out-of-window access is a panic the existing
  vectors already trigger. `ExampleNewCollector` and `ExampleNewHookMetrics` no
  longer share `http.DefaultServeMux`, and `applyCallOptions`' escape comment
  now records what the allocation gate observes — the caller's `len(opts) > 0`
  guard, not the `//go:noinline` directive beside it (#742, #743, #744, #749,
  #750, #751, #752, #753, #758, #760, #761, #762, #763, #794).

- **Twelve `http3` coverage gaps from the #722 sweep, most of them boundaries or
  orderings that no fixture could reach.** Every cap was tested from the reject
  side only, so any of them could tighten by one and stay green: the 1xx count cap,
  the per-frame declared-length cap, the retained-total cap on both of
  `dispatchFrame`'s arms, and `release`'s pooled-buffer cap — whose ACCEPT side is
  the whole reason `frameBufPool` exists, and which could be reduced to "never
  reuse a grown array" with the suite silent. Two orderings were named by tests
  that could not observe them: `markDead`'s store-before-close, where a PARKED
  observer never lands in a two-instruction window and a spinning one catches the
  swap on the first run, and `Close`'s latch-before-cancel, invisible because the
  fake latched its terminal error only in `CloseWithError` where `quic.Conn`
  latches it in `Poll` too — so a graceful shutdown and a reader teardown that
  tells the peer `H3_INTERNAL_ERROR` were indistinguishable. `sendAll`'s
  zero-progress park had no fixture at all (the fake stream always made progress),
  so losing it — a hot spin on a flow-control-blocked stream, same bytes on the
  wire — was unobservable. Also: two of the three required-pseudo-header cases
  were being rejected by the §4.3.1 authority rule rather than the rule they name;
  an explicit `SETTINGS_MAX_FIELD_SECTION_SIZE` of 0 was never told apart from the
  absent case, which §7.2.4.1 gives the opposite meaning; `h3TLSConfig`'s
  single-Initial curve default was pinned in neither direction; and the UDP
  offload race test asserted nothing and now skips honestly when the raw fd is
  unavailable, reporting what it engaged. `DoStream` ignoring a DATA frame after
  the trailer section, where buffered `Do` answers `H3_FRAME_UNEXPECTED`, is
  recorded as a two-sided drift tripwire rather than decided (#773, #774, #786,
  #795, #796, #797, #807, #808, #809, #815, #816, #817).

- **Twelve `grpc` coverage gaps closed, three of them tests that named a
  property they could not observe.** `TestIntegration_ResetStreamMapsToStatus`
  asserted only "not OK", which `InvokeInto`'s empty-response guard produces on
  its own, so the reset-code mapping was never checked through the wiring; the
  RST arm is now driven through `NewStream`/`Recv` over six chosen codes, which
  needed a mock peer able to send a reset code of the test's choosing —
  net/http2's server only ever emits `INTERNAL_ERROR`, the same value every
  unmapped code falls through to. `TestStream_SendErrorIsSticky` killed the whole
  `ClientConn`, so every later call failed on its own and `sendErr` was never
  read; the latch now has a test whose first send fails on an already-done
  context with the connection provably still carrying traffic.
  `TestInvokeInto_DrainDoesNotClobberTheResponse` never called `InvokeInto`.
  Alongside them: `terminal()`'s status-before-truncation ordering, which needed
  a peer that aborts mid-message *and* says why; `validContentType`'s delimiter,
  which had no refusing case, so `application/grpc-web` read as gRPC; the
  decoder's compact slide, whose 1 MiB ceiling the slide-less form also passed;
  both empty-chunk guards; `MetadataValue`'s case-insensitivity; `InvokeInto`'s
  `dst[:0]` error return; and two of the three places the pooled send vector is
  cleared. Two fixtures were repaired rather than added to —
  `TestStreamBufs_OversizeIsNotPooled` grew `dec.buf` where the pool reads
  `dec.own`, and `TestStreamBufs_CloseIsTheOnlyReturn` checked fields that were
  nil before the code under test ran. The streaming allocation gate gained the
  absolute ceiling its unary sibling already carried, and
  `TestIntegration_ContextCancelStopsRecv`'s second `Recv` is bounded, so losing
  the receive latch now fails on that test's own assertion instead of burning the
  whole test timeout. `appendTimeout`'s final clamp is recorded as unreachable by
  construction rather than untested: no value a `time.Duration` can carry reaches
  it (#788, #789, #790, #791, #792, #793, #803, #804, #805, #806, #812, #813).

- **Seventeen `quic` coverage gaps from the #722 sweep, fifteen closed by adding
  the missing case.** Every mutation those issues recorded survived the whole
  `./quic/` suite twice and now dies twice: the §18.2 ack-delay limits, pinned
  only from the reject side, so an endpoint refusing the largest legal
  `ack_delay_exponent` or `max_ack_delay` a peer may advertise looked correct;
  the 512-range reassembly cap, bracketed at 64 and ~515 and therefore free to
  move by 8x in either direction; `permitInSpace`'s known/unknown frame
  boundary, probed only at `0x30`, eighteen types past the edge, so `0x1f` and
  `0x20` could be answered with the wrong transport error code; the 64-segment
  GSO cap, which no fixture ever made the binding one (the byte cap filled
  first, and quadrupling the segment cap changed nothing) even though exceeding
  it is an `EINVAL` that drops the whole burst; `detectLost`'s RFC 9002 §6.1
  precondition that a packet be sent prior to an acknowledged one, without which
  the time threshold alone retransmits packets the peer has had no chance to
  acknowledge; `maxPendingCtrl` at all three regrant sites, a peer-driven memory
  bound no test reached; the receive-window grant threshold and the connection
  flow-control limit, both tested comfortably inside and neither at its value;
  `readCID`'s 20-byte connection-ID bound, whose only fixture was so short that
  the truncation check rejected it and the bound never ran; the
  at-most-one-Retry rule, for which the SCID rule was firing instead because the
  fixture replayed the same packet; an ACK whose largest packet number was never
  in flight, which without its guard feeds a two-millennia RTT sample into the
  estimator; the stateless-reset token, matched over all sixteen bytes rather
  than a prefix; `initial_max_streams_uni = 0`, the one equivalence class of the
  §4.6 limit with no case; and the §10.3.1 deletion of a retired connection ID's
  reset token, now gated directly instead of through the lookup scoping that was
  masking it. `pto_ctx_test.go`'s three tests each asserted an error that a
  second mechanism also produced — they now count `Read` calls, close the ~667 ms
  anti-deadlock escape hatch that was being credited to the watchdog, and set
  `handshakeConfirmed` so the "PTO path armed" premise those tests state is
  actually true. The `max_idle_timeout` behaviour in #798 is **pinned, not
  decided**: the new test writes down today's ladder — a 1 s advertised timeout
  and a 46.08 s deadline at the last rung — so the open design question has to be
  answered deliberately rather than drifted into. #839 is left open and
  unchanged: its platform gate is a build-tagged `const`, so on Linux
  `if groCanCoalesce` and `if true` are the same program after constant folding
  and no Linux test can tell them apart, while the existing test does catch the
  regression on `windows/amd64` — the gap is CI's OS matrix, not the test.
  Separately, the steady-state send allocation gate was stale and one-sided,
  admitting a full allocation per datagram of regression against a measured 0.00;
  it is now 0 and two-sided like its cold sibling (#798, #827, #828, #836, #837,
  #838, #839, #840, #842, #843, #844, #846, #847, #853, #854, #855, #856).

- **Thirteen coverage gaps in the two header codecs, most of them boundaries
  only ever tested past the limit.** `hpack` accepts a §6.3 Dynamic Table Size
  Update, a §4.4 dynamic-table entry and a header list at exactly their limits
  and refuses one past — but nothing sat ON any of those three comparisons, so
  narrowing each to `>=` left the suite green while turning a conformant peer's
  largest legal input into a connection error. `decodeInteger` carries two
  independent §5.1 bounds and only the value one was tested: a run of
  zero-payload continuation octets never grows the value, so the shift ceiling
  is the only thing that can stop it, and relaxing that ceiling let the peer
  decide how long the loop ran. Index 61 — the last static entry, the line
  between the static and dynamic tables, wire-visible and shared with every peer
  — was never decoded; and 53 of the 61 Appendix A rows were asserted by nothing
  at all, so they are now checked against a second transcription taken from the
  RFC text rather than generated from the table under test. In `qpack` the
  static-index guard on the Literal-with-Name-Reference path had no test (its
  Indexed twin does, and the same one-byte relaxation there is caught as a
  panic); neither string reader had ever been handed a Huffman literal that
  fails to decode, though the two map to different HTTP/3 connection error
  codes; a SHRINKING Set Dynamic Table Capacity never evicted in any test;
  `compactArena` was never invoked at all, leaving the arena a peer-driven
  unbounded allocation with nothing gating it; the §7.1 N bit was pinned only on
  the branch a secret is least likely to take, not on the literal-name form that
  carries `x-api-key`; and three small encode contracts — the Required Insert
  Count wraparound, the constructor's capacity clamp, the static name-only
  tie-break — had no assertion behind them. Each issue's mutant survived the old
  suite twice and dies against the new one twice (#755, #756, #757, #759, #765,
  #766, #767, #768, #771, #772, #776, #777).

  The pair filed as #764 was measured and closed rather than covered. The copies
  guarding the arena-aliasing reads in Insert With Name Reference and Duplicate
  are defence in depth, not a live requirement: a differential probe over 180,000
  encoder instructions — 11,484 of them driving an insert that evicted the whole
  table, 605 with the referenced entry at a non-zero arena offset — produced a
  bit-identical digest with both copies removed, under `-race` and `GOGC=1`. The
  only in-place arena mutation is the reset performed when the table empties, and
  every destination it then writes lies at or below its source, so `copy`'s
  memmove semantics keep the aliased read correct. The copies stay; the tests
  that would have "proved" them are not written, because their mutants survive
  before and after.

- **Thirteen `conn` coverage gaps the #722 sweep filed, closed together.** Every
  one carried a mutation that survived the whole package twice; ten are now
  caught, two were already covered elsewhere, one is reported as the masked pair
  it is. The flow-control decisions were only ever approached from a distance —
  a 100-byte window overflowed with 200 bytes, a retroactive
  INITIAL_WINDOW_SIZE delta parked far past 2^31-1 — so `limit`, `limit+1` and
  the batching threshold's lower side are now driven at the edge; `deliverEnd`'s
  `end` conjunct, `Stream.recv`'s `released` gate on each of the three close
  exits that do **not** recycle, `streamRefundThreshold`'s floor of 1, the
  CONTINUATION flood bound and `PaddingStrategy`'s `Min>0/Max==0` class all get
  their first test. Three tests were repaired rather than added: the closewake
  fixture polled a field its own Arrange block pinned, so its park detection was
  a no-op behind a fixed 100 ms sleep; `TestStreamClose_IdempotentAfterRecycle`
  never reached a recycled struct; and the `ConnOptions.WriteBufferSize` and TLS
  1.2-floor tests asserted things that held whether or not the option reached
  the code. `PaddingStrategy`'s `Min>0/Max==0` answer is **pinned** at the
  current behaviour — disabled — rather than changed; the doc comment that
  reads the other way is left for a separate change, since this batch touches
  no non-test file (#800, #801, #802, #810, #814, #818, #819, #825, #826, #832,
  #833, #834, #852).

- **Twenty-two `client` coverage gaps closed, and thirty-eight tests that ran
  nowhere given one policy.** The #722 sweep filed twenty-seven gaps against
  `client`; every one was re-measured, and the mutation each names now dies where
  it previously survived — among them the HTTP/1.1 pool's release-path eviction
  (three sites masked each other, so all three could go at once unnoticed),
  `serveWaiters`' FIFO promise, a double release with a caller parked,
  `h3SpareStreamCapacity` counting dead and at-cap connections, both prune
  helpers silently owing a queued caller its one reply, `defaultBackoff`'s
  ±25% jitter (a constant passed), four of five `isHardStop` sentinels, `Random`
  and `Hash` both satisfied by a constant pick, `Request.Timeout` during the dial
  and during the body send, `Response.Reset` truncating the body, `Quantile`'s
  `q < 0` clamp, the HTTP/1.1 close-observability gate (exact counts and reason,
  as its HTTP/2 sibling already had), the buffered gzip/deflate response bodies,
  two reset tests satisfied by `io.EOF`, the three-digit `:status` guard, a
  pooled `compressingReader` handed to two requests, a managed pool reporting
  `ErrNoAddresses` when every backend refused, and the send-tail body-source
  failure the unused `errAfterN` fixture had been staged for. Two conformance
  tests stopped self-disabling — their `t.Skipf` preconditions are now
  assertions, so `conformance-gate` can no longer count a skip as coverage. The
  thirty-eight end-to-end tests split across an unconditional `t.Skip` and an
  unset build tag are now behind one `e2e_remote` tag and were run: all thirty-
  eight pass, and doing so uncovered two assertions that could only ever fail
  (`uint64` compared against `int64` counters) plus two that measured the remote
  server's policy rather than this client. Five gaps closed as already covered or
  as equivalent mutants, with the measurement rather than an opinion (#845, #848,
  #850, #860, #861, #862, #863, #869, #870, #871, #873, #874, #875, #876, #877,
  #878, #879, #882, #883, #884, #885, #886, #887, #888, #889, #900).

- **Four boundary classes that were never asserted, two of them on peer bytes.**
  `bufx.StripPadding` was never called with `padLen == len(raw)`, the first
  illegal pad length; `bytesx.ReadVarint` never saw an 8-byte prefix truncated to
  3–7 bytes, though the 2- and 4-byte guards were probed at theirs. Weakening
  either guard by one leaves the whole suite green and turns a rejected frame
  into a panic on attacker-supplied input — `raw[1:0]` in the first case, a read
  of `b[7]` past a 7-byte slice in the second. Alongside them,
  `bytesx.WriteVarint` was only ever handed an oversized `[8]byte`, so the tight
  `VarintLen(v)` bound its contract states went unexercised, and
  `header.Field.Sensitive()` had no case for an `IndexingMode` outside the
  declared three — a value an importer can construct, since the type is an
  exported `uint8` with no constructor. Every one of those mutations now dies;
  the out-of-range mode is **pinned** at the current answer, `false`, not changed
  (#741, #745, #746, #739).


- **Six `client/coverage_test.go` tests could not fail; four of them were
  measuring a fixture that did nothing.** Written to push line coverage past
  90%, they reached their target lines and asserted nothing about them — one
  conceded it in a trailing comment ("Just verifying no panic; branch coverage
  is the goal") — so each passed identically against a correct implementation
  and against one where the function under test did nothing (#859).

  Three of them shared a "server resets the stream" fixture that asked the
  `http.ResponseWriter` for an `http.Hijacker` and closed the raw conn.
  net/http's HTTP/2 writer does not implement `http.Hijacker`, so the type
  assertion always failed: nothing was hijacked, nothing was reset, and
  `Recv`, `WaitTrailers` and `drainResponse`'s reset arms were exercised
  against a clean 200 with an empty body. They now abort with
  `http.ErrAbortHandler`, which puts a real `RST_STREAM(INTERNAL_ERROR)` on the
  wire, and assert that the PEER'S code reaches the caller verbatim — the half
  no sibling covers, and the one `Retryer` decides on (RFC 9113 §8.7).

  Two more had false premises rather than weak assertions.
  `TestPool_EvictDeadSilent_Via_Stats` closed the client and *then* called
  `PoolStats`, which selects on `p.closedCh` and returns a zero `Stats` without
  the actor running — `evictDeadSilent` was never reached. It now kills the
  peer instead and pins both halves of "silent": `ConnsClosed` records the
  eviction, `OnConnClose` stays quiet. `TestPool_HandleClose_GoAwayConn` used
  `srv.Close()`, which drops the socket without a GOAWAY; the reason observed
  was `CloseManual`, so the `CloseGoAway` branch it existed for had never once
  executed. It now injects a real GOAWAY with `srv.Config.Shutdown` and closes
  with the stream still in flight, which is what makes `handleClose` the
  mechanism rather than `handleRelease`'s identical-looking copy.

  Every event-shaped test gained a control arm that injects nothing and an
  injection count read off the wire through a `trace.Tracer`, because a run in
  which the peer never reset or never drained passes exactly like a real one.
  `TestDNSResolver_Resolve_AllFilteredReturnsErrNoAddresses` was deleted: it
  resolved a `.invalid` host, so the lookup FAILED and `Resolve` returned two
  branches before the `ErrNoAddresses` it was named for, which
  `TestResolve_SuccessfulEmpty_DoesNotServeStale` already asserts through the
  internal `dnsLookup` seam.

- **The H2 retention test now measures a quiet connection, which is what makes
  its bound reachable at all.**
  `TestIT_H2_StreamedDownload_RetentionStaysBounded` took its live-heap baseline
  after `DoStream` returned — with the server already streaming 64 MiB. Two
  things followed from that, and the second is the worse one.

  It reset. conn's reader fills a stream's event channel whether or not anybody
  is reading it, and `push` drops the frame and sends `RST_STREAM(CANCEL)` once
  `StreamEventBuffer` (8 here) events are queued, so the two blocking GCs inside
  the baseline were a window the reader could win. Measured: `GOMAXPROCS=1` loses
  it 2 runs in 3, and Gremlins' coverage sweep — `go test ./...`, no `-race`,
  every package at once — lost it twice out of twice on a 4-vCPU runner, both
  times identically at “262141 of 67108864 bytes”, i.e. before the drain loop
  had run once. It is not the flake it resembles: no CI path had ever run this
  suite CPU-starved *and* without `-race` until the mutation gate did.

  And when it did not reset, the baseline had absorbed up to `ltPipelineBytes` of
  the very body being bounded, so `after - baseline` came out **negative** —
  around −1.3 MiB against a 1 MiB bound — and the retention comparison, guarded
  by `after > baseline`, was never evaluated. Cutting the bound to 64 KiB, well
  under the real 134–196 KiB delta, leaves the old test green 3/3 and turns the
  new one red 3/3.

  The fixture now flushes the response headers and parks the handler on a
  `release` channel that the test closes once the baseline is taken. Nothing is
  in flight during the measurement; `ltEventBuf` and `ltRetentionBound` are
  untouched, because widening the channel to buy scheduling slack would have
  loosened the bound the test exists to assert. The delta is now positive, stable
  and about an eighth of the bound, and `GOMAXPROCS=1` is 10/10.

- **The mTLS rejection conformance test waits for the close instead of racing
  it.** `TestConformance_RFC9001_Sec48_ListenerClosesOnRejectedClientCert` called
  `Poll` once and required that one call to have observed the server's
  `CONNECTION_CLOSE`. `Poll` is one step of the connection event loop, so `nil`
  means "made progress", not "the peer closed", and nothing orders the client's
  first `Poll` against the server's rejection — so on a loaded runner the poll won
  and the `rfc` gate went red on unrelated pull requests (#785). It now polls
  until a terminal error arrives, bounded by the same 5-second context, whose
  expiry `Poll` returns — a silent server still fails, and with a truer message.
  The assertions are unchanged.

- **The suite is migrating to Arrange–Act–Assert with `testify` assertions, and
  `header` is the first package through.** `CLAUDE.md` requires both of new and
  edited tests; #722 tracks bringing the existing 2427 across, one issue per
  package. `header/header_test.go` is converted in full — four tests, three
  visible blocks each, `t.Errorf` mapped to `assert` and not to `require` so a run
  reports every mismatch rather than the first, and each original failure message
  carried over as `msgAndArgs`, because `testify` prints expected-vs-actual and
  nothing about why the property matters.

  **`github.com/stretchr/testify` v1.11.1 therefore enters `go.mod`.** Only
  `_test.go` files import it, so it reaches no consumer binary, but it is a direct
  requirement and does appear in an importer's module graph.

  The rewrite was checked by mutation rather than by reading, since an assertion
  that still passes but no longer catches is the failure mode a mass conversion
  produces. Dropping the value length from `Field.Size`, moving `EntryOverhead`
  off 32, pointing `Sensitive` at the wrong mode, and taking the zero value away
  from `IndexIncremental` are each caught 2/2. A fifth, widening `Sensitive` to
  `>=`, survives — a gap in the cases rather than in the conversion, filed as
  #739. This entry covers the sweep; later batches do not each add one.

- **A gRPC conformance test no longer infers a reset from `io.EOF`.**
  `TestConformance_RFC9113_Sec8_1_SendAfterBenignResetStillFails` drained to
  `io.EOF` and then sent, on the stated assumption that "the reset follows the
  trailers on the wire, so the stream is latched closed by the time EOF is
  reported". `io.EOF` reports END_STREAM being *consumed*; `RST_STREAM` is the
  next frame, and nothing orders the two — so on a loaded runner the send reached
  a stream the reader had not torn down yet, and succeeded (#709).

  It now waits for `EventReset` on the transport stream underneath. That orders
  the send for a concrete reason: `endWithReset` pushes the event and sets the
  stream's closed flag inside one `s.mu` section, and `sendData` takes the same
  mutex before testing that flag. The assertion is unchanged — a send after a
  benign reset must still fail. Reproduced deterministically before fixing, by
  delaying the reader's RST handling 50 ms: the old form fails 3/3 and the new
  one passes 3/3, while both pass without the delay.

- **The three remaining `pc.Write` sites in `quic` are gated against a failing
  socket.** A datagram that never left the host is not a lost packet: loss is
  self-healing, since the peer's missing ACK arms a PTO and the frame is resent,
  so a swallowed local write error is the one send failure the transport cannot
  repair, and it presents as a connection that simply waits. #674 gated
  `conn_seal.go`'s and left the `failWritePC` fixture behind; the mutation
  battery that found it deferred the rest under a one-round budget (#676).

  All three propagate correctly today — these are the gates that keep them doing
  so, since deleting any of the checks left the whole `quic` and `http3` suites
  green. They sit where each site is actually observable, which is not uniform:
  `flushBatch` (`gso.go`) through the `Stream.Send` path, `writeAppFrames`
  (`send.go`) through `Stream.Reset`, and `flushControl` (`conn_recv.go`) through
  nothing at all — its only non-test caller discards the error deliberately when
  granting receive credit on the consumer's goroutine, so the gate is on the
  return value and that caller's choice is left alone.

- **Requests are now correlated with their own responses under concurrency, so
  stream mixing can fail a test.** The suite checked that a response came back,
  not that it was *this* request's response: every concurrent test fired the
  identical `GET /healthz` and asserted only the status, so every reply was the
  same two bytes and delivering stream A's response to stream B left both
  assertions passing. The tests were structurally incapable of failing on mixing
  (#651).

  That matters more here than in most clients, because this one pools buffers and
  decodes header blocks into reused storage — the failure mode is not a crash but
  a response, or a header value, belonging to a neighbouring request, and nothing
  above `conn/multistream_test.go` (10 streams, 13-byte bodies, below `client` and
  the pool) could see it.

  Two matrix tests give every in-flight request a distinct identity on both
  channels a response can carry and make each prove it got its own back. Shown to
  discriminate rather than merely to pass: with all responses sharing one body
  buffer, the new test reports 119 mismatches across the four peers while both
  pre-existing concurrency tests stay green; with one header set aliased across
  streams, the header test reports 28 while the existing header tests stay green.

- **`X-Echo-Headers` is implemented by every peer that claims it.**
  `fixtures/CONTRACT.md` has specified it for `/echo` since it was written, and
  only Undertow implemented it — repo-wide the string occurred exactly twice, the
  contract line and that implementation, so no test had ever read a request header
  back. Added to the Go fixture and to nginx; nghttpx inherits it from Undertow,
  its origin.

- **`conn/sendflow_test.go` compares the uploaded bytes, not their count.** It is
  the one upload in the suite that crosses the send window, so it is chunked,
  credited and reassembled across many DATA frames — and a length check passes
  through all of that unchanged, which made the closest thing to an
  upload-corruption detector unable to detect corruption.

- **An interim-then-close request is now proven un-replayed end to end.**
  `ErrServerClosedIdle` means no part of a response ever arrived, which is what
  makes replaying safe; an interim response is the opposite, since `100 Continue`
  is the server saying it has the request head and wants the body — the strongest
  evidence available on that connection that it is acting on the request. `http1`
  pinned the classification over a pipe and `client` pinned how the retry
  classifier reads the error value, but neither drove the retry loop, so nothing
  proved what happens to such a request in practice (#677).

  Driven with GET rather than POST on purpose: `canRetry` refuses a non-idempotent
  method outright, so a POST would be un-replayed for a reason unrelated to the
  interim and the gate would pass with the classification broken. A control arm —
  same fixture, same method, same `Retryer`, differing only in whether any part of
  a response arrived — asserts the replay *does* happen there, without which the
  gate is satisfied by a client that never retries anything.

- **The TCP path now has the leak gate that guards HTTP/3.** The H3 soak exists
  because receive-path resource exhaustion was a bug class here; the TCP path has
  its own long-lived-connection state that nothing soaked — the stream registry,
  the pooled `conn.Stream` free list, the pool's sweep — and its bug history is
  that same shape (the pooled-stream reset class, the conn recycle race). A pooled
  `Stream` keeping one field of the previous response is invisible to a
  request-count assertion and shows up only as a footprint that grows with elapsed
  load (#649).

  `TestSoak_H2ConnStability` and `TestSoak_H2PoolConnStability` mirror the H3 pair
  behind the same `soak` build tag, with a `make h2-soak` target against the
  integration stack. Off the PR path, like `h3-soak`. Shown to discriminate: a
  goroutine leaked per request takes the count from 101,906 to 307,929 and trips
  the ceiling, and a retained-body heap leak trips the heap ceiling at the default
  duration.

### Added

- **The four RFC 9114 §8.1 error codes the constant block was missing** —
  `H3GeneralProtocolError` (0x0101), `H3RequestIncomplete` (0x010d),
  `H3ConnectError` (0x010f) and `H3VersionFallback` (0x0110). Thirteen of
  §8.1's seventeen codes were defined, so `h3ErrorCodeName` returned `""` for the
  other four and a peer sending one of them printed as a bare number. 0x010d had
  a caller waiting: §4.1 requires it of a SERVER, and `poseidon-http-server` had
  to spell the value out locally because this package is otherwise its single
  source for every §8.1 code it sends. Additive only — nothing the client sends
  or interprets changes (#775).

- **Per-request load-test statistics on `client.RequestCompleteEvent`.** The
  event carried enough to count requests and time them end to end, and nothing
  else a load generator records per sample: it could not say when the attempt
  started, when the first byte arrived, how long the request waited for a
  connection, whether it paid for the dial, which protocol carried it, or which
  backend answered. `Attempt` was declared and hard-coded to `0`, so a request
  the `Retryer` replayed three times produced three events indistinguishable
  from three unrelated requests.

  Adds `Start`, `TTFB`, `Acquire`, `Connect`, `Proto` and `RemoteAddr`, and
  fills in `Attempt` for real. The timings nest — `Connect` inside `Acquire`,
  `Acquire` inside `TTFB`, `TTFB` inside `Latency` — so a report can subtract
  them: `Latency - TTFB` is body transfer, `TTFB - Acquire` is server think time
  plus a network turn, and `Acquire` alone is pool queueing.

  `Connect` is charged only to the attempt that actually dialled, which is the
  rule JMeter reports its connect column under; the pooled transports dial on
  their actor goroutine, so a request waiting for a pool dial sees that time in
  `Acquire` and a zero `Connect`. `RemoteAddr` is the only place a managed
  client exposes which backend its `Selector` picked for a given request.

  Internally the transports now return an `exchangeStats` **by value** from
  `openExchange`, and `Retryer` drives `doAttempt`/`doStreamAttempt` rather than
  the exported `Do`/`DoStream`. Both are unexported seams; the per-request
  allocation gates are unchanged at 6 (H2) and 4 (H1.1).

  A failed attempt reports what it observed rather than zeroes: a response head
  that arrived before a reset still carries its `Status` and `BytesRecv`, and a
  managed attempt that never got a connection still names the backend it failed
  against — `RemoteAddr` is empty only when no backend was picked at all. Those
  are the attempts a load test most needs attributed, and zeroing them made a
  503-then-reset indistinguishable from a request that never reached a server.

  Two JMeter per-sample columns are deliberately absent: `responseMessage`
  (HTTP/2 and HTTP/3 have no reason phrase and the HTTP/1.1 parser discards it)
  and byte counts including headers (`BytesSent`/`BytesRecv` stay payload-only;
  an HPACK/QPACK-compressed header block is not comparable with JMeter's
  figure).

- **`trace.ProtoUnknown`.** `trace.Protocol` starts its `iota` at `ProtoH1`, so
  a zero value is indistinguishable from HTTP/1.1 — and `TransportALPN`, the one
  transport that does not know its protocol until the handshake, reports on
  attempts that never got there. Those now say `ProtoUnknown` rather than
  claiming a negotiation that did not happen. Appended to the enum rather than
  made the zero value, so no existing value is renumbered — which means a
  producer must set the field deliberately on every path, failures included.

- **`conn.ConnOptions.ReadBufferSize` and `conn.ConnOptions.StaticConnWindowSize`.**
  Two receive-path parameters could not be set from outside the package, and both
  are parameters a cross-library comparison has to pin before its numbers mean
  anything (#696).

  `ReadBufferSize` is the counterpart to `WriteBufferSize`, replacing the
  compile-time constant that sized the buffered reader. Its floor is a page rather
  than the write side's frame-plus-header: a writer smaller than a frame cannot
  coalesce the header with its payload so every frame costs two writes, while a
  reader has no such cliff — `bufio` refills transparently — so a smaller buffer
  costs syscalls in proportion rather than doubling them.

  `StaticConnWindowSize` raises the connection-level receive window without
  enabling the tuner. `SETTINGS_INITIAL_WINDOW_SIZE` cannot express this: it
  governs per-stream windows only, and the connection window moves only by
  `WINDOW_UPDATE` on stream 0, which previously only `AutoTuneRecvWindow` would
  send — so a caller wanting a larger static window had to accept a tuner whose
  algorithm, ceiling and probe policy are all its own. It matters outside
  benchmarking too: one 65535-byte window per round trip for the whole connection
  is about 6.5 MB/s at 10 ms RTT however fast the link is. Ignored when
  `AutoTuneRecvWindow` is set, because one value written by two policies is the
  state worth not having.

  **Defaults are unchanged.** Zero on either field reproduces the previous
  constants, and a connection that sets neither refunds exactly what it spent.

- **`quic.Conn.RemoteAddr() net.Addr`.** A `Listener` knew every accepted
  connection's peer address and kept it private, so nothing built on the server
  role could learn it: an HTTP/3 server could not populate
  `http.Request.RemoteAddr`, and any IP-keyed policy above it — per-client rate
  limiting, allowlists, abuse logging — was blind. `Listener.Addr()` is the local
  socket, and `tls.ClientHelloInfo.Conn` is nil under QUIC, so neither was a
  substitute (#710).

  Implemented as a type assertion on the connection's `PacketConn` rather than by
  widening the `PacketConn` interface, which would have broken every in-memory
  transport in the tree. `connPacketConn` — the listener's per-connection view of
  its shared socket, and the only thing that knows the peer, since the shared
  socket is unconnected — now answers it. The client role is unchanged and gets
  the method for free: a `*net.UDPConn` from `net.DialUDP` already reports its
  peer. A transport that cannot report one yields nil rather than panicking.

  Documented as "the peer's address as last observed": connection migration
  (RFC 9000 §9) is not implemented, so the value is fixed for the connection's
  life today, and that wording keeps the door open.

### Performance

- **One HTTP/1.1 exchange costs 25 allocations instead of 30.**
  `commitHeaderLine` built each header field as
  `header.Field{Name: []byte(name), Value: []byte(value)}` — two allocations per
  line, measured at 5.0 and 4.0 allocs/op of the benchmark's 30 with
  `-memprofilerate=1` attributed to those two lines. Both are substrings of the
  same logical field line, so they now share one backing array and cost 5.0
  together (#630).

  `BenchmarkReadResponse_Head` goes 30 → **25 allocs/op** and 1184 → 1176 B/op,
  stable across five runs. The saving scales with header count: the benchmark's
  fixture carries five headers, and a real response carrying ten or fifteen pays
  proportionally more.

  The three-index slice on `Name` is load-bearing rather than decoration — without
  the cap, an `append` to a caller's `Name` would write into `Value`'s bytes
  instead of copying, which is the defect #485 fixed on the push path's slab.
  The slab is deliberately per-field and not one buffer for the whole block: a
  growing shared slab reallocates mid-block, leaving the earlier fields pinning
  the old array, and `http1` has no ownership handle like `conn`'s `HeaderBlock`
  to hand a pooled one out behind.

  `exchangeAllocCeiling` drops to 25 and its accounting comment is re-measured
  rather than adjusted. The gate is two-sided, and was checked in both
  directions: 24 fails (a regression came back) and 26 fails (the win was not
  locked in).

  Not touched, and worth recording so it is not hunted twice:
  `asciiLowerHeaderName` still lowers in place, because
  `http1/conn.go:1725-1735` documents that this is what keeps retained bytes ≤
  bytes received — "which is what makes the header cap mean what it says. Found
  by FuzzReadResponse." And `validFieldValue([]byte(value))` does not appear in
  the profile at all: escape analysis already keeps that conversion off the heap.

- **A standalone ACK no longer allocates.** `flush` built the frame payload for a
  packet carrying no STREAM data from a nil slice, so the standalone-ACK path
  allocated once per ACK on a lossless in-order connection and up to four times as
  ACK ranges accumulate under loss — measured with `buildACK` from a nil
  destination: 1.00 allocs/op at one and two ranges, 2.00 at six, 4.00 at twenty,
  against 0.00 for a reused destination (#689).

  This is a different site from the one #475 fixed beside it. That scratch is the
  ACK Range section, which is empty when a single contiguous range is
  acknowledged — which is why it measured 0.00 → 0.00 on a clean path and only paid
  under loss. This one is the frame payload buffer, which has to exist for every
  ACK however few ranges it carries, so it pays on the lossless path too. The
  existing gate could not see it because it hands `buildACK` a pre-allocated
  destination, so it measures the Range scratch and not the caller's `nil`.

### Fixed

- **`quic.ErrNoProgress`: giving up on an unresponsive peer is no longer reported
  as a raw socket error.** When a read deadline expired with nothing left to do —
  no packet in flight to probe, no loss timer due, no ACK owed, and either no idle
  timeout in effect or the probe backoff exhausted — the receive path returned the
  transport's own read error verbatim. A caller therefore received
  `*net.OpError` ("read udp …: i/o timeout") out of `Do` and could not tell "the
  peer went quiet" from "the socket broke" without matching on the message text
  (#717).

  That branch now reports `ErrNoProgress`, distinct from `ErrIdleTimeout`, which
  means the negotiated `max_idle_timeout` elapsed (RFC 9000 §10.1) — a different
  event that can be much later, or never, since a connection may negotiate no idle
  timeout at all.

  **Nothing that classified this error before stops working.** The value returned
  still reports `Timeout() true` and unwraps to the original read error, so
  `errors.As` into `net.Error` and `errors.Is(err, os.ErrDeadlineExceeded)` both
  still succeed. That is why it is a small error type rather than a `fmt.Errorf`
  wrap: the engine's own `isTimeout` classifies with a direct type assertion, not
  `errors.As`, and a plain wrap would have silently changed what every caller doing
  the same assertion sees.

  Only the error surface is addressed. Whether that branch was reached correctly in
  the incident that prompted the report is unreproduced and remains open on #717.

- **Three `NewClient` option errors are classifiable again.** A missing or
  whitespace-bearing `Addr`, a missing `ConnOpts.Dialer`, and a missing
  `TLSConfig` on an HTTP/3 transport were built with a bare `fmt.Errorf` and
  wrapped no sentinel at all, so the documented `errors.Is(err,
  client.ErrInvalidOptions)` check missed them and a caller fell through to
  whatever its generic branch was — treating an unusable configuration as a
  transport failure. Their siblings in the same validation path already wrapped
  it (#713).

  Their messages change shape accordingly, from
  `client: ClientOptions.Addr must be …` to
  `client: invalid ClientOptions: Addr must be …`. Nothing in the tree matched on
  the old text; `errors.Is` was always the supported check and now works.

  `ErrInvalidOptions`' own doc comment said "internally inconsistent", which
  never described these three — they are missing required fields — so it now
  covers both, and names the three sibling sentinels (`ErrInvalidPoolOptions`,
  `ErrALPNProtocolMismatch`, `ErrInvalidTransportKind`) that the other validation
  paths return, since "any option was rejected" means testing for all four.
  `docs/CLIENT_GUIDE.md` claimed this whole family "returns a wrapped sentinel
  error" while quoting one of the three unwrapped messages verbatim; it now names
  the sentinel per path and documents the HTTP/3 `TLSConfig` requirement it
  omitted entirely.
- **A `quic.Listener` that refuses a client's certificate now says so.** When the
  server's TLS handshake failed — a rejected client certificate under
  `ClientAuth: RequireAndVerifyClientCert` being the ordinary case — the listener
  dropped the half-open connection without sending anything. The failure was
  silent at both ends: `Accept` blocked forever, and the client's `Establish`
  returned success, because a TLS 1.3 client is finished once it sends its own
  Finished and never learns its certificate was refused. An operator saw clients
  dial successfully and no requests arrive, with no error anywhere (#711).

  The listener now seals a transport `CONNECTION_CLOSE` (0x1c) carrying
  `CRYPTO_ERROR` — `0x0100` plus the TLS alert, per RFC 9001 §4.8 — into the
  Handshake packet-number space before abandoning the connection, so the client
  surfaces it from `Poll` as a `*PeerClosedError`. The mapping is `closeCodeFor`,
  the one the client role already used from `Conn.fail`; only the server role had
  no sender for it.

  Scoped deliberately to the one abandonment where the peer has proved it holds
  the Handshake keys. A malformed Initial still gets no reply, and a handshake
  that fails before Handshake keys exist (an ALPN mismatch, say) is still silent —
  that path needs an Initial-level close and is tracked in #715.

  **mutual TLS itself was never broken**: a client presenting a certificate valid
  for client authentication completes the handshake and is accepted, before this
  change and after it.

- **A server reaping an idle HTTP/1.1 keep-alive is now recognised on Windows,
  so the request is replayed instead of failing.** `ErrServerClosedIdle` — the
  one H1 failure `client`'s retry classifier is allowed to replay, because no
  part of a response ever arrived — was raised only for `io.EOF`. HTTP/1.1 has
  no protocol signal for a reaped keep-alive, so what the caller sees is
  whatever the local stack reports for a socket the peer has already destroyed,
  and that differs by platform: Linux delivers the queued FIN ahead of the RST
  the next write provokes and the read ends in `io.EOF`, while Windows reports
  `WSAECONNABORTED` and no EOF ever arrives. On Windows the classification
  therefore never fired and the caller got a failed request where every other
  platform transparently reused the connection — on both the pooled and the
  single-connection transports, neither of which probes inside
  `h1ProbeIdleAfter` by design, so replay is the only recovery there is (#684).

  The guard's boundary is unchanged: `firstRead` and `readConsumedNothing` are
  what carry "no part of a response arrived", and neither depends on the errno,
  so an abort arriving after the server began answering is still not replayed.

  Note for anyone writing a similar check: `syscall.ECONNRESET` and
  `syscall.ECONNABORTED` are defined on Windows as synthetic
  `APPLICATION_ERROR` values that no socket call ever returns, so a
  portable-looking `errors.Is` against them compiles and matches nothing. The
  Winsock codes are needed, which is why this is per-platform.

### CI

- **The mutation diff gate no longer fails a pull request whose changed lines
  hold no mutant.** Gremlins scores an empty mutant set as `Test efficacy:
  0.00%` and exits 10, which is indistinguishable from "mutants in your diff
  survived". #903 was merged red on exactly that: its only non-test change was
  four `const` declarations and four `case X: return "STRING"` arms, and no
  Gremlins operator applies to either (#905).

  `mutation.yml`'s existing step already skipped a diff that touches no mutable
  *file*, and it stays — it is also what keeps the shallow-clone hole closed.
  This is the case that filter cannot see, where the file is mutable and its
  changed lines are not. `scripts/mutation-gate.sh` now wraps the Gremlins
  invocation and reinterprets **only** exit 10, and **only** when the run printed
  `Killed: 0, Lived: 0`. Every other status passes through untouched, and an
  exit 10 whose output carries no summary line keeps the failure rather than
  being read as an empty set — so a change in Gremlins' output format breaks the
  build loudly instead of quietly turning the gate into a pass.

  Verified against the real tool on three arms, and the script's decision table
  exercised directly: a `const`-only diff now exits 0 where it exited 10; a diff
  carrying one surviving `CONDITIONALS_BOUNDARY` mutant (`Killed: 1, Lived: 1`,
  efficacy 50%) still exits 10; a fully-killed diff still exits 0. The fix is in
  the Makefile rather than the workflow so that `make mutation` locally and the
  CI job give the same verdict, which is the reason both already shell out to the
  same target.

- **`make lint` runs again on a current Go toolchain.** golangci-lint v2.5.0 is
  built against go1.25 and panics on source the local toolchain compiles as 1.26
  — `panic: file requires newer Go version go1.26` out of
  `goanalysis/runner_loadingpackage.go` — so the target was unrunnable for anyone
  not pinned to an older release. `GOTOOLCHAIN` is now set on the linter
  invocations only (#699).

  Scoping matters here and is the whole content of the fix: the same recipe runs
  `go vet ./...` and the nested-module leg, and `ci.yml` pins a job to
  `GOTOOLCHAIN: local` precisely to assert vet runs under the newer release. A
  recipe-wide export would move vet back to 1.25 and leave that job asserting
  nothing.

- **Lint now type-checks the six files behind `soak`, `e2e_remote` and
  `allocbench`.** None of the three tags was in `.golangci.yml`, so
  golangci-lint reported "build constraints exclude all Go files" for them and
  then printed `0 issues.` — green over those files, and always would have been.
  It is the same defect the config's own note already records for the
  integration harness (#705).

  Measured rather than assumed: with the three tags added the run goes from
  `0 issues.` to exactly two, both `nolintlint` in `client/soak_test.go`
  reporting `//nolint:gosec` directives that are unused because G402 is already
  disabled for this repo. Those two directives are removed here and the run is
  back to `0 issues.`. Nothing else was hiding — the hypothesis that an
  uncompiled lint scope had accumulated real debt does not survive the
  measurement.

- **The nightly mutation-fuzz matrix now covers every `http3` and `qpack` parser.**
  The module has 34 `Fuzz` targets and the matrix listed 15. The other 19 were not
  unrun — ordinary CI replays their seed corpus — but nothing mutated them, so they
  proved the parsers still handle the inputs we already thought of while searching
  for none we had not (#687).

  This is the first slice: `http3` 2 → 7 and `qpack` 3 → 5, the two packages closest
  to unauthenticated peer bytes that the matrix under-covered relative to `quic` and
  `frame`. Each was fuzzed locally before being listed rather than added blind, and
  every one found inputs its seed corpus never reached. The remaining 12 (`client`
  4, `grpc` 3, `http1` 3, `hpack` 1, `quic` 1) are staged deliberately: a target
  that finds something at 3am needs an owner, and adding all 19 at once risks a red
  nightly nobody reads.

## [v0.13.0] — 2026-08-15

### Changed (breaking)

- **`conn.StreamEvent.Slab *[]byte` is now `conn.StreamEvent.Block
  *conn.HeaderBlock`, released with `StreamEvent.Release`, and
  `conn.GetHeaderSlabPool` is gone.** A decoded header block used to be two
  things with one owner: the bytes, a pooled `*[]byte` the consumer had to Put
  into a pool it fetched through an exported accessor, and the field slice, a
  plain `make([]header.Field, len(fields))` on the heap. "How not to leak" was
  therefore part of the interface, and all three consumers reimplemented it —
  `client`, `grpc`, and `conn`'s own benchmarks, which got it wrong and
  silently misreported every allocation figure `conn` had ever published about
  itself (#574).

  `HeaderBlock` owns both. That costs one allocation less per header block —
  one per request in `conn`, two per RPC in `grpc`, since a gRPC response
  carries HEADERS and TRAILERS — and it is safe precisely because the two
  already had the same required lifetime everywhere: every `Name` and `Value`
  points into the bytes, so a consumer keeping the fields had to keep the bytes
  anyway. `TestConn_Roundtrip_AllocsPerRequest` is now **0 allocs/request**
  (was 1) and `grpc`'s `unaryAllocCeiling` **2** (was 4).

  **Migration:** replace `conn.GetHeaderSlabPool().Put(ev.Slab)` — and any
  nil-check around it — with `ev.Release()`. It is nil-safe, so it is correct
  on every arm of a receive loop including those that carry no headers, and it
  nils the event's own pointer so a second call is a no-op rather than a
  double-Put. To retain a block past the event, keep the `*conn.HeaderBlock`
  and `Release` it when done, as `client` does for trailers. Code synthesising
  its own `StreamEvent` can build one with `conn.NewHeaderBlock`, or leave
  `Block` nil and point `Headers` at storage it owns — `client`'s HTTP/1.1
  transport does the latter.

  The lifetime rule is unchanged: everything reachable through `Headers` is
  valid until release and belongs to the next drawer of that block afterwards.
  `DataSlab` and `GetDataBufPool` are deliberately untouched — see the note on
  `StreamEvent.Release` for why.

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

- **17 `quic` symbols are unexported.** Ten frame builders —
  `AppendDataBlocked`, `AppendHandshakeDone`, `AppendNewToken`, `AppendPadding`,
  `AppendPathResponse`, `AppendPing`, `AppendRetireConnectionID`,
  `AppendStopSending`, `AppendStreamDataBlocked`, `AppendStreamsBlocked` — plus
  `BuildInitialPacket`, `NewClientHandshake`, `ProcessDatagram` with the
  `KeySet` and `DatagramResult` types that exist only as its parameter and
  return, and the `ErrNoClientHello` sentinel.

  This is the uncontested tranche of a full audit of the package's 134 orphaned
  exports, classified against `quic/doc.go` and `docs/QUIC_SERVER_DESIGN.md`:
  104 came back as contract and 13 of the remaining 30 were disputed. These are
  what nobody argued for. The builders go because the package emits those
  frames *reactively*, from paths a caller cannot stand in for — PATH_RESPONSE
  from the PATH_CHALLENGE handler, DATA_BLOCKED when connection credit runs out
  — and three had no production caller at all. `BuildInitialPacket` was a
  client-side convenience over the exported `SealPacket`, driven from one site.

  **Migration:** none was needed in practice — no reference to any of the 17
  existed outside `quic/`. A caller building packets by hand uses `SealPacket`;
  a caller opening a connection uses `NewConn`. `ErrNoClientHello` is the one
  judgement call: it does reach an external caller through `Establish`, but the
  package's own docs call it an internal invariant failure and no caller action
  differs from any other handshake failure. Exports 154 → 137, orphans 134 →
  117, with `TestExportedSurfaceDoesNotLeakUnexportedTypes` confirming no
  exported symbol still names any of them — a check `go build` alone does not
  perform. (#611, #609)

- **`conn.ErrFlowControlExhausted` and `frame.ErrUnknownFrameType` are deleted.**
  Neither was ever returned by anything. `ErrFlowControlExhausted` was
  "reserved for future explicit non-blocking write paths" — in a file whose
  sentinels are documented as stable across releases, which made it a
  compatibility promise about a value that did not exist. `ErrUnknownFrameType`
  is not returned either: RFC 7540 §5.5 says unknown frame types are ignored,
  which is exactly what the codec does. Any `errors.Is` against either was
  unreachable code; it now fails to compile instead of silently never matching.
  (#594)

- **`hpack.Decoder.Feed` and `Finish`, called without `Begin`, now return the
  new `hpack.ErrNotStreaming` instead of `ErrInvalidPrefix`.** The two say
  opposite things about who is at fault: `ErrInvalidPrefix` is the sentinel for
  a malformed representation byte *from the peer*, which RFC 7541 §5 makes a
  connection error, while calling `Feed` without `Begin` is the caller's own
  sequencing and entirely local. Anyone mapping sentinels to RFC sections — the
  way `conn`'s dispatch does — was told a local bug came off the wire.
  `ErrNotStreaming` deliberately does **not** match `ErrInvalidPrefix` under
  `errors.Is`: a dedicated sentinel that still matched would look like a fix
  while misinforming the same caller. Narrow in practice — `ErrInvalidPrefix`
  is referenced nowhere outside `hpack`. (#589)

### Added

- **`quic.WithHandshakeTimeout` — the whole-handshake bound is no longer
  hardcoded.** `Establish` wrapped its context in a fixed 10-second timeout, so
  the bound could only ever be *lowered* (by handing it a context with a nearer
  deadline) and never raised. On a lossy path 10 seconds is too tight: the PTO
  ladder doubles per probe (RFC 9002 §6.2.1) and, before any RTT sample has
  shrunk the base, exactly four probes fit inside it — 0.67 + 1.33 + 2.67 +
  5.33 = 9.99 s. A handshake still legitimately probing under
  §6.2.2.1's anti-deadlock rule was abandoned as though the server were gone,
  which is what the interop runner's `handshakeloss` and `handshakecorruption`
  cells hit (50 sequential handshakes at 30% loss).

  ```go
  conn, err := quic.NewConn(pc, tlsCfg, tp, quic.WithHandshakeTimeout(60*time.Second))
  // or, through the HTTP/3 client:
  client.ClientOptions{H3ConnOptions: []quic.ConnOption{quic.WithHandshakeTimeout(60 * time.Second)}}
  ```

  **The default is unchanged at 10 seconds**, and a non-positive value selects
  it, so no existing caller moves. The option composes with the caller's
  context — whichever expires first wins — so a *tighter* bound is still
  expressible through the context alone.

  The bound stays **absolute rather than idle-based** (quic-go's
  `HandshakeIdleTimeout` shape). That is a measured choice, not a preference:
  the failure it has to survive makes no progress to reset on. During the
  outage no ACK, no CRYPTO and no packet of any kind arrives, so an idle bound
  of 10 seconds would expire at the same instant the absolute one does.
  `TestConn_HandshakeTimeout_RescuesALiveHandshake` stages exactly that — a
  live server behind a 15-second outage, abandoned at 10 s under the default
  and completing at 20.6 s when the bound is raised.

- **A frame tracer: `trace.Tracer`, wired through HTTP/2 end to end.** There was
  no way to watch what the client was doing on the wire. `client.Hooks` is
  request-level and `conn.ConnOptions` had no callback at all, so a bug report
  could carry a description of the symptom and nothing else — for HTTP/2 the
  answer was tcpdump plus a way to decrypt TLS, and for HTTP/3 not even that,
  since the QUIC handshake is ours and exposes no keylog. The seam was already
  there and simply not exposed: every inbound frame funnels through
  `frame.Framer.ReadFrame` and every outbound one through its `writeHeader`.

  New package `trace` holds the vocabulary — `Tracer`, the `FrameInfo` value
  struct it receives, and `TextTracer`, a buffered one-line-per-frame renderer.
  It is protocol-neutral for the same reason `header` is: the HTTP/1.1 and
  HTTP/3 seams are later steps, and neither should have to import RFC 7540 to
  borrow a `Direction` constant. Set it via `client.ClientOptions.Tracer`,
  `client.WithTracer`, or `conn.ConnOptions.Tracer`:

  ```
  21:41:41.434055 h2 -> HEADERS stream=1 len=16 flags=END_STREAM|END_HEADERS
  21:41:41.434495 h2 <- DATA stream=1 len=16384
  21:41:41.434566 h2 -> WINDOW_UPDATE stream=1 len=4 incr=32768
  21:41:41.434702 h2 -> GOAWAY stream=0 len=8 last_stream=0 code=NO_ERROR
  ```

  Three things it deliberately does. It reports frames **before** they are
  validated, so a frame that fails its length check or violates §6.10 — the one
  worth seeing — still produces a line. It carries a `Detail` bitmask saying
  which type-specific fields were filled, so a GOAWAY with `NO_ERROR` prints its
  code instead of being indistinguishable from one whose code was never decoded
  (the shape of #570). And it reports **no header values and no DATA payloads**:
  `authorization` and `cookie` live in the header block, and a debug log is the
  thing people paste into a public issue.

  No lock is taken on the emit path: `frame` has no mutex at all, and a
  `Framer`'s tracer state is per-connection. The one lock in the design is
  `TextTracer`'s, and it is held only for a buffer copy — the line is rendered
  into a stack buffer first, because a tracer shared by every connection makes
  its critical section a cross-connection serialization point. Measured flat at
  ~270-360 ns/frame from 2 to 8 contending goroutines, 0 allocs; under backlog
  the drop path is ~17 ns and takes no lock at all.

  Zero *allocation* cost when off is enforced, not asserted: the bench-gate holds
  `frame` at 0 B/op with a tracer installed and discarding
  (`BenchmarkFramer_WriteData_Traced`, `BenchmarkFramer_ReadFrame_Traced`),
  while `TestFramer_Trace_AddsNoAllocations` requires the traced and untraced
  allocation counts to match on every frame type.

  It is not free in wall-clock terms, and the allocation gate does not speak to
  that. Every write path now ends in `writeHeader`'s unconditional `traceOut`
  call, which is a nil check when no tracer is installed but is still a call on
  a path that had none. Measured v0.12.0 → this release, `-count=24`, two
  independent repetitions: `BenchmarkFramer_WritePing` goes 9.82 → 11.08 and
  11.14 ns/op, +12.8% / +13.4%, p=0.000, non-overlapping. That is real, not
  noise — the same-binary-against-itself floor on this machine is ±4.2% — and
  it is ~1.3 ns against an in-memory `bytes.Buffer`, roughly 0.1% of a real
  TLS-plus-socket frame write. Recorded because it is a measured cost of this
  release, not because it is expected to be visible on a socket. (#686)

  A tracer must not block — it fires on the reader goroutine and under the write
  lock — so `TextTracer` buffers, writes from a background goroutine, and drops
  with a reported count rather than waiting on a slow writer.

  Scope: the emit sites are in `frame`, so the HTTP/2 transports are traced.
  The HTTP/1.1 and HTTP/3 seams are separate steps of the same issue; a tracer
  set alongside `TransportH1*`/`TransportH3*` is a no-op today and will start
  producing output without an API change. (#610)

- **`POSEIDON_DEBUG=frames` turns the frame log on in a binary that already
  shipped.** `-tags poseidondebug` exists but is a *build* tag carrying one
  thing, the `Close()`-leak finalizer. `trace.FromEnv` reads the environment
  instead, and `examples/loadgen` honours it. `all`/`1`/`true` are aliases;
  `streams` and `flow` parse and select nothing, reserved for the
  connection-level seam; anything else is a startup error rather than a silent
  fallback to off. (#610)

- **Names for the wire vocabularies.** `frame.FrameType`, `frame.ErrCode` and
  `frame.SettingID` grew `String()`, `frame.FrameType` grew
  `FlagNames(Flags)`, and `http3.FrameTypeName`, `http3.SettingName` and
  `quic.FrameTypeName` name their own registries. Before this, exactly one
  `String()` existed across `frame`, `hpack`, `conn`, `http1`, `http3`, `quic`
  and `qpack` in non-test code. Beyond the tracer it improves error messages for
  free: `conn.ConnError`, `GoAwayError` and `StreamError` format their code with
  `%v` and now print `code=PROTOCOL_ERROR` instead of `code=1`.

  Flag naming hangs off `FrameType`, not `Flags`, because a flag bit has no
  meaning on its own — `0x1` is `END_STREAM` on DATA and `ACK` on SETTINGS, so a
  `Flags.String()` would have to pick one and be wrong about half the
  connection. (#610)
- **`grpc.BorrowMetadata()`, a call option that takes the response header and
  trailer copies from the stream's pooled buffers instead of the heap.** Both
  blocks arrive in a buffer the transport reclaims as soon as the event is
  handled, so `grpc` copies each one out: two allocations a block, four an RPC.
  `DiscardMetadata` already removed them for a caller that never reads the
  metadata, and `Invoke` sets it for itself; a caller that *does* read `Header`
  or `Trailer` had no way out and paid all four. With this option a
  metadata-reading RPC costs exactly what a metadata-discarding one costs —
  measured against the in-process peer, 10 allocations per streaming RPC become
  6, matching `DiscardMetadata` exactly while still returning the metadata.

  It is opt-in because the saving is bought with a lifetime rule: what `Header`
  and `Trailer` return is then valid only until `Close`, since those buffers go
  back to a pool the next RPC on the connection draws from. Copy out anything
  you keep — `string(f.Value)` is a copy, `f.Value` is not. `Close` nils both
  views rather than leaving them pointing into the recycled arena, so reading
  `Trailer()` after `Close` comes back empty instead of carrying another call's
  metadata; that is a backstop, not permission. The default is unchanged and
  remains the answer that cannot be got wrong. `DiscardMetadata` wins when both
  are set. `Status()` is unaffected either way — its message is copied out of
  the live block. See `docs/GRPC_GUIDE.md`.

- **`conn.Conn.SendBatch` — several streams' frames in one transport write.**
  Every send takes the connection's write lock, emits, flushes and releases, so N
  concurrent requests on one connection cost N writes and, over TLS, N records.
  At load-generator concurrency that write is the largest single item in the
  profile: 44% of CPU in the measurements recorded in
  [docs/H2_RAW_FRAMES_DESIGN.md](docs/H2_RAW_FRAMES_DESIGN.md) §7a. `SendBatch`
  emits a `[]BatchEntry` under one hold of the lock and flushes once, so a batch
  is one write while its bytes fit in the write buffer. Measured on the h2c
  harness: 2.002 writes/req for `SendHeaders`+`SendData`, 1.002 for the fused
  `SendHeadersAndData`, **0.033 for a batch of 32** — with `wirebytes/req`
  identical to two decimal places across all three, which is the point stated as
  an invariant. End to end through `examples/h2gen`, a batch of 32 raised
  throughput 74% *and* lowered p50 from 962 µs to 555 µs.

  It never waits. Credit for an entry's body is taken without blocking, because
  blocking under the write lock stalls every other stream and a frame left
  buffered while its writer waits is a deadlock against the peer's
  WINDOW_UPDATE; an entry the windows cannot cover reports the new
  `conn.ErrNoCredit`, having emitted its HEADERS (which are not flow-controlled)
  and not its body, so the caller finishes it with `SendDataV`. It does not defer
  either: it ends in the immediate flush, not the group-commit one, so #360's
  waiting mechanism is not in the path — while still releasing any group-commit
  writer parked behind it.

  It takes requests, not frames, which is a deliberate departure from the design
  sketch this comes from (#438 proposed `WriteFrames(ctx, []FrameChunks)` with
  caller-built wire bytes). Caller-encoded HPACK was that design's premise and
  its own R0 measurements refuted it — +77 bytes per request to save 1.4% of CPU
  — while a caller-chosen stream id cannot advance `nextID` (so a later
  `NewStream` reissues it, an RFC 9113 §5.1.1 violation) and a caller-chosen
  frame type escapes the connection's bookkeeping (a raw RST_STREAM leaks a
  `MAX_CONCURRENT_STREAMS` slot, a raw PING steals a real `Ping`'s ACK). The
  measured win was coalescing all along, and coalescing needs a submission point
  rather than caller-built bytes. §7b of the design doc records this in full.

- **`conn.ConnOptions.WriteBufferSize`**, previously a 16 KiB constant. It is
  what bounds a coalesced write: a batch is one write while it fits and splits
  into `ceil(bytes/WriteBufferSize)` writes when it does not, so a generator
  batching many streams per write is the caller that needs to raise it. Zero
  keeps the old value; values outside [16393, 1 MiB] are clamped, the floor
  being one maximum-size frame plus its header, below which the header/payload
  coalescing the buffer exists for stops working. The group-commit convoy
  threshold was `const groupCommitFlushBytes = writeBufferSize / 2`, evaluated at
  compile time; it now follows the option, since a fixed 8 KiB threshold against
  a 256 KiB buffer — or against a small one — is the exact hazard the threshold
  exists to avoid.

- **`examples/h2gen`**, a framer-shaped load generator built directly on `conn`.
  It exists as the acceptance test for the seam set rather than as a tool: N
  workers per connection hand requests to one sender goroutine that coalesces
  them into a single `SendBatch`, which is ozontech/framer's architecture minus
  the parts poseidon's own measurements refuted. `-batch 1` turns the coalescing
  off and is the control; it prints writes/req either way.

- **`conn/bench_loadgen_test.go`**, the load-generator-shaped benchmark harness
  the numbers above come from: h2c, one connection, N concurrent streams,
  response drained and discarded, against a zero-allocation `frame.Framer` peer.
  The existing `conn` benchmarks measure a TLS round trip against an in-process
  `net/http2` server, which charges the client's `B/op` with every allocation the
  server makes. `BenchmarkLoadGen_Fused_*` also pins the fused one-shot send at
  1.002 writes/req, which nothing did before — every previous harness used the
  split send.

- **A vectored and fused send path.** `SendBatch` above is the coalescing
  submission point; these are the per-stream primitives it and its callers are
  built from. `conn.StreamRef` gains `SendDataV`, `SendHeadersAndData` and
  `SendHeadersAndDataV`; `frame.Framer` gains `WriteDataV` and
  `WriteDataVPadded`. The fused one-shot send is 1.002 writes/req against
  `SendHeaders`+`SendData`'s 2.002. Two new sentinels come with them:
  `conn.ErrNoCredit`, reported by a batch entry the flow-control windows cannot
  cover, and `conn.ErrVecUnderrun`, when a vectored write returns fewer bytes
  than were credited. `conn.DefaultMaxFrameSize` names the 16 KiB constant
  those paths chunk at, which was an unnamed literal in four places (#590).

- **A new top-level `header` package — the RFC-neutral header vocabulary.**
  `http1` imported `hpack` for one symbol and `http3` for two, so RFC 9112 and
  RFC 9114 both appeared, from their import lists alone, to depend on RFC 7541
  header compression. Neither does; they borrowed a struct, because HTTP/2 was
  written first and there was nothing else to share one with. `header` holds
  `Field`, `IndexingMode`, and the dynamic-table entry-size rule — the same
  formula and the same 32 bytes in RFC 7541 §4.1 and RFC 9204 §3.2.1, now named
  `header.EntryOverhead`.

  **Nothing breaks.** `hpack.HeaderField` and `hpack.IndexingMode` are
  **aliases** of the `header` types and the mode constants are re-exported, so
  every existing caller compiles and every existing value stays assignable. The
  signatures that now read `[]header.Field` —
  `hpack.Encoder.EncodeFieldSection`, `http1.Exchange.WriteRequest` and
  `ReadResponse`, `conn.StreamRef.SendHeaders` and
  `SendHeadersWithPriority`, `http3.BodyReader.Trailers`,
  `http3.DecodeTrailers` — are the same type spelled differently. `Size()` and
  `Sensitive()` moved onto `header.Field`, since methods cannot be declared on
  an alias to a non-local type. `conn` and `qpack` keep their `hpack` imports
  on purpose: `conn` *is* HTTP/2, and `qpack` reuses the prefixed-integer codec
  and the Huffman table that QPACK specifies for itself. A CI step checks
  direct imports, because `go list -deps` still shows `hpack` under `http3`
  transitively through `qpack` — the edge that is meant to stay. (#543)

- **`grpc` grows four caller-facing options.**

  `DiscardMetadata()` declines the response header and trailer copies for a
  call that never reads them — four allocations per RPC that no caller could
  previously refuse. `Invoke` is the whole unary path and calls neither
  `Header` nor `Trailer`, so it sets the option for itself, after applying the
  caller's options so nothing asked for is overridden: `Invoke` goes 10 → 6
  allocs/RPC and `InvokeInto` 9 → 5. (`BorrowMetadata()`, above, is the other
  half of the same question — the option for a caller that *does* read the
  metadata.) (#469)

  `Options.ContentSubtype` sends the subtype in `application/grpc+proto` /
  `+json` / a custom codec's. The asymmetry it fixes is the sharp part:
  `validContentType` has always *accepted* a subtype from the server, so the
  package accepted from a peer exactly the thing it had no way to say itself,
  and a server routing on `+json` could not be talked to at all. Validated as
  an RFC 9110 token at `Dial`/`NewClientConn` rather than silently at send
  time — neither `conn` nor `hpack` validates outbound fields, so this is the
  only gate between a caller's string and the wire, and a CR or LF there is a
  request-splitting vector at any HTTP/1.1 downgrading hop. Rendered once per
  connection, so `BenchmarkGRPC_BuildHeaders` stays at 0 B/op. (#468)

  `Options.AllowReservedMetadata` exempts named keys from the `grpc-` namespace
  check, and `(*ClientConn).AppendMetadata` is its connection-bound sibling.
  Refusing the whole prefix is right by the specification — it is reserved for
  *future* protocol use, not only today's names — and wrong for one case:
  `grpc-trace-bin` and `grpc-tags-bin` are written by grpc-go's own
  instrumentation, so a poseidon client pointed at a census-instrumented
  deployment produced orphan spans with no way not to. An allowlist rather than
  two hard-coded names, so one tracing ecosystem's vocabulary is not baked into
  a transport that knows nothing about tracing. It exempts keys from **that
  check only**: the pseudo-header and reserved-key gates run first, pinned by a
  test that allowlists `content-type`, `te`, `grpc-timeout` and `:method` and
  requires every one to still be refused. (#467)

- **Transport failures now arrive as `*grpc.Status`.** A peer resetting the
  stream already became a `*Status`; a connection-level failure that `conn`
  reports by returning an error from `Recv` — a cancelled context, the
  connection closed before the stream was reset — leaked the transport error
  verbatim. Which family a caller got depended on whether `conn` delivered the
  failure as an event or as an error, an implementation detail no caller should
  have to know: complete retry classification needed `errors.As(*Status)` *and*
  `errors.Is(conn.ErrConnClosed)`, and nothing said so. `Status` gains an
  unexported cause and `Unwrap`, so this **adds** a family rather than
  replacing one and the transport error stays reachable through `errors.Is`.
  Context codes stay distinct from `Unavailable` on purpose: a deadline the
  caller set is not the server being unavailable, and conflating them retries a
  request whose deadline has already passed. A `Status` the peer sent has no
  cause — it carried a code and a message, not a Go error. (#532)

- **`http3.H3ConnError` carries the RFC 9114 §8.1 code.** `connError` put the
  code on the wire and returned the bare `ErrH3Control` sentinel, so
  `H3_FRAME_ERROR`, `H3_SETTINGS_ERROR`, `QPACK_DECOMPRESSION_FAILED` and the
  rest were one error above this layer — a pool, a retry policy, a metric or a
  test could not tell a peer's framing bug from a local QPACK failure. The
  HTTP/2 engine already typed this (`conn.ConnError`) and this package already
  typed the stream-level case (`StreamResetError`); only the connection-level
  case was untyped. `errors.Is(err, ErrH3Control)` still matches, so code
  written against the sentinel keeps working; direct `==` comparison does not.
  (#531)

- **`http1.ErrServerClosedIdle`, and an H1 arm in the retry classifier.**
  `builtinShouldRetry` had an H2 arm and an H3 arm and no H1 arm — no `http1`
  import at all — so the canonical retryable HTTP/1.1 failure, a pooled
  keep-alive the server reaps between the checkout probe and the write,
  surfaced as an opaque wrapped EOF and was never retried, while
  `REFUSED_STREAM`, GOAWAY and `H3_REQUEST_REJECTED` always were. The new
  sentinel names the one H1 failure carrying the same guarantee those signals
  carry: the first status-line read returned EOF having consumed nothing, so no
  response existed and replaying cannot duplicate an applied effect. Narrow on
  purpose — an EOF after *any* response byte means the server was answering and
  stopped, which is no evidence about processing, and three boundary cases pin
  that it is not this error. Classifying on the error alone is sound because
  `canRetry` already refuses non-idempotent requests and streaming bodies
  before any classification runs. (#530)

- **`test/interop/quic` — the client endpoint of the quic-interop-runner
  matrix.** A nested module holding the Go binary, its entrypoint script and a
  multi-platform Dockerfile, which the gate below builds from this tree.

  Two wire protocols, because the runner uses two: everything except the
  `http3` case is HTTP/0.9 over QUIC with ALPN `hq-interop`, driven through the
  exported `quic.Conn`/`Stream` API, since the runner's servers offer no other
  ALPN there. `http3` goes through `http3.Dial`. Test-case dispatch is one map
  — every string the runner can send is a row carrying either a function or the
  reason it exits 127, and an absent name exits 127 too, which is what the
  runner's compliance check sends.

  The ClientHello is pinned to X25519. Go also offers X25519MLKEM768 by
  default, whose ~1.2 KB key share made the ClientHello ~1.4 KB; the initial
  flight puts the whole thing in one Initial packet, so the datagram came out
  at 1522 bytes and was IP-fragmented, which RFC 9000 §14 forbids and the
  simulator does not deliver. Every handshake timed out until this was pinned.
  Splitting a large ClientHello across Initial packets is a library-side fix
  not attempted here. The server certificate chain is verified against the
  runner's own CA at `/certs/ca.pem`, falling back to the system roots when
  absent; no path disables verification. (#646, #663)

### Fixed

- **`quic.Conn.Establish` did not latch `terminateLocked`, so an abandoned
  handshake leaked the `crypto/tls` handshake goroutine.** `terminateLocked` is
  the single-close latch, and its own doc says it "runs once on every terminal
  path, which makes it the one place that must not miss" — `Establish` was a
  path it missed. All five of its error returns went straight back to the
  caller, so `c.hs.Close()` never ran. `crypto/tls` parks a `QUICConn`'s
  handshake on a goroutine that only `Close` releases, so every handshake given
  up on stranded that goroutine plus the `tls.QUICConn` and its buffers, for the
  process lifetime. A client dialling an unreachable or brownout peer
  accumulated one per attempt, and `http3.Dial` is such a caller: it closes the
  socket on a failed `Establish` and nothing else.

  `Establish` now latches by wrapping its body rather than by a call at each
  return, so a future error return cannot be added outside the latch. Nothing
  observable changes for a successful handshake; for a failed one the connection
  reports itself terminated, which is what it always claimed to be. Two smaller
  gaps of the same shape went with it: `Poll`'s doc promised "every error return
  latches `terminateLocked`" while its two `ctx.Err()` returns did not, and the
  "rerouting the other teardown paths through it is PR 2c" notes in `conn.go`
  and `close.go` described work that had since been done everywhere but here.

  On Go 1.25 the leak is invisible: `crypto/tls` wires the handshake goroutine's
  escape hatch to the context handed to `QUICConn.Start`, which is the one
  `Establish` cancels on its way out, so the goroutine is released either way.
  Go 1.26 replaced that with an unconditional channel send, and the leak became
  permanent and deterministic — it is what turned `#678`'s own
  `TestConn_HandshakeTimeout_RescuesALiveHandshake` into a `synctest` deadlock
  panic there. `TestConn_Establish_LatchesOnEveryErrorPath` therefore asserts the
  toolchain-independent half of the same single call — after a failed
  `Establish` a caller parked on the connection wakes carrying the handshake's
  error instead of hanging — with one row per error return, and the new
  `quic-next-toolchain` CI job runs `quic` and `http3` under Go 1.26 so the
  goroutine itself stays watched.

- **`Shutdown` armed a 200 ms write deadline on the transport and never cleared
  it, so every send in the graceful drain failed with `i/o timeout`.**
  `writeGoAwayBestEffort` bounds its own write with `closeGoAwayDeadline` so an
  unresponsive peer cannot wedge a teardown, exactly as its sibling
  `writeRSTStreamBestEffort` does — but the sibling clears the deadline again
  inside the same lock hold and this one did not. Under `Close` that was
  invisible: the transport dies immediately afterwards. Under `Shutdown` it was
  the opposite of the contract, whose whole promise is that the connection stays
  alive for `gracefulTimeout` so in-flight streams can finish; every one of those
  writes inherited a deadline 200 ms after the GOAWAY, so any drain worth asking
  for failed the very sends it existed to permit. Pinned by
  `TestShutdown_DoesNotStrandAWriteDeadline`, which waits four times the deadline
  before writing and fails on the pre-fix code.

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

- **A pushed header's value could be appended over the next header's bytes.**
  Every header field handed to a caller views one shared slab. The response
  path clamps each `Name` and `Value` with a three-index slice; the
  push-promise copy, written as "same pattern as `emitHeaderBlock`", used
  two-index slices, so capacity ran to the end of the slab. A caller appending
  to a pushed header's value overwrote its neighbour in place — no copy, no
  error. (#522)

- **Four QUIC correctness fixes, three of them RFC-cited.** A stream's final
  size could change after it was known: RFC 9000 §4.5 requires
  `FINAL_SIZE_ERROR` on a RESET_STREAM or STREAM frame that reports a different
  one, and the companion rule — the final size may not fall below data already
  received — was live but gated behind `connRecvMax != 0`, a condition
  inherited from the flow-control accounting it sits next to and unrelated to
  the check (#645, #643). Stream-quadrant checks were role-blind: quadrants are
  relative to the endpoint reading them (RFC 9000 §2.1), so which quadrant is
  send-only flips between a client and a server `Conn`, and five frame handlers
  had the client's answer written in as a literal (#524). A MAX_DATA /
  MAX_STREAM_DATA grant was queued once and never retransmitted, on the
  reasoning that a later grant supersedes a lost one — but a later grant is
  only produced by consuming more data, and the peer stops sending the moment
  it reaches the limit the lost grant would have raised, so there is no later
  grant and the transfer stalls (#472). And `Finished`, `ResetReceived` and
  `ResetCode` read fields the reader goroutine mutates under `conn.mu` without
  taking it; `RecvState`'s doc three lines away already said a consumer must
  take the lock "rather than via the individual lock-free accessors", so the
  type offered both the rule and the way around it, and `http3`'s control-stream
  servicing took the way around it (#525).

- **Three `frame` codec bugs, all the same shape: a 31-bit field checked before
  it was masked.** `WriteWindowUpdate` rejected a zero increment and *then*
  masked, so `0x80000000` was non-zero, passed the guard, and went out as an
  increment of zero — which the reader, who masks first, treats as a
  PROTOCOL_ERROR (#592). Both priority write sites OR'd the E flag into an
  unmasked `StreamDep`, so a dependency with its high bit set — out of range for
  the field — forged the exclusive flag (#584). And `SetReadBuffer` replaced
  `readBuf` while leaving the pool handle `NewFramer` had taken, so the two
  described different buffers and `Close` donated the **caller's** buffer to the
  shared pool (#595).

- **`Encoder.Reset` did not restore the dynamic table's own cap.**
  `dynamicTable.clear` reset the entries and the arena but not `maxSize`, so
  `Reset` put `localLimit` back to the default while the table went on evicting
  at whatever cap was last set. The encoder then indexed against a budget of
  4096 into a table holding far less: compression degraded for the life of the
  connection, with no size update pending to explain it. (#523)

- **A CONNECTION_CLOSE could carry no transport error code.** `closeCodeFor`
  was a switch, and a switch is a registration point nobody is forced to visit
  — a connection-error sentinel nobody remembered to add there failed nothing,
  and the peer was simply told nothing about why the connection went away. It
  is now a table the test walks, and it sees 29 sentinels where it saw 27.
  (#583)

- **Five client and HTTP/1.1 lifecycle fixes.** A single-conn dial was not
  bounded by `DialTimeout` (#528). The single-conn warmup latched a cancel func
  and never cleared it, so every later `Warmup` returned immediately —
  including the retry after a warmup whose dial failed, the case the latch is
  least entitled to block — and left a 30-second timer armed (#527); warmup
  also ran on the caller's goroutine (#538) and its release needed a
  `sync.Once` (#549). A pool re-dial woke one waiter instead of the whole batch
  (#606). `Connection: close` on an *interim* response was ignored, so the
  connection went back to the pool (#548). And `http3`'s field-value validation
  had drifted from `conn`'s: the two call themselves deliberate mirrors, rule
  for rule, but HTTP/2 grew the edge-whitespace rule and HTTP/3 kept only the
  NUL/CR/LF check, so a value of `" x "` was a stream error on one transport
  and handed to the caller on the other (#529).

- **A must-flush control frame no longer strands the group-commit convoy.**
  WINDOW_UPDATE, PING ACK, RST_STREAM, SETTINGS ACK and the BDP tuner's PING
  cannot be deferred and flushed the buffered writer directly, which pushes a
  deferring writer's bytes out without releasing the writer waiting on them.
  (#581)

- **Connection observability gaps.** Not every connection close was reported to
  the hooks (#540), and the read-buffer pool was sized to a round 16 KiB while
  its one production consumer, `frame.NewFramer`, asks for a whole maximum
  HTTP/2 frame — 16384 payload bytes plus the 9-byte header. Nine bytes short,
  so `New` could never satisfy a request and the pool missed every time (#526).

### Changed

- **The QUIC varint decoder now inlines at every call site.** RFC 9000 §16
  varints are read roughly nine times per packet — packet numbers, frame types,
  stream IDs, offsets, lengths — and `bytesx.ReadVarint` cost 169 against the
  compiler's inlining budget of 80, so it was never inlined at any of its twelve
  call sites. Reading the 2-, 4- and 8-byte forms through `encoding/binary`
  (one load and a byte swap, instead of a hand-rolled shift-and-or chain) brings
  the cost to 74 and all twelve sites inline.

  Measured v0.12.0 → this release, interleaved `-count=24` with an A/A control
  in the same session: `quic.ParseFrames` **−15.6%**, `http3.ParseFrameHeader`
  **−6.2%**, `bytesx.ReadVarint_1` **−40.2%**, `_2` **−16.6%**, all at p≤0.001.
  The 4- and 8-byte forms are unchanged — those are bound by the switch dispatch,
  not by the load.

  **The trade is `http3.ReadStreamType`, which stops inlining and costs +54%.**
  It runs about three times per connection against a saving on every packet, so
  the exchange is lopsided in the right direction, but it is a real cost and is
  recorded rather than netted out. (#695)

- **`BenchmarkStaticIndex_Hit` measured the one input the static-table lookup is
  worst at, and it was the only `staticIndex` benchmark there was.** It pinned
  `:method`/`GET` — table index 2, which the linear scan #459 replaced resolved
  in two comparisons — so it reported the map index as a 51.7% regression while
  the change was worth 2.9× on every real field set. It is renamed
  `BenchmarkStaticIndex_Hit_BestCaseForScan`, body unchanged so the historical
  numbers still compare, and joined by `BenchmarkStaticIndex_ScanVsMap` (eleven
  probes across the table, each run through both the map and the pre-#459 scan
  kept as the correctness oracle) and `BenchmarkStaticIndex_RequestSet` (one
  iteration = one request's worth of lookups). `TestStaticIndex_FixtureDistribution`
  pins the distribution the fixtures claim to model, so the rationale fails CI
  rather than going stale (#685).

  Two structural facts drive the whole picture. Only static-table rows 2..16
  carry a non-empty value, so a field with a non-empty value can never full-match
  past index 16; and the scan returned early **only** on a full match, walking all
  61 rows for a name-only match or a miss. A real request therefore gets two early
  exits — `:method` and `:scheme` — against seven to ten full table walks.

  Measured on the honest distribution, map vs scan in one binary, `-count=24`,
  three independent repetitions (so the comparison does not ride on this host's
  release-to-release noise):

  | lookups | scan | map | delta |
  |---|---:|---:|---:|
  | browser request set, 12 fields | 643.7–677.8 ns | 95.7–101.3 ns | **−84.8% to −85.9%** |
  | gRPC request set, 9 fields | 439.1–459.9 ns | 64.6–67.8 ns | **−85.2% to −86.0%** |

  All at p=0.000, n=24, reproduced in all three repetitions. That reconciles the
  encoder number independently: the lookup saves 546–581 ns per browser request
  set, and `BenchmarkEncoder_RealRequest_Warm` moved 866.6 → 301.0 ns, a 566 ns
  drop. The static-table lookup accounts for essentially all of that −65%.

  **The crossover is between full-match index 4 and index 7.** The scan is ahead
  only where it exits almost immediately: index 2 by 39–55%, index 4 by 12–22%,
  index 3 indistinguishable (the sign flips across repetitions). By index 7 —
  `:scheme`/`https`, which every request sends — the map is 38–44% ahead, and on
  name-only matches and misses it is 76–87% ahead regardless of position. The
  scan's remaining advantage is confined to `:method`/`GET` and, marginally,
  `:path`/`/`.

  A hybrid — scan the first N rows for a full match, fall through to the map —
  was measured and rejected, at every N. The prefix scan is paid in full by every
  field that does *not* short-circuit, and that is 10 of 12 fields on the browser
  set and 8 of 9 on the gRPC one: N=2 costs +61 ns per request against a 97.6 ns
  base, N=4 costs +78 ns, and N=7 costs +157 ns because `:scheme` at index 7 is
  already slower to scan than to hash. No N repairs that, so no change is
  proposed to `staticIndex`.

  The one weak spot the new benchmarks do expose is `:status`: seven rows share
  the name, and walking that value list costs 22.2 ns against 6.6–9.0 ns for
  every other name. It is still 3.9× faster than the scan's 91.7 ns, and this
  client encodes `:status` only when acting as a server, so it is recorded rather
  than acted on.

- **`hpack` indexes the static table by name instead of scanning it.**
  `staticIndex` walked all 61 rows for every field, so a nine-field gRPC request
  cost roughly 549 `bytes.Equal` calls — the bulk of a warm encode. Keyed by
  name only, deliberately: a name+value key would have to be built per lookup
  and concatenating two byte slices allocates, which this package's absolute
  zero-allocation gate forbids, whereas `m[string(name)]` does not allocate
  because the compiler elides the conversion for a map read. The ordering rules
  are pinned directly — a name-only match returns the lowest index carrying that
  name and a full match returns that value's own row, which matters most for
  `:status`, the one header every response carries and the only name with seven
  rows.

  Measured v0.12.0 → this release, `-count=24`:
  `BenchmarkEncoder_RealRequest_Warm` 866.6 → 301.0 ns/op, **−65%**.

  It is slower in exactly one benchmark — the best case for a linear scan — and
  that is the expected trade. The entry above records the crossover, the rejected
  hybrid and the fixture test that now pins the distribution. (#459, #685)

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

- **The build toolchain is pinned to go1.25.13, and that is the documented
  patch floor.** `govulncheck` had been red on every open PR since 2026-08-13,
  on two standard-library advisories rather than on anything in this repo:
  GO-2026-6090 (`crypto/tls`, KeyUpdate handshake-message DoS, CVE-2026-56862,
  reachable from `quic.TLSHandshake`, `conn.FlexDialer.Dial` and
  `http1.Exchange`) and GO-2026-5972 (`encoding/asn1`, stack exhaustion,
  CVE-2026-33818, reached via `client.StreamResponse.Close`). Both are fixed in
  go1.25.13 and go1.26.6.

  Five whys on the red check: the scan found them in go1.26.5; the job ran
  1.26.5 because it asked `setup-go` for `stable`; `stable` resolves only
  through the `actions/go-versions` manifest; that manifest lags a Go security
  release by days — so the job that exists to catch a fresh advisory was
  structurally guaranteed to run on the last unpatched toolchain for exactly as
  long as the advisory was newest. No code change could have cleared it. The
  scan now takes its toolchain from `go.mod`, which `setup-go` will fetch from
  go.dev/dl when the manifest lacks it, so an exact patch version is always
  installable and a future bump is a one-line edit.

  **The `go` directive stays at 1.25.0.** That is this library's compatibility
  promise to importers, and `toolchain` applies only when this module is the
  main one — our builds and CI move to 1.25.13, consumers are not forced to.
  `SECURITY.md` previously told readers to build with "Go 1.26.5 or later",
  which was wrong twice: it named the wrong release line (every other version
  claim in the repo targets 1.25) and go1.26.5 is not a patched toolchain — it
  carries both of the advisories above. The section now names go1.25.13 as the
  floor, with go1.26.6 as the 1.26 equivalent and an explicit warning not to
  read go1.26.5 as patched. (#640, #641)

- **The full benchmark sweep moved to tags; pull requests get a fast allocation
  gate.** `bench-gate` *was* the pull-request wall clock — 16m56s while every
  other job finished by 4m — and more than a third of that bought no
  enforcement: the base-branch bench existed only to feed a benchstat `ns/op`
  diff that was explicitly labelled informational and gated nothing.

  What the job enforces is a boolean, not a measurement: `bench-gate.sh` fails
  on any `Benchmark` line reporting non-zero `B/op` or `allocs/op`. A boolean
  does not need 2s per benchmark to be true — but it does need a *duration*
  rather than a fixed iteration count, because a fixed count cannot amortise
  the one-time setup charged to iteration 1. Measured over all 43 benchmarks in
  the seven gated packages, with verdicts taken from the real gate script:

  | benchtime | benchmarks flipping to a false failure |
  |---|---:|
  | `1x` | 11 of 43 |
  | `10x` | 11 of 43 |
  | `100x` | 5 of 43 |
  | `100ms` | **0 of 43** — identical verdicts, green 4/4 runs, 563× margin |

  So pull requests keep the real gate at `-benchtime=100ms -count=5` over all
  seven packages (~2 min in CI) and lose only the informational diff. The full
  2s sweep plus that diff now runs on `v*` tags and on `workflow_dispatch`,
  with the previous release as its baseline — `github.base_ref` is empty on a
  tag push — and the package list filtered to what exists at that tag, since
  `internal/bufx` does not exist at v0.12.0. The `//go:build !race`
  `AllocsPerRun` gates in `conn`, `client`, `grpc` and `http1` are untouched;
  they run in the test job and are the only defence there. (#664)

  **What a contributor sees:** a PR no longer reports `ns/op` deltas against
  its base branch. Allocation regressions still fail the PR exactly as before.
  To get the numbers back on a branch, dispatch `bench-gate` manually.

- **A new `quic-interop` gate asserts the supported/unsupported partition in
  both directions.** The gate is not "the matrix passed". The observed matrix
  is compared, cell for cell, against a partition committed at
  `.github/interop/expected.json`: every cell declared supported must come back
  `succeeded`, and every cell declared unsupported must come back exactly
  `unsupported`.

  The second half is the half a "no failures" check cannot do. The runner does
  not count `unsupported` as a failure, so a supported case that starts exiting
  127 — a capability regression — passes such a check silently. In the other
  direction, if key update or ECN ever lands and nobody removes the row from
  the support table, we keep publishing "cannot do it" about a client that can.
  Both are red here until someone edits the table.

  Pull requests get a short leg (two servers × handshake, transfer, retry,
  2m45s); tags and `workflow_dispatch` get the whole non-measurement matrix
  plus the exit-127 assertion, about 13 minutes. The partition is per server
  where it genuinely is — `rebind-port` passes against quic-go and fails
  against ngtcp2, and the two servers disagree about `connectionmigration`
  rather than disagreeing about us.

  Against flakiness: every third-party image is pinned by digest and the runner
  by commit, so nobody else's release moves this gate, and there is no retry.
  Three full-matrix runs agreed on 19 of 22 cells against both servers; the
  five `(cell, server)` pairs that did not are all network fault injection, are
  declared not-asserted with their per-run outcomes, and are still printed every
  run. Two runs were not enough to establish that — after two, two of those
  cells looked stable, and the third run contradicted it. Five silent-skip
  guards exist because a workflow that no-ops is worse than no workflow:
  tshark's version is asserted rather than assumed, `run.py`'s exit status is
  deliberately ignored (it is the FAILED count, which is zero when the
  compliance check skipped every pairing), the result file is uploaded with
  `if-no-files-found: error`, a null cell is a hard error, and the set of cells
  that ran must equal the set being asserted. The endpoint image is built from
  this tree in the job, so the gate depends on nothing published to any
  registry. The three `h3-interop*` jobs are untouched. (#679)

- **Two of the nightly fuzz job's sixteen cells were fuzzing nothing, and the
  job reported them green.** `go test -fuzz` exits 0 when its pattern matches no
  target — it prints `no fuzz tests to fuzz` and passes — so a misplaced or
  misspelled target name is indistinguishable from a clean five-minute run in
  the job summary. The `./internal/bytesx` cell carried no `target` key at all,
  which expanded to `-fuzz '^$'`; the `./internal/bufx` cell named
  `FuzzReadVarint`, which lives in `bytesx`. Both exited 0 in 0.007s. The
  practical consequence is that the QUIC varint decoder — which parses peer
  bytes ahead of every packet and frame field — had never been mutation-fuzzed,
  for as long as those cells existed.

  All sixteen were checked mechanically rather than by eye, by walking every
  `func Fuzz*` in the module and resolving each cell against it: 14 OK, 2
  broken, none naming a target absent from the whole repo. `bytesx` gets its
  real target; the `bufx` cell is dropped rather than given a ceremonial one,
  since its only peer-input parser is `StripPadding`, already reached by
  `FuzzFramerReadFrame` through all three `frame.dispatch*` padded paths. The
  matrix goes 16 → 15.

  The actual fix is the **new guard step**, which resolves each target with
  `go test -list` before fuzzing and fails loudly when it is absent — this is
  the defect that hid both cells, and it was invisible in the job summary. The
  campaign that should have been running all along was then run: 10m00s,
  356,116,126 executions on `FuzzReadVarint`, PASS, no crash and no reproducer.
  Honestly caveated — the corpus saturated at 12 entries in the first three
  seconds and never grew, so that is many draws from a small space rather than
  broad exploration. The newly-wired cell was proved to have teeth by mutating
  the 8-byte bounds guard, which it caught in 0.10s. No product code changed,
  and no defect was found; the gap predates this release. (#682)

- **`internal/bytesx` is split into `internal/bytesx` and `internal/bufx`.**
  One directory held two unrelated utility sets with **zero** overlapping
  consumers: `frame` uses the read-buffer pool, the big-endian uint24/uint31
  helpers and the RFC 7540 padding strip; `quic` and `http3` use the QUIC
  varint codec. They shared a directory because both are "low-level byte
  stuff", which describes the files rather than any consumer. `bytesx` keeps
  the varint codec — the thing its name most suggests, and the half with the
  most importers — and `bufx` takes the HTTP/2 helpers. Both are `internal/`,
  so no public API moves. `bench-gate` and `nightly` list their packages
  explicitly and now name `./internal/bufx`; without that the read-buffer
  pool's zero-alloc benchmark would have kept passing by not being run, which
  looks identical to passing. (#542)

- **`frame.Framer.SetMaxReadFrameSize` is renamed `SetMaxFrameSize`, with the
  old name kept as a deprecated alias.** The limit bounds reads *and* every
  write — `writeFrame` checks it and eight more write paths check it directly —
  and the old doc comment spent its first sentence explaining the name away,
  which is the tell. No caller breaks; the alias is pinned by behaviour rather
  than by compiling, since the test drives a too-large frame through the write
  path under both names, and the write half is exactly what the old name
  denied. (#593)

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
