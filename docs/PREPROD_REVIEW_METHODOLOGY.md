# Poseidon HTTP Client — Строгая методика пред-продакшен ревью

> **Версия:** 1.0  
> **Дата:** 2026-06-16  
> **Цель:** Системный аудит кодовой базы перед production-релизом для high-throughput HTTP/2 reverse proxy backend client.  
> **Критерии готовности:** Все P0-находки закрыты или имеют подтверждённый mitigation. P1-находки имеют tracking issue и план.  
> **Модули:** `frame/`, `hpack/`, `conn/`, `client/`, `internal/bytesx/`

---

## Соглашения

### Уровни критичности

| Уровень | Значение | Действие |
|---------|----------|----------|
| **P0** | Blocker — продакшен невозможен | Блокирует релиз. Обязательно исправить или подтвердить mitigation. |
| **P1** | Critical — высокий риск инцидента | Блокирует релиз. Нужен tracking issue + план до релиза. |
| **P2** | Important — средний риск | Желательно закрыть в текущем цикле. Может быть отложено с обоснованием. |
| **P3** | Nice-to-have — улучшение качества | Backlog. Не блокирует релиз. |

### Инструментарий проверки

```bash
# Race detector — основной инструмент конкарренси-аудита
go test -race -count=20 -timeout=300s ./...
# CPU/mem профайлинг
go test -bench=. -benchmem -cpuprofile=cpu.out -memprofile=mem.out -run=^$ ./...
# Escape analysis
go build -gcflags='-m -m' ./... 2>&1 | grep 'escapes\|moved to heap'
# Lint
golangci-lint run --timeout=5m ./...
# Coverage (детальный)
go test -race -coverprofile=cover.out ./... && go tool cover -func=cover.out
# Fuzz
go test -fuzz=FuzzFrame -fuzztime=5m ./frame/
go test -fuzz=FuzzDecoder -fuzztime=5m ./hpack/
```

---

## 1. Concurrency Safety

**Критичность: P0**  
**Владелец:** Senior Go engineer с опытом HTTP/2 internals

### 1.1 Критерии проверки

| # | Что искать | Как проверять | Критичность |
|---|-----------|---------------|-------------|
| C1.1 | **Lock hierarchy violations** — нет ли инверсного порядка захвата мьютексов, ведущего к дедлоку | Трассировка всех пар захвата: `wmu→smu`, `smu→wmu`, `fcMu→smu`, `fcOutMu→smu`, `fcOutMu→pingMu` | P0 |
| C1.2 | **Goroutine leaks** — горутины, которые никогда не завершаются | `go test -race -count=1 -run=X` + `runtime.NumGoroutine()` до/после. Проверить все `go func()` (conn.go:646, stream.go:194, drainResponse:801) | P0 |
| C1.3 | **Channel deadlock** — запись в unbuffered/buffered channel без читателя | Проверить `events chan StreamEvent` — `push()` non-blocking, но `shutdownStreams` делает `s.events <- ...` под `smu` — если буфер полон, блокировка | P0 |
| C1.4 | **TOCTOU на `closed` atomic** — проверка и действие не атомарны | `writeHeadersWithPriority`: проверяет `closed.Load()`, затем берёт `wmu` — между проверкой и записью conn может закрыться | P1 |
| C1.5 | **`acquireSendCredits` watchdog goroutine** — спавнится на каждый вызов, корректность отмены | conn.go:638-686. Watchdog goroutine завершается через `defer close(watchdog)`. Проверить что нет гонки между `ctx.Done()` и `watchdog` close | P1 |
| C1.6 | **`s.mu` vs `c.fcOutMu` вложенность** — `acquireSendCredits` берёт `fcOutMu`, затем `s.mu` внутри цикла | conn.go:666-678. Убедиться, что ни один другой путь не берёт `s.mu`→`fcOutMu` | P1 |
| C1.7 | **Race на `headersReceived`** — поле помечено "single-goroutine", но `push()` может быть вызван reader-горутиной, а `Stream.Close()` — пользователем | stream.go:89-93. Проверить, что `headersReceived` действительно недоступен из user goroutine | P1 |
| C1.8 | **`recycleStream` race** — возврат в pool при активном читателе | stream.go:153-170. Если один goroutine вызывает `Close()`→`recycleStream`, а другой в это время в `Recv()`, `s.events` может быть закрыт повторно | P0 |
| C1.9 | **`Conn.Shutdown` dual-call** — CAS на `draining`, но wait на `drainDone` | conn.go:310-350. Если первый caller ждёт `drainDone` и таймаут истекает, он вызывает `Close()`, второй caller тоже вызывает `Close()` — idempotent? | P1 |
| C1.10 | **Pool.run select loop** — deadlock если `closeCh` и `acquireCh` одновременно | client/pool.go `Pool.run()`. Проверить порядок обработки в select, особенно `handleClose` vs pending waiters | P1 |
| C1.11 | **rateLimiter cond broadcast** — `Take()` блокирует на cond, но `refillLocked` не всегда broadcast | client/ratelimit.go. Проверить что refill broadcast'ит cond, иначе starvation | P2 |
| C1.12 | **push() goroutine leak** — `go func() { _ = s.w.writeRSTStream(...) }()` на channel overflow | stream.go:194. Если conn закрыт, `writeRSTStream` берёт `wmu` навсегда? Проверить таймаут | P1 |

### 1.2 Файлы и функции для аудита

```
conn/conn.go:
  - Conn struct fields (mutexes: wmu, smu, fcMu, fcOutMu, psMu, pingMu, originsMu, altSvcMu)
  - acquireSendCredits (638-686) — watchdog goroutine, nested locking
  - shutdownStreams (1096-1110) — close(s.events) under smu, potential panic
  - writeHeadersWithPriority (370-435) — smu inside wmu, TOCTOU on closed
  - Shutdown (300-355) — CAS + drainDone race
  - readerLoop (1065-1077) — single reader goroutine lifecycle
  - keepaliveLoop (1027-1057) — Close() self-call from keepalive goroutine
  - onWindowUpdate (688-730) — fcOutMu + s.mu ordering

conn/stream.go:
  - push (175-200) — goroutine spawn on overflow, channel writes after close check
  - recycleStream (153-170) — concurrent access safety
  - Close (305-322) — bothEnded check, recycle race
  - Recv (288-298) — select on events/resetSignal/ctx.Done

client/pool.go:
  - Pool.run (select loop) — all channels: acquireCh, releaseCh, dialDoneCh, statsCh, closeCh
  - handleAcquire/handleRelease — state transitions
  - evict/evictIdle/evictDead — conn lifecycle from goroutine

client/client.go:
  - drainResponse (736-806) — go drainPushedStream goroutine lifecycle
  - Do/do (main request path) — stream Close guarantee on all error paths
```

### 1.3 Обязательные тесты (если отсутствуют)

- [ ] `go test -race -count=50` на всей codebase под нагрузкой
- [ ] Goroutine leak test: `NumGoroutine()` snapshot до/после N requests
- [ ] Concurrent `NewStream` + `Close` + `Shutdown` на одном Conn
- [ ] Concurrent `Do` + `Client.Close` на пуле conns

---

## 2. HTTP/2 Protocol Compliance (RFC 7540/7541)

**Критичность: P0**  
**Владелец:** Engineer с глубоким знанием HTTP/2 spec

### 2.1 Критерии проверки

| # | Что искать | RFC секция | Как проверять | Критичность |
|---|-----------|------------|---------------|-------------|
| C2.1 | **Connection preface** — клиент отправляет magic + SETTINGS | RFC 7540 §3.5 | Проверить `NewClientConn` — отправляет ли `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` + SETTINGS | P0 |
| C2.2 | **Stream ID monotonicity** — client stream IDs только нечётные, строго возрастающие | §5.1.1 | `nextID += 2` в `writeHeadersWithPriority`. Проверить `lastClientStreamID` | P0 |
| C2.3 | **Flow control window max 2^31-1** — overflow должен вызывать FLOW_CONTROL_ERROR | §6.9.1 | `onWindowUpdate`: проверка `newVal > maxWindow` (conn.go:688-730). Проверить DATA frame padding учитывается в flow control | P0 |
| C2.4 | **SETTINGS_INITIAL_WINDOW_SIZE affects all active streams** — при изменении нужно скорректировать все открытые потоки | §6.9.2 | `applyPeerSettings` — проверяет ли она delta применительно к существующим streams? | P0 |
| C2.5 | **GOAWAY: last_stream_id semantics** — только streams ≤ last_stream_id продолжаются | §6.8 | `onGoAwayReceived` + `drainHigherStreams`. Проверить что existing streams > last_stream_id RST'd | P0 |
| C2.6 | **RST_STREAM: immediate stream teardown** — после RST все pending DATA для этого stream отбрасываются | §6.4 | `OnRSTStream` в handler.go — закрывает ли events channel? signalReset? | P0 |
| C2.7 | **HPACK: dynamic table size update** — SETTINGS_HEADER_TABLE_SIZE от peer должен быть соблюдён | RFC 7541 §4.2 | `applyPeerSettings` → `enc.SetMaxDynamicTableSize`. Проверить enforcement | P1 |
| C2.8 | **CONTINUATION frame handling** — последовательность HEADERS+CONTINUATION до END_HEADERS | §6.10 | `connHandler.emitHeaderBlock` / `OnContinuation`. Проверить лимит на размер header block (DoS защита) | P0 |
| C2.9 | **PUSH_PROMISE: only on server-initiated (even) streams** | §8.2 | `OnPushPromise` + `registerPushedStream`. Проверить валидацию stream ID parity | P1 |
| C2.10 | **PING: ACK must have same payload** | §6.7 | `writePingAck` / `deliverPingAck`. Проверить correlation | P1 |
| C2.11 | **Connection-level flow control** — DATA frames декремтируют conn-level окно тоже | §6.9.1 | `onDataReceived` — дебетует `connRecvWindow` и stream-level `recvWindow` | P0 |
| C2.12 | **SETTINGS ACK within reasonable time** — после приёма peer SETTINGS нужно отправить ACK | §6.5 | `OnSettings` → `writeSettingsAck`. Проверить что ACK отправляется синхронно | P1 |
| C2.13 | **Max frame size enforcement** — DATA payload не превышает MAX_FRAME_SIZE | §4.2, §6.9 | Проверить chunking в `writeData` по `peerSettings.MaxFrameSize` | P1 |
| C2.14 | **Header field validation** — pseudo-headers первыми, `:method`/`:path`/`:scheme` обязательны | §8.1.2 | `buildHeaders` в client.go. Проверить валидацию входящих headers | P1 |
| C2.15 | **0-length DATA with END_STREAM** — валидный half-close | §6.1 | Тест-кейс: empty body POST | P2 |
| C2.16 | **Stream state machine** — переходы idle→open→half-closed→closed | §5.1 | Проверить что нет отправки на closed/half-closed-local stream (возвращает ErrStreamClosed) | P1 |
| C2.17 | **HPACK Huffman bomb / oversized header** — DoS mitigation | RFC 7541 §7 | `hpack/decoder.go` — есть ли лимит на декодированный размер? MaxHeaderListSize enforcement? | P0 |
| C2.18 | **SETTINGS frame limits** — не больше одного SETTINGS per ACK; unknown settings ignored | §6.5.1/§6.5.2 | `OnSettings` — проверка duplicate settings, unknown ID handling | P2 |
| C2.19 | **Stream concurrency limit** — не превышать peer's MAX_CONCURRENT_STREAMS | §5.1.2 | `NewStream` — проверяет ли `inflight < peerMaxConcurrent`? Или рассчитывает на RST_REFUSED_STREAM? | P1 |

### 2.2 Файлы и функции для аудита

```
conn/conn.go:
  - NewClientConn — preface + SETTINGS exchange
  - writeHeadersWithPriority — stream ID assignment, flow-control seed
  - writeData — chunking, flow-control
  - onDataReceived — conn + stream window debit
  - onWindowUpdate — overflow check, broadcast
  - applyPeerSettings / applyInitialPeerSettings — delta window adjustment
  - onGoAwayReceived — last_stream_id handling
  - drainHigherStreams (если есть) — graceful drain

conn/handler.go:
  - OnHeaders / OnContinuation — END_HEADERS assembly, header block size limit
  - OnData — flow control enforcement, padding
  - OnRSTStream — stream teardown
  - OnSettings — ACK, delta detection
  - OnPushPromise — stream ID validation
  - OnGoAway — last_stream_id propagation
  - OnPing — ACK correlation
  - OnWindowUpdate — overflow detection

hpack/decoder.go:
  - Decode — Huffman bomb protection
  - MaxHeaderListSize enforcement
  - Dynamic table size compliance

hpack/encoder.go:
  - EncodeBlock — Never-indexed fields (Authorization, Cookie)
  - Dynamic table size sync with peer SETTINGS

frame/framer.go:
  - WriteHeaders / WriteData — frame size enforcement
  - ReadFrame — frame size limit (SETTINGS_MAX_FRAME_SIZE)
  - Frame type validation

docs/RFC_COVERAGE.md — проверить полноту заявленного покрытия
```

### 2.3 Обязательные тесты

- [ ] Запустить все `*conformance_test.go` с `-tags=integration`
- [ ] h2spec (инструмент [summerwind/h2spec](https://github.com/summerwind/h2spec)) против poseidon как client
- [ ] Curl HTTP/2 interop test (nghttp2, h2load)
- [ ] Fuzz: случайные байтовые последовательности в `fr.ReadFrame`

---

## 3. Resource Management

**Критичность: P0**  
**Владелец:** Senior engineer

### 3.1 Критерии проверки

| # | Что искать | Как проверять | Критичность |
|---|-----------|---------------|-------------|
| C3.1 | **Goroutine leaks при Conn.Close** — readerLoop, keepaliveLoop должны завершиться | `runtime.Stack()` дамп при high-concurrency test. Проверить `<-c.readerDone` в keepaliveLoop | P0 |
| C3.2 | **File descriptor leaks** — TLS conn не закрыт при ошибке handshake | `conn/dial.go:Dial()` — `_ = transport.Close()` при ошибке `NewClientConn`. Проверить все error paths | P0 |
| C3.3 | **Stream leak при partial failure** — если `drainResponse` возвращает error, stream не закрыт/recycled | client.go `do()` / `sendRequest()` — гарантирован ли `s.Close()` на всех error путях? `defer s.Close()`? | P0 |
| C3.4 | **Pool slab leak** — `headerSlabPool` slabs не возвращены если пользователь не вызвал `Response.Reset()` | handler.go slab ownership transfer. Проверить leak detection test | P0 |
| C3.5 | **Pool goroutine leak** — `Pool.run()` не завершается если `closeCh` не закрыт | `Pool.Close()` → `closeCh` → `handleClose` → `closedCh`. Проверить таймаут | P1 |
| C3.6 | **Evicted conn cleanup** — `evictDead`/`evictIdle` закрывают conn и уведомляют waiters | client/pool.go. Проверить что evicted conn не возвращается в pool | P1 |
| C3.7 | **gzipReaderPool leak** — `pooledZlibReader` не возвращён в pool если Read error | client/decompress.go. Проверить `Close()` контракт | P1 |
| C3.8 | **encBufPool correctness** — buffer возвращается после `WriteHeaders`, но что при error? | conn.go:425-435 `*buf = block[:0]; encBufPool.Put(buf)`. Проверить defer path | P1 |
| C3.9 | **TLS conn idle timeout** — conn не закрывается при простое (если keepalive off) | `PoolOptions.IdleTimeout` — проверяет ли pool expired conns? `handleTick` | P1 |
| C3.10 | **drainPushedStream goroutine leak** — `go drainPushedStream(...)` fire-and-forget | client.go:801. Если ctx отменен, goroutine может зависнуть на `s.Recv()` | P1 |
| C3.11 | **Waiter leak при pool close** — blocked `acquire()` callers получают ошибку? | `Pool.Close()` → waiters в `acquireCh` получают reply? Проверить `notifyClose` | P1 |
| C3.12 | **singleConn warmup goroutine** — уже известная проблема (SELF_REVIEW #3) | client/single_conn.go. Подтвердить что фикс применён | P2 |

### 3.2 Файлы и функции для аудита

```
conn/conn.go: Close, Shutdown, readerLoop, keepaliveLoop
conn/dial.go: Dial — transport.Close() on error
conn/handler.go: headerSlabPool usage, emitHeaderBlock
conn/stream.go: recycleStream, Close
client/client.go: Do, sendRequest, do, drainResponse — stream lifecycle
client/decompress.go: pooledZlibReader.Close, gzipReaderPool
client/pool.go: Pool.run, handleClose, evict*, serveWaiters, notifyClose
client/single_conn.go: warmup goroutine lifecycle
internal/bytesx/pool.go: GetReadBuf/PutReadBuf
```

### 3.3 Обязательные тесты

- [ ] `ulimit -n 256` + 10000 requests через pool — FD count stable
- [ ] Goroutine count: baseline → N concurrent requests → wait → count returns to baseline
- [ ] Pool slab accounting: total allocated == total returned after GC
- [ ] `lsof -p <pid>` для проверки FD leak

---

## 4. Error Handling

**Критичность: P1**  
**Владелец:** Senior engineer

### 4.1 Критерии проверки

| # | Что искать | Как проверять | Критичность |
|---|-----------|---------------|-------------|
| C4.1 | **Sentinel error coverage** — все публичные error пути возвращают типизированные ошибки | `errors.Is(err, ErrConnClosed)` / `errors.As(err, &StreamResetError{})`. Проверить conn/errors.go и client/errors.go | P1 |
| C4.2 | **Error wrapping** — `fmt.Errorf("...: %w", err)` для traceability | grep `fmt.Errorf` без `%w`. Особенно в pool, retry, client | P2 |
| C4.3 | **Panic safety** — нет ли `panic` в production коде (только в tests) | `grep -rn 'panic(' --include='*.go' | grep -v _test.go` | P0 |
| C4.4 | **Partial failure в upload** — если body upload прерван на середине, stream корректно RST'd? | `writeRequestBody` / `writeBodyReader` — error path закрывает stream? | P1 |
| C4.5 | **Swallowed errors** — `_ = c.fr.WriteGoAway(...)` игнорирует ошибку записи | conn.go:330 Shutdown. Проверить все `_ = ...` patterns | P2 |
| C4.6 | **Retry on stream reset** — REFUSED_STREAM должен ретраиться, CANCEL — нет | client/retry.go `shouldRetryErr`. Проверить differentiate | P1 |
| C4.7 | **DialError propagation** — адрес сохраняется в ошибке для debug | client/errors.go `DialError{Addr, Err}`. Проверить что pool передаёт | P2 |
| C4.8 | **Context cancellation vs stream cleanup** — ctx.Done() в acquireSendCredits/Recv/Take освобождает ресурсы | Проверить что cancel ctx → stream.Close() гарантирован | P1 |
| C4.9 | **Connection-level error vs stream-level** — PROTOCOL_ERROR закрывает conn, STREAM_CLOSED закрывает stream | conn/handler.go — какие ошибки возвращаются наверх в readerLoop? | P1 |
| C4.10 | **GOAWAY error code propagation** — user видит ErrGoAway, retry layer видит что это retryable на новом conn | conn/errors.go `ErrGoAway` + client/retry.go | P1 |

### 4.2 Файлы и функции для аудита

```
conn/errors.go: ErrALPNFailed, ErrTooManyStreams, ErrConnClosed, ErrStreamClosed,
                ErrFlowControlExhausted, ErrUnexpectedPushPromise, ErrGoAway, ErrConnDraining
                ConnError, StreamError — Error(), Unwrap()
client/errors.go: 14 sentinel errors + StreamResetError + DialError
conn/conn.go:     emitConnGoAwayIfTyped — error type classification
client/retry.go:  shouldRetryErr, isHardStop, canRetry
client/client.go: Do/do error propagation, sendRequest error wrapping
conn/handler.go:  On* methods — error return handling
```

---

## 5. Memory / Allocation Profile

**Критичность: P1** (zero-alloc — core value proposition)

### 5.1 Критерии проверки

| # | Что искать | Как проверять | Критичность |
|---|-----------|---------------|-------------|
| C5.1 | **`unsafeStringToBytes` safety** — `unsafe.Slice(unsafe.StringData(s), len(s))` | client.go:553-555. Если результирующий `[]byte` мутируется — corruption. Проверить все call sites | P0 |
| C5.2 | **Pool double-return** — slab возвращается в pool дважды → use-after-free | headerSlabPool, encBufPool, streamPool, gzipReaderPool, readBufPool, replyPool, statsReplyPool | P0 |
| C5.3 | **Pool use-after-recycle** — данные из recycled Stream используются после Put | `recycleStream` обнуляет поля, но если user goroutine держит ссылку на `*Stream` — data race | P0 |
| C5.4 | **Escape analysis на hot path** — параметры, которые должны stay on stack | `go build -gcflags='-m' ./client/ ./conn/` — grep `moved to heap` | P1 |
| C5.5 | **Slice header escape** — `hdrSlicePool`, `uploadBufPool` возвращают `*[]byte` (pointer to slice) — правильно | Проверить что Get возвращает указатель, а не значение | P1 |
| C5.6 | **1 alloc mystery (r.slabs)** — известная проблема, 1 alloc/op не объяснён | Запустить `go test -bench=BenchmarkMockTransport -memprofile` и проанализировать pprof | P2 |
| C5.7 | **HPACK encoder alloc** — `EncodeBlock` не должен аллоцировать на steady state | hpack/encoder.go. Проверить dynamic table reuse | P1 |
| C5.8 | **Response.Body append growth** — `resp.Body = append(resp.Body, ev.Data...)` | client.go:772. Для больших response bodies — reallocation. Проверить pre-allocation если Content-Length известен | P2 |
| C5.9 | **copyHeaders alloc** — `copyHeaders(ev.Headers)` создаёт новый slice | client.go:796. Для push promise — alloc на каждый push | P2 |
| C5.10 | **Benchmark regression detection** — bench-gate.sh должен ловить regressions | `scripts/bench-gate.sh` — проверить baseline в `docs/BENCH_BASELINE.md` | P2 |
| C5.11 | **sync.Pool New func alloc** — `streamPool.New` создаёт `make(chan StreamEvent, ...)` — 1 alloc при cold start | Проверить что pool warmup (pre-fill) покрывает production ramp | P3 |

### 5.2 Бенчмарк-цели (из контекста)

| Benchmark | Цель | Текущее | Статус |
|-----------|------|---------|--------|
| MockTransport Do | ≤ 1 alloc/op, ≤ 64 B/op | 1 alloc/op, 64 B/op | ✅ (mystery alloc) |
| Real conn (h2c) | < 10 alloc/op after warmup | TBD | ❓ Измерить |
| HPACK encode | 0 alloc/op | TBD | ❓ |
| HPACK decode | 0 alloc/op | TBD | ❓ |

### 5.3 Файлы и функции для аудита

```
client/client.go:
  - unsafeStringToBytes (553-555) — unsafe.Slice usage
  - buildHeaders — hdrSlicePool, uploadBufPool
  - drainResponse — resp.Body append, resp.slabs tracking
  - copyHeaders — alloc per push

conn/conn.go:
  - writeHeadersWithPriority — encBufPool get/put
  - allocStream / NewStream — streamPool

conn/handler.go:
  - emitHeaderBlock — headerSlabPool ownership transfer
  - scratch slice reuse

conn/stream.go:
  - recycleStream — field zeroing, channel recreation
  - newStream — pool integration

internal/bytesx/pool.go:
  - GetReadBuf / PutReadBuf — capacity growth path

client/decompress.go:
  - gzipReaderPool — pooledZlibReader lifecycle
```

---

## 6. Security

**Критичность: P0**  
**Владелец:** Security-conscious engineer

### 6.1 Критерии проверки

| # | Что искать | Как проверять | Критичность |
|---|-----------|---------------|-------------|
| C6.1 | **TLS MinVersion** — `TLSDialer.Config` с `MinVersion: tls.VersionTLS12` | conn/dial.go:30-37. Если Config nil → `&tls.Config{}` без MinVersion → TLS 1.0! | P0 |
| C6.2 | **Cert validation default** — `InsecureSkipVerify` не должен быть true по умолчанию | Проверить что `TLSDialer` не устанавливает InsecureSkipVerify. Но Config из user — user может | P0 |
| C6.3 | **ALPN enforcement** — соединение закрывается если ALPN != "h2" | conn/dial.go:48-51. `ErrALPNFailed`. Проверить — да, есть | ✅ |
| C6.4 | **Header injection** — user-supplied headers с CRLF или pseudo-header duplication | `buildHeaders` — валидирует ли `\r`, `\n`, `:` в значениях? Проверить `:method`, `:path` не перезаписываются | P0 |
| C6.5 | **HTTP/2 smuggling vectors** — CL/TE не релевантны для h2, но pseudo-header confusion | Проверить что `content-length` и `:method`/`:path` не могут быть注入ены через user headers | P1 |
| C6.6 | **Request smuggling via :path** — `:path` с `//` или `..` может обойти upstream routing | client.go `buildHeaders` — валидация path | P1 |
| C6.7 | **HPACK CRASH injection** — Huffman decode с crafted sequence → panic/OOM | hpack/decoder.go + huffman_fsm.go. Проверить bounds checking | P0 |
| C6.8 | **Connection coalescing security** — ORIGIN frame от сервера может перенаправить трафик | conn/conn.go `storeOrigins` / `CanCoalesce`. Проверить origin validation | P1 |
| C6.9 | **ALTSVC redirect security** — ALTSVC frame может перенаправить на альтернативный backend | conn/conn.go `storeAltSvc`. Проверить что client не следует ALTSVC автоматически | P2 |
| C6.10 | **Proxy CONNECT security** — conn/proxy.go, проверка host в CONNECT | conn/proxy.go. Header injection через proxy host? | P1 |
| C6.11 | **TLS cipher suite restrictions** — слабые шифры (RC4, 3DES) должны быть исключены | Проверить что Go's crypto/tls defaults применяются. Go 1.21+ defaults are safe | P2 |
| C6.12 | **Cert pinning support** — production proxy может требовать custom RootCAs | TLSDialer.Config передаётся — user-controlled. Достаточно? | P3 |
| C6.13 | **Connection pool isolation** — разные authority → разные conn, не сross-contamination | Pool keyed by addr. Проверить что conn для host A не обслуживает host B | P1 |
| C6.14 | **Decompression bomb** — gzip/deflate decompression без лимита размера | client/decompress.go `decompressFully` — есть ли max size? Zlib bomb | P0 |
| C6.15 | **Frame size limit from peer** — oversized frame → OOM | frame/framer.go `ReadFrame` — есть ли MaxFrameSize enforcement на receive side? | P0 |

### 6.2 Файлы и функции для аудита

```
conn/dial.go:     TLSDialer.Dial — TLS config defaults, ALPN, cert validation
conn/proxy.go:    CONNECT proxy — host validation, auth
conn/conn.go:     storeOrigins, storeAltSvc, CanCoalesce — coalescing security
hpack/decoder.go: Huffman decode — bounds, OOM protection
hpack/huffman_fsm.go: State machine — panic safety
frame/framer.go:  ReadFrame — max frame size, max header list
client/client.go: buildHeaders — header validation, pseudo-header protection
client/decompress.go: decompressFully — decompression bomb protection
```

### 6.3 Обязательные тесты

- [ ] TLS scanner: `testssl.sh` или `nmap --script ssl-enum-ciphers` против poseidon client TLS handshake
- [ ] Fuzz: malformed HPACK → no panic
- [ ] Fuzz: oversized DATA frame → graceful error, no OOM
- [ ] Header injection test: `\r\n` in method/path/headers → rejected
- [ ] Decompression bomb: 1KB gzip → 1GB uncompressed → bounded

---

## 7. API Surface

**Критичность: P1**  
**Владелец:** API design reviewer

### 7.1 Критерии проверки

| # | Что искать | Как проверять | Критичность |
|---|-----------|---------------|-------------|
| C7.1 | **Close/Shutdown idempotency** — повторный вызов не паникует | `Conn.Close`, `Conn.Shutdown`, `Pool.Close`, `Client.Close` — CAS/atomic guard? | P0 |
| C7.2 | **Use-after-Close** — вызов `Do` после `Client.Close` возвращаем error, не panic | client.go `Do` — проверка `closed` перед операцией | P0 |
| C7.3 | **Ctx propagation** — все блокирующие public методы принимают `context.Context` | `Do(ctx,...)`, `DoStream(ctx,...)`, `Recv(ctx)`, `Ping(ctx)`. Найти методы без ctx | P1 |
| C7.4 | **Nil safety** — `NewClient(opts)` с nil fields → defaults, не panic | `ClientOptions.defaulted()`, `ConnOptions.defaulted()` | P1 |
| C7.5 | **Backward compatibility** — нет breaking changes без major version bump | Проверить все exported types/functions в `client/doc.go` | P1 |
| C7.6 | **Response.Reset() must-call contract** — утечка если пользователь не вызвал | Это misuse-prone API. Рассмотреть finalizer или автоматический return | P1 |
| C7.7 | **StreamResponse.Close() ditto** — то же для streaming API | client/response.go StreamResponse | P1 |
| C7.8 | **Options validation** — невалидные PoolOptions/ClientOptions возвращают error | `newPool`, `NewClient` — валидация перед construction | P1 |
| C7.9 | **DrainMode / PushHandler** — задокументированы ли contracts? | client/client.go ClientOptions | P2 |
| C7.10 | **Transport interface** — extensibility для custom transport | client/transport.go, pool_transport.go | P2 |
| C7.11 | **Hooks thread safety** — user callbacks вызываются из goroutine клиента, не блокируют? | client/hooks.go — вызываются синхронно? Документировано? | P1 |
| C7.12 | **Metrics concurrent access** — MetricsSnapshot — copy, не live reference | client/metrics.go `Snapshot()` — возвращаем value type? | P1 |

### 7.2 Файлы и функции для аудита

```
client/client.go:     Client struct, Do, DoStream, Close, Shutdown, Warmup, SetHooks
client/client.go:     ClientOptions — all exported fields, defaults
client/request.go:    Request struct — exported fields, validation
client/response.go:   Response struct, StreamResponse — Reset/Close contract
client/errors.go:     All exported error variables and types
client/hooks.go:      Hooks struct — callback contracts
client/metrics.go:    Metrics, MetricsSnapshot, Counters — snapshot semantics
client/pool.go:       PoolOptions — validation, defaults
conn/conn.go:         Conn public methods — Close, Shutdown, Stats, IsAlive
conn/options.go:      ConnOptions, AdvertisedSettings
conn/dial.go:         Dial, TLSDialer, PlaintextDialer
client/doc.go:        Package-level documentation
```

---

## 8. Test Coverage & Quality

**Критичность: P1**  
**Владелец:** QA-minded engineer

### 8.1 Критерии проверки

| # | Что искать | Как проверять | Критичность |
|---|-----------|---------------|-------------|
| C8.1 | **Coverage ~52% — ниже industry standard 80%** | `go tool cover -func=cover.out` — найти пакеты < 60% | P1 |
| C8.2 | **Fake-green tests** — тесты, которые проходят, но ничего не проверяют | Известный случай: `TestWarmup_Pool_CappedByMaxConns` (исправлен?). Аудит всех assertion на `<= 0` или no-op | P0 |
| C8.3 | **Docker integration tests не запускались** — 51 test в `client/integration_test/` | `make it-test` — проверить, что Docker backends (undertow, nginx, nghttpx) запускаются | P0 |
| C8.4 | **Missing error path tests** — error returns не покрыты | `go tool cover` — найти uncovered error branches. Особенно в handler.go, retry.go | P1 |
| C8.5 | **Race detector flakes** — `-count=20` проходит стабильно? | `go test -race -count=50 -timeout=600s ./...` | P0 |
| C8.6 | **Pre-existing E2E failures** — `TestE2E_Google_ConnStats`, `TestStress_SingleConn_50Sequential` | Проверить что эти failures не indicat реальных багов (Google TLS hang может быть network) | P1 |
| C8.7 | **Stress test gap** — нет тестов для длительной нагрузки (1M+ requests) | Добавить soak test с goroutine leak detection | P1 |
| C8.8 | **Chaos / fault injection** — нет тестов с network corruption, partial writes, slow server | Добавить тесты с `MockTransport` эмулирующим задержки, обрывы | P2 |
| C8.9 | **Fuzz coverage** — frame, hpack, conn fuzz targets | Запустить `go test -fuzz` на всех fuzz targets по 5 мин каждый | P1 |
| C8.10 | **Timeout magic numbers** — известная проблема из SELF_REVIEW | Проверить что все test timeouts выведены из параметров, а не "от балды" | P2 |
| C8.11 | **Edge case coverage** — 0-length body, large headers (>16KB), many trailers, 100+ concurrent streams | Составить матрицу edge cases и проверить покрытие | P1 |
| C8.12 | **Conformance tests** — RFC-specific test cases | `*conformance_test.go` — проверить покрытие всех RFC 7540 frame types | P1 |
| C8.13 | **Lint baseline** — 288 lines golangci-lint warnings (pre-existing) | Просмотреть warnings на P0/P1 severity. `golangci-lint run --severity=high` | P2 |

### 8.2 План повышения покрытия

| Пакет | Целевое покрытие | Приоритет |
|-------|-----------------|-----------|
| `conn/conn.go` (1188 lines) | > 75% | P1 — ядро протокола |
| `conn/handler.go` (351 lines) | > 80% | P1 — frame dispatch |
| `client/client.go` (849 lines) | > 75% | P1 — public API |
| `client/pool.go` | > 80% | P1 — resource management |
| `client/retry.go` | > 80% | P1 — resilience |
| `hpack/` | > 90% | P1 — security critical |
| `frame/` | > 85% | P1 — protocol critical |

### 8.3 Обязательные тестовые прогоны перед релизом

```bash
# 1. Full race test suite
go test -race -count=3 -timeout=300s ./...

# 2. Stress with goroutine leak check
go test -race -count=1 -timeout=600s -run='TestStress|TestE2E' ./client/...

# 3. Docker integration
make it-test

# 4. Fuzz (min 5 min per target)
go test -fuzz=FuzzFrame -fuzztime=5m ./frame/
go test -fuzz=FuzzDecoder -fuzztime=5m ./hpack/
go test -fuzz=FuzzConn -fuzztime=5m ./conn/

# 5. h2spec compliance
h2spec -h 127.0.0.1 -p 443 <poseidon-test-server>

# 6. Coverage gate
make coverage COVERAGE_MIN=75
```

---

## 9. Performance

**Критичность: P1** (production target: high-throughput)  
**Владелец:** Performance engineer

### 9.1 Критерии проверки

| # | Что искать | Как проверять | Критичность |
|---|-----------|---------------|-------------|
| C9.1 | **`wmu` — single global write mutex** — сериализует все записи на conn | conn.go. При 100 concurrent streams все serialize на wmu. Это design choice (RFC требует serial writes), но contention метрика нужна | P1 |
| C9.2 | **`smu` — stream map lock contention** — каждый NewStream и markStreamDone берёт smu | conn.go `writeHeadersWithPriority` (smu under wmu), `markStreamDone`. Для high-concurrency это bottleneck | P1 |
| C9.3 | **`acquireSendCredits` — spinning goroutine** — watchdog спавнится на каждый SendData call | conn.go:638-686. При many streams × multiple SendData calls → goroutine explosion | P1 |
| C9.4 | **WINDOW_UPDATE coalescing** — `recvWindowRefundThreshold = 32768` — это batch size | conn.go. Проверить что refund не создаёт stalls при bursty traffic | P2 |
| C9.5 | **HPACK dynamic table hit rate** — при low hit rate больше Huffman encoding alloc | Проверить что encoder использует dynamic table для common headers (Cookie, Authorization) | P2 |
| C9.6 | **Syscall coalescing** — WriteV vs single Write для DATA frames | frame/framer.go — использует ли buffered writer? Проверить что HEADERS+DATA не = 2 syscalls | P1 |
| C9.7 | **Pool acquire latency** — channel-based acquire/release под contention | client/pool.go `acquire()` → `acquireCh`. Проверить p99 latency при MaxConnsPerHost=10, 1000 concurrent | P1 |
| C9.8 | **Rate limiter contention** — `Take()` берёт mutex на каждый request | client/ratelimit.go. Для 100K RPS — lock contention. Рассмотреть atomic token bucket | P2 |
| C9.9 | **GC pressure** — sync.Pool Starrvation при GC pause | Benchmark before/after GC: `BenchWithGcTrace` | P2 |
| C9.10 | **CPU profile hotspots** — top 10 functions by CPU | `go test -bench=. -cpuprofile=cpu.out && go tool pprof cpu.out` | P1 |
| C9.11 | **Memory profile hotspots** — top allocators | `go test -bench=. -memprofile=mem.out && go tool pprof mem.out` | P1 |
| C9.12 | **Connection warmup effectiveness** — pre-dialed conns vs cold start | client/warmup.go. Benchmark: cold vs warm p99 latency | P2 |

### 9.2 Benchmark план

```bash
# Baseline benchmarks
go test -bench=. -benchmem -benchtime=3s -count=10 -run=^$ ./... | tee bench-baseline.txt

# Under contention (parallel)
go test -bench=BenchmarkMockTransport -benchmem -cpu=1,2,4,8 -count=5 ./client/

# CPU profile
go test -bench=BenchmarkDo_RealConn -cpuprofile=cpu.out -benchtime=10s -run=^$ ./client/
go tool pprof -top -cum cpu.out

# Memory profile
go test -bench=BenchmarkDo_RealConn -memprofile=mem.out -benchtime=10s -run=^$ ./client/
go tool pprof -top -alloc_space mem.out

# Lock profile (requires runtime.SetBlockProfileRate)
go test -bench=. -blockprofile=block.out -blockprofilerate=1000 -run=^$ ./client/
go tool pprof block.out

# Compare against baseline
benchstat bench-baseline.txt bench-current.txt
```

### 9.3 Производственные цели (предлагаемые)

| Метрика | Цель | Измерение |
|---------|------|-----------|
| p99 latency (1 conn, 100 rps) | < 5ms overhead over raw TCP | `BenchmarkDo_RealConn` |
| Allocs per request (warm) | ≤ 2 | `-benchmem` |
| Max throughput (1 conn) | > 10K rps (no body) | parallel benchmark |
| Goroutines per 1K active streams | ≤ 1.05K (watchdog overhead < 5%) | `NumGoroutine()` |

---

## 10. Observability

**Критичность: P2** (важно для production ops, но не блокер)  
**Владелец:** SRE-minded engineer

### 10.1 Критерии проверки

| # | Что искать | Как проверять | Критичность |
|---|-----------|---------------|-------------|
| C10.1 | **Metrics completeness** — Counters покрывают ключевые SLOs | client/metrics.go: RequestsStarted/Succeeded/Errored, Retries, DialsAttempted/Failed, ConnsClosed, GoAwaysReceived. Добавить: StreamResets, FlowControlStalls | P2 |
| C10.2 | **Latency histogram** — есть, но buckets правильные? | client/metrics.go `Histogram`. Проверить bucket boundaries (5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s) | P2 |
| C10.3 | **Hook coverage** — OnRequestStart/Complete, OnRetry, OnDial, OnConnClose, OnResolverUpdate | client/hooks.go. Добавить: OnStreamReset, OnFlowControlStall, OnSettingsChange? | P2 |
| C10.4 | **Hook non-blocking contract** — user callbacks не должны блокировать event loop | Проверить — вызываются ли hooks синхронно из request goroutine? Если да — user может заблокировать | P1 |
| C10.5 | **ConnStats atomicity** — snapshot not cross-field consistent (задокументировано) | conn.go `Stats()`. Для production мониторинга это OK? Рассмотреть lock-based snapshot | P3 |
| C10.6 | **Pool.Stats()** — ActiveConns, InFlightStreams, Waiters, InFlightDials | client/pool.go. Достаточно для capacity planning? | P2 |
| C10.7 | **Logging hooks** — нет structured logging. Только hooks. | Рассмотреть slog integration или leave to user via hooks | P3 |
| C10.8 | **Debug/dump mode** — нет способа dump frame log для debugging | Рассмотреть `ConnOptions.DebugWriter io.Writer` для frame-level trace | P3 |
| C10.9 | **Metrics thread safety** — Counters используют atomic? Histogram под lock? | client/metrics.go `Observe` — lock-based? Performance impact? | P2 |
| C10.10 | **Connection close reason tracking** — CloseReason enum (Idle/Dead/GoAway/Manual) | client/hooks.go CloseReason. Проверить что OnConnClose всегда получает корректную причину | P2 |
| C10.11 | **Prometheus/OpenTelemetry export** — нет нативного экспорта | MetricsSnapshot → user must bridge. Достаточно? Или нужен adapter? | P3 |
| C10.12 | **pprof endpoints** — нет встроенного pprof HTTP handler | Production app должен регистрировать `net/http/pprof`. Документировать | P3 |

### 10.2 Рекомендуемые метрики для production dashboard

```
# Request-level
poseidon_requests_started_total{method,status}
poseidon_requests_duration_seconds_bucket{method,status}
poseidon_requests_in_flight

# Connection-level
poseidon_conns_active{addr}
poseidon_conns_inflight_streams{addr}
poseidon_dials_total{addr,result}
poseidon_goaways_received_total

# Error/health
poseidon_requests_errored_total{error_type}
poseidon_retries_total
poseidon_stream_resets_total{code}
poseidon_flow_control_stalls_total

# Pool
poseidon_pool_waiters{addr}
poseidon_pool_acquire_latency_seconds_bucket
```

### 10.3 Файлы и функции для аудита

```
client/metrics.go:   Counters, Histogram, Metrics, MetricsSnapshot, Observe, Snapshot
client/hooks.go:     Hooks struct, all Event types, CloseReason
client/pool.go:      Stats struct, Pool.Stats()
conn/conn.go:        ConnStats, Stats(), atomic counters
client/client.go:    observeStart, observeDone, Metrics(), MetricsSnapshot()
```

---

## Процесс ревью

### Этапы

```
Phase 1: Static Analysis (1-2 дня)
├── golangci-lint review (288 baseline warnings)
├── Escape analysis dump
├── go vet ./...
├── grep audit: panic(, unsafe., go func(), _ = err
└── Coverage report per-file

Phase 2: Dynamic Analysis (2-3 дня)
├── Race detector: -count=50 на всех пакетах
├── Fuzz: frame, hpack, conn (5 min each)
├── Goroutine leak detection
├── CPU/Mem/Block profiling
└── Docker integration: make it-test

Phase 3: Protocol Compliance (1-2 дня)
├── h2spec run
├── RFC_COVERAGE.md audit vs actual
├── Edge case matrix execution
└── Interop: nghttp2, curl, undertow

Phase 4: Security Audit (1 день)
├── TLS config review
├── Header injection tests
├── Decompression bomb test
├── Frame size fuzz
└── HPACK bomb fuzz

Phase 5: Synthesis (1 день)
├── Findings consolidated
├── P0/P1 triage
├── Mitigation plan
└── Go/No-Go decision
```

### Чеклист готовности к продакшену

- [ ] **P0 находок: 0** (все закрыты или подтверждён mitigation)
- [ ] **P1 находок: все имеют tracking issue + owner + план**
- [ ] `go test -race -count=50 ./...` — PASS, без flakes
- [ ] `make it-test` — PASS (Docker backends запущены и зелёные)
- [ ] Coverage ≥ 70% на `conn/` и `client/`
- [ ] h2spec — PASS (или known deviations задокументированы)
- [ ] Goroutine leak test — PASS
- [ ] FD leak test — PASS
- [ ] Benchmark regression — нет ухудшения vs baseline
- [ ] TLS config — MinVersion TLS 1.2, no weak ciphers
- [ ] Fuzz (5 min per target) — no crash
- [ ] CHANGELOG.md обновлён
- [ ] README.md — production usage guide актуален

---

## Приложение A: Карта известных проблем

| ID | Описание | Файл | Статус | Источник |
|----|---------|------|--------|---------|
| K1 | Fake-green test `TestWarmup_Pool_CappedByMaxConns` | client/warmup_test.go | Fixed (462c179) | SELF_REVIEW |
| K2 | False "lock-free" comment in ratelimit.go | client/ratelimit.go:11 | Fixed | SELF_REVIEW |
| K3 | singleConn warmup goroutine leak | client/single_conn.go | Fixed | SELF_REVIEW |
| K4 | 1 alloc mystery (r.slabs) | client/response.go | Open | Context |
| K5 | Pre-existing lint warnings (288 lines) | All | Baseline | golangci-lint |
| K6 | TestE2E_Google_ConnStats BytesReceived undercount | client/e2e_test.go | Pre-existing | SELF_REVIEW |
| K7 | TestStress_SingleConn_50Sequential Google TLS hang | client/e2e_stress_test.go | Pre-existing | SELF_REVIEW |
| K8 | decompress.go partially unused code | client/decompress.go | Open | Context |
| K9 | Docker backends prepared but never tested | test/integration/ | Open | Context |
| K10 | Coverage gate not enforced (~52%) | Makefile COVERAGE_MIN=80 | Open | Context |

---

## Приложение B: Глоссарий mutex-иерархии

Для предотвращения deadlock-ов, все захваты мьютексов должны следовать следующей иерархии (захват только в порядке возрастания номера):

```
Level 1: wmu (write mutex)           — сериализует все writes to framer
Level 2: smu (stream mutex)          — guards streams map, nextID, inflight
Level 3: fcMu (recv flow-control)    — guards connRecvWindow, connRefundPending
Level 4: fcOutMu (send flow-control) — guards peerConnSendWindow, cond
Level 5: s.mu (per-stream mutex)     — guards stream state, sendWindow
Level 6: psMu (peer settings, RWMutex) — guards peerSettings
Level 7: pingMu                      — guards pingWaiters
Level 8: originsMu / altSvcMu (RWMutex)

VIOLATIONS TO CHECK:
  - smu taken under wmu? YES (writeHeadersWithPriority) — Level 1→2 OK ✓
  - s.mu taken under fcOutMu? YES (acquireSendCredits) — Level 4→5 OK ✓
  - fcOutMu taken under s.mu? MUST NOT HAPPEN — Level 5→4 ✗ CHECK
  - psMu taken under wmu? YES (writeHeadersWithPriority) — Level 1→6 OK ✓
  - fcMu taken under smu? CHECK onDataReceived — Level 2→3 OK ✓
```

> **Важно:** Это предлагаемая иерархия выведена из анализа кода. Ревьюер должен подтвердить или исправить её, трассировав все пути захвата.

---

*Документ поддерживается в актуальном состоянии. Каждая находка добавляется с ID, критичностью, статусом и ссылкой на issue/commit.*
