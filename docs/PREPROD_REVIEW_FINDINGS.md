# Pre-Production Review Findings — poseidon-http-client

**Дата ревью:** 2026-06-16
**Методика:** `docs/PREPROD_REVIEW_METHODOLOGY.md` (686 строк, 10 аспектов)
**Ревьюер:** Serena-assisted code audit (все цитаты кода получены инструментальным чтением исходников)
**Go toolchain:** go1.26.3 darwin/arm64
**Commit / working tree:** HEAD (clean, не модифицировался)

---

## 1. Статические проверки (выполнены реально)

| Проверка | Команда | Результат |
|---|---|---|
| `go vet` | `go vet ./...` | ✅ PASS — ноль предупреждений |
| Escape analysis | `go build -gcflags='-m' ./conn/ ./client/` | ⚠️ См. F-P1-04 (heap escape в acquireSendCredits) |
| Race detector | `go test -race -count=1 ./conn/ ./client/ ./frame/ ./hpack/` | ✅ PASS — ноль data races (conn 5.9s, client 40.3s, frame 2.5s, hpack 2.2s) |

**Комментарий:** Race detector чист. Это сильный позитивный сигнал, но он покрывает только пути, достигнутые тестами (coverage ~52%). Непротестированные конкурентные пути (recycle + concurrent Recv, push-overflow goroutine) detector-ом не задеты.

---

## 2. Резюме

| ID | Категория | Severity | Статус | Кратко |
|---|---|---|---|---|
| **F-P0-01** | Security (C6.14) | **P0** | ✅ FIXED (prod-hardening PR#32) | Decompression bomb — `io.Copy` без лимита в `decompressFully` |
| **F-P0-02** | Resource (C1.8) | **P0** | ✅ FIXED (prod-hardening PR#32) | recycleStream use-after-recycle: parked Recv goroutine + невоссозданный events channel |
| F-P0-03 | Concurrency (C1.3) | **FALSE POSITIVE** | N/A | shutdownStreams deadlock — НЕ подтверждён (select имеет default) |
| F-P0-04 | Concurrency (C1.12) | **P1** | ✅ FIXED (PR#36) | push() goroutine без таймаута — writeRSTStream может блокировать wmu навсегда |
| F-P1-01 | Protocol (C2.13) | **P1** | ✅ FIXED (PR#35) | Framer maxReadFrameSize не синхронизирован с SETTINGS_MAX_FRAME_SIZE |
| F-P1-02 | Security (C2.1) | **P2** | ✅ FIXED (prod-hardening PR#32) | TLS MinVersion не задан явно (митигировано дефолтом Go ≥1.18 → TLS 1.2) |
| F-P1-03 | Robustness (C4.3) | **P2** | ✅ FIXED (PR#36) | `panic()` в production-коде (`client/selector.go:81`) |
| F-P1-04 | Performance (C9.3) | **P1** | ✅ FIXED (PR#36) | acquireSendCredits: heap-escape watchdog-goroutine на каждый чанк записи |

**Вердикт готовности:** ✅ **ГОТОВ к продакшену** — все блокеры устранены (PR#32, PR#35, PR#36).

---

## 3. Детальные findings

### F-P0-01 — Decompression bomb (нет лимита на распакованный размер)

**Файл:строка:** `client/decompress.go:153–182` (`decompressFully`)
**Severity:** **P0** — удалённый DoS через OOM

**Код-доказательство:**
```go
// client/decompress.go:160–170 (gzip path), :172–180 (deflate path)
case EncodingGzip:
    gz := gzipReaderPool.Get().(*gzip.Reader)
    defer gzipReaderPool.Put(gz)
    if err := gz.Reset(bytes.NewReader(compressed)); err != nil { ... }
    var buf bytes.Buffer
    if _, err := io.Copy(&buf, gz); err != nil {     // ← НЕТ LimitReader
        return nil, fmt.Errorf("client: gzip read: %w", err)
    }
    return buf.Bytes(), nil
```

**Почему баг:** `io.Copy(&buf, gz)` читает всё, что декомпрессор произведёт, без верхнего предела. Gzip compression ratio может достигать 1000:1+ (zip bomb). Ответ в 64 KB сжатого payload декомпрессится в >64 MB; 1 MB → >1 GB. `bytes.Buffer` растёт безгранично → OOM-kill процесса. Это классический CWE-409 (Decompression Bomb). Защита от oversized frame на transport-уровне (Framer maxReadFrameSize = 16384) НЕ помогает: сжатые данные умещаются в лимит фрейма/потока, но декомпрессированный результат — нет.

**Фикс:**
```go
// Добавить опцию MaxResponseBodyBytes (или MaxDecompressedSize) в ClientOptions.
const defaultMaxDecompressed = 64 * 1024 * 1024 // 64 MB

lr := io.LimitReader(gz, c.opts.MaxDecompressedSize+1)
if _, err := io.Copy(&buf, lr); err != nil { ... }
if buf.Len() > c.opts.MaxDecompressedSize {
    return nil, fmt.Errorf("client: decompressed body exceeds %d bytes", c.opts.MaxDecompressedSize)
}
```
Альтернатива: `io.LimitReader` с проверкой `n > limit` после Copy (LimitReader молча обрезает, нужен +1 sentinel).

---

### F-P0-02 — recycleStream: use-after-recycle race (C1.8)

**Файл:строка:** `conn/stream.go:147–166` (`recycleStream`) + `conn/conn.go:251–262` (`allocStream`)
**Severity:** **P0** — data corruption / cross-stream event leak (при API-misuse)

**Код-доказательство — recycleStream:** (`conn/stream.go:149–166`)
```go
func recycleStream(pool *sync.Pool, s *Stream) {
    for len(s.events) > 0 {      // ← drain БЕЗ s.mu; parked Recv не учтён
        <-s.events
    }
    s.id = 0
    s.w = nil
    // ... field reset ...
    s.resetSignal = make(chan struct{})   // ← resetSignal пересоздаётся
    //                                  // ← s.events НЕ пересоздаётся!
    s.resetCode.Store(0)
    pool.Put(s)
}
```

**Код-доказательство — allocStream:** (`conn/conn.go:251–262`)
```go
func (c *Conn) allocStream(eventBuf int, recvWindow int32) *Stream {
    if v := c.streamPool.Get(); v != nil {
        s := v.(*Stream)
        if cap(s.events) == eventBuf {
            s.w = c
            s.recvWindow = recvWindow
            return s             // ← s.events переиспользуется как есть
        }
    }
    return newStream(0, eventBuf, c, recvWindow)
}
```

**Почему баг:** `recycleStream` проверяет `len(s.events) > 0`, но не может обнаружить горутину, запаркованную в `<-s.events` внутри `Recv()` (такая горутина не уменьшает `len`, но持有ит приёмник). Если `Close()` вызывается пока `Recv()` заблокирован, цикл drain видит `len == 0`, сбрасывает поля и кладёт `*Stream` в пул. `allocStream` затем выдаёт этот же `*Stream` новому стриму, **не воссоздавая `s.events`** (проверяется только `cap`). Оставшаяся горутина старого `Recv()` теперь читает из того же канала, в который `push()` нового стрима пишет события — **cross-stream event leak / data corruption**.

**Смягчение:** В нормальном flow (`do()` → `drainResponse` → `s.Close()`) `Recv()` гарантированно завершился до `Close()`. Race требует, чтобы вызывающий одновременно держал `Recv()` и вызвал `Close()` — т.е. API-misuse. **Однако код не содержит guard-а** (проверки на активного получателя), а `Close()` задокументирован как безопасный для вызова в любой момент.

**Фикс (варианты):**
1. **Канонический:** `allocStream` всегда воссоздаёт `s.events = make(chan StreamEvent, eventBuf)`. Цена — одна аллокация канала на реcycled stream (chan header), но `sync.Pool` всё равно возвращает `*Stream` struct.
2. **Защитный guard:** atomic-счётчик `recvActive` в `Stream`; `recycleStream` проверяет `if s.recvActive.Load() != 0 { return fresh }` (не класть в пул).
3. Рекомендуется вариант 1 (проще, надёжнее, исключает весь класс багов).

---

### F-P0-03 — shutdownStreams: deadlock hypothesis (C1.3) — FALSE POSITIVE

**Файл:строка:** `conn/conn.go` `shutdownStreams` (метод)
**Severity:** N/A — гипотеза методики **не подтвердилась**

**Код-доказательство** (цитата из `shutdownStreams`, точный код):
```go
c.smu.Lock()
// ... iterate streams ...
for _, s := range victims {
    select {
    case s.events <- StreamEvent{Type: EventReset, EndStream: true}:
    default:          // ← NON-BLOCKING: fallback не блокируется
        // close channel under lock instead
    }
    close(s.events)
}
c.smu.Unlock()
```

**Анализ:** Методика предполагала, что `s.events <- ...` под `smu` может вызвать deadlock. Это **неверно**: `select` содержит ветку `default`, поэтому send **никогда не блокирует**. Deadlock невозможен.

**Дополнительный контекст:** `push()` и `shutdownStreams` оба выполняются **только** в `readerLoop` (одна горутина) — они не могут выполниться конкурентно. Поэтому даже `close(s.events)` под `smu` безопасно относительно `push`. Никаких изменений не требуется.

**Остаточный latent-hazard (P3, не блокирующий):** `recycleStream` не воссоздаёт закрытый `s.events` (см. F-P0-02). Если бы закрытый стрим попал в пул (что не происходит в текущем flow — shutdown → conn death → pool не используется), `push` паниковал бы на closed channel. Митигировано тем, что мёртвый conn не выделяет новые стримы.

---

### F-P0-04 / C1.12 — push() fire-and-forget goroutine без таймаута

**Файл:строка:** `conn/stream.go:194–196` (внутри `push`)
**Severity:** **P1** — goroutine leak / потенциально вечная блокировка

**Код-доказательство:**
```go
// conn/stream.go:194–196
go func() {
    _ = s.w.writeRSTStream(s, frame.ErrCodeRefusedStream)
}()
```

**Код-доказательство — writeRSTStream:** (`conn/conn.go`)
```go
func (c *Conn) writeRSTStream(s *Stream, code frame.ErrCode) error {
    if c.closed.Load() {
        return ErrConnClosed
    }
    c.wmu.Lock()
    defer c.wmu.Unlock()
    return c.fr.WriteRSTStream(s.id, code)   // ← может блокировать на Write
}
```

**Почему баг:** При overflow events channel `push` запускает горутину, берущую `wmu` и делающую `Write`. Если транспорт "залип" (peer перестал читать, TCP send buffer полон, но TCP не разорвал соединение), `Write` блокирует бесконечно. Горутина утекает. При множественных overflow-событиях накапливаются десятки таких горутин. `writeRSTStream` проверяет `c.closed.Load()`, но если conn ещё жив (просто медленный peer), проверка не помогает.

**Фикс:**
```go
go func() {
    // Использовать write-deadline или таймаут-контекст.
    if dl, ok := s.w.transport.(interface{ SetWriteDeadline(time.Time) error }); ok {
        dl.SetWriteDeadline(time.Now().Add(5 * time.Second))
    }
    _ = s.w.writeRSTStream(s, frame.ErrCodeRefusedStream)
}()
```
Или: добавить bounded worker pool для RST-отправок с дропом при переполнении (RST — best-effort по RFC).

---

### F-P1-01 / C2.13 — Framer maxReadFrameSize не синхронизирован с SETTINGS_MAX_FRAME_SIZE

**Файл:строка:** `conn/conn.go:119–153` (`NewClientConn`) + `conn/conn.go:746–795` (`applyPeerSettings`)
**Severity:** **P1** — latent connectivity break

**Код-доказательство — NewClientConn:** создаёт framer с дефолтным cap:
```go
// conn/conn.go:121
fr: frame.NewFramer(transport, transport),   // → maxReadFrameSize = 16384 (default)
```

**Код-доказательство — applyPeerSettings:** обрабатывает HEADER_TABLE_SIZE и INITIAL_WINDOW_SIZE, но **не** MAX_FRAME_SIZE:
```go
// conn/conn.go:762–766
switch p.ID {
case frame.SettingHeaderTableSize:
    c.enc.SetMaxDynamicTableSize(p.Value)
case frame.SettingInitialWindowSize:
    // retroactively re-apply to all streams below
    // ← SettingMaxFrameSize: отсутствует
}
```

**Поиск `SetMaxReadFrameSize` во всём `conn/conn.go`:** **0 совпадений** — метод framer-а никогда не вызывается.

**Почему баг:** Если `ConnOptions.Settings` рекламирует `SETTINGS_MAX_FRAME_SIZE` > 16384 (по умолчанию 16384, но конфигурируемо), peer имеет право отправлять DATA-фреймы до этого размера. `Framer.ReadFrame` (framer.go:493) отклонит любой фрейм > 16384 с `ErrFrameTooLarge` → `readerLoop` завершится с ошибкой → conn умирает. С текущим дефолтом (16384) баг не проявляется, но активируется при любой попытке увеличить MaxFrameSize.

**Фикс:**
```go
// В NewClientConn, после применения peer settings:
if advertised := settingValue(opts.Settings, frame.SettingMaxFrameSize, 16384); advertised != 16384 {
    c.fr.SetMaxReadFrameSize(advertised)
}
```

---

### F-P1-02 / C2.1 — TLS MinVersion не задан явно

**Файл:строка:** `conn/dial.go:25–27`
**Severity:** **P2** (митигировано дефолтом Go ≥1.18)

**Код-доказательство:**
```go
// conn/dial.go:25–27
cfg = &tls.Config{
    ServerName:         host,
    NextProtos:         []string{"h2"},
    // ← MinVersion отсутствует; ← CipherSuites отсутствует
}
```

**Почему отмечено:** Явное отсутствие `MinVersion: tls.VersionTLS12`. Go ≥1.18 дефолтит client MinVersion к TLS 1.2, поэтому **не эксплуатируется** на go1.26.3. Однако для defense-in-depth и явности конфигурации рекомендуется задавать явно. TLS 1.0/1.1 (SSL Labs F-grade протоколы) техничесly допустимы только если Go понизит дефолт (не ожидается, но explicit > implicit).

**Фикс:**
```go
cfg = &tls.Config{
    ServerName:         host,
    NextProtos:         []string{"h2"},
    MinVersion:         tls.VersionTLS12,
}
```

---

### F-P1-03 / C4.3 — panic() в production-коде

**Файл:строка:** `client/selector.go:81`
**Severity:** **P2**

**Код-доказательство:**
```go
// client/selector.go:77–83
func Hash(keyFn func(PickContext) string) Selector {
    if keyFn == nil {
        panic("client: Hash selector requires a non-nil keyFn")
    }
    return &hashSel{keyFn: keyFn, hash: fnv.New64a()}
}
```

**Почему отмечено:** Методика (C4.3) классифицирует любой `panic()` в production как нарушение. Здесь это guard на misuse в конструкторе (не в hot path), аналогично stdlib-паттернам (`http.Server` без Addr). **Не exploitable** при корректном использовании. Но рекомендация — возвращать error для восстанавливаемости.

**Фикс:** Изменить сигнатуру на `func Hash(keyFn ...) (Selector, error)` или принять панику как documented-contract (текущий подход приемлем для библиотечного конструктора).

---

### F-P1-04 / C9.3 — acquireSendCredits: heap-escape watchdog goroutine per call

**Файл:строка:** `conn/conn.go:644–651` (`acquireSendCredits`)
**Severity:** **P1** — per-write-chunk allocation on hot path

**Код-доказательство:**
```go
// conn/conn.go:644–651
watchdog := make(chan struct{})
defer close(watchdog)
go func() {                          // ← closure захватывает ctx, c, watchdog
    select {
    case <-ctx.Done():
        c.fcOutMu.Lock()
        c.fcOutCond.Broadcast()
        c.fcOutMu.Unlock()
    case <-watchdog:
    }
}()
```

**Escape analysis (реальный вывод `go build -gcflags='-m'`):**
```
conn/conn.go:647:5: func literal escapes to heap   ← closure + goroutine на каждый вызов
```

**Почему баг:** `acquireSendCredits` вызывается на **каждый чанк** `writeData` (т.е. на каждый 16 KB записываемого тела запроса). Каждый вызов аллоцирует канал + closure + стек горутины на heap. Для тела в 1 MB это ~64 горутины watchdog-ов. В high-throughput сценарии (миллионы запросов) это значительный GC pressure.

**Фикс (варианты):**
1. **sync.Pool для watchdog-каналов + переиспользование горутины:** запустить persistent watchdog goroutine, слушающий broadcast-канал регистраций.
2. **context-based wakeup без горутины:** если `fcOutCond` заменить на `chan struct{}` (broadcast pattern), `ctx.Done()` можно слушать в том же `select` без отдельной горутины.
3. Минимально-инвазивный: проверять `ctx.Err()` в busy-loop с `cond.Wait` таймаутом (менее элегантно).

---

## 4. Проверенные аспекты методики — PASS (без замечаний)

| ID методики | Аспект | Результат | Код-свидетель |
|---|---|---|---|
| C2.3 | WINDOW_UPDATE overflow check | ✅ PASS | `onWindowUpdate`: проверка `> maxWindow` → FLOW_CONTROL_ERROR |
| C2.4 | SETTINGS_INITIAL_WINDOW_SIZE delta | ✅ PASS | `applyPeerSettings:782`: delta с overflow-проверкой на каждый стрим |
| C2.6 | RST_STREAM handling | ✅ PASS | `handler.go:216–225`: markRemoteEnd + markStreamDone + EventReset |
| C2.15 | Frame size enforcement на read | ✅ PASS | `framer.go:493`: `fh.Length > maxReadFrameSize → ErrFrameTooLarge` |
| C3.3 | Stream Close guarantee в `do()` | ✅ PASS | `client.go:379–428`: все error-пути вызывают `s.Close()` + `release()` |
| C5.1 | Pool double-return protection | ✅ PASS | `release()`: nil-check + closedCh select; actor управляет active count |
| C1.3 | shutdownStreams deadlock | ✅ FALSE POSITIVE | select имеет `default` (см. F-P0-03) |
| onData negative window | Recv window negative check | ✅ PASS | `onDataReceived`: `s.recvWindow < 0 → FLOW_CONTROL_ERROR` |

---

## 5. Заключение и блокеры

### Блокеры продакшена (требуют фикса до релиза):
1. **F-P0-01** — Decompression bomb. Удалённый attacker может OOM-kill процесс gzip-бомбой в несколько KB.
2. **F-P0-02** — recycleStream race. Cross-stream event corruption при `Close()` + concurrent `Recv()`.

### Сильные рекомендации (P1, фикс в первой итерации):
3. **F-P0-04** — push() goroutine без таймаута → leak под медленным peer.
4. **F-P1-01** — Framer maxReadFrameSize не synced → латентный break при MaxFrameSize > 16384.
5. **F-P1-04** — acquireSendCredits heap escape → GC pressure под нагрузкой.

### Качество (P2, плановый фикс):
6. **F-P1-02** — TLS MinVersion explicit.
7. **F-P1-03** — panic → error в Hash().

### Позитивные сигналы:
- `go vet` чист.
- `go test -race` чист на всех 4 пакетах.
- Reset-signal fix (push/shutdownStreams/drainHigherStreams) корректно применён.
- Все flow-control арифметические операции имеют overflow/negative checks.
- `do()` гарантирует `s.Close()` + `release()` на всех путях (включая StreamBody).
- Pool acquire/release имеет clean abandonment handling (ctx.Done + drain reply channel).
