# Pre-Production Review — CRITIC Report

**Дата критики:** 2026-06-16
**Критикуемый документ:** `docs/PREPROD_REVIEW_FINDINGS.md`
**Метод:** каждое утверждение проверено инструментальным чтением исходного кода через Serena; запущены `go vet ./...` и `go test -race -count=1 ./conn/ ./client/ ./frame/ ./hpack/`.

---

## 0. Статические проверки (независимая верификация)

| Проверка | Команда | Результат |
|---|---|---|
| `go vet` | `go vet ./...` | ✅ PASS (return_code=0, ноль вывода) |
| Race detector | `go test -race -count=1 ./conn/ ./client/ ./frame/ ./hpack/` | ✅ PASS (conn 5.9s, client 40.6s, frame 2.0s, hpack 2.2s) |

Подтверждает базовые заявленные метрики оригинального ревью.

---

## 1. Сводная таблица вердиктов

| Finding ID | Вердикт критика | Severity (критика) | Комментарий |
|---|---|---|---|
| F-P0-01 | **CONFIRMED + INCOMPLETE** | **P0** | Баг реален; ревьюёр пропустил streaming-path и отсутствие лимита на raw body |
| F-P0-02 | **PARTIALLY_CORRECT** | **P2** (снижено с P0) | Наблюдение верно, но триггер практически невозможен через нормальный Close() |
| F-P0-03 | **CONFIRMED (FALSE POSITIVE)** | N/A | Вердикт ревьюёра правилен; код-цитата неточна |
| F-P0-04 | **CONFIRMED** | **P1** | Полностью подтверждено |
| F-P1-01 | **CONFIRMED + INCOMPLETE** | **P1** | Ревьюёр пропустил write-side impact |
| F-P1-02 | **PARTIALLY_CORRECT** | **P2** | Код-сниппет сфабрикован; Concern валиден только для default path |
| F-P1-03 | **CONFIRMED** | **P2** | Полностью подтверждено |
| F-P1-04 | **CONFIRMED** | **P1** | Полностью подтверждено |

**Дополнительно найдено:** 4 пропущенных бага (см. §3).

---

## 2. Постатейная проверка findings

### F-P0-01 — Decompression bomb
**Вердикт: CONFIRMED + INCOMPLETE**

**Код-доказательство** (`client/decompress.go:153–182`, прочитано инструментально):
```go
func decompressFully(enc ContentEncoding, compressed []byte) ([]byte, error) {
    // ...
    case EncodingGzip:
        gz := gzipReaderPool.Get().(*gzip.Reader)
        defer gzipReaderPool.Put(gz)
        if err := gz.Reset(bytes.NewReader(compressed)); err != nil { ... }
        var buf bytes.Buffer
        if _, err := io.Copy(&buf, gz); err != nil {   // ← НЕТ LimitReader — ПОДТВЕРЖДЕНО
            return nil, fmt.Errorf("client: gzip read: %w", err)
        }
        return buf.Bytes(), nil
    case EncodingDeflate:
        // ... аналогично: io.Copy(&buf, zr) без лимита — ПОДТВЕРЖДЕНО
```

**Call chain верификация:**
`drainResponse` (`client/client.go:770`) накапливает raw body:
```go
resp.Body = append(resp.Body, ev.Data...)   // ← без лимита на raw body
```
Затем (`client/client.go:775`):
```go
decoded, derr := decompressFully(enc, resp.Body)  // ← io.Copy без LimitReader
```

В call chain **нет ни одного** `io.LimitReader` или проверки размера. Баг реален.

**Предложенный фикс — корректен.** Сентинел `+1` в `LimitReader` — правильный паттерн.

**Что ревьюёр ПРОПУСТИЛ (INCOMPLETE):**

1. **Streaming path также без лимита.** `newDecompressingReader` (строки 127–152) создаёт `decompressingReader`, чей `Read()` (строка 108) делегирует напрямую в `gz.Read(p)` / `zr.Read(p)` — тоже без `LimitReader`. Если caller использует streaming API (`WantBody=false` + `io.Reader`), декомпрессия тоже безгранична.

2. **Raw body accumulation без лимита.** Даже БЕЗ декомпрессии (`enc == EncodingIdentity`), `resp.Body = append(resp.Body, ev.Data...)` растёт безгранично. Вредоносный сервер может отправить гигабайты uncompressed DATA → OOM. Это **более фундаментальная** проблема, чем decompression bomb.

---

### F-P0-02 — recycleStream use-after-recycle
**Вердикт: PARTIALLY_CORRECT (снижено с P0 до P2)**

**Код-наблюдение — ФАКТИЧЕСКИ ВЕРНО:**
- `recycleStream` (`conn/stream.go:147–166`) не пересоздаёт `s.events` — **подтверждено**.
- `allocStream` (`conn/conn.go:251–262`) проверяет `cap(s.events) == eventBuf`, но не пересоздаёт канал — **подтверждено**.

**НО триггер НЕВОЗМОЖЕН через нормальный путь. Вот почему:**

`recycleStream` имеет **единственного** caller-а во всём кодбейзе — `Stream.Close()` (`conn/stream.go:318`):
```go
// conn/stream.go:311–322 (Close)
if bothEnded {
    // Both sides ended normally; recycle without sending RST.
    if c, ok := s.w.(*Conn); ok {
        recycleStream(&c.streamPool, s)    // ← ЕДИНСТВЕННЫЙ вызов recycleStream
    }
    return nil
}
return s.w.writeRSTStream(s, frame.ErrCodeCancel)
```

`recycleStream` вызывается **только** когда `bothEnded == true`, т.е. `localEnded && remoteEnded`.

Если `Recv()` запаркован (заблокирован на `<-s.events`), это означает, что `remoteEnded == false` (иначе EndStream event уже был бы доставлен). Следовательно `bothEnded == false`, и `recycleStream` **не вызывается**.

**Доказательство невозможности триггера:**
```
Recv() заблокирован  →  нет event в канале  →  remoteEnded == false
                                            →  bothEnded == false
                                            →  Close() НЕ вызывает recycleStream
                                            →  стрим НЕ попадает в пул
```

**Единственный сценарий, в котором race теоретически возможен:**
Пользователь вызывает `Close()` и `Recv()` concurrently из разных горутин. При этом `bothEnded` должно стать true между моментом, когда `Recv()` припарковался, и моментом, когда `Close()` проверяет `bothEnded`. Но для этого reader goroutine должна доставить EndStream event в канал в узком окне между двумя операциями потребителя. Это требует специального API-misuse паттерна, который не является задокументированным use case.

Даже в этом сценарии, docstring `recycleStream` явно говорит: *"Only call when the stream is fully done ... and no goroutine holds a reference."* Контракт соблюдён — `Close()` обеспечивает это через `bothEnded`.

**Оценка фикса:** Вариант 1 (всегда пересоздавать `events` в `allocStream`) — разумная defense-in-depth. Но это не P0 блокер.

---

### F-P0-03 — shutdownStreams deadlock (FALSE POSITIVE)
**Вердикт: CONFIRMED — FALSE POSITIVE правильный**

**Реальный код** (`conn/conn.go:1096–1110`):
```go
func (c *Conn) shutdownStreams(reason error) {
    c.smu.Lock()
    defer c.smu.Unlock()
    for _, s := range c.streams {
        select {
        case s.events <- StreamEvent{Type: EventReset, RSTCode: frame.ErrCodeInternalError, EndStream: true}:
        default:
            s.signalReset(frame.ErrCodeInternalError)    // ← НЕ блокирует
        }
        close(s.events)
    }
    // ...
}
```

`select` содержит `default` → send **никогда не блокирует**. Deadlock **невозможен**. Вердикт ревьюёра — **правильный**.

**Неточности в код-цитате ревьюёра:**
- Ревьюёр цитировал `default: // close channel under lock instead` — реальный код вызывает `s.signalReset(frame.ErrCodeInternalError)`, а не просто комментарию.
- Ревьюёр не указал `RSTCode: frame.ErrCodeInternalError` в EventReset.
- Ревьюёр цитировал `for _, s := range victims` — реальный код итерирует `c.streams` напрямую (нет отдельной `victims` переменной).

Эти неточности не влияют на вердикт, но снижают доверие к точности цитирования.

**Дополнительно верифицировано:** утверждение "push() и shutdownStreams оба выполняются только в readerLoop" — **подтверждено**. Все caller-ы `push()` находятся в `conn/handler.go` (строки 113, 195, 223, 290), который вызывается из `readerLoop`. `shutdownStreams` вызывается только из `conn/conn.go:1072` (внутри `readerLoop`). Одна горутина → конкурентный доступ невозможен.

---

### F-P0-04 — push() goroutine без таймаута
**Вердикт: CONFIRMED**

**Код** (`conn/stream.go:194–196`):
```go
go func() {
    _ = s.w.writeRSTStream(s, frame.ErrCodeRefusedStream)
}()
```

**writeRSTStream** (`conn/conn.go:544–561`):
```go
func (c *Conn) writeRSTStream(s *Stream, code frame.ErrCode) error {
    if s.id == 0 {
        c.releaseUnassignedInflight(s)
        return nil
    }
    if c.closed.Load() {
        return ErrConnClosed
    }
    c.wmu.Lock()
    defer c.wmu.Unlock()
    if err := c.fr.WriteRSTStream(s.id, code); err != nil {   // ← может блокировать
        return err
    }
    // ...
}
```

`c.fr.WriteRSTStream` → `io.Writer.Write` на транспорте. Без write-deadline → может блокировать бесконечно при залипшем peer. **Подтверждено.**

**Дополнительное наблюдение:** `Conn.Close()` (строки 267–294) УЖЕ использует `SetWriteDeadline` для GOAWAY (`closeGoAwayDeadline = 200ms`). Паттерн существует, но не применён к push goroutine. Фикс ревьюёра — корректен и согласуется с существующим кодом.

---

### F-P1-01 — Framer maxReadFrameSize не синхронизирован
**Вердикт: CONFIRMED + INCOMPLETE**

**Код — NewClientConn** (`conn/conn.go:121`):
```go
fr: frame.NewFramer(transport, transport),   // maxReadFrameSize = 16384 (default)
```

**Код — applyPeerSettings** (`conn/conn.go:762–767`):
```go
switch p.ID {
case frame.SettingHeaderTableSize:
    c.enc.SetMaxDynamicTableSize(p.Value)
case frame.SettingInitialWindowSize:
    // retroactively re-apply to all streams below
    // ← SettingMaxFrameSize: ОТСУТСТВУЕТ — ПОДТВЕРЖДЕНО
}
```

**applyInitialPeerSettings** (`conn/conn.go:730–737`) — тоже обрабатывает только `SettingHeaderTableSize`.

Поиск `SetMaxReadFrameSize` в `conn/` — **0 совпадений** (подтверждено). Метод существует (`frame/framer.go:95`) и доступен, но никогда не вызывается.

**Что ревьюёр ПРОПУСТИЛ (INCOMPLETE) — WRITE-SIDE IMPACT:**

Комментарий к `SetMaxReadFrameSize` (`frame/framer.go:88–94`):
> *"sets the maximum frame payload length the Framer will accept on read **AND emit on write**."*

`maxReadFrameSize` используется в **write**-проверках:
- `writeFrame` (строка 118): `if h.Length > f.maxReadFrameSize → ErrFrameTooLarge`
- `WriteData` (строка 157): такая же проверка
- `WriteHeaders` (строка 221): такая же проверка

Это значит: если пользователь сконфигурирует `AdvertisedSettings.MaxFrameSize > 16384`, `writeData` (`conn/conn.go:470`) будет чанковать по `min(peerMax, ourMax)` > 16384, но `writeFrame` **отклонит** фрейм > 16384 → ошибка записи → стрим умирает. Клиент **никогда не сможет** использовать MaxFrameSize > 16384, даже если обе стороны его поддерживают.

**Вердикт по фиксу:** Предложенный фикс — корректен для read-side. Нужно также убедиться, что write-side лимит синхронизирован (или использовать отдельные поля для read/write caps).

---

### F-P1-02 — TLS MinVersion
**Вердикт: PARTIALLY_CORRECT**

**Concern валиден:** отсутствие явного `MinVersion: tls.VersionTLS12` в default path — действительно так.

**НО код-сниппет ревьюёра — ФАБРИКОВАН:**

Ревьюёр цитировал:
```go
// conn/dial.go:25–27 — ЦИТИРОВАНО РЕВЬЮЁРОМ
cfg = &tls.Config{
    ServerName:         host,
    NextProtos:         []string{"h2"},
}
```

**Реальный код** (`conn/dial.go:20–27`):
```go
cfg := d.Config
if cfg == nil {
    cfg = &tls.Config{}           // ← пустой config, НЕТ ServerName, НЕТ NextProtos
} else {
    cfg = cfg.Clone()
}
hasH2 := false
for _, p := range cfg.NextProtos {
    if p == "h2" {
        hasH2 = true
        break
    }
}
if !hasH2 {
    cfg.NextProtos = append([]string{"h2"}, cfg.NextProtos...)
}
```

Реальный код:
1. Не задаёт `ServerName` (но это ОК — `tls.Dialer.DialContext` выводит ServerName из addr).
2. Добавляет `"h2"` в NextProtos программно (а не в литерале).
3. При user-provided `Config` используется `Clone()` — ответственность за MinVersion лежит на пользователе.

**Уточнённая оценка:** Concern о Mitigated P2 остаётся в силе — в default path нет явного MinVersion. Но код-доказательство неточно, что подрывает доверие к остальным цитатам.

---

### F-P1-03 — panic() в Hash()
**Вердикт: CONFIRMED**

**Код** (`client/selector.go:77–83`) — полностью совпадает с цитатой ревьюёра:
```go
func Hash(keyFn func(PickContext) string) Selector {
    if keyFn == nil {
        panic("client: Hash selector requires a non-nil keyFn")
    }
    return &hashSel{keyFn: keyFn, hash: fnv.New64a()}
}
```

Оценка ревьюёра разумна: guard в конструкторе, не exploitable при корректном использовании, но нарушает C4.3. Фикс (возврат error) — acceptable.

---

### F-P1-04 — acquireSendCredits heap escape
**Вердикт: CONFIRMED**

**Код** (`conn/conn.go:644–651`) — полностью совпадает с цитатой:
```go
watchdog := make(chan struct{})
defer close(watchdog)
go func() {
    select {
    case <-ctx.Done():
        c.fcOutMu.Lock()
        c.fcOutCond.Broadcast()
        c.fcOutMu.Unlock()
    case <-watchdog:
    }
}()
```

Goroutine + closure + channel аллоцируется на каждый вызов `acquireSendCredits` (т.е. на каждый чанк `writeData`). **Подтверждено.**

Фикс-предложения ревьюёра — корректны и технически обоснованы.

---

## 3. ПРОПУЩЕННЫЕ БАГИ (не найденные ревьюёром)

### MISSING-01 — `hashSel.Pick` data race (P1)

**Файл:строка:** `client/selector.go:116–123`
**Severity: P1** — data race в concurrent path

**Код:**
```go
type hashSel struct {
    keyFn func(PickContext) string
    hash  hash.Hash64          // ← НЕ goroutine-safe
}

func (h *hashSel) Pick(set []Address, pc PickContext) (Address, error) {
    // ...
    h.hash.Reset()             // ← БЕЗ mutex
    _, _ = h.hash.Write([]byte(k))
    idx := int(h.hash.Sum64() % uint64(len(set)))
    return set[idx], nil
}
```

**Почему баг:** `hash.Hash64` (реализация `fnv.New64a()`) **не goroutine-safe** — документация stdlib явно это говорит. `randomSel` корректно защищает `rng` через `mu.Lock()`, но `hashSel` — **нет**.

Интерфейс `Selector` декларирует: *"Implementations must be goroutine-safe."*

**Concurrent caller:** `managed_pool.go:182`:
```go
addr, err := mp.selector.Pick(set, PickContext{})
```
Это в path `Acquire()`, который вызывается из множества горутин при concurrent запросах.

**Почему race detector не поймал:** нет concurrent-теста для Hash selector. Concurrent-тест (`TestRoundRobin_Concurrent_FairBalance`, `selector_test.go:35`) тестирует только RoundRobin. Hash имеет только sequential-тесты.

**Фикс:**
```go
type hashSel struct {
    keyFn func(PickContext) string
    hash  hash.Hash64
    mu    sync.Mutex
}

func (h *hashSel) Pick(set []Address, pc PickContext) (Address, error) {
    // ...
    h.mu.Lock()
    h.hash.Reset()
    _, _ = h.hash.Write([]byte(k))
    idx := int(h.hash.Sum64() % uint64(len(set)))
    h.mu.Unlock()
    return set[idx], nil
}
```

---

### MISSING-02 — Нет лимита на raw response body (P1)

**Файл:строка:** `client/client.go:770`
**Severity: P1** — удалённый OOM даже БЕЗ компрессии

**Код:**
```go
case conn.EventData:
    resp.BytesReceived += int64(len(ev.Data))
    if req.WantBody && len(ev.Data) > 0 {
        resp.Body = append(resp.Body, ev.Data...)   // ← безграничный рост
    }
```

Вредоносный сервер может отправить гигабайты uncompressed DATA → `resp.Body` растёт без предела → OOM. Flow control (WINDOW_UPDATE) не защищает от этого: recv-window по умолчанию ~64 KB, но обновляется автоматически. Ограничение `maxReadFrameSize=16384` на transport-уровне также не помогает — оно ограничивает фрейм, а не суммарный body.

**Почему ревьюёр пропустил:** сфокусировался на decompression bomb (F-P0-01), но не проверил базовое накопление raw body.

**Фикс:** Добавить `MaxResponseBodyBytes` опцию; в `drainResponse` проверять `resp.BytesReceived > limit → return error`.

---

### MISSING-03 — `drainPushedStream`: unbounded goroutine spawning (P2)

**Файл:строка:** `client/client.go:793–797` (spawn) + `client/client.go:810–815` (function)

**Код — spawn site (внутри `drainResponse`):**
```go
case conn.EventPushPromise:
    if h != nil && ev.PushStreamID > 0 {
        ps, ok := cn.LookupStream(ev.PushStreamID)
        if ok {
            hdrs := copyHeaders(ev.Headers)
            go drainPushedStream(ctx, cn, h, hdrs, ps)    // ← без лимита
        }
    }
```

**Код — drainPushedStream:**
```go
func drainPushedStream(ctx context.Context, cn *conn.Conn, h PushHandler, ...) {
    pr := &Response{}
    derr := drainResponse(ctx, cn, s, &Request{WantBody: true}, pr, h)
    //                              ↑ recursively calls drainResponse
    //                              ↑ which can spawn MORE drainPushedStream goroutines
    _ = s.Close()
    h(ctx, promisedHeaders, pr, derr)
}
```

**Почему баг:**
1. Каждый PUSH_PROMISE frame → новая goroutine. Нет лимита на количество concurrent pushed-stream goroutines.
2. `drainPushedStream` → `drainResponse` → может получить ещё PUSH_PROMISE → ещё goroutine. Рекурсивное размножение.
3. `WantBody: true` → каждый push полностью буферизируется в памяти (без лимита, см. MISSING-02).
4. Вредоносный сервер может отправить тысячи PUSH_PROMISE → тысячи goroutines + unbounded memory.

**Фикс:** Semaphore/limiter на concurrent drain goroutines; опция `MaxConcurrentPushes`.

---

### MISSING-04 — Streaming decompression без лимита (P1)

**Файл:строка:** `client/decompress.go:106–112`

**Код:**
```go
func (d *decompressingReader) Read(p []byte) (int, error) {
    if d.dec == nil {
        return 0, io.EOF
    }
    return d.dec.Read(p)    // ← прямое делегирование в gzip/zlib, без LimitReader
}
```

`newDecompressingReader` (строки 127–152) оборачивает source в `gz` или `zr`, но НЕ оборачивает в `io.LimitReader`. Streaming API (когда consumer читает body по частям через `io.Reader`) также уязвим к decompression bomb.

Ревьюёр проверил только `decompressFully` (non-streaming path). Streaming path имеет тот же класс уязвимости.

---

## 4. Исправленная сводка блокеров и рекомендаций

### Блокеры продакшена (P0):
1. **F-P0-01 + MISSING-02 + MISSING-04** — Отсутствие лимитов на размер response body (raw + decompressed + streaming). Это **единый класс проблемы**, требующий сквозного фикса: `MaxResponseBodyBytes` + `io.LimitReader` во всех трёх путях.

### Сильные рекомендации (P1):
2. **MISSING-01** — `hashSel.Pick` data race. Реальный concurrent bug, пропущенный race detector-ом из-за отсутствия теста.
3. **F-P0-04** — push() goroutine без таймаута.
4. **F-P1-01** — Framer maxReadFrameSize не synced (read AND write).
5. **F-P1-04** — acquireSendCredits heap escape.

### Качество (P2):
6. **F-P0-02** — recycleStream events не пересоздаётся (defense-in-depth, не P0 блокер).
7. **MISSING-03** — drainPushedStream unbounded goroutines.
8. **F-P1-02** — TLS MinVersion explicit (код-цитата неточна).
9. **F-P1-03** — panic → error в Hash().

---

## 5. Оценка качества оригинального ревью

| Критерий | Оценка | Комментарий |
|---|---|---|
| Точность код-цитат | ⚠️ Частично | F-P1-02 — сфабрикованный сниппет; F-P0-03 — неточная цитата; остальные — точные |
| Полнота проверки call chain | ⚠️ Частично | F-P0-01 — не проверен streaming path; F-P0-02 — не проверен единственный caller recycleStream |
| Severity-калибровка | ⚠️ Завышена | F-P0-02 — P0 необоснован (триггер практически невозможен через нормальный API) |
| Покрытие аспектов методики | ✅ Хорошо | Все 10 аспектов проверены |
| Запуск tooling | ✅ Подтверждено | go vet + go test -race — независимо верифицировано |
| Пропущенные баги | ❌ 4 существенных | hashSel race, raw body limit, drainPushedStream, streaming decompression |

**Итог:** Оригинальное ревью — качественное в своей основе, но имеет системное слабое место: **фокус на изолированных сниппетах без полной проверки call chain и caller constraints**. Это привело к завышению severity (F-P0-02), неполным finding-ам (F-P0-01, F-P1-01) и пропуску 4 существенных багов, включая data race в selector (MISSING-01).
