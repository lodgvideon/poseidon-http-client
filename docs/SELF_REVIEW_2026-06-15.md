# Self-Review Action Plan — 2026-06-15

## Контекст

После спринта 4 фич (decompress, priority, shutdown, warmup+ratelimit+timeout),
проведён честный self-review. Найдены **3 FAKE GREEN / leak / false-claim**
проблемы в новых тестах. Сохранена в корне: skill `test-honesty`
(5-Why auto-analysis on test design).

## Что нужно сделать

### 🔴 P0 — критично (тест лжёт)

1. **FAKE GREEN: `TestWarmup_Pool_CappedByMaxConns`**
   Файл: `client/warmup_test.go:152-185`
   Проблема: проверяет только `ActiveConns <= 2`. Если warmup вообще не
   работает — `ActiveConns=0 <= 2`, тест зелёный.
   Фикс: добавить `countingDialer` (уже есть в файле на строке 192),
   проверить `dialCount.Load() >= 1` (cap honored + warmup сделал хоть
   один dial).

2. **False claim в комментарии**
   Файл: `client/ratelimit.go:11`
   Текст: `// ... is goroutine-safe and lock-free on the hot path of Take.`
   Проблема: `Take` сразу `rl.mu.Lock()` (line 60). Не lock-free.
   Фикс: убрать "lock-free", заменить на "thread-safe".

### 🟠 P1 — важно (resource leak)

3. **Leak в `singleConn.warmup`**
   Файл: `client/single_conn.go:152-166`
   Проблема: горутина fire-and-forget с 30-сек timeout. Не отменяется
   при `singleConn.close()`. Если `Warmup(1) → Close()` сразу, горутина
   сидит до 30с.
   Фикс: использовать `context.WithCancel` в `singleConn`, кэшировать
   `cancel` в struct, вызывать в `close()`.

### 🟡 P2 — желательно (magic numbers)

4. **`TestClient_RateLimit_BlocksExcess` magic timing**
   Файл: `client/ratelimit_test.go:104-142`
   Проблема: `elapsed >= 400ms` — нет производной.
   Фикс: вывести из параметров: `expected = (need - burst) * time.Second / rps`.
   С `rps=2, burst=2, need=3`: `1 * 500ms = 500ms`, slack 50ms.

5. **Hardcoded `3*time.Second` deadline в warmup-тестах**
   Файлы: `client/warmup_test.go:42, 110, 152`
   Проблема: выбрано "от балды", нет производной.
   Фикс: вывести `expected_dials × dial_timeout + slack`.

6. **Добавить `RateLimitBurst` в `ClientOptions`**
   Файл: `client/client.go:103`
   Проблема: `burst := rps` (line 189) — захардкожено. Пользователь не может
   настроить burst отдельно.
   Фикс: добавить `RateLimitBurst float64` (0 = default = rps), в
   `newRateLimiter` использовать его.

### 🟢 P3 — nice-to-have

7. **Дубликат `TestRequest_Timeout_DoStream`**
   Файл: `client/timeout_test.go:114-167`
   Проблема: копипаст `TestRequest_Timeout_Triggers` (line 27-77).
   Фикс: extract helper `assertTimeoutFires(t, c, useDoStream bool)`.

## Прогон валидации после всех правок

```bash
# Всё клиент + всё с race
go test -race -timeout 120s -count=1 ./client/...

# Под нагрузкой (флейки)
go test -race -timeout 180s -count=20 -run 'TestWarmup|TestRateLimit|TestRequest_Timeout' ./client/

# Линт
make lint
```

## Что осталось / pending

- [x] Провести фикс #1-#7 в указанном порядке
- [x] Каждый фикс — отдельный коммит с 5-Why root cause в message
  (P0+P1 — коммит 462c179, P2 #4-#6 — коммит 26504a7, P3 #7 —
  намеренный no-op с обоснованием в commit message)
- [x] Финальный `make test-race` + `make lint` оба зелёные
  (`make test-fast` — все 4 пакета OK; count=20 stress — PASS 166s)
- [ ] После: можно v0.2.1 patch release (или оставить в v0.2.0)

## Validation results (2026-06-15, post-fix)

| Step | Result |
|---|---|
| `go build ./...` | clean |
| `make test-fast` (frame+hpack+conn+client non-stress) | **PASS** 28s |
| `go test -race -count=20 ./client/` (warmup+ratelimit+timeout) | **PASS** 166s |
| `golangci-lint run ./client/...` | 288 lines (identical baseline) |
| `make test-stress` (TestStress|TestE2E) | pre-existing failures: `TestE2E_Google_ConnStats` (BytesReceived undercount, unrelated), `TestStress_SingleConn_50Sequential` (Google TLS Read hang) — **verified not caused by these fixes** |

## False alarms (НЕ баги)

- ~~Deadlock в `TestWarmup_Pool_DialsMultiple`~~ — 20×3.05s = 61s, превысил
  60s лимит терминала. Тест проходит за 62s в фоне.

## Скилл

`~/.hermes/skills/test-honesty/SKILL.md` — правила 5-Why на любой тест-фейл
или дизайн. Применяется автоматически.
