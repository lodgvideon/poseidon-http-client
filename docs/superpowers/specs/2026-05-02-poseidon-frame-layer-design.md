# Poseidon HTTP/2 Client — Phase A: Frame Layer + HPACK

**Status:** Design (proposed)
**Date:** 2026-05-02
**Module:** `github.com/lodgvideon/poseidon-http-client`
**Go version:** 1.24
**License:** MIT (default; revisit if owner specifies otherwise)

---

## 1. Context and Goals

Poseidon — низкоуровневый HTTP/2 клиент, целевое применение — генераторы нагрузки. Стандартная библиотека `net/http` и официальный модуль `golang.org/x/net/http2` НЕ используются: и то и другое аллоцирует на горячем пути и не подходит под цель zero-allocation. Клиент пишется с нуля по принципам SOLID, чтобы оставаться расширяемым (новые transport'ы, discovery, стратегии балансировки).

Проект декомпозирован на три фазы:

| Фаза | Содержание | Когда |
|------|------------|-------|
| **A — Frame Layer + HPACK** *(этот документ)* | Кодек HTTP/2 фреймов (RFC 7540) и HPACK блоков (RFC 7541). Без сети, без TLS, без stream state machine. Самостоятельная библиотека-фундамент. | Сейчас |
| **B — Connection Layer** | Поверх A: TLS+ALPN handshake, SETTINGS обмен, stream state machine, flow control, GOAWAY/PING/RTT. Один коннект, один поток управления. | После Phase A |
| **C — Client + Pool + Discovery + Stats** | Поверх B: пул коннектов на хост, лимит стримов, retry/timeout, request/response API, stats hooks, discovery interface. Это публичная поверхность для генераторов нагрузки. | После Phase B |

Фаза A — самостоятельный артефакт: пакет, который можно использовать отдельно для разбора/сборки h2-фреймов (например, в анализаторах траффика или собственных серверах).

### Non-goals для Phase A

- Любая работа с сетью (`net.Conn`, TLS, ALPN, h2c upgrade) — Phase B.
- Stream state machine, flow-control аккаунтинг, обмен SETTINGS как процесс — Phase B (фрейм-типы кодируются/декодируются, но логики жизненного цикла нет).
- Connection pool, request API, discovery, stats — Phase C.
- Server push логика (фрейм `PUSH_PROMISE` кодируется/декодируется как сущность, но клиент Poseidon его в Phase C **не будет принимать** — `SETTINGS_ENABLE_PUSH=0`).

---

## 2. Architecture Overview

### 2.1 SOLID разрезы

| Принцип | Применение в Phase A |
|---------|----------------------|
| **S** — Single responsibility | `frame` (фрейм-кодек), `hpack` (HPACK), `internal/bytesx` (буферные утилиты) — три пакета, три ответственности. |
| **O** — Open–Closed | Новые frame types добавляются регистрацией type-code → decoder без правки core путей. |
| **L** — Liskov | Все конкретные frame-типы взаимозаменяемы как `frame.Frame`. |
| **I** — Interface segregation | Раздельные интерфейсы `FrameReader`, `FrameWriter`, `HPACKEncoder`, `HPACKDecoder` — клиенты не зависят от ненужного. |
| **D** — Dependency inversion | Кодек работает с `io.Reader`/`io.Writer` (тесты — `bytes.Buffer`, прод — TLS-сокет в Phase B). frame-layer не знает про сеть. |

### 2.2 Слои Phase A

```
internal/bytesx   ─ uint24/uint31 read/write, padding strip, integer codec helpers
   │
hpack             ─ static table, dynamic table (ring), Huffman, encoder/decoder
   │
frame             ─ Framer (Read/Write), 10 frame types, FRAME_SIZE/SETTINGS validation
```

`frame` зависит от `hpack` только в тех фрейм-типах, что несут header block (HEADERS, PUSH_PROMISE, CONTINUATION). И там — для целостности контракта блока, без декодирования: декодирование делает caller отдельным вызовом `hpack.Decoder.DecodeBlock(headerBlock, visitor)`. Это даёт caller'у контроль над жизненным циклом памяти под заголовки.

### 2.3 Zero-alloc дисциплина (best-effort ≤1 alloc/op)

- Пулы (`sync.Pool`) для read/write scratch буферов на уровне `Framer`-а (per-instance, амортизированно нолевая аллокация).
- На горячем пути нет `string`, `fmt.Errorf`, `defer`, `interface{}`-боксинга примитивов.
- Все sentinel-ошибки — package-level `var` с `errors.New(...)` (одноразовая аллокация при инициализации, ноль на путь).
- Декодер пишет в caller-предоставленный visitor (`Handler` для frame, `FieldVisitor` для HPACK); slice-views в pooled buffer валидны до следующего вызова — caller, желающий сохранить, копирует сам.
- POD-структуры (`FrameHeader`, `Priority`, `SettingsParams`, `HeaderField`) передаются by value, не уходят в кучу.
- `SettingsParams` — фиксированный массив пар, **не** map.
- Bounds-check elimination где возможно (hoist single bound check, `for range`).

Что СОЗНАТЕЛЬНО допускаем:
- Аллокация при `NewEncoder()`/`NewDecoder()`/`NewFramer()` — амортизируется до нуля per-connection.
- Аллокация при росте dynamic-table arena (peak grow) — амортизированная.
- Структурные ошибки (`*FrameSizeError`) — только в редких path'ах (close-with-GOAWAY и т. п.).

---

## 3. Package Layout

```
poseidon-http-client/
├── go.mod                            # module github.com/lodgvideon/poseidon-http-client
├── go.sum
├── README.md
├── LICENSE
├── docs/
│   └── superpowers/specs/            # design docs (этот файл — здесь)
├── frame/                            # PUBLIC: HTTP/2 framing (RFC 7540)
│   ├── frame.go                      # Frame interface, FrameType const, Flags
│   ├── header.go                     # FrameHeader (9 bytes) read/write
│   ├── data.go                       # DataFrame
│   ├── headers.go                    # HeadersFrame
│   ├── priority.go                   # PriorityFrame
│   ├── rst_stream.go                 # RSTStreamFrame
│   ├── settings.go                   # SettingsFrame + SettingsParams
│   ├── push_promise.go               # PushPromiseFrame
│   ├── ping.go                       # PingFrame
│   ├── goaway.go                     # GoAwayFrame
│   ├── window_update.go              # WindowUpdateFrame
│   ├── continuation.go               # ContinuationFrame
│   ├── framer.go                     # Framer (FrameReader/FrameWriter), pool
│   ├── errors.go                     # sentinel errors, ErrCode (RFC §7)
│   ├── *_test.go                     # table-driven unit tests
│   ├── conformance_test.go           # RFC 7540 §4–§6 vectors
│   └── bench_test.go                 # bench gates (allocs/op, ns/op)
├── hpack/                            # PUBLIC: HPACK (RFC 7541)
│   ├── hpack.go                      # public API: Encoder, Decoder, HeaderField
│   ├── static_table.go               # 61-entry static table (RFC 7541 App. A)
│   ├── dynamic_table.go              # ring-buffer dynamic table, eviction
│   ├── huffman.go                    # canonical Huffman encode/decode (RFC 7541 App. B)
│   ├── integer.go                    # N-bit prefix integer codec (RFC 7541 §5.1)
│   ├── string_literal.go             # string literal w/ optional Huffman (§5.2)
│   ├── encoder.go                    # incremental indexing, never-indexed, raw
│   ├── decoder.go                    # streaming decode w/ visitor callback
│   ├── errors.go                     # ErrInvalidIndex, ErrIntegerOverflow, ...
│   ├── *_test.go                     # RFC 7541 App. C vectors
│   └── bench_test.go                 # bench gates
├── internal/
│   └── bytesx/                       # private utils
│       ├── uint.go                   # ReadUint24/31, WriteUint24/31
│       ├── padding.go                # padded payload strip
│       └── pool.go                   # typed sync.Pool helpers
├── testdata/
│   ├── rfc7540/                      # per-frame golden bytes (hex)
│   └── rfc7541/                      # HPACK App. C vectors as golden
└── .github/workflows/
    ├── ci.yml                        # vet, staticcheck, test -race, golangci-lint
    ├── bench-gate.yml                # benchstat compare vs main, fail on regressions
    ├── conformance-gate.yml          # RFC vector tests, allocs/op gate
    └── nightly.yml                   # fuzz (FuzzFramerReadFrame, FuzzHPACKDecode)
```

Принципы:
- `frame/` и `hpack/` публичны (импортируются Phase B/C и сторонними).
- `internal/` гарантирует, что приватные утилиты не утекают в API.
- Циклов зависимостей нет: `frame → hpack → internal/bytesx`.
- Vendoring не используется: poseidon — единый модуль для всех phase'ов.

---

## 4. Public API: `frame` package

### 4.1 Типы и константы

```go
package frame

// FrameType — RFC 7540 §11.2.
type FrameType uint8

const (
    FrameData         FrameType = 0x0
    FrameHeaders      FrameType = 0x1
    FramePriority     FrameType = 0x2
    FrameRSTStream    FrameType = 0x3
    FrameSettings     FrameType = 0x4
    FramePushPromise  FrameType = 0x5
    FramePing         FrameType = 0x6
    FrameGoAway       FrameType = 0x7
    FrameWindowUpdate FrameType = 0x8
    FrameContinuation FrameType = 0x9
)

// Flags — bitmask, semantics depend on FrameType.
type Flags uint8

// FrameHeader — fixed 9-byte prefix (RFC 7540 §4.1).
type FrameHeader struct {
    Length   uint32 // 24-bit
    Type     FrameType
    Flags    Flags
    StreamID uint32 // 31-bit, R-bit masked off
}

// ErrCode — RFC 7540 §7.
type ErrCode uint32

const (
    ErrCodeNoError            ErrCode = 0x0
    ErrCodeProtocolError      ErrCode = 0x1
    ErrCodeInternalError      ErrCode = 0x2
    ErrCodeFlowControlError   ErrCode = 0x3
    ErrCodeSettingsTimeout    ErrCode = 0x4
    ErrCodeStreamClosed       ErrCode = 0x5
    ErrCodeFrameSizeError     ErrCode = 0x6
    ErrCodeRefusedStream      ErrCode = 0x7
    ErrCodeCancel             ErrCode = 0x8
    ErrCodeCompressionError   ErrCode = 0x9
    ErrCodeConnectError       ErrCode = 0xA
    ErrCodeEnhanceYourCalm    ErrCode = 0xB
    ErrCodeInadequateSecurity ErrCode = 0xC
    ErrCodeHTTP11Required     ErrCode = 0xD
)

// Sentinel errors — никаких fmt.Errorf на hot path.
var (
    ErrFrameTooLarge       = errors.New("poseidon/frame: frame exceeds SETTINGS_MAX_FRAME_SIZE")
    ErrInvalidStreamID     = errors.New("poseidon/frame: stream id violates RFC 7540 rules")
    ErrInvalidPadding      = errors.New("poseidon/frame: pad length exceeds payload")
    ErrUnknownFrameType    = errors.New("poseidon/frame: unknown frame type")
    ErrSettingsAck         = errors.New("poseidon/frame: SETTINGS ACK with non-empty payload")
    ErrPriorityWrongLength = errors.New("poseidon/frame: PRIORITY frame length != 5")
    // ... один на каждое connection-error правило RFC §6.x
)
```

### 4.2 Visitor для декодера

```go
// Handler — callback per decoded frame. Реализация решает,
// клонировать ли byte-slice (default — НЕ клонирует; slice валиден до
// следующего ReadFrame).
type Handler interface {
    OnData(h FrameHeader, payload []byte, padLen uint8) error
    OnHeaders(h FrameHeader, hb HeaderBlock, prio *Priority, padLen uint8) error
    OnPriority(h FrameHeader, p Priority) error
    OnRSTStream(h FrameHeader, code ErrCode) error
    OnSettings(h FrameHeader, s SettingsParams) error
    OnPushPromise(h FrameHeader, promisedID uint32, hb HeaderBlock, padLen uint8) error
    OnPing(h FrameHeader, opaqueData [8]byte) error
    OnGoAway(h FrameHeader, lastStreamID uint32, code ErrCode, debug []byte) error
    OnWindowUpdate(h FrameHeader, increment uint32) error
    OnContinuation(h FrameHeader, hb HeaderBlock) error
}

// HeaderBlock — opaque view над raw header block fragment.
// Декодируется через hpack.Decoder.DecodeBlock(hb, visitor).
type HeaderBlock []byte

// Priority — POD, передаётся by value.
type Priority struct {
    StreamDep uint32 // 31-bit
    Exclusive bool
    Weight    uint8  // RFC weight = Weight + 1
}

// SettingID — RFC 7540 §6.5.2.
type SettingID uint16

const (
    SettingHeaderTableSize      SettingID = 0x1
    SettingEnablePush           SettingID = 0x2
    SettingMaxConcurrentStreams SettingID = 0x3
    SettingInitialWindowSize    SettingID = 0x4
    SettingMaxFrameSize         SettingID = 0x5
    SettingMaxHeaderListSize    SettingID = 0x6
)

// SettingsParams — фиксированный массив пар (вместо map — zero-alloc).
type SettingsParams struct {
    Pairs [16]struct {
        ID    SettingID
        Value uint32
    }
    N int // active prefix length
}
```

### 4.3 Framer

```go
type Framer struct {
    // unexported: pooled read/write buffers, max sizes, encoder/decoder hooks
}

// NewFramer — w для отправки, r для приёма. Любой из них может быть nil
// для unidirectional использования (например, в тестах кодека).
func NewFramer(w io.Writer, r io.Reader) *Framer

// Limits.
func (*Framer) SetMaxReadFrameSize(n uint32)            // SETTINGS_MAX_FRAME_SIZE peer-а
func (*Framer) SetMaxHeaderListSize(n uint32)
func (*Framer) SetReadBuffer(buf []byte)                // user-supplied scratch (zero-alloc path)

// ReadFrame заполняет caller-supplied Handler. Возвращает FrameHeader для
// внешнего accounting (flow-control, stats), nil-error при EOF.
func (*Framer) ReadFrame(ctx context.Context, h Handler) (FrameHeader, error)

// Write side — explicit per frame type, no allocs.
type WriteHeadersParams struct {
    StreamID      uint32
    BlockFragment []byte    // pre-encoded HPACK block (caller вызвал hpack.Encoder)
    EndStream     bool
    EndHeaders    bool
    Priority      *Priority // nil = not present
    PadLength     uint8     // 0 = no padding
}

func (*Framer) WriteData(streamID uint32, endStream bool, data []byte) error
// WriteDataPadded — pad-length задаётся числом; framer заполняет padding нулями
// из package-level var paddingZeros [256]byte (без аллокаций caller'у).
func (*Framer) WriteDataPadded(streamID uint32, endStream bool, data []byte, padLen uint8) error
func (*Framer) WriteHeaders(p WriteHeadersParams) error
func (*Framer) WriteContinuation(streamID uint32, endHeaders bool, blockFragment []byte) error
func (*Framer) WritePriority(streamID uint32, p Priority) error
func (*Framer) WriteRSTStream(streamID uint32, code ErrCode) error
func (*Framer) WriteSettings(s SettingsParams) error
func (*Framer) WriteSettingsAck() error
func (*Framer) WritePushPromise(streamID, promisedID uint32, blockFragment []byte, endHeaders bool, pad uint8) error
func (*Framer) WritePing(ack bool, data [8]byte) error
func (*Framer) WriteGoAway(lastStreamID uint32, code ErrCode, debug []byte) error
func (*Framer) WriteWindowUpdate(streamID uint32, increment uint32) error

// Connection preface (RFC 7540 §3.5) — клиентская сторона.
func (*Framer) WriteClientPreface() error // "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
```

Концептуальные свойства:
- Декодер не аллоцирует структуры фреймов — данные идут визитором с slice-views в read buffer.
- HEADERS/CONTINUATION/PUSH_PROMISE возвращают `HeaderBlock` (raw bytes); HPACK decode — отдельный шаг, чтобы caller контролировал lifetime header storage.
- Все write methods — explicit, без variadic option-структур (zero alloc).
- Конфигурация (`SetMaxReadFrameSize`) — отдельные методы, не options.
- `Framer` НЕ goroutine-safe by design (документируется в комментариях).
- `WritePushPromise` существует ради симметрии кодека и для целей тестирования декодера; Phase B/C клиентская реализация НЕ должна его вызывать (сервер push отключим через `SETTINGS_ENABLE_PUSH=0`). На уровне frame-layer он валидируется и кодируется как все остальные типы для полноты RFC 7540 поддержки.

---

## 5. HPACK Sub-design (`hpack` package)

HPACK — собственный, RFC 7541. Делю на 5 узких юнитов; у каждого один контракт.

### 5.1 Integer codec (`hpack/integer.go`) — RFC 7541 §5.1

```go
// EncodeInteger пишет I в dst по правилу префикса n. Возвращает обновлённый dst.
// Не аллоцирует: dst — caller-owned scratch.
func EncodeInteger(dst []byte, n uint8, prefixByte byte, i uint64) []byte

// DecodeInteger читает целое из src по правилу префикса n. Возвращает значение
// и количество прочитанных байт. ErrTruncated если src обрывается.
// Защита от integer overflow: ErrIntegerOverflow при попытке прочитать > 2^32-1.
func DecodeInteger(src []byte, n uint8) (val uint64, consumed int, err error)
```

### 5.2 Huffman (`hpack/huffman.go`) — RFC 7541 App. B (canonical Huffman)

- Encode: lookup-table (`[256]huffmanCode{ bits uint32; nbits uint8 }`) + bit-packing в caller-owned `[]byte`.
- Decode: state-machine на 4-bit nibbles (proven approach из nghttp2) — таблица переходов compile-time, ~1 KiB. Hot loop без аллокаций, без bit-by-bit парсинга.

```go
func HuffmanEncodedLen(src []byte) int
func HuffmanEncode(dst, src []byte) []byte
func HuffmanDecode(dst, src []byte) ([]byte, error) // dst — pooled buffer
```

### 5.3 Static table (`hpack/static_table.go`) — RFC 7541 App. A (61 запись)

Compile-time `[62]staticEntry{ name, value string }` (нулевая запись зарезервирована, валидные индексы 1–61). Lookup для encoder: `staticIndex(name, value []byte) (idx uint64, fullMatch bool)` — линейный по 61 записи. Это укладывается в ≤200 ns с нулевыми аллокациями; map дал бы аллокации и проиграл бы линейному поиску на короткой таблице.

### 5.4 Dynamic table (`hpack/dynamic_table.go`) — RFC 7541 §2.3.2, §4

Реализация — **ring buffer** с двумя scratch-областями:
- `entries []dynEntry` — кольцо записей (`head/tail/count`).
- `arena []byte` — единый буфер для name+value байт; каждая `dynEntry` хранит `nameOff/nameLen/valueOff/valueLen` в `arena`.
- При eviction освобождаем хвост; при переполнении arena — компактим (один `copy`, амортизированно редкое).

Учёт размера согласно RFC §4.1: `entrySize = nameLen + valueLen + 32`. Resize по SETTINGS_HEADER_TABLE_SIZE: эвикция до соблюдения нового лимита (RFC §4.3).

API внутренний — не публичный.

### 5.5 Encoder/Decoder (public API в `hpack/hpack.go`)

```go
package hpack

// HeaderField — view-структура; поля — slices в caller-buffer.
// НЕ владеет памятью. Жизненный цикл — до следующего вызова.
type HeaderField struct {
    Name      []byte
    Value     []byte
    Sensitive bool // never-indexed (RFC §6.2.3)
}

// Размер записи в dynamic table (RFC §4.1).
func (f HeaderField) Size() uint32

// === Encoder ===

type Encoder struct {
    // unexported: dynamic table, scratch buffer, peer's SETTINGS_HEADER_TABLE_SIZE
}

func NewEncoder() *Encoder
func (*Encoder) SetMaxDynamicTableSize(n uint32)        // peer SETTINGS update
func (*Encoder) SetMaxDynamicTableSizeLimit(n uint32)   // local cap
func (*Encoder) Reset()                                  // per-connection reuse via pool

// EncodeBlock дописывает закодированный header block в dst, возвращает обновлённый dst.
// fields — caller-owned, не сохраняется. Никаких аллокаций при достаточной ёмкости dst.
func (*Encoder) EncodeBlock(dst []byte, fields []HeaderField) []byte

// Грануларный API — для генератора нагрузки, чтобы добавлять заголовки
// из шаблона без сборки []HeaderField на каждый запрос.
func (*Encoder) WriteField(dst []byte, name, value []byte, sensitive bool) []byte

// === Decoder ===

type Decoder struct {
    // unexported: dynamic table, partial-block state for CONTINUATION
}

// FieldVisitor вызывается ровно один раз на каждое декодированное поле.
// f.Name/f.Value валидны ТОЛЬКО на время одного вызова visitor'а; следующий
// вызов может переиспользовать ту же область arena. Если caller хочет сохранить
// поле — должен скопировать байты внутри тела visitor'а до возврата.
type FieldVisitor func(f HeaderField) error

func NewDecoder() *Decoder
func (*Decoder) SetMaxDynamicTableSize(n uint32) // local cap (отправляется в SETTINGS)
func (*Decoder) Reset()

// DecodeBlock — однократный полный блок (HEADERS без CONTINUATION).
func (*Decoder) DecodeBlock(block []byte, visit FieldVisitor) error

// Streaming API для HEADERS+CONTINUATION+...:
//   d.Begin()
//   d.Feed(part1, visit)
//   d.Feed(part2, visit)
//   d.Finish()
func (*Decoder) Begin()
func (*Decoder) Feed(fragment []byte, visit FieldVisitor) error
func (*Decoder) Finish() error
```

Ключевые свойства:
- `HeaderField.Name/Value` — slices в scratch arena декодера. Lifetime — ровно один вызов visitor'а. После возврата из visitor'а слайсы могут быть переписаны при декодировании следующего поля. Если caller хочет сохранить — копирует внутри тела visitor'а.
- Encoder стратегия (по умолчанию): static-table indexed (6.1) → dynamic-table indexed (6.1) → literal-with-incremental-indexing (6.2.1) → literal-without-indexing (6.2.2). `Sensitive=true` форсит never-indexed (6.2.3).
- Pool: `frame.Framer` держит `*hpack.Encoder` и `*hpack.Decoder` в своих полях; на уровне connection (Phase B) `Framer` пулится целиком per-connection.

---

## 6. Zero-alloc Patterns (нормативные правила Phase A)

### 6.1 Buffer ownership

| Слой | Кто владеет read scratch | Кто владеет write scratch |
|------|--------------------------|---------------------------|
| `Framer.ReadFrame` | `Framer` (pooled `[]byte` ≥ `maxReadFrameSize+9`) | n/a |
| `Framer.WriteX` | n/a | caller передаёт payload, либо `Framer` берёт из pool |
| `hpack.Decoder` | `Decoder` (pooled arena, growable) | n/a |
| `hpack.Encoder.EncodeBlock` | n/a | caller передаёт `dst []byte`, encoder только append'ит |

Контракт: **slice, отданный visitor'у, валиден до следующего вызова `ReadFrame` / `Feed`**. Если callee хочет сохранить — копирует.

### 6.2 Pools

```go
// internal/bytesx/pool.go
var readBufPool = sync.Pool{
    New: func() any { b := make([]byte, 16<<10); return &b }, // 16 KiB начальный
}

func GetReadBuf(min int) *[]byte
func PutReadBuf(p *[]byte)
```

`Framer` держит свой read buffer на инстанс (НЕ из pool на каждый ReadFrame — ломало бы lifetime контракт). Pool используется на уровне `Framer.NewFramer` / `Close` (Phase B per-connection).

### 6.3 Sentinel-only errors на hot path

```go
var ErrFrameSizeError = errors.New("...")
// НЕ: fmt.Errorf("frame size %d exceeds max %d", got, max)  ← аллоцирует
```

Если нужен контекст в ошибке — структурный тип:

```go
type FrameSizeError struct {
    Got, Max uint32
}
func (e *FrameSizeError) Error() string { ... }
```

`*FrameSizeError` тоже аллоцирует, поэтому в hot path возвращаем sentinel. Структурный — только в редких path'ах (close-with-GOAWAY).

### 6.4 No `string` conversion на hot path

- HPACK Name/Value — `[]byte`. Static table сравнивается через `bytes.Equal`. Сравнение литералов — `bytes.Equal(n, nameBytes)` где `nameBytes` — package-level `var [...]byte`.
- Никаких `string([]byte)` / `[]byte(string)` в кодеке. Конверсия `string → []byte` для compile-time literals — только в `init()`, не на hot path.

### 6.5 No interface boxing для примитивов

- В hot loop не передаём `any` / `interface{}`. Visitor `Handler` имеет конкретные методы — Go devirtualises монолитный call site, если caller передаёт concrete type.
- HPACK `FieldVisitor` — `func(HeaderField) error`, не `interface { Visit(HeaderField) error }`. Closures inline-able.

### 6.6 No `defer` в hot loops

`defer` стоит ~30 ns + escape анализа. `Framer.ReadFrame` — без `defer`, ошибки обрабатываются явно. `defer` ок в `Close()`, `Reset()`.

### 6.7 No `time.Now()` per-frame внутри пакета

Time stamps — отдельный hook (Phase C, для stats). `frame` пакет не вызывает clock.

### 6.8 Bounds-check elimination hints

В hot loops (HPACK integer decode, Huffman decode) пишем код, который позволяет компилятору убрать bounds checks (hoist single bound check в начале, `for range`). Проверяем флагом `-gcflags="-d=ssa/check_bce/debug=1"` в bench-gate.

### 6.9 Escape-analysis discipline

Все hot-path структуры (`FrameHeader`, `Priority`, `SettingsParams`, `HeaderField`) — POD, by value, не уходят в кучу. Проверяем `go build -gcflags="-m"` в CI: gate реджектит коммит, если `Framer.ReadFrame` имеет новые `escapes to heap` для не-pool путей.

### 6.10 Что СОЗНАТЕЛЬНО допускаем

- Аллокация при `NewEncoder()`, `NewDecoder()`, `NewFramer()` — кэшируется per-connection, амортизируется до нуля.
- Аллокация при росте dynamic-table arena (peak grow) — амортизированно нулевая.
- Структурные ошибки (`*FrameSizeError`-style) — только в редких code-path'ах (close-with-GOAWAY).

---

## 7. Testing Strategy

TDD — обязательная дисциплина: каждая публичная функция и каждое RFC-нормативное правило получают тест ДО реализации. CI блокирует merge при падении любого gate.

### 7.1 Уровни тестов

| Уровень | Папка | Что покрывает |
|---|---|---|
| Unit (table-driven) | `frame/*_test.go`, `hpack/*_test.go` | Каждый frame type: encode∘decode == identity; пограничные значения; rejected inputs |
| RFC vectors (golden) | `frame/conformance_test.go`, `hpack/hpack_test.go` | Байт-в-байт сверка с RFC 7540 §6 / RFC 7541 App. C |
| Property-based (fuzz) | `frame/fuzz_test.go`, `hpack/fuzz_test.go` | Go native fuzz: decode не паникует на любом input; encode→decode roundtrip |
| Bench (gate) | `*_bench_test.go` | ns/op + allocs/op фиксированные пороги; `benchstat` сравнение vs baseline |

### 7.2 Структура table-driven (пример)

```go
// frame/headers_test.go
func TestHeadersFrame_Encode(t *testing.T) {
    cases := []struct {
        name    string
        params  WriteHeadersParams
        wantHex string
        wantErr error
    }{
        {
            name: "minimal_no_priority_no_padding",
            params: WriteHeadersParams{
                StreamID:      1,
                BlockFragment: hexDecode("8285"), // :method GET, :path /  (RFC 7541 C.2.4)
                EndStream:     true,
                EndHeaders:    true,
            },
            wantHex: "0000020105000000018285",
        },
        // ... padding, priority, CONTINUATION split, max-size, oversized → ErrFrameTooLarge
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) { ... })
    }
}
```

### 7.3 RFC golden vectors

- `testdata/rfc7541/c_*.txt` — все примеры из RFC 7541 App. C (C.1.1–C.6.3): plain integer, literal-with-indexing, indexed-header-field, request sequence, response sequence (с/без Huffman).
- `testdata/rfc7540/` — собственные собранные vectors per frame type, помеченные ссылками на §RFC.
- Loader: `loadGolden(t, name) (input, want []byte)` — common helper в `internal/testutil` (test-only).

### 7.4 Fuzz

```go
//go:build go1.18
func FuzzFramerReadFrame(f *testing.F) {
    f.Add([]byte{0,0,0, 0x04, 0, 0,0,0,0}) // empty SETTINGS
    f.Fuzz(func(t *testing.T, data []byte) {
        fr := NewFramer(nil, bytes.NewReader(data))
        var h dropHandler
        for {
            _, err := fr.ReadFrame(context.Background(), &h)
            if err != nil { return }
        }
        // invariants: no panic, no infinite loop (timeout enforced by `go test`)
    })
}
```

В CI:
- На PR — corpus replay (`go test -run=Fuzz`).
- На nightly — `go test -fuzz=... -fuzztime=10m`.

### 7.5 Race

`go test -race ./...` обязательный gate. Frame layer не concurrent (`Framer` не goroutine-safe by design — документируем), но HPACK encoder/decoder тестируем под `-race` чтобы поймать sloppy state.

### 7.6 Bench gate — целевые числа

Стартовые пороги (на reference machine; точная фиксация — в `BENCH_BASELINE.md` после первого запуска):

| Бенч | Цель ns/op | allocs/op | B/op |
|---|---|---|---|
| `BenchmarkFramer_WriteData_1KB` | ≤ 80 | 0 | 0 |
| `BenchmarkFramer_WriteHeaders_minimal` | ≤ 200 | 0 | 0 |
| `BenchmarkFramer_ReadFrame_DATA_1KB` | ≤ 100 | 0 | 0 |
| `BenchmarkFramer_ReadFrame_HEADERS` | ≤ 250 | 0 | 0 |
| `BenchmarkHPACK_EncodeBlock_3req_static` | ≤ 150 | 0 | 0 |
| `BenchmarkHPACK_DecodeBlock_3req_static` | ≤ 200 | 0 | 0 |
| `BenchmarkHPACK_HuffmanEncode_path` | ≤ 50 | 0 | 0 |
| `BenchmarkHPACK_HuffmanDecode_path` | ≤ 80 | 0 | 0 |
| `BenchmarkHPACK_IntegerDecode_max` | ≤ 15 | 0 | 0 |

`allocs/op == 0` и `B/op == 0` — **жёсткие gate'ы** после warmup. ns/op — целевые на reference hardware; для GitHub-hosted runners используем относительный gate (10% regression vs main).

### 7.7 Bench gate — механика CI

```yaml
# .github/workflows/bench-gate.yml (псевдокод)
- run: go test -bench=. -benchmem -count=10 -run=^$ ./... > new.txt
- run: git checkout main && go test -bench=. -benchmem -count=10 -run=^$ ./... > base.txt
- run: benchstat -alpha 0.05 base.txt new.txt > diff.txt
- run: ./scripts/bench-gate.sh diff.txt   # парсит diff, fail при росте allocs или ns/op +>10%
```

Локально те же скрипты — через `make bench-gate`.

### 7.8 RFC compliance gate (Phase A scope)

- Все table-driven RFC tests должны быть зелёные.
- Job `conformance-gate` запускает `go test -run=Conformance -count=1 ./...` отдельно.
- Per-section coverage matrix (RFC 7540 §4–§6, RFC 7541 §2–§7) — `docs/RFC_COVERAGE.md`. Каждый раз при добавлении frame-type/feature ставим check.

Naming convention: `func TestConformance_RFC7540_§6_5_2_SettingsParameters(t *testing.T)` → парсер вытаскивает RFC секцию и составляет матрицу покрытия.

Phase B (forward-look) добавит `h2spec` job против тестового сервера — здесь не реализуется.

### 7.9 TDD цикл (rigid)

1. Red — пишем падающий тест (table case или RFC vector).
2. Green — минимальная реализация, проходящая только этот тест.
3. Refactor — чистим, прогоняем все тесты + bench gate.
4. Commit (`wip:` или final по git-discipline). Никогда не пишем реализацию до теста.

---

## 8. CI / QA Gateway Pipelines

Платформа — **GitHub Actions** (репо на GitHub). Если потребуется GitLab — пайплайны мапятся 1:1.

### 8.1 `ci.yml` — обязательный на каждый PR + push в main

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:

jobs:
  lint:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.24', cache: true }
      - run: go vet ./...
      - uses: golangci/golangci-lint-action@v6
        with: { version: v1.62 }
      - run: go install honnef.co/go/tools/cmd/staticcheck@latest && staticcheck ./...

  test:
    runs-on: ubuntu-24.04
    strategy:
      matrix: { go: ['1.24'] }
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: ${{ matrix.go }}, cache: true }
      - run: go test -race -count=1 -coverprofile=cover.out ./...
      - run: go tool cover -func=cover.out | tee coverage.txt
      - run: ./scripts/coverage-gate.sh coverage.txt 90   # ≥90% per-package
      - uses: actions/upload-artifact@v4
        with: { name: coverage, path: cover.out }

  fuzz-corpus-replay:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.24' }
      - run: go test -run=Fuzz -count=1 ./...   # replay seed corpus, без fuzz-running
```

`golangci-lint` config (`.golangci.yml`) включает: `errcheck`, `govet`, `staticcheck`, `revive`, `gosec`, `unconvert`, `misspell`, `gocyclo` (limit 15), `gosimple`, `unused`, `prealloc`. `// nolint` без обоснования запрещён.

### 8.2 `bench-gate.yml` — на каждый PR

```yaml
name: bench-gate
on: [pull_request]
jobs:
  bench:
    runs-on: ubuntu-24.04   # фиксированный класс машины — критично для стабильности
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - uses: actions/setup-go@v5
        with: { go-version: '1.24' }
      - run: go test -bench=. -benchmem -benchtime=2s -count=10 -run=^$ ./... | tee head.txt
      - run: |
          git checkout ${{ github.base_ref }}
          go test -bench=. -benchmem -benchtime=2s -count=10 -run=^$ ./... | tee base.txt
          git checkout -
      - run: |
          go install golang.org/x/perf/cmd/benchstat@latest
          benchstat -alpha 0.05 base.txt head.txt | tee diff.txt
      - run: ./scripts/bench-gate.sh diff.txt
```

`scripts/bench-gate.sh` fail'ит коммит, если:
- `allocs/op` для любого бенча в HEAD > 0 при target 0;
- `B/op` > 0 при target 0;
- `ns/op` regression > 10% (statistically significant per benchstat).

GitHub-hosted runners шумные — для абсолютных порогов (Section 7.6) запускаем nightly-job на dedicated раннере (вне MVP, фиксируется как future TODO).

### 8.3 `conformance-gate.yml` — на каждый PR

```yaml
name: conformance-gate
on: [pull_request]
jobs:
  rfc:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.24' }
      - run: go test -run=Conformance -count=1 -v ./frame ./hpack | tee rfc.txt
      - run: ./scripts/rfc-coverage-gate.sh rfc.txt
      - run: ./scripts/rfc-matrix-check.sh docs/RFC_COVERAGE.md rfc.txt
```

### 8.4 `nightly.yml`

```yaml
name: nightly
on:
  schedule:
    - cron: '0 3 * * *'
jobs:
  fuzz:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: '1.24' }
      - run: go test -fuzz=FuzzFramerReadFrame -fuzztime=10m ./frame
      - run: go test -fuzz=FuzzHPACKDecode -fuzztime=10m ./hpack
      - if: failure()
        uses: actions/upload-artifact@v4
        with: { name: fuzz-corpus, path: testdata/fuzz/ }
```

Crash → авто-коммит crash inputs в `testdata/fuzz/` + GitHub issue.

### 8.5 Pre-commit hook (local, opt-in)

`.githooks/pre-commit`:

```bash
#!/usr/bin/env bash
set -e
go vet ./...
go test -short -race ./...
golangci-lint run
```

Активация: `git config core.hooksPath .githooks` (упоминается в README).

### 8.6 Branch protection

`main` защищена. Merge возможен только если зелёные:
- `ci/lint`
- `ci/test`
- `ci/fuzz-corpus-replay`
- `bench-gate/bench`
- `conformance-gate/rfc`

### 8.7 Release / tagging

Phase A — pre-1.0. Версионирование SemVer 0.x. Релизы — git tags `v0.X.Y`. Никакой автопубликации в Go module proxy — пользователи берут `go get github.com/lodgvideon/poseidon-http-client@v0.X.Y`. README покрывает совместимость и нестабильность API в 0.x.

---

## 9. Forward Compatibility (Phase B/C interfaces preview)

Не реализуем в Phase A, но фиксируем формы интерфейсов, чтобы Phase A API не пришлось ломать. Если Phase B/C проработка покажет, что текущее API не подходит, — правим Phase A до релиза.

### 9.1 Phase B (Connection layer) — пакет `conn/`

```go
package conn

// Conn — единичный HTTP/2 коннект поверх net.Conn. Внутри — *frame.Framer.
type Conn struct { /* ... */ }

// Dialer — Open-Closed: разные транспорты (TLS, h2c, mTLS) реализуют интерфейс.
type Dialer interface {
    Dial(ctx context.Context, addr string) (net.Conn, error)
}

func NewConn(ctx context.Context, transport net.Conn, opts ConnOptions) (*Conn, error)

// Stream — один HTTP-запрос. Closer закрывает stream через RST_STREAM или END_STREAM.
type Stream interface {
    SendHeaders(ctx context.Context, headers []hpack.HeaderField, endStream bool) error
    SendData(ctx context.Context, data []byte, endStream bool) error
    Recv(ctx context.Context) (StreamEvent, error) // headers / data / trailer / closed
    Close() error
}

func (*Conn) NewStream(ctx context.Context) (Stream, error)
func (*Conn) Stats() ConnStats
```

Контракты Phase A → Phase B:
- `Conn` создаёт `*frame.Framer(tlsConn, tlsConn)` и держит один `*hpack.Encoder` + один `*hpack.Decoder` на инстанс.
- frame-layer event-поток (через `Handler`) транслируется в stream-events.

### 9.2 Phase C (Client + pool + discovery + stats) — пакет `client/`

```go
package client

type Client struct { /* ... */ }

// Discoverer — Open-Closed: DNS, статический список, Consul, k8s, custom corp.
type Discoverer interface {
    Resolve(ctx context.Context, host string) ([]Endpoint, error)
    Watch(ctx context.Context, host string) (<-chan ResolveEvent, error)
}

type Endpoint struct {
    Addr   string
    Weight int
    Meta   map[string]string
}

type Resolver struct { /* ... */ }

type LoadBalancer interface {
    Pick(endpoints []Endpoint) Endpoint
}

type PoolOptions struct {
    MaxConnsPerHost     int
    MaxStreamsPerConn   int   // ≤ peer SETTINGS_MAX_CONCURRENT_STREAMS
    DialTimeout         time.Duration
    IdleTimeout         time.Duration
    Dialer              conn.Dialer
}

type Request struct {
    Method, Path, Authority, Scheme []byte
    ExtraHeaders []hpack.HeaderField
    Body         io.Reader
}

type Response struct {
    Status   int
    Headers  []hpack.HeaderField
    Body     []byte
    Stats    RequestStats
}

type RequestStats struct {
    DNSStart, DNSDone           time.Time
    DialStart, DialDone         time.Time
    TLSHandshakeDone            time.Time
    HeadersSent, HeadersRecvd   time.Time
    FirstByte, LastByte         time.Time
    ConnReused                  bool
    StreamID                    uint32
    BytesSent, BytesRecvd       int64
}

func (c *Client) Do(ctx context.Context, req *Request, resp *Response) error
```

Контракты Phase A → Phase C:
- `RequestStats` — Phase C обвешивает Phase B вызовы тайм-стампами; frame-layer чист от clock/stats.
- Discovery полностью изолирована от frame/conn — Phase C компонует.

### 9.3 Что Phase A фиксирует для forward compat

| Решение | Зафиксировано |
|---|---|
| `hpack.HeaderField` slice-view, не string | да — Phase B/C получают дешёвые заголовки |
| `frame.Handler` visitor + caller-buffer ownership | да — Phase B оборачивает в stream-events без копий |
| `Framer` НЕ goroutine-safe | да (документируется) — Phase B mutex-ит на уровне Conn |
| Sentinel errors | да — Phase B/C reuse'ят для сетевых ошибок (e.g. GOAWAY mapping) |
| `SettingsParams` — fixed-size POD | да — Phase B обменивается без heap |

### 9.4 Что НЕ фиксируем (свободно меняется в Phase B/C)

- Конкретные пороги bench gate для Phase B/C (другие — там сетевой шум).
- Точная форма `RequestStats` (поля могут расширяться).
- Discovery API (несколько прототипов проработаются в Phase C дизайне).

---

## 10. Open Questions / Future Work

1. **Reference hardware для абсолютных bench-thresholds** — выбираем класс машины для baseline; nightly job на dedicated runner.
2. **GitLab vs GitHub Actions** — текущий план GitHub; миграция тривиальна, если потребуется.
3. **License** — по умолчанию MIT. Подтвердить с владельцем репо.
4. **Phase B early prototype** — до релиза Phase A полезно сделать прототип TLS+ALPN handshake + один SETTINGS обмен, чтобы убедиться, что `frame.Framer` API живёт без изменений в реальной сети.

---

## 11. Acceptance Criteria для Phase A

Phase A считается готовой к релизу `v0.1.0`, если выполнено всё:

- [ ] Все 10 frame types из RFC 7540 §6 кодируются и декодируются (encode∘decode == identity для всех валидных входов).
- [ ] HPACK (RFC 7541) проходит все vectors из App. C (C.1–C.6, с/без Huffman).
- [ ] `go test -race ./...` — зелёный.
- [ ] `golangci-lint` — зелёный.
- [ ] `bench-gate` — все hot-path benchmarks с `allocs/op == 0` и `B/op == 0` после warmup.
- [ ] `conformance-gate` — все RFC-помеченные тесты зелёные; `docs/RFC_COVERAGE.md` покрывает RFC 7540 §4–§6 и RFC 7541 §2–§7 без дыр.
- [ ] `nightly fuzz` — не находит crash'ей в течение последних 7 ночей перед тегом.
- [ ] README с описанием API, ограничениями (`Framer` не goroutine-safe), примером usage'а кодека.
- [ ] `BENCH_BASELINE.md` зафиксирован (machine, ns/op цифры).
