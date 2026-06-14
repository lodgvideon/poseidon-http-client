# RFC Coverage Matrix

Each row maps an RFC section to the tests that exercise it. `Conformance`
tests build the wire-byte fixture by hand from the RFC's diagrams and feed
it through the parser; `Roundtrip` tests use the package's own Write* path
and round-trip through ReadFrame. The conformance row is what the
`conformance-gate` CI job enforces.

## RFC 7540 — HTTP/2

| Section | Type        | Test |
|---------|-------------|------|
| §3.5    | Conformance | TestConformance_RFC7540_Sec35_ClientPreface |
| §3.5    | Roundtrip   | TestFramer_ClientPreface |
| §4.1    | Conformance | TestConformance_RFC7540_Sec41_FrameHeader_RBitMasked |
| §4.1    | Roundtrip   | TestReadFrameHeader_Sample, TestWriteFrameHeader |
| §6.1    | Conformance | TestConformance_RFC7540_Sec61_DataFrame_PaddedEndStream |
| §6.1    | Roundtrip   | TestFramer_Data_Roundtrip, TestFramer_DataPadded_Roundtrip |
| §6.2    | Conformance | TestConformance_RFC7540_Sec62_HeadersFrame_PriorityPaddedEndHeaders |
| §6.2    | Roundtrip   | TestFramer_Headers_RoundTrip, TestFramer_HeadersWithPriority_RoundTrip, TestFramer_HeadersPadded_RoundTrip |
| §6.3    | Conformance | TestConformance_RFC7540_Sec63_PriorityFrame |
| §6.3    | Roundtrip   | TestFramer_Priority_RoundTrip |
| §6.4    | Conformance | TestConformance_RFC7540_Sec64_RstStreamFrame |
| §6.4    | Roundtrip   | TestFramer_RSTStream_RoundTrip |
| §6.5    | Conformance | TestConformance_RFC7540_Sec65_SettingsFrame, TestConformance_RFC7540_Sec65_SettingsAck |
| §6.5    | Roundtrip   | TestFramer_Settings_RoundTrip, TestFramer_SettingsAck_RoundTrip |
| §6.6    | Conformance | TestConformance_RFC7540_Sec66_PushPromiseFrame |
| §6.6    | Roundtrip   | TestFramer_PushPromise_RoundTrip |
| §6.7    | Conformance | TestConformance_RFC7540_Sec67_PingFrame |
| §6.7    | Roundtrip   | TestFramer_Ping_RoundTrip |
| §6.8    | Conformance | TestConformance_RFC7540_Sec68_GoAwayFrame |
| §6.8    | Roundtrip   | TestFramer_GoAway_RoundTrip |
| §6.9    | Conformance | TestConformance_RFC7540_Sec69_WindowUpdateFrame |
| §6.9    | Roundtrip   | TestFramer_WindowUpdate_RoundTrip, TestFramer_WindowUpdate_ZeroIncrementRejected |
| §6.10   | Conformance | TestConformance_RFC7540_Sec610_ContinuationFrame |
| §6.10   | Roundtrip   | TestFramer_Continuation_RoundTrip |

### B.1 / B.2.1 / B.2.2 / B.2.3 / B.2.4 / B.2.5 / B.2.6 connection-layer integration

Phase B.1 added a `conn/` package on top of the codec. Phase B.2.1
lifts the single-stream cap to a configurable
`AdvertisedSettings.MaxConcurrentStreams` (default 100) and assigns
stream IDs at first-HEADERS write time under the writer mutex,
preserving the RFC 7540 §5.1.1 monotonic-id ordering across
concurrent `NewStream` callers. Phase B.2.2 wires receive-side
flow control: per-stream and connection recv windows debited on each
inbound DATA frame (RFC 7540 §6.9.1); WINDOW_UPDATE refunds batched
once an accumulated counter crosses 32 KiB; peer overruns surface as
typed `StreamError` / `ConnError(FLOW_CONTROL_ERROR)`. Phase B.2.3
adds outbound flow control: `writeData` chunks at
`min(peer MAX_FRAME_SIZE, our advertised MAX_FRAME_SIZE)` and blocks
in `acquireSendCredits` until both per-stream and connection-level
peer-advertised send windows have credit; `OnWindowUpdate` bumps
those windows and broadcasts the writer cond; 2^31-1 overflow on
either scope returns a typed `StreamError` / `ConnError`. Phase B.2.4
adds dynamic SETTINGS handling: `connHandler.OnSettings` merges
non-ACK frames into `c.peerSettings`, applies side effects
(HPACK encoder resize, retroactive `INITIAL_WINDOW_SIZE` delta on
every open stream — RFC §6.9.2), and emits a SETTINGS ACK
(RFC §6.5.3). Phase B.2.5 honors peer-advertised
`SETTINGS_MAX_CONCURRENT_STREAMS`: `NewStream` gates inflight on
`min(local advertised, peer-advertised)`; dynamic shrinks via
`applyPeerSettings` refuse new streams without disturbing open ones
(RFC §6.5.2). Phase B.2.6 finishes the lifecycle: `connHandler.OnGoAway`
records the GOAWAY state, refuses new streams with `ErrGoAway`, drains
streams whose id exceeds `lastStreamID` with `EventReset(REFUSED_STREAM)`,
and wakes blocked writers (RFC §6.8); `connHandler.OnPing` echoes
non-ACK PING frames with `ACK=1` and the original 8-byte payload
(RFC §6.7). Rows below cite tests in the `conn` package.

| Section | Type        | Test |
|---------|-------------|------|
| §3.5    | Conformance | TestConformance_RFC7540_Sec3_ClientPreface_OnTheWire (conn) |
| §3.5    | Integration | TestIntegration_EmptyGET (handshake + preface byte sequence on the wire) |
| §6.5    | Integration | TestConn_HandshakeAndIdle, TestHandshakeSettings_RoundTripsAgainstPipePeer (handshake + ack roundtrip) |
| §5.1    | Integration | TestIntegration_EmptyGET, TestIntegration_POST_1KB_Echo (single-stream end-to-end) |
| §5.1.1  | Integration | TestIntegration_TenConcurrentStreams_Echo (10 concurrent streams; monotonic-id wire order) |
| §5.1.1  | Unit        | TestConn_NewStream_RespectsAdvertisedLimit, TestConn_NewStream_ConcurrentAllocation_RespectsCap |
| §6.4    | Integration | TestIntegration_ContextCancel_TearsDownStream (context-cancel surfaces RST_STREAM(CANCEL)) |
| §6.6    | Negative    | TestHandler_OnPushPromise_ReturnsConnError (PUSH_PROMISE rejected with PROTOCOL_ERROR while ENABLE_PUSH=0) |
| §6.9.1  | Integration | TestIntegration_LargeBody_RefundsRecvWindow_NoStall (>65535-byte body completes only when WINDOW_UPDATE is emitted) |
| §6.9.1  | Unit        | TestConn_OnData_EmitsWindowUpdate_OnceThresholdReached (per-stream + conn refund frames) |
| §6.9.1  | Negative    | TestConn_OnData_PeerOverflowsConnWindow_ReturnsConnError, TestConn_OnData_PeerOverflowsStreamWindow_ReturnsStreamError |
| §6.9.1  | Integration | TestIntegration_LargePOST_RespectsPeerSendWindow (200 KiB upload completes via WINDOW_UPDATE-driven send credit) |
| §6.9.1  | Unit        | TestConn_AcquireSendCredits_BlocksUntilWindowUpdate, TestConn_AcquireSendCredits_HonorsCtxCancel, TestConn_WriteData_ChunksByPeerMaxFrameSize |
| §6.9.1  | Negative    | TestConn_OnWindowUpdate_OverflowsConn_ReturnsConnError, TestConn_OnWindowUpdate_OverflowsStream_ReturnsStreamError |
| §6.5.3  | Unit        | TestOnSettings_AckFlag_IsNoop, TestOnSettings_NonAck_WritesAckFrame |
| §6.9.2  | Unit        | TestApplyPeerSettings_InitialWindowSizeDelta_AppliesToAllStreams, TestApplyPeerSettings_NegativeDelta_AllowsNegativeWindow |
| §6.9.2  | Negative    | TestApplyPeerSettings_OverflowDelta_ReturnsConnError |
| §6.5.2  | Unit        | TestSetPeerSetting_MergesAndReplaces, TestApplyPeerSettings_HeaderTableSize_PropagatesToEncoder |
| §6.5.2  | Unit        | TestLookupPeerSetting_PresentVsAbsent, TestNewStream_PeerLimitTighterThanLocal_Wins, TestNewStream_PeerLimitAbsent_FallsThroughToLocal, TestNewStream_PeerLimitLargerThanLocal_LocalWins, TestNewStream_PeerLimitZero_BlocksAllNewStreams, TestApplyPeerSettings_LowerMaxConcurrent_DoesNotCloseExistingStreams |
| §6.7    | Unit        | TestOnPing_AckFrame_IsNoop (ACK routed to deliverPingAck; no echo), TestOnPing_NonAck_EchoesPayloadWithAckFlag |
| §6.7    | Integration | TestConn_Ping_RTT (client-initiated PING; RTT measured after wmu flush), TestConn_Ping_ConcurrentSafe (20 concurrent PINGs; race-clean), TestConn_Ping_CtxCancelledBeforeACK (ctx-cancel cleans waiter), TestConn_Ping_AfterClose (ErrConnClosed fast-path) |
| §6.7    | Integration | TestConn_Keepalive_HealthyConn (periodic PING; live conn not closed), TestConn_Keepalive_ClosesDeadConn (TCP FIN → readerDone → close), TestConn_Keepalive_PingTimeout (PING unanswered → KeepaliveTimeout → close), TestConn_DeliverPingAck_UnsolicitedIsNoop (unsolicited ACK silently ignored) |
| §6.8    | Unit        | TestOnGoAway_BlocksNewStream, TestOnGoAway_StreamsAtOrBelowLastID_Survive, TestOnGoAway_WakesAcquireSendCredits |
| §8.1    | Conformance | TestConformance_RFC7540_Sec8_1_StreamBody_EndStream (client/) |
| §8.1    | Integration | TestIntegration_Client_StreamBody_Small, TestIntegration_Client_StreamBody_Large, TestIntegration_Client_StreamBody_CloseEarly (client/) |
| §8.1.2.1 | Conformance | TestConformance_RFC7540_Sec8_1_2_1_PseudoHeadersFirst (client/) |
| §5.1.2   | Conformance | TestConformance_RFC7540_Sec5_1_2_PoolGatesOnPeerMaxStreams (client/) |
| §6.8     | Conformance | TestConformance_RFC7540_Sec6_8_PoolDrainsOnGoAway (client/) |
| §6.8     | Conformance | TestConformance_RFC7540_Sec6_8_PoolEjectsDeadConnOnRelease (client/) — pool evicts dead conn via release path, not health-check tick |
| §8.1.3   | Conformance | TestConformance_RFC7540_Sec8_1_3_RequestTrailers (client/) — request trailer HEADERS+END_STREAM sent after body DATA frames |
| §8.1.4   | Conformance | TestRetryer_Do_RefusedStream_Retries (client/) — retry layer retries on REFUSED_STREAM (RFC 7540 §8.1.4 — request not processed) |
| §4.2     | Unit        | TestPaddingStrategy_Disabled, TestPaddingStrategy_Fixed, TestPaddingStrategy_Range, TestPaddingStrategy_MaxLessThanMin, TestPaddingStrategy_DataOnly, TestPaddingStrategy_BothFrames (conn/) — PaddingStrategy for DATA and HEADERS frames |
| §6.1.1   | Roundtrip   | TestFramer_DataPadded_Roundtrip (frame/) — padded DATA frame encode/decode |
| §6.2.2   | Roundtrip   | TestFramer_HeadersPadded_RoundTrip (frame/) — padded HEADERS frame encode/decode |
| §8.2     | Integration | TestConn_PushPromise_DeliveredToParentStream (conn/) — EventPushPromise with PushStreamID; pushed stream registered and headers decoded |
| §8.2     | Negative    | TestConn_PushPromise_DisabledReturnsProtocolError (conn/) — PUSH_PROMISE rejected with PROTOCOL_ERROR when EnablePush=false |
| §8.2     | Integration | TestIntegration_Push_HandlerInvoked (client/) — PushHandler callback receives fully drained pushed Response |
| §8.2     | Negative    | TestIntegration_Push_Disabled (client/) — push disabled when PushHandler=nil; server push rejected |

## RFC 8336 — ORIGIN Frame

| Section | Type        | Test |
|---------|-------------|------|
| §2.1   | Unit        | TestDispatchOrigin_Valid (frame/) — TLV parsing of ORIGIN frame payload |
| §2.1   | Negative    | TestDispatchOrigin_RejectsNonZeroStream (frame/) — stream-0 enforcement |
| §2.1   | Negative    | TestDispatchOrigin_MalformedTrailingByte (frame/) — malformed trailing byte detection |
| §2.1   | Negative    | TestDispatchOrigin_LengthOverflow (frame/) — origin-string length overflow |
| §2.1   | Negative    | TestDispatchOrigin_Empty (frame/) — empty ORIGIN frame accepted |

## RFC 8441 — Bootstrapping WebSockets with HTTP/2

| Section | Type        | Test |
|---------|-------------|------|
| §4     | Unit        | TestConn_ConnectProtocolSupported_True, TestConn_ConnectProtocolSupported_False, TestConn_ConnectProtocolSupported_ZeroValue (conn/) — SETTINGS_ENABLE_CONNECT_PROTOCOL advertisement check |
| §5     | Unit        | TestBuildHeaders_ProtocolExtendedConnect (client/) — `:protocol` pseudo-header emitted for CONNECT+Protocol |
| §5     | Negative    | TestBuildHeaders_NoProtocolWhenEmpty (client/) — `:protocol` omitted when Request.Protocol is empty |
| §5     | Conformance | TestBuildHeaders_ProtocolOrdering (client/) — `:protocol` appears after `:path`, before regular headers |

## HTTP/1.1 CONNECT Proxy (RFC 7231 §4.3.6 tunneling)

| Section | Type        | Test |
|---------|-------------|------|
| §4.3.6  | Integration | TestProxyDialer_Plaintext (conn/) — plaintext proxy tunnel via CONNECT |
| §4.3.6  | Integration | TestProxyDialer_BasicAuth (conn/) — proxy auth via Proxy-Authorization header |
| §4.3.6  | Negative    | TestProxyDialer_NilURL (conn/) — nil proxy URL returns error |
| §4.3.6  | Negative    | TestProxyDialer_BadResponse (conn/) — non-200 proxy response returns error |

## RFC 7541 — HPACK

| Section  | Type        | Test |
|----------|-------------|------|
| §5.1     | Roundtrip   | TestEncodeInteger_RFCExamples, TestDecodeInteger_RFCExamples, TestDecodeInteger_Truncated, TestDecodeInteger_Overflow |
| §5.2     | Roundtrip   | TestEncodeStringLiteral_*, TestDecodeStringLiteral_*, TestHuffmanEncode_*, TestHuffmanDecode_* |
| §C.2.1   | Conformance | TestConformance_RFC7541_C2_1_LiteralIndexing |
| §C.2.2   | Conformance | TestConformance_RFC7541_C2_2_LiteralNoIndexing |
| §C.2.3   | Conformance | TestConformance_RFC7541_C2_3_NeverIndexed |
| §C.2.4   | Conformance | TestConformance_RFC7541_C2_4_Indexed |
| §C.3.1   | Conformance | TestConformance_RFC7541_C3_1_FirstRequest |
| §C.4.1   | Conformance | TestConformance_RFC7541_C4_1_FirstRequestHuffman |
| §C.3 / sequence | Roundtrip | TestConformance_RFC7541_RoundTrip_C3_FirstRequest, TestConformance_RFC7541_RoundTrip_RequestSequence |

## Gate

`scripts/rfc-coverage-gate.sh` requires at least one passing
`TestConformance_RFC7540_*` AND `TestConformance_RFC7541_*` test, and fails
on any conformance-test failure. It is wired to the `conformance-gate` job
in `.github/workflows/conformance-gate.yml`.
