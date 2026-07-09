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
| §6.5.2  | Integration | TestConformance_RFC7540_Sec6_5_2_MaxFrameSize_FramerReadLimit (conn) — advertised MaxFrameSize >16384 synced to Framer read limit; peer may send frames up to advertised size |
| §6.7    | Unit        | TestOnPing_AckFrame_IsNoop (ACK routed to deliverPingAck; no echo), TestOnPing_NonAck_EchoesPayloadWithAckFlag |
| §6.7    | Integration | TestConn_Ping_RTT (client-initiated PING; RTT measured after wmu flush), TestConn_Ping_ConcurrentSafe (20 concurrent PINGs; race-clean), TestConn_Ping_CtxCancelledBeforeACK (ctx-cancel cleans waiter), TestConn_Ping_AfterClose (ErrConnClosed fast-path) |
| §6.7    | Integration | TestConn_Keepalive_HealthyConn (periodic PING; live conn not closed), TestConn_Keepalive_ClosesDeadConn (TCP FIN → readerDone → close), TestConn_Keepalive_PingTimeout (PING unanswered → KeepaliveTimeout → close), TestConn_DeliverPingAck_UnsolicitedIsNoop (unsolicited ACK silently ignored) |
| §6.8    | Unit        | TestOnGoAway_BlocksNewStream, TestOnGoAway_StreamsAtOrBelowLastID_Survive, TestOnGoAway_WakesAcquireSendCredits |
| §6.2    | Conformance | TestConformance_RFC7540_Sec6_2_HeadersSplitIntoContinuation (conn) — oversized HEADERS block split into HEADERS+CONTINUATION frames |
| §6.10   | Conformance | TestConformance_RFC7540_Sec6_10_ContinuationFlagsAndPadding (conn) — padding/priority only on HEADERS; END_HEADERS only on final CONTINUATION |
| §8.1    | Conformance | TestConformance_RFC7540_Sec8_1_BodyStream_EndStream (client/) |
| §8.1    | Integration | TestIntegration_Client_BodyStream_Small, TestIntegration_Client_BodyStream_Large, TestIntegration_Client_BodyStream_CloseEarly (client/) |
| §8.1.2.1 | Conformance | TestConformance_RFC7540_Sec8_1_2_1_PseudoHeadersFirst (client/) |
| §8.1.2.2 | Negative    | TestValidateRequest_PseudoHeaderInRegular (client/) — request validator rejects pseudo-header (':authority') in regular Headers slice |
| §8.1.2.3 | Negative    | TestConformance_RFC7540_Sec8_1_2_3_ForbidsConnection, _ForbidsKeepAlive, _ForbidsProxyConnection, _ForbidsTransferEncoding, _ForbidsUpgrade (client/) — request validator rejects RFC 7540 §8.1.2.3 connection-specific headers as request-smuggling vectors |
| §8.1.2.3 | Negative    | TestConformance_RFC7540_Sec8_1_2_3_TEOnlyTrailers (client/) — TE header allowed ONLY with value "trailers"; any other value rejected |
| §8.1.2.6 | Negative    | TestConformance_RFC7540_Sec8_1_2_6_... (client/) — pseudo-header values conform to token grammar (method / path / scheme / authority / status) |
| RFC 8441 §4 | Negative | TestConformance_RFC8441_Sec4_ProtocolRequiresConnectMethod (client/) — :protocol pseudo-header MUST NOT appear unless Method=CONNECT |
| RFC 8441 §4 | Conformance | TestConformance_RFC8441_Sec4_Protocol_EmptyOnConnect_OK, _NonEmptyOnConnect_OK (client/) — CONNECT with/without :protocol is allowed; :protocol emission controlled by Protocol field |
| §7     | Negative    | TestConformance_RFC7540_Sec7_RetryOnInternalError, _RetryOnEnhanceYourCalm, _NoRetryOnProtocolError, _NoRetryOnCancel (client/) — builtinShouldRetry classifies RST codes: REFUSED_STREAM/INTERNAL_ERROR/ENHANCE_YOUR_CALM → retry; PROTOCOL_ERROR/CANCEL → no retry |
| §6.8   | Conformance | TestConformance_RFC7540_Sec6_8_RetryGoAway_StillRetries (client/) — GOAWAY continues to be retried (regression check) |
| §5.1.2   | Conformance | TestConformance_RFC7540_Sec5_1_2_PoolGatesOnPeerMaxStreams (client/) |
| §6.8     | Conformance | TestConformance_RFC7540_Sec6_8_PoolDrainsOnGoAway (client/) |
| §6.8     | Conformance | TestConformance_RFC7540_Sec6_8_PoolEjectsDeadConnOnRelease (client/) — pool evicts dead conn via release path, not health-check tick |
| §8.1.3   | Conformance | TestConformance_RFC7540_Sec8_1_3_RequestTrailers (client/) — request trailer HEADERS+END_STREAM sent after body DATA frames |
| §8.1.4   | Conformance | TestRetryer_Do_RefusedStream_Retries (client/) — retry layer retries on REFUSED_STREAM (RFC 7540 §8.1.4 — request not processed) |
| §4.2     | Unit        | TestPaddingStrategy_Disabled, TestPaddingStrategy_Fixed, TestPaddingStrategy_Range, TestPaddingStrategy_MaxLessThanMin, TestPaddingStrategy_DataOnly, TestPaddingStrategy_BothFrames (conn/) — PaddingStrategy for DATA and HEADERS frames |
| §6.1.1   | Roundtrip   | TestFramer_DataPadded_Roundtrip (frame/) — padded DATA frame encode/decode |
| §6.2.2   | Roundtrip   | TestFramer_HeadersPadded_RoundTrip (frame/) — padded HEADERS frame encode/decode |
| §8.2     | Integration | TestConn_PushPromise_DeliveredToParentStream (conn/) — EventPushPromise with PushStreamID; pushed stream registered and headers decoded |
| §8.2     | Integration | TestIntegration_Push_HandlerInvoked, _Disabled (client/) — server push end-to-end; handler invocation and PROTOCOL_ERROR when push disabled |
| §8.2     | Integration | TestIntegration_Push_HandlerReceivesNon2xx, _MultipleConcurrent (client/) — pushed stream with non-2xx status reaches handler; 4 concurrent PUSH_PROMISE frames all delivered |
| §8.2     | Negative    | TestConn_PushPromise_DisabledReturnsProtocolError (conn/) — PUSH_PROMISE rejected with PROTOCOL_ERROR when EnablePush=false |
| §5.2     | Negative    | TestConformance_RFC7541_C5_2_HuffmanDecode_InvalidCode, _PrefixOfEos, _EmptyInput (hpack/) — malformed Huffman input returns ErrInvalidHuffman; empty input is valid |
| §5.2     | Roundtrip   | TestConformance_RFC7541_C5_2_HuffmanDecode_LongString_RoundTrip (hpack/) — 1024-byte ASCII string round-trips through Huffman encode/decode |

## RFC 2616 — HTTP/1.1 (http1/ package)

| Section | Type        | Test |
|---------|-------------|------|
| §3.6.1  | Conformance | TestConformance_RFC2616_Sec3_6_1_MultipleChunks — multi-chunk body reassembled across ReadBodyChunk calls |
| §3.6.1  | Conformance | TestConformance_RFC2616_Sec3_6_1_EmptyChunkedBody — terminal 0-chunk produces empty body immediately |
| §4.4 R3 | Conformance | TestConformance_RFC2616_Sec4_4_Rule3_ChunkedWinsContentLengthFirst — Transfer-Encoding beats Content-Length (CL first) |
| §4.4 R3 | Conformance | TestConformance_RFC2616_Sec4_4_Rule3_ChunkedWinsTransferEncodingFirst — Transfer-Encoding beats Content-Length (TE first) |
| §6.1    | Conformance | TestConformance_RFC2616_Sec6_1_HTTP10StatusLineParsed — HTTP/1.0 status line accepted by parser |
| §8.1    | Conformance | TestConformance_RFC2616_Sec8_1_HTTP10DefaultClose — HTTP/1.0 without Connection header → KeepAlive() false |
| §8.1    | Conformance | TestConformance_RFC2616_Sec8_1_HTTP10KeepAliveHeader — HTTP/1.0 + Connection: keep-alive → KeepAlive() true |
| §10.3.5 | Conformance | TestConformance_RFC2616_Sec10_3_5_304NoBody — 304 body skipped even when Content-Length present |
| §14.23  | Conformance | TestConformance_RFC2616_Sec14_23_HostHeaderInRequest — request wire includes Host derived from :authority |

## RFC 8336 — ORIGIN Frame

| Section | Type        | Test |
|---------|-------------|------|
| §2.1   | Unit        | TestDispatchOrigin_Valid (frame/) — TLV parsing of ORIGIN frame payload |
| §2.1   | Negative    | TestDispatchOrigin_RejectsNonZeroStream (frame/) — stream-0 enforcement |
| §2.1   | Negative    | TestDispatchOrigin_MalformedTrailingByte (frame/) — malformed trailing byte detection |
| §2.1   | Negative    | TestDispatchOrigin_LengthOverflow (frame/) — origin-string length overflow |
| §2.1   | Negative    | TestDispatchOrigin_Empty (frame/) — empty ORIGIN frame accepted |

## RFC 7838 — HTTP Alternative Services (ALTSVC)

| Section | Type        | Test |
|---------|-------------|------|
| §4     | Roundtrip   | TestFramer_AltSvc_RoundTrip (frame/) — server-wide ALTSVC: origin + alt-value TLV encode/decode |
| §4     | Roundtrip   | TestFramer_AltSvc_PerStream_RoundTrip (frame/) — per-stream ALTSVC: empty origin, non-zero stream |
| §4     | Roundtrip   | TestFramer_AltSvc_EmptyClears (frame/) — empty entries = clear all alt-svc |
| §4     | Negative    | TestDispatchAltSvc_MalformedTrailingBytes (frame/) — trailing bytes after last entry |
| §4     | Negative    | TestDispatchAltSvc_OriginOverflow (frame/) — origin-length exceeds payload |
| §4     | Negative    | TestDispatchAltSvc_AltValueOverflow (frame/) — alt-value-length exceeds payload |

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

## RFC 9000 — QUIC transport (Phase G — HTTP/3)

Phase G builds a from-scratch HTTP/3 stack (see
[HTTP3_DESIGN.md](HTTP3_DESIGN.md)). G.2 lands the QUIC variable-length
integer codec (`internal/bytesx`), the primitive under every QUIC/H3 length,
offset, stream ID, frame type, and transport-parameter value. G.4 lands the
`quic/` frame codec: parse + serialize the RFC 9000 §19 frames a client sends
and receives, dispatched through a `FrameHandler` visitor. G.4b adds the
packet-header codec (§17): parse/write long, short, Retry, and Version
Negotiation headers, locating the protected packet-number offset. Packet
protection (RFC 9001) and the connection engine are later phases.

| Section | Type        | Test |
|---------|-------------|------|
| §16     | Conformance | TestConformance_RFC9000_Sec16_VarintRoundTrip, TestConformance_RFC9000_Sec16_NonMinimalDecode, TestConformance_RFC9000_Sec16_IncompleteInput |
| §16     | Roundtrip   | TestVarint_ExhaustiveRoundTrip |
| §19.3   | Conformance | TestConformance_RFC9000_Sec193_AckFrame, TestConformance_RFC9000_Sec193_AckECN |
| §19.8   | Conformance | TestConformance_RFC9000_Sec19_StreamFrame, TestParseStream_NoLength |
| §19.15  | Conformance | TestConformance_RFC9000_Sec1915_NewConnectionID |
| §19.17  | Conformance | TestConformance_RFC9000_Sec1917_PathChallenge |
| §8.2.2  | Conformance | TestConformance_RFC9000_Sec822_PathChallengeEchoed (a received PATH_CHALLENGE queues a PATH_RESPONSE echoing its 8 bytes), TestConn_PathChallenge_SentByFlush (the PATH_RESPONSE is written on the next flush with the echoed data) |
| §19.19  | Conformance | TestConformance_RFC9000_Sec1919_ConnectionClose |
| §19.1   | Conformance | TestConformance_RFC9000_Sec191_Padding |
| §19 (all frames) | Roundtrip | TestFrames_RoundTrip |
| §12.4   | Negative    | TestParseFrames_Malformed, TestParseFrames_MoreMalformed (malformed → ErrFrameEncoding) |
| §17.2.2 | Conformance | TestConformance_RFC9000_Sec172_InitialHeader |
| §17.2.5 | Conformance | TestConformance_RFC9000_Sec1725_RetryHeader |
| §17.2.1 | Conformance | TestConformance_RFC9000_Sec171_VersionNegotiation |
| §17.3   | Conformance | TestConformance_RFC9000_Sec173_ShortHeader |
| §12.2   | Conformance | TestParseHeader_Coalesced (coalesced-packet walk via PacketLen) |
| §17     | Roundtrip   | TestPacketHeader_RoundTrip |
| §17     | Negative    | TestParseHeader_Malformed (malformed header → ErrPacketEncoding) |
| §14.1   | Conformance | TestConformance_RFC9000_Sec141_InitialFlight (real ClientHello → padded ≥1200 Initial → protect → parse+decrypt round-trip) |
| §12.2   | Unit        | TestProcessDatagram_ServerInitial, TestProcessDatagram_Coalesced (split coalesced packets, decrypt per level, dispatch frames) |
| §12.2   | Negative    | TestProcessDatagram_SkipNoKeys, TestProcessDatagram_AuthFailure, TestProcessDatagram_Retry, TestProcessDatagram_Malformed |
| §13.2 / §19.3 | Unit  | TestAckTracker_RoundTrip, TestAckTracker_PendingAndLargest (received PNs → ACK ranges → decode back to the exact set) |
| §7.1 / §14.1 | Unit   | TestConn_SendInitialFlight (Conn drives the handshake to a ClientHello and sends one padded Initial datagram that decrypts back to it) |
| §7 / RFC 9001 §4 | Integration | TestConn_Establish_InMemory (**full QUIC v1 + TLS 1.3 handshake** completes between the client Conn and an in-memory server over a datagram pipe: Initial + Handshake flights, key installs, handshake done) |
| §19.6   | Unit        | TestConnFrameHandler_OnCrypto_ReassemblesByOffset (out-of-order CRYPTO frames reassembled by offset before feeding TLS — a real server's certificate flight spans many frames) |
| §2.2    | Conformance | TestConformance_RFC9000_Sec2_StreamReassembly (out-of-order STREAM frames reassembled to correct byte stream; complete only once FIN + all preceding bytes present) |
| §2.1    | Unit        | TestConn_OpenStream_IDs (client bidi stream IDs 0, 4, 8, …), TestConn_OnStream_DeliversToOpenStream (inbound STREAM routed to opened stream), TestConn_OpenUniStream_IDs (client uni stream IDs 2, 6, 10 + initial_max_streams_uni gate) |
| §2.2 / §13 | Unit     | TestStream_RecvAndFinished (Recv returns newly-contiguous bytes; Finished flips on FIN), TestConn_Poll_DeliversStreamData (post-handshake Poll decrypts a 1-RTT packet and delivers STREAM data to the open stream) |
| §18 / §18.2 | Conformance | TestConformance_RFC9000_Sec18_TransportParamsParse, TestConformance_RFC9000_Sec182_BidiRemoteBoundsClientStream (server params parsed to send limits; a request stream is bounded by initial_max_stream_data_bidi_remote 0x06, not _local) |
| §18 / §7.3 | Conformance | TestConformance_RFC9000_Sec18_TransportParamsEncode (client encodes the params it advertises — receive credit via initial_max_stream_data_bidi_local 0x05, server-uni limits, initial_source_connection_id — and the decoder accepts them) |
| §18.2   | Unit        | TestTransportParams_UnknownAndGREASEIgnored, TestTransportParams_AbsentDefaults (unknown/GREASE skipped; absent flow-control params default to 0) |
| §7.4    | Negative    | TestConformance_RFC9000_Sec74_DuplicateParam, TestConformance_RFC9000_Sec74_MalformedParam, TestConformance_RFC9000_Sec74_InvalidValue (duplicate / malformed encoding / invalid value → ErrTransportParameter = TRANSPORT_PARAMETER_ERROR) |
| §7.3    | Conformance | TestConformance_RFC9000_Sec73_InitialSCIDAuthenticated (the server's initial_source_connection_id is authenticated against the SCID the client adopted; a mismatch or an absent parameter → TRANSPORT_PARAMETER_ERROR) |
| §4.6    | Conformance | TestConformance_RFC9000_Sec46_StreamLimit (OpenStream past peer initial_max_streams_bidi → ErrTooManyStreams; zero limit forbids the first) |
| §2.1    | Conformance | TestConformance_RFC9000_Sec21_AcceptServerUniStream (a server-initiated uni stream, id&3==3, is accepted, reassembled, and queued once for AcceptUniStream), TestConformance_RFC9000_Sec21_ServerBidiRejected (a server-initiated bidi stream → ErrServerBidiStream / STREAM_LIMIT_ERROR), TestConn_AcceptUniStream_Order |
| §4.6    | Conformance | TestConformance_RFC9000_Sec46_UniStreamLimit (a server uni stream beyond the advertised initial_max_streams_uni → ErrTooManyUniStreams / STREAM_LIMIT_ERROR) |
| §4.6 / §19.11 | Conformance | TestConformance_RFC9000_Sec46_MaxStreamsRaisesLimit (a MAX_STREAMS frame raises the cumulative limit so OpenStream succeeds past the initial grant), TestConformance_RFC9000_Sec46_MaxStreamsTooLarge (a MAX_STREAMS value > 2^60 → FRAME_ENCODING_ERROR), TestConn_MaxStreams_NonIncreasingIgnored, TestConn_MaxStreams_Uni |
| §4.1    | Conformance | TestConformance_RFC9000_Sec41_SendClampsToMinLimit (send never exceeds min(stream, conn) credit) |
| §4.1    | Unit        | TestConn_ReceiveFlowControl_GrantsCredit, TestConn_ReceiveFlowControl_NoGrantBelowThreshold (as the app consumes a response the client raises its advertised limits, queueing MAX_STREAM_DATA / MAX_DATA, batched at half a window) |
| §4.1    | Conformance | TestConformance_RFC9000_Sec41_StreamFlowControlEnforced (data past the advertised per-stream limit → FLOW_CONTROL_ERROR), TestConformance_RFC9000_Sec41_ConnFlowControlEnforced (combined data past the connection limit → FLOW_CONTROL_ERROR), TestConn_FlowControl_RetransmitNoDoubleCount (re-delivered bytes count once, keyed on the highest received offset) |
| §19.8   | Conformance | TestConformance_RFC9000_Sec198_StreamSendChunking (body larger than one datagram → multiple STREAM frames, monotonic offsets, LEN set, FIN on last) |
| §19.9   | Conformance | TestConformance_RFC9000_Sec199_MaxDataRaisesLimit (MAX_DATA raises the absolute conn ceiling; non-increasing ignored) |
| §19.10  | Conformance | TestConformance_RFC9000_Sec1910_MaxStreamDataRaisesLimit (MAX_STREAM_DATA raises the stream ceiling; FIN withheld while data is blocked, then flushed) |
| §19.12  | Conformance | TestConformance_RFC9000_Sec1912_DataBlocked (zero conn credit → DATA_BLOCKED) |
| §19.13  | Conformance | TestConformance_RFC9000_Sec1913_StreamDataBlocked (zero stream credit → STREAM_DATA_BLOCKED, once per limit) |
| §4.5    | Conformance | TestConformance_RFC9000_Sec45_FinBypassesFlowControl (a FIN consumes no credit — bare FIN sent at zero credit), TestConformance_RFC9000_Sec45_FinalSizeLatch (Send after FIN → ErrStreamFinished) |
| §4.5    | Conformance | TestConformance_RFC9000_Sec45_DataPastFinalSize, TestConformance_RFC9000_Sec45_FinBelowReceived, TestConformance_RFC9000_Sec45_ConflictingFin (received data past / below / inconsistent with a stream's final size → FINAL_SIZE_ERROR), TestConformance_RFC9000_Sec45_ResetFinalSizeBelow, TestConformance_RFC9000_Sec45_ResetFinalSizePastLimit (a RESET_STREAM final size below received → FINAL_SIZE_ERROR, past the limit → FLOW_CONTROL_ERROR), TestConn_ResetFinalSize_CreditsConn (the reset final size is credited to connection flow control) |
| §19.4   | Conformance | TestConformance_RFC9000_Sec194_ResetStream (Stream.Reset sends RESET_STREAM with the final size; Send then returns ErrStreamReset; idempotent) |
| §19.5   | Conformance | TestConformance_RFC9000_Sec195_StopSending (Stream.StopSending sends a STOP_SENDING frame) |
| §3.5    | Conformance | TestConformance_RFC9000_Sec35_StopSendingTriggersReset (a received STOP_SENDING resets our send side with the same code), TestConformance_RFC9000_Sec35_ResetStreamFinishesRecv (a received RESET_STREAM finishes the receive side), TestConformance_RFC9000_Sec35_ResetAfterCompleteIgnored (a RESET_STREAM after a fully-received stream has no effect — the receive side stays complete, not reset) |
| §13.3   | Conformance | TestConformance_RFC9000_Sec133_ResetRetransmittedOnLoss (a lost RESET_STREAM is retransmitted), TestConformance_RFC9000_Sec133_NoStreamRetransmitAfterReset (a reset stream's STREAM data is not retransmitted) |
| §10.2   | Unit        | TestConn_CloseWithError_SendsAppConnectionClose (CloseWithError emits one CONNECTION_CLOSE on the 1-RTT space and closes the transport), TestConn_Close_Idempotent (a second Close sends nothing more), TestConn_CloseWithError_DowngradesAppBeforeOneRTT (§10.2.3: an application close before 1-RTT is sent as a transport CONNECTION_CLOSE with APPLICATION_ERROR) |
| §10.2   | Conformance | TestConn_Poll_MalformedFrame_SendsConnectionClose (a received malformed frame makes Poll emit a CONNECTION_CLOSE with FRAME_ENCODING_ERROR), TestConn_Fail_SendsCloseForProtocolError (a protocol-violation error is signalled with the mapped transport code), TestConn_Fail_NoCloseForIOError (an I/O error sends nothing) |
| §4.1    | Unit        | TestConn_SendStream_Wire (client Send → decrypt → STREAM bytes match), TestStream_Grantable_Unit (credit-clamp accounting), TestStream_Send_NotEstablished (Send before 1-RTT keys → ErrNotEstablished) |

## RFC 9001 — Using TLS to Secure QUIC (Phase G — HTTP/3)

G.5a lands the QUIC key schedule (HKDF-Expand-Label + Initial secrets, §5.1–5.2);
G.5b adds AEAD packet protection (§5.3) and header protection (§5.4) — `Sealer` /
`Opener` over AES-128-GCM + AES-ECB, plus packet-number reconstruction
(RFC 9000 §A.3). G.5c adds the `crypto/tls.QUICConn` handshake driver
(`TLSHandshake` + `KeysFromSecret`, §4), verified by a full in-memory TLS 1.3
QUIC handshake. All pure stdlib (`crypto/hkdf`, `crypto/aes`, `crypto/cipher`,
`crypto/tls`; Go 1.24). The full Appendix A.2 (client Initial seal) and A.3
(server Initial open) packet known-answer tests reproduce the RFC's protected
bytes exactly, and the client interoperates with a live Caddy HTTP/3 server.

| Section | Type        | Test |
|---------|-------------|------|
| §5.1–5.2 | Conformance | TestConformance_RFC9001_AppA1_InitialKeys (client+server key/iv/hp vs Appendix A.1 vectors) |
| App. A.2 | Conformance | TestConformance_RFC9001_AppA2_ClientInitial (building the client Initial reproduces the RFC's protected packet byte-for-byte: keys + CRYPTO framing + PADDING + AEAD + header protection) |
| App. A.3 | Conformance | TestConformance_RFC9001_AppA3_ServerInitial (opening the server's protected Initial recovers packet number 1 and the exact payload: HP removal + PN recovery + AEAD decrypt) |
| §5.1    | Conformance | TestConformance_RFC9001_Sec51_ExpandLabel (client_initial_secret vs Appendix A.1) |
| §5.3    | Conformance | TestConformance_RFC9001_Sec53_Nonce (IV XOR packet number vs A.1-derived values) |
| §5.3    | Roundtrip   | TestPacketProtection_RoundTrip (seal → open recovers pn + payload) |
| §5.3    | Negative    | TestPacketProtection_AuthFailure (tampered / wrong-key → ErrCryptoDecrypt) |
| §5.4    | Roundtrip   | TestHeaderProtection_Reversible, TestPacketProtection_ShortSample |
| §A.3    | Conformance | TestDecodePacketNumber (packet-number reconstruction, §A.3 example) |
| §4      | Integration | TestTLSHandshake_InMemory (full TLS 1.3 QUIC handshake client↔server via crypto/tls.QUICConn; secrets match, keys derive, ALPN h3) |
| §4.1    | Unit        | TestKeysFromSecret_UnsupportedSuite (ChaCha20 → ErrCryptoSuite, deferred) |
| §6.1    | Conformance | TestKeyUpdate_QuicKuVector (next-generation secret = HKDF-Expand-Label(secret, "quic ku", "", Hash.length)) |
| §6.1    | Conformance | TestConformance_RFC9001_Sec61_UpdateBeforeConfirmed (a key update is dropped until the handshake is confirmed via HANDSHAKE_DONE, not merely TLS-complete) |
| §6.2    | Conformance | TestConformance_RFC9001_Sec62_KeyUpdateResponder (a peer-initiated Key Phase 0→1 update is trial-decrypted with the pre-derived next generation, committed, and the client flips its own send phase; HP key not rotated) |
| §6.3    | Conformance | TestConformance_RFC9001_Sec63_PrevKeysReordered (reordered previous-generation packet below the boundary decrypts with retained prev keys; prev discarded after 3×PTO) |
| §6.4    | Conformance | TestConformance_RFC9001_Sec64_ForgedPhaseBitBounded (a forged Key Phase bit costs exactly one AEAD attempt and never commits an update) |
| §6.6    | Conformance | TestConformance_RFC9001_Sec66_ConfidentialityLimitCloses (at the AES-GCM confidentiality limit the client — a pure key-update responder — closes with AEAD_LIMIT_REACHED), TestConn_AEADConfidentiality_CounterIncrements, TestConn_AEADConfidentiality_ResetOnKeyUpdate (a key update resets the send counter), TestConn_AEADIntegrity_CountsAuthFailures (a failed authentication counts toward the integrity limit) |
| §4.8    | Conformance | TestConformance_RFC9001_Sec48_TLSAlertToCryptoError (a TLS alert maps to a CRYPTO_ERROR code 0x0100 + the alert description, through a wrapping error), TestConn_Fail_TLSAlert_SendsCryptoErrorClose (a handshake alert sends a transport CONNECTION_CLOSE with that code) |

## RFC 9002 — QUIC Loss Detection and Congestion Control (Phase G — HTTP/3)

G.6h-1 lands the acknowledgement-processing foundation: the client tracks the
packets it sends per space, processes inbound ACK frames (§19.3 range walk), and
maintains the RTT estimates (§5). G.6h-2 adds ACK-driven loss detection (§6.1
packet + time thresholds) and frame-level retransmission (§13.3) of lost CRYPTO
and STREAM data. G.6h-3 adds the probe timeout (§6.2): the receive loop bounds
each read by the PTO when data is in flight and, on expiry, resends the oldest
unacknowledged packet as a probe (with exponential backoff), recovering a fully
lost tail that no ACK would otherwise detect. G.6l adds NewReno congestion
control (§7): the client tracks bytes in flight, grows the window in slow start
and congestion avoidance, halves it once per loss episode, and gates its own
data sends on the window. Persistent congestion (§7.6) is deferred (documented
in cc.go).

| Section | Type        | Test |
|---------|-------------|------|
| §5.2–5.3 | Unit       | TestRTTStats_FirstSample, TestRTTStats_Subsequent, TestRTTStats_AckDelay (smoothed_rtt / rttvar / min_rtt update; ack-delay subtraction floored at min_rtt) |
| §5.1 / §19.3 | Unit   | TestSentSpace_Ack, TestConnFrameHandler_OnAck_UpdatesRTT, TestConnFrameHandler_OnAckRange_Gap (sent-packet tracking; ACK range walk removes acked packets and samples RTT from the largest; gaps leave packets in flight) |
| §6.1.1  | Conformance | TestConformance_RFC9002_Sec611_PacketThresholdLoss (a packet ≥ kPacketThreshold=3 numbers below the largest acknowledged is declared lost) |
| §6.1.2  | Conformance | TestConformance_RFC9002_Sec612_TimeThresholdLoss (a packet sent before now − 9/8·max(srtt,latest) is declared lost), TestRTTStats_LossDelay, TestConn_DetectLost_NoLossWithinThresholds |
| §6.1 / §13.3 | Unit | TestConn_Retransmit_CryptoResendsBytesAtOffset (lost CRYPTO resent at its offset; retransmit Initial datagram padded to 1200, RFC 9000 §14.1), TestConn_Retransmit_StreamResendsBytesAtOffsetAndFin (lost STREAM resent at offset+FIN, accounting not re-advanced), TestConn_Retransmit_AckedPacketNotResent, TestConn_AckOnlyPacketNotRetransmittable |
| §6.2.1  | Unit        | TestConn_PTOPeriod (PTO = srtt + max(4·rttvar, granularity) + max_ack_delay, doubled per backoff; 2·kInitialRtt pre-sample), TestConn_PTOCount_ResetOnAck (backoff reset on a newly-acked ack-eliciting packet) |
| §6.2.4  | Unit        | TestConn_OnPTO_QueuesProbe (probe resends the oldest unacked packet + backs off), TestConn_ReadWithPTO_ProbesOnTimeout (a read timeout with data in flight sends a probe and retries) |
| §6 (whole) | Interop | `make h3-interop-loss` — the GET/POST/16 KiB interop suite run against live Caddy through a UDP relay that drops ~10% of datagrams each way; passing proves the handshake, request, and response recover via retransmission and the probe timeout (verified up to 20% loss). |
| §7.3.1  | Conformance | TestConformance_RFC9002_Sec731_SlowStart (an ack in slow start grows cwnd by the acked bytes and frees them from bytes_in_flight), TestConformance_RFC9002_Sec731_HalveOncePerRecovery (a loss halves cwnd once per recovery episode; same-episode losses do not re-halve) |
| §7.3.3  | Conformance | TestConformance_RFC9002_Sec733_CongestionAvoidance (byte-accumulator growth of one max_datagram_size per window acked; does not freeze at a large window) |
| §7 (gate) | Unit      | TestCC_GateClampedByWindow (grantable clamps to the remaining window and reports blockCong — no frame — when full), TestCC_PureAckNotCounted (pure-ACK packets are not in flight), TestCC_DisabledSentinel (cwnd==0 leaves the send path unthrottled) |

## RFC 9204 — QPACK (Phase G — HTTP/3)

G.3 lands the `qpack/` static-table codec: a static-table-only encoder and a
decoder for the static + literal representations, reusing the `hpack`
prefixed-integer + Huffman codecs. The dynamic table and the encoder/decoder
instruction streams are deferred (a client advertising
`SETTINGS_QPACK_MAX_TABLE_CAPACITY=0` forbids the peer from using them).

| Section | Type        | Test |
|---------|-------------|------|
| App. A  | Roundtrip   | TestStaticTable_Shape (99-entry 0-based static table) |
| §4.5.1–2 | Conformance | TestConformance_RFC9204_Sec45_StaticIndexedEncode (section prefix + Indexed Field Line) |
| §4.5.4  | Conformance | TestConformance_RFC9204_Sec454_LiteralNameRefDecode |
| §4.5.6  | Conformance | TestConformance_RFC9204_Sec456_LiteralNameDecode |
| §4.5    | Conformance | TestConformance_RFC9204_Sec45_DecodeErrors (dynamic-ref / malformed → QPACK_DECOMPRESSION_FAILED) |
| §2.2 / §6 | Conformance | TestConformance_RFC9204_Sec22_DecompressionFailedClosesConn (a field section the static-only decoder cannot decode → QPACK_DECOMPRESSION_FAILED, an application-layer CONNECTION_CLOSE, not a per-stream reset) |
| §4.5    | Roundtrip   | TestQPACK_RoundTrip, TestQPACK_EmptySection |
| §4.2    | Conformance | TestConformance_RFC9204_Sec42_QPACKStreamClosed (server closing its QPACK encoder stream → H3_CLOSED_CRITICAL_STREAM), TestConformance_RFC9204_Sec42_DuplicateQPACKStream (a second QPACK encoder stream → H3_STREAM_CREATION_ERROR) |

## RFC 9114 — HTTP/3 (Phase G)

G.7a lands the `http3/` frame codec (§7.2): the Type+Length framing common to
every HTTP/3 frame, typed writers for the frames a client emits (DATA, HEADERS,
SETTINGS, GOAWAY, MAX_PUSH_ID, CANCEL_PUSH), a `ParseFrameHeader`, and a SETTINGS
payload codec. The header/settings writers and the header parser are zero-alloc
(bench-gated). G.7b adds the streaming `FrameReader` (buffers a stream that
arrives in pieces and yields whole frames), the unidirectional stream-type codec
(§6.2), and client control-stream construction (type 0x00 + first SETTINGS,
§6.2.1). G.7c adds the request/response header mapping (§4): a request is
encoded pseudo-headers-first and QPACK-compressed into a HEADERS frame, and a
response field section is decoded into a status + validated headers, rejecting
malformed messages (bad :status, misordered/forbidden pseudo-headers, invalid
field names/values, connection-specific fields). G.7d wires it to QUIC: a
`Client` over an established `quic.Conn` opens the control stream (SETTINGS
first), sends a request on a bidi stream, and reads the response by polling the
connection — validated against a fake QUIC transport. `http3.Dial` wires this
over a real `net.UDPConn` (with the "h3" ALPN and the client's transport-
parameter encoding), giving a caller `Dial` → `Do`; a live-server handshake is
exercised in the Docker-peer interop phase.

| Section | Type        | Test |
|---------|-------------|------|
| §7.2    | Conformance | TestConformance_RFC9114_Sec72_FrameRoundTrip (DATA/HEADERS/GOAWAY/MAX_PUSH_ID/CANCEL_PUSH write → header parse → payload) |
| §7.2.4  | Conformance | TestConformance_RFC9114_Sec724_SettingsRoundTrip, TestConformance_RFC9114_Sec724_DuplicateSetting (SETTINGS round-trip; repeated id → H3_SETTINGS_ERROR) |
| §7.1    | Negative    | TestParseFrameHeader_Incomplete (truncated Type/Length varint → ErrH3Frame) |
| §7.2.4  | Negative    | TestParseSettings_Truncated (identifier without value → ErrH3Settings) |
| §6.2 / §6.2.1 | Conformance | TestConformance_RFC9114_Sec62_ControlStream (client control stream = type 0x00 + first SETTINGS frame; peeled + read back) |
| §7.1    | Unit        | TestFrameReader_MultipleFrames, TestFrameReader_SplitAcrossFeeds, TestFrameReader_HugeLength (streaming frame reader: back-to-back frames, frame split across feeds, huge length stays ErrNeedMore without overflow), TestReadStreamType |
| §4.3.1  | Conformance | TestConformance_RFC9114_Sec431_RequestPseudoHeadersFirst (request pseudo-headers precede regular headers; QPACK round-trip in a HEADERS frame), TestRequest_OmitsEmptyAuthority |
| §4.2    | Negative    | TestConformance_RFC9114_Sec42_RequestValidation (missing pseudo-header / uppercase / connection-specific / CR-LF value → ErrH3Message; client never emits a malformed request) |
| §4.1.2 / §4.3.2 | Conformance | TestConformance_RFC9114_Sec412_ResponseDecode (:status + regular headers decoded) |
| §4.1.2  | Negative    | TestConformance_RFC9114_Sec412_MalformedResponse (missing/duplicate/out-of-range :status, pseudo-after-regular, non-:status pseudo, uppercase/space name, CR-LF value, connection-specific / te≠trailers → ErrH3Message) |
| §4.1.2  | Conformance | TestConformance_RFC9114_Sec412_ContentLengthMismatch (Content-Length ≠ Σ DATA payloads → malformed, stream aborted H3_MESSAGE_ERROR), TestConformance_RFC9114_Sec412_ContentLengthMatch, TestClient_ContentLength_NoContentStatusExempt (204/304 anticipatory length exempt), TestClient_ContentLength_ConflictingMalformed (conflicting Content-Length → malformed) |
| §6.2.1 / §4 | Integration | TestClient_RequestResponse (client opens control stream + SETTINGS, sends a request, decodes response HEADERS + DATA over a Poll loop), TestClient_SendDrainsUnderFlowControl (partial Send is drained so the request/SETTINGS are never truncated) |
| §4.1    | Conformance | TestClient_DataBeforeHeaders (DATA before HEADERS on a request stream — an invalid frame sequence → a H3_FRAME_UNEXPECTED connection error) |
| §8.1    | Unit        | TestClient_Close (Client.Close sends an application CONNECTION_CLOSE with H3_NO_ERROR through the QUIC connection) |
| §6.2.1  | Conformance | TestConformance_RFC9114_Sec621_ReadsServerSettings (the server control stream is accepted and its first SETTINGS read), TestConformance_RFC9114_Sec621_MissingSettings (a non-SETTINGS first frame → H3_MISSING_SETTINGS), TestConformance_RFC9114_Sec621_ControlStreamClosed (the server closing its control stream → H3_CLOSED_CRITICAL_STREAM) |
| §7.2.8  | Negative    | TestConformance_RFC9114_Sec728_ForbiddenControlFrame (a HEADERS/DATA/PUSH_PROMISE/reserved frame on the control stream → H3_FRAME_UNEXPECTED) |
| §7.2.4 / §7.2.8 | Conformance | TestConformance_RFC9114_Sec724_SettingsOnRequestStream, TestConformance_RFC9114_Sec728_ReservedFrameOnRequestStream (a control-only or reserved HTTP/2-carryover frame on a request stream → H3_FRAME_UNEXPECTED connection error) |
| §7.2.5 / §4.6 | Conformance | TestConformance_RFC9114_Sec725_PushPromiseOnRequestStream (a PUSH_PROMISE without MAX_PUSH_ID → H3_ID_ERROR) |
| §7.1    | Conformance | TestConformance_RFC9114_Sec71_TruncatedFrameAtStreamEnd, TestConformance_RFC9114_Sec71_TruncatedHeaderAtStreamEnd (a request stream ending mid-frame — truncated payload or header — → H3_FRAME_ERROR connection error) |
| §4.1.1  | Conformance | TestConformance_RFC9114_Sec411_RequestReset (a server RESET_STREAM aborting the response → StreamResetError carrying the app error code; H3_REQUEST_REJECTED reported retryable; connection not torn down), TestClient_RequestReset_NonRetryable |
| §5.2    | Conformance | TestConformance_RFC9114_Sec52_GoAwayGatesRequests (after GOAWAY a request on a stream ≥ the id → ErrGoAway), TestConformance_RFC9114_Sec52_GoAwayMustNotIncrease (an increasing GOAWAY id → H3_ID_ERROR) |
| §7.2.6  | Conformance | TestConformance_RFC9114_Sec726_GoAwayNonRequestStreamID (a GOAWAY whose id is not a client-initiated bidirectional request stream — low two bits nonzero → H3_ID_ERROR) |
| §6.2.5  | Negative    | TestConformance_RFC9114_Sec625_PushStreamRejected (a server push stream without MAX_PUSH_ID → H3_ID_ERROR) |
| §7.1    | Unit        | TestClient_ControlFrameTooLarge (an oversized control frame → H3_EXCESSIVE_LOAD), TestClient_UniStream_PartialType (a stream-type varint split across reads is carried until complete) |
| §4.1    | Conformance | TestClient_InterimAndTrailers (the full response sequence: 1xx informational → final response → DATA → trailers, decoded into Response.Interim/Trailers), TestClient_InterimWithoutFinal (a 1xx with no final response → ErrH3Message) |
| §4.1    | Conformance | TestClient_MessageOrderErrors (DATA before the final response, DATA after a 1xx, and any frame after the trailers — all invalid frame sequences → a H3_FRAME_UNEXPECTED connection error), TestDecodeTrailers (a trailer section rejects pseudo-headers) |
| §3.1    | Unit        | TestH3TLSConfig (Dial applies the "h3" ALPN token and a TLS 1.3 floor), TestUDPConn_Loopback / TestUDPConn_ReadDeadline (UDP PacketConn adapter), TestDialConn_EstablishError (dial closes the transport on handshake failure) |
| Whole stack | Interop | TestInterop_GET / _POST / _LargeResponse (build tag `interop`) — against a live Caddy (quic-go) server over UDP: a GET → 200, a POST with a body (DATA frame) → 200, and a 16 KiB response reassembled correctly (many DATA/STREAM frames across datagrams). Harness: `make h3-interop` (test/integration/http3). Validates the full path end-to-end against an independent implementation. |

## Gate

`scripts/rfc-coverage-gate.sh` requires at least one passing
`TestConformance_RFC7540_*`, `TestConformance_RFC7541_*`,
`TestConformance_RFC9000_*`, `TestConformance_RFC9001_*`,
`TestConformance_RFC9002_*`, `TestConformance_RFC9204_*`, AND
`TestConformance_RFC9114_*` test, and fails on any conformance-test failure. It
is wired to the `conformance-gate` job in
`.github/workflows/conformance-gate.yml`.
