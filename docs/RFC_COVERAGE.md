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
| §4.6    | Conformance | TestConformance_RFC9000_Sec46_StreamLimit (OpenStream past peer initial_max_streams_bidi → ErrTooManyStreams; zero limit forbids the first) |
| §4.1    | Conformance | TestConformance_RFC9000_Sec41_SendClampsToMinLimit (send never exceeds min(stream, conn) credit) |
| §19.8   | Conformance | TestConformance_RFC9000_Sec198_StreamSendChunking (body larger than one datagram → multiple STREAM frames, monotonic offsets, LEN set, FIN on last) |
| §19.9   | Conformance | TestConformance_RFC9000_Sec199_MaxDataRaisesLimit (MAX_DATA raises the absolute conn ceiling; non-increasing ignored) |
| §19.10  | Conformance | TestConformance_RFC9000_Sec1910_MaxStreamDataRaisesLimit (MAX_STREAM_DATA raises the stream ceiling; FIN withheld while data is blocked, then flushed) |
| §19.12  | Conformance | TestConformance_RFC9000_Sec1912_DataBlocked (zero conn credit → DATA_BLOCKED) |
| §19.13  | Conformance | TestConformance_RFC9000_Sec1913_StreamDataBlocked (zero stream credit → STREAM_DATA_BLOCKED, once per limit) |
| §4.5    | Conformance | TestConformance_RFC9000_Sec45_FinBypassesFlowControl (a FIN consumes no credit — bare FIN sent at zero credit), TestConformance_RFC9000_Sec45_FinalSizeLatch (Send after FIN → ErrStreamFinished) |
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

## RFC 9002 — QUIC Loss Detection and Congestion Control (Phase G — HTTP/3)

G.6h-1 lands the acknowledgement-processing foundation: the client tracks the
packets it sends per space, processes inbound ACK frames (§19.3 range walk), and
maintains the RTT estimates (§5). Loss detection, retransmission, and the probe
timeout build on this next. Congestion control (§7) is deferred.

| Section | Type        | Test |
|---------|-------------|------|
| §5.2–5.3 | Unit       | TestRTTStats_FirstSample, TestRTTStats_Subsequent, TestRTTStats_AckDelay (smoothed_rtt / rttvar / min_rtt update; ack-delay subtraction floored at min_rtt) |
| §5.1 / §19.3 | Unit   | TestSentSpace_Ack, TestConnFrameHandler_OnAck_UpdatesRTT, TestConnFrameHandler_OnAckRange_Gap (sent-packet tracking; ACK range walk removes acked packets and samples RTT from the largest; gaps leave packets in flight) |

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
| §4.5    | Roundtrip   | TestQPACK_RoundTrip, TestQPACK_EmptySection |

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
| §6.2.1 / §4 | Integration | TestClient_RequestResponse (client opens control stream + SETTINGS, sends a request, decodes response HEADERS + DATA over a Poll loop), TestClient_SendDrainsUnderFlowControl (partial Send is drained so the request/SETTINGS are never truncated) |
| §4.1    | Negative    | TestClient_DataBeforeHeaders (DATA before HEADERS on a request stream → ErrH3FrameUnexpected = H3_FRAME_UNEXPECTED) |
| §3.1    | Unit        | TestH3TLSConfig (Dial applies the "h3" ALPN token and a TLS 1.3 floor), TestUDPConn_Loopback / TestUDPConn_ReadDeadline (UDP PacketConn adapter), TestDialConn_EstablishError (dial closes the transport on handshake failure) |
| Whole stack | Interop | TestInterop_GET / _POST / _LargeResponse (build tag `interop`) — against a live Caddy (quic-go) server over UDP: a GET → 200, a POST with a body (DATA frame) → 200, and a 16 KiB response reassembled correctly (many DATA/STREAM frames across datagrams). Harness: `make h3-interop` (test/integration/http3). Validates the full path end-to-end against an independent implementation. |

## Gate

`scripts/rfc-coverage-gate.sh` requires at least one passing
`TestConformance_RFC7540_*`, `TestConformance_RFC7541_*`,
`TestConformance_RFC9000_*`, `TestConformance_RFC9001_*`,
`TestConformance_RFC9204_*`, AND `TestConformance_RFC9114_*` test, and fails on
any conformance-test failure. It is wired to the `conformance-gate` job in
`.github/workflows/conformance-gate.yml`.
