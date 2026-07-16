# RFC Coverage Matrix

Each row maps an RFC section to the tests that exercise it. `Conformance`
tests build the wire-byte fixture by hand from the RFC's diagrams and feed
it through the parser; `Roundtrip` tests use the package's own Write* path
and round-trip through ReadFrame. The conformance row is what the
`conformance-gate` CI job enforces.

## RFC 7540 — HTTP/2

| Section | Type        | Test |
|---------|-------------|------|
| §10.3 | Conformance | TestConformance_RFC7540_Sec10_3_ResponseFieldValueCRLFNUL_Malformed (conn/) — "Any request or response that contains a character not permitted in a header field value MUST be treated as malformed": CR, LF and NUL in a response value are a stream error PROTOCOL_ERROR. HPACK is length-prefixed so they cannot split the H2 wire — the damage is downstream, in whatever copies the value into an HTTP/1.1 message |
| §10.3 | Conformance | TestConformance_RFC7540_Sec10_3_ResponseFieldNameInvalid_Malformed (conn/) — "Requests or responses containing invalid header field names MUST be treated as malformed": upper case (named by §8.1.2.6), spaces, colons, control and non-ASCII bytes, and the empty name |
| §8.1.2.2 | Conformance | TestConformance_RFC7540_Sec8_1_2_2_ConnectionSpecificResponseField_Malformed (conn/) — "any message containing connection-specific header fields MUST be treated as malformed" binds the receive path too. "te" is rejected at any value: the §8.1.2.2 exception is request-only |
| §10.3 | Conformance | TestConformance_RFC7540_Sec10_3_LegalResponseFieldsAccepted (conn/) — the over-rejection guard: SP/HTAB inside a value, obs-text, high-bit bytes, every tchar in a name and an empty value are all ordinary traffic and must still be accepted |
| §3.5    | Conformance | TestConformance_RFC7540_Sec35_ClientPreface |
| §3.5    | Roundtrip   | TestFramer_ClientPreface |
| §4.1    | Conformance | TestConformance_RFC7540_Sec41_FrameHeader_RBitMasked |
| §4.1    | Roundtrip   | TestReadFrameHeader_Sample, TestWriteFrameHeader |
| §6.1    | Conformance | TestConformance_RFC7540_Sec61_DataFrame_PaddedEndStream |
| §6.1    | Roundtrip   | TestFramer_Data_Roundtrip, TestFramer_DataPadded_Roundtrip |
| §6.2    | Conformance | TestConformance_RFC7540_Sec62_HeadersFrame_PriorityPaddedEndHeaders |
| §6.2    | Roundtrip   | TestFramer_Headers_RoundTrip, TestFramer_HeadersWithPriority_RoundTrip, TestFramer_HeadersPadded_RoundTrip |
| §6.3    | Conformance | TestConformance_RFC7541_Sec63_DecoderHonorsAdvertisedTableSize, TestConformance_RFC7541_Sec63_DecoderRejectsAboveAdvertised (conn) — a dynamic table size update above the SETTINGS_HEADER_TABLE_SIZE limit is a decoding error; the decoder's limit must be the value we advertised, not hpack's 4096 default, or a peer honouring our own offer is rejected |
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
| §6.8    | Conformance | TestConformance_RFC7540_Sec68_RealPeerGoAwayPartition (conn) — real net/http2 peer picks lastStreamID (= its maxClientStreamID) on graceful Shutdown with 6 streams in flight; all 6 land at or below it and MUST complete over the post-GOAWAY transport. The TestOnGoAway_* unit rows above drive onGoAwayReceived on a transportless Conn and choose the boundary themselves, so no stream ever has to finish. |
| §6.2    | Conformance | TestConformance_RFC7540_Sec6_2_HeadersSplitIntoContinuation (conn) — oversized HEADERS block split into HEADERS+CONTINUATION frames |
| §6.10   | Conformance | TestConformance_RFC7540_Sec6_10_ContinuationFlagsAndPadding (conn) — padding/priority only on HEADERS; END_HEADERS only on final CONTINUATION |
| §10.5.1 | Conformance | TestConformance_RFC7540_Sec10_5_1_HeaderListSizeCap_EnhanceYourCalm (conn) — decoded header list exceeding local SETTINGS_MAX_HEADER_LIST_SIZE rejected as connection error ENHANCE_YOUR_CALM; decompressed-size DoS gate (HPACK expansion bomb) enabled by default (8 MiB), announced to peer and enforced on decode. Supporting: TestAdvertisedSettings_Defaulted_BoundsHeaderListSize, TestEncodeAdvertised_AnnouncesHeaderListSize, TestHeaderListSizeCap_WithinLimit_Succeeds |
| §8.1    | Conformance | TestConformance_RFC7540_Sec8_1_BodyStream_EndStream (client/) |
| §8.1    | Integration | TestIntegration_Client_BodyStream_Small, TestIntegration_Client_BodyStream_Large, TestIntegration_Client_BodyStream_CloseEarly (client/) |
| §8.1    | Conformance | TestConformance_RFC7540_Sec8_1_InterimHeadersNotTrailers (conn) — vs. real net/http2 peer sending 103 Early Hints then 200: a 1xx block does not latch Stream.headersReceived, so it surfaces as EventInterimHeaders and the following final HEADERS is EventHeaders, not a trailer section. §8.1 keys trailers on a *final* (non-informational) status |
| §8.1    | Conformance | TestConformance_RFC7540_Sec8_1_TrailersWithoutEndStream_StreamError, TestConformance_RFC7540_Sec8_1_TrailersWithEndStream_Accepted (conn) — "An endpoint that receives a HEADERS frame without the END_STREAM flag set after receiving a final (non-informational) status code MUST treat the ... response as malformed"; §8.1.2.6 routes malformed to a stream error of type PROTOCOL_ERROR (§5.4.2), so the connection survives. Bounds the trailer-block flood at its source |
| §8.1    | Conformance | TestConformance_RFC7540_Sec8_1_InterimFlood_Bounded, TestConformance_RFC7540_Sec8_1_InterimWithEndStream_StreamError (conn) — 1xx blocks capped at conn.maxInterimResponses (100, matching http1/http3) then ENHANCE_YOUR_CALM stream error; a 1xx carrying END_STREAM is malformed (an informational response is not a complete response) → PROTOCOL_ERROR |
| §8.1    | Conformance | TestConformance_RFC7540_Sec8_1_InterimResponse_FinalStatusWins, _DoStream, _BodyReader, _Flood (client/) — vs. real net/http2 peer sending 103 then 200: the final status wins on all three response paths (buffered Do, DoStream, Do+BodyStream), the 200 header block never lands in Trailers, and a 1xx flood is rejected rather than pumped forever. 1xx are not surfaced by Client.Do, matching http1 and http3 |
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
| §6.8     | Conformance | TestConformance_RFC7540_Sec6_8_PoolDrainsInflightOnGoAway (client/) — a GOAWAY'd conn with in-flight streams MUST NOT be closed by the pool: streams at or below lastStreamID are allowed to complete. Two subtests, one per eviction site that sees !IsAlive(): tick (handleTick→evictDead) and stats (handleStats→evictDeadSilent, reachable from public PoolStats()). Also pins GoAwaysReceived == 1 per conn, not per draining stream. |
| §8.1.3   | Conformance | TestConformance_RFC7540_Sec8_1_3_RequestTrailers (client/) — request trailer HEADERS+END_STREAM sent after body DATA frames |
| §8.1.4   | Conformance | TestRetryer_Do_RefusedStream_Retries (client/) — retry layer retries on REFUSED_STREAM (RFC 7540 §8.1.4 — request not processed) |
| §4.2     | Unit        | TestPaddingStrategy_Disabled, TestPaddingStrategy_Fixed, TestPaddingStrategy_Range, TestPaddingStrategy_MaxLessThanMin, TestPaddingStrategy_DataOnly, TestPaddingStrategy_BothFrames (conn/) — PaddingStrategy for DATA and HEADERS frames |
| §6.1.1   | Roundtrip   | TestFramer_DataPadded_Roundtrip (frame/) — padded DATA frame encode/decode |
| §6.2.2   | Roundtrip   | TestFramer_HeadersPadded_RoundTrip (frame/) — padded HEADERS frame encode/decode |
| §8.2     | Integration | TestConn_PushPromise_DeliveredToParentStream (conn/) — EventPushPromise with PushStreamID; pushed stream registered and headers decoded |
| §8.2     | Integration | TestIntegration_Push_HandlerInvoked, _Disabled (client/) — server push end-to-end; handler invocation and PROTOCOL_ERROR when push disabled |
| §8.2     | Integration | TestIntegration_Push_HandlerReceivesNon2xx, _MultipleConcurrent (client/) — pushed stream with non-2xx status reaches handler; 4 concurrent PUSH_PROMISE frames all delivered |
| §8.2     | Negative    | TestConn_PushPromise_DisabledReturnsProtocolError (conn/) — PUSH_PROMISE rejected with PROTOCOL_ERROR when EnablePush=false |
| §5.1.2 / §6.5.2 | Conformance | TestConformance_RFC7540_Sec512_PushedStreamDoesNotFreeOurSlot (conn/) — MAX_CONCURRENT_STREAMS is directional: a pushed stream is server-initiated, so closing one must not return an in-flight slot to the peer's gate on the streams we open. TestConformance_RFC7540_Sec512_PushedStreamCloseKeepsShutdownBlocking (conn/) — the drain count follows the same split, so a pushed Close cannot let Shutdown return with a request stream still open |
| §6.6 / §6.5.2 | Conformance | TestConformance_RFC7540_Sec66_PushRegistryBoundedByOurMaxStreams (conn/) — the value *we* advertise in SETTINGS_MAX_CONCURRENT_STREAMS bounds concurrent server-initiated streams; promises beyond it are rejected with RST_STREAM(REFUSED_STREAM) on the promised id rather than growing the stream registry |
| §6.6 / §5.1.1 | Conformance | TestConformance_RFC7540_Sec66_IllegalPromisedIDIsProtocolError (conn/) — a PUSH_PROMISE promising an id that is not idle (zero, odd, duplicate, or decreasing) → connection error PROTOCOL_ERROR. TestConn_ReservePushedStream_RejectsIllegalIDs, TestConn_ReservePushedStream_ReleasesToItsOwnCounter (conn/) — registry identity and counter routing at the reservation boundary |
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

## RFC 9112 — HTTP/1.1 message syntax (http1/, client/)

RFC 9112 obsoletes RFC 7230 and is the current HTTP/1.1 message-framing spec.
The RFC 2616 rows above predate it and are keyed on that document's own section
numbers, which do not track RFC 9112 §6.3.

| Section | Type        | Test |
|---------|-------------|------|
| §6.3 R1 / §11.1 | Conformance | TestConformance_RFC9112_Sec6_3_Rule1_BodylessStatusDeclaringBodyNotPooled (http1/) — a 204 with Content-Length or Transfer-Encoding (both MUST NOTs: RFC 9110 §8.6, RFC 9112 §6.1), or a 304 with Transfer-Encoding, declares a body rule 1 forbids reading. The declared bytes stay on the socket, so the conn must not be pooled: "A client MUST NOT process, cache, or forward such extra data as a separate response, since such behavior would be vulnerable to cache poisoning" |
| §5.2 | Conformance | TestObsFoldAccumulation_IsLinearInBytesReceived (http1/) — joining N obs-fold continuations must cost O(total bytes). The first cut was O(n²): 688 KB of legal, under-cap header allocated 5 GB. maxHeaderListBytes bounds the bytes a server sends, not the work they buy |
| §5.2 | Conformance | TestObsFoldAccumulation_UnfoldsCorrectlyAtScale (http1/) — the control: the cheap path must still join every fold, so the cost bound cannot be met by dropping them |
| §5.2 | Conformance | TestConformance_RFC9112_Sec5_2_ObsFoldDoesNotSmuggleContentLength (http1/) — a fold line was split on ':' and became a REAL Content-Length the server never sent, reframing the body and stranding the rest on a pooled socket. Header smuggling: "X-Junk: a
| §5.2 | Conformance | TestConformance_RFC9112_Sec5_2_ObsFoldDoesNotSmuggleTransferEncoding (http1/) — the same primitive aimed at the other framing header |
| §5.2 | Conformance | TestConformance_RFC9112_Sec5_2_ObsFoldUnfoldsToSP (http1/) — "A user agent ... MUST replace each received obs-fold with one or more SP octets prior to interpreting the field value": SP or HTAB, one or many, repeated folds, and a value that starts on the fold |
| §5.2 | Conformance | TestConformance_RFC9112_Sec5_2_ObsFoldWithNoPrecedingField (http1/) — `obs-fold = OWS CRLF RWS` exists only after a field line; a block opening with one is not a field block, so it is refused and the conn condemned |
| §5.2 | Conformance | TestConformance_RFC9112_Sec5_2_ObsFoldInTrailerSection (http1/) — a trailer section is a header block, so it unfolds identically; the over-rejection guard is that a well-formed chunked response with a folded trailer stays poolable |
| §7.1 | Conformance | TestConformance_RFC9112_Sec7_1_InvalidChunkSizeRejected (http1/) — `chunk-size = 1*HEXDIG`: non-hex, empty, signed (`+5` — strconv.ParseInt accepts a sign, the ABNF does not) and int64-overflow sizes are refused, and each leaves KeepAlive() false. Only the negative case previously cleared it; the rest returned an error on a still-poolable mid-stream socket |
| §7.1 | Conformance | TestConformance_RFC9112_Sec7_1_ValidChunkSizeAccepted (http1/) — over-rejection guard: legal spellings (single digit, leading zeros) still frame a body |
| §7.1 | Conformance | TestConformance_RFC9112_Sec7_1_HexDigitsAcceptedBothCases (http1/) — a-f and A-F are both HEXDIG; a 0xab-byte chunk makes a case-folding slip show as a wrong length, not a parse failure |
| §7 / §6.1 | Conformance | TestConformance_RFC9112_Sec7_QuotedCommaIsNotAListDelimiter (http1/) — a comma inside a quoted transfer-parameter is data, not a list delimiter: `gzip;a=", chunked;x=1"` is ONE coding, so chunked is not final and rule 4 applies. Splitting the raw value on "," forged a final "chunked", chunk-framed the body and left the rest on a poolable socket — response splitting (§11.1) |
| §7 / §6.1 | Conformance | TestConformance_RFC9112_Sec7_ChunkedFinalAfterParameters (http1/) — the over-rejection guard: chunked-final must still frame as chunked through OWS, no-OWS, ";"-parameters, a trailing empty element (RFC 9110 §5.6.1) and mixed case (§7: coding names are case-insensitive). Pins the two normalization steps whose deletion previously survived both packages' suites |
| §6.3 R4 | Conformance | TestConformance_RFC9112_Sec7_NonChunkedFinalReadsUntilClose (http1/) — plain non-chunked finals ("chunked, gzip", "not-chunked", "chunkedx") read until close, covering rule 4 independently of the quoted-string machinery |
| §6.1 | Conformance | TestConformance_RFC9112_Sec6_1_NoContentLengthWithTransferEncoding (http1/) — "A sender MUST NOT send a Content-Length header field in any message that contains a Transfer-Encoding header field": the client must not emit the smuggling pair itself, for any spelling of the caller's header (RFC 9110 §5.1 makes names case-insensitive) |
| §6.1 | Conformance | TestConformance_RFC9112_Sec6_1_SingleContentLengthOnBodylessPost (http1/) — a bodyless POST/PUT/PATCH with a caller-supplied Content-Length emits exactly one; the client's own "Content-Length: 0" must not be appended beside it (CL.CL, §11.2, in the request direction) |
| §6.1 | Conformance | TestConformance_RFC9112_Sec6_1_BodylessPostStillGetsContentLengthZero (http1/) — the control: with no caller-supplied Content-Length the client still adds its own, so the duplicate fix cannot silently drop the header |
| §6.3 R4 | Conformance | TestConformance_RFC9112_Sec6_3_Rule4_ChunkedNotFinalReadsUntilClose (http1/) — "Transfer-Encoding: chunked, gzip": chunked is present but not the final coding, so the body length is determined by reading until the server closes, not by chunk framing |
| §6.3 R4 | Conformance | TestConformance_RFC9112_Sec6_3_Rule4_UnknownCodingReadsUntilClose (http1/) — "Transfer-Encoding: not-chunked" is a different §7 token from "chunked"; matching the field as a substring reads it as chunked and desyncs the stream |
| §6.3 R3 | Conformance | TestConformance_RFC9112_Sec6_3_Rule3_ContentLengthFirst_TEOverrides (http1/) — TE overrides CL when CL is parsed first; the override must undo a framing decision already made, and the response is not poolable |
| §6.3 R3 | Conformance | TestConformance_RFC9112_Sec6_3_Rule3_TransferEncodingFirst_CLIgnored (http1/) — same rule in the opposite header order: a Content-Length arriving after Transfer-Encoding must not reinstate length framing |
| §6.3 R3 | Conformance | TestConformance_RFC9112_Sec6_3_Rule3_ChunkedPlusCLNotReusable (http1/) — TE:chunked + CL frames as chunked (RFC 2616 §4.4 R3 rows) but MUST close: KeepAlive() is false |
| §6.3 R3 / §11.2 | Conformance | TestConformance_RFC9112_Sec6_3_Rule3_SmuggledResponseNotPooled (client/) — the MUST-close consequence at the pool layer: a TE+CL response evicts its conn (h1Pool.handleRelease), so the next request redials rather than reusing a socket whose framing the peer disputed |
| §3      | Conformance | TestConformance_RFC9112_Sec3_RequestLine_NotWritten (http1/) — request-line = method SP request-target SP HTTP-version; SP or CTL in method or target re-cuts the line, so both are refused. client/ already rejects whitespace in Method/Path (containsAnyWhitespace uses unicode.IsSpace, true for CR and LF — verified) but not NUL, and http1 is a public package a caller can use directly |
| §11.2   | Conformance | Covered by the §5.5 / §5.6.2 / §3 rows above — smuggling is the consequence those rules exist to prevent, not a separate assertion. Sharpest demonstration is TestConformance_RFC9112_Sec3_RequestLine_NotWritten/method_with_CRLF: with the fix reverted, one WriteRequest call puts two complete requests on one socket |
| §6.3 R3 | Conformance | TestConformance_RFC9112_Sec6_3_Rule3_InvalidContentLengthIgnoredWhenChunked (http1/) — Transfer-Encoding overrides Content-Length, so a chunk-framed response is not failed over an invalid Content-Length. Both header orders: rule 5's fatal case is scoped to a message "without Transfer-Encoding", and that absence is not known until the blank line |
| §6.3 R5 | Conformance | TestConformance_RFC9112_Sec6_3_Rule5_ConflictingContentLengthsRejected (http1/) — two Content-Length field lines that disagree combine (RFC 9110 §5.3) into a non-1\*DIGIT value → unrecoverable: typed error + connection not poolable. The CL.CL desync |
| §6.3 R5 | Conformance | TestConformance_RFC9112_Sec6_3_Rule5_ConflictingContentLengthsRejectedOrderIndependent (http1/) — same verdict when the conflicting pair straddles another field |
| §6.3 R5 | Conformance | TestConformance_RFC9112_Sec6_3_Rule5_CommaListDifferingValuesRejected (http1/) — "Content-Length: 5, 10", the same message with the duplication pre-folded onto one line, is not rule 5's all-same exception |
| §6.3 R5 | Conformance | TestConformance_RFC9112_Sec6_3_Rule5_NonNumericContentLengthRejected (http1/) — a single invalid value is unrecoverable, not a silent degrade to rule 8's read-until-close |
| §6.3 R5 | Conformance | TestConformance_RFC9112_Sec6_3_Rule5_IdenticalDuplicatesAccepted (http1/) — the obligation NOT to over-reject: duplicate identical field lines are processed with that single value as the length |
| §6.3 R5 | Conformance | TestConformance_RFC9112_Sec6_3_Rule5_CommaListIdenticalValuesAccepted (http1/) — rule 5's literal exception: a comma-separated list whose values are all valid and all the same |
| §6.3 R5 / §11.2 | Conformance | TestConformance_RFC9112_Sec6_3_Rule5_PoisonedConnNotPooled (client/) — the pool-layer payoff: a conn poisoned by conflicting Content-Length is evicted, not reused. Pins that request 2 dials fresh and does not read the response smuggled inside request 1's body |
| §6.3 R4 | Conformance | TestConformance_RFC9112_Sec6_3_Rule4_TENotFinalIgnoresValidCL (http1/) — a present-but-not-chunked Transfer-Encoding overrides even a *valid* Content-Length; rule 4 hands the length to connection close, and re-framing at the Content-Length truncates the body and strands the tail on the socket |
| §6.3 R5 | Conformance | TestConformance_RFC9112_Sec6_3_Rule5_ScopedToMessagesWithoutTE (http1/) — rule 5 is scoped to a message received "without Transfer-Encoding": with the field present its discard path must not fire however invalid the Content-Length. Rule 3 still forbids reuse, so KeepAlive() is false |
| §6.3 R5 | Conformance | TestConformance_RFC9112_Sec6_3_Rule5_InvalidCLWithoutTEStillRejected (http1/) — the control for that scoping: strip the Transfer-Encoding and rule 5 applies again unchanged, so narrowing the gate cannot silently disable it |

## RFC 9110 — HTTP semantics (http1/, client/)

| Section | Type        | Test |
|---------|-------------|------|
| §8.6 | Conformance | TestConformance_RFC9110_Sec8_6_ContentLengthOn304StaysPoolable (http1/) — the over-rejection guard: "A server MAY send a Content-Length header field in a 304 (Not Modified) response to a conditional GET request". It describes the representation, not a body, so the conn stays poolable; evicting would cost a connection per conditional GET |
| §9.3.2 | Conformance | TestConformance_RFC9110_Sec9_3_2_ContentLengthOnHeadStaysPoolable (http1/) — the other guard: rule 1 makes a HEAD response bodyless too, but "The server SHOULD send the same header fields in response to a HEAD request as it would have sent if the request method had been GET", so Content-Length is normal there |
| §5.3 | Conformance | TestConformance_RFC9110_Sec5_3_RepeatedTELinesAreOneList (http1/) — repeated field lines are ONE list: §5.3 appends each value "to the initial field line value in order, separated by a comma". Two Transfer-Encoding lines and the equivalent one-line list must frame identically; each line overwriting the verdict let an empty final line erase chunked framing |
| §5.3 | Conformance | TestConformance_RFC9110_Sec5_3_EmptyTELineAloneStillPresent (http1/) — the boundary: a line contributing no codings still makes the FIELD present, which RFC 9112 §6.3 rule 3 and rule 5 both key on. Skipping its framing effect must not make it invisible |
| §5.5    | Conformance | TestConformance_RFC9110_Sec5_5_HeaderValueCRLF_NotWritten (http1/) — a caller header value containing CRLF is refused with ErrInvalidRequest and **no bytes reach the socket**; asserted on captured wire bytes, not on the error. Without the check the value's tail arrives at the origin as extra header fields of the client's own request (RFC 9112 §11.2 request splitting) |
| §5.5    | Conformance | TestConformance_RFC9110_Sec5_5_HeaderValueNUL_NotWritten (http1/) — NUL in a field value refused; not a delimiter here, but it terminates a C string, so one value can mean different things to this client and to a C proxy |
| §5.5    | Conformance | TestConformance_RFC9110_Sec5_5_AuthorityCRLF_NotWritten (http1/) — :authority becomes the Host field value and answers to §5.5; CRLF there is refused. This is the vector client/validateRequest never sees: it checks Method and Path but not Authority |
| §5.5    | Conformance | TestConformance_RFC9110_Sec5_5_ClientDo_HeaderValueCRLF_Refused, _AuthorityCRLF_Refused (client/) — the refusal survives the full Client.Do path against a real net/http origin, whose parsed header set is the oracle. Proves the http1-layer check is genuinely reached and not bypassed by client/ |
| §5.5    | Conformance | TestConformance_RFC9110_Sec5_5_LegalValuesUnaffected (http1/), _ClientDo_LegalRequestUnaffected (client/) — over-rejection guard: field-value permits SP and HTAB internally, so a normal request still goes out intact |
| §5.6.2  | Conformance | TestConformance_RFC9110_Sec5_6_2_HeaderNameToken_NotWritten (http1/) — a field name is a token; CRLF, ':', SP or NUL in a name is refused. A ':' in a name forges a field boundary exactly as a CR forges a line boundary. (§5.1 is what makes names case-insensitive, hence the writer may lower-case them; §5.6.2 is what constrains the bytes) |
| §3.2    | Conformance | TestConformance_RFC9112_Sec3_2_EmptyAuthorityEmptyHost (http1/) — "If the target URI's authority component is missing or undefined, then a client MUST send a Host header field with an empty field value": an empty :authority emits the literal line `Host: ` and is **not** an error. Boundary row — it fails if the §5.5 validator overshoots into rejecting an empty authority |
| §8.6    | Conformance | TestConformance_RFC9110_Sec8_6_SignedContentLengthRejected (http1/) — `Content-Length = 1*DIGIT` admits no sign; "+5" and "-5" are invalid framing, not values to reinterpret (strconv.ParseInt accepts both) |
| §8.6    | Conformance | TestConformance_RFC9110_Sec8_6_OverflowContentLengthRejected (http1/) — "a recipient MUST anticipate potentially large decimal numerals and prevent parsing errors due to integer conversion overflows": a numeral past int64 is refused, not wrapped |
| §8.6    | Conformance | TestConformance_RFC9110_Sec8_6_MaxInt64ContentLengthAccepted (http1/) — the other side of that boundary: MaxInt64 is valid 1\*DIGIT, so the overflow guard must not be an off-by-one |

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
| §16     | Fuzz        | FuzzReadVarint (arbitrary bytes → never panic/over-read; a short input returns (0,0), a complete prefix decodes within the 62-bit space and re-encodes to itself) |
| §19.3   | Conformance | TestConformance_RFC9000_Sec193_AckFrame, TestConformance_RFC9000_Sec193_AckECN |
| §19.3.1 | Conformance | TestConformance_RFC9000_Sec1931_AckFirstRangeNegative (First ACK Range > Largest Acknowledged → FRAME_ENCODING_ERROR), TestConformance_RFC9000_Sec1931_AckRangeNegative (a Gap or Length underflowing the running lower bound → FRAME_ENCODING_ERROR), TestConformance_RFC9000_Sec1931_AckNegativeViaParser (rejected end-to-end through ParseFrames; a valid multi-range ACK still accepted) |
| §19.8   | Conformance | TestConformance_RFC9000_Sec19_StreamFrame, TestParseStream_NoLength |
| §19.15  | Conformance | TestConformance_RFC9000_Sec1915_NewConnectionID (frame codec), TestConn_NewConnectionID_DuplicateSeqConflict (reusing a sequence number for a different connection ID → PROTOCOL_VIOLATION; identical retransmit accepted) |
| §5.1.1  | Conformance | TestConformance_RFC9000_Sec511_ActiveCIDLimit (a NEW_CONNECTION_ID past active_connection_id_limit, default 2 including the handshake CID at seq 0 → CONNECTION_ID_LIMIT_ERROR) |
| §5.1.2  | Conformance | TestConformance_RFC9000_Sec512_RetirePriorToSwitchesCID (an increased Retire Prior To retires lower-sequence CIDs, switches the CID in use if it was retired, and queues a RETIRE_CONNECTION_ID on the retransmit queue; already-retired sequences are not re-queued) |
| §19.16  | Conformance | TestConformance_RFC9000_Sec1916_RetireConnectionID (this client offers a zero-length source connection ID, so any received RETIRE_CONNECTION_ID → PROTOCOL_VIOLATION) |
| §19.17  | Conformance | TestConformance_RFC9000_Sec1917_PathChallenge |
| §8.2.2  | Conformance | TestConformance_RFC9000_Sec822_PathChallengeEchoed (a received PATH_CHALLENGE queues a PATH_RESPONSE echoing its 8 bytes), TestConn_PathChallenge_SentByFlush (the PATH_RESPONSE is written on the next flush with the echoed data), TestConformance_RFC9000_Sec822_PathResponsePaddedTo1200 (the datagram carrying the PATH_RESPONSE is expanded to at least 1200 bytes, while an ordinary control-frame datagram is not) |
| §19.19  | Conformance | TestConformance_RFC9000_Sec1919_ConnectionClose |
| §19.1   | Conformance | TestConformance_RFC9000_Sec191_Padding |
| §19 (all frames) | Roundtrip | TestFrames_RoundTrip |
| §12.4   | Negative    | TestParseFrames_Malformed, TestParseFrames_MoreMalformed (malformed → ErrFrameEncoding) |
| §12.4 / §19 | Fuzz    | FuzzParseFrames (arbitrary decrypted-payload bytes through ParseFrames → rejected with ErrFrameEncoding or accepted, never a panic/hang/unbounded alloc — the top attacker-controlled surface once the AEAD is opened) |
| §12.4 / §12.5 | Conformance | TestConformance_RFC9000_Sec124_FrameTypePermittedBySpace (a frame carried in a space that does not permit its type → PROTOCOL_VIOLATION: Initial/Handshake permit only PADDING/PING/ACK/CRYPTO/CONNECTION_CLOSE-0x1c, so HANDSHAKE_DONE, STREAM, MAX_DATA, or an application CONNECTION_CLOSE-0x1d there is rejected — closing the forged-Initial HANDSHAKE_DONE handshake-completion hole — while the permitted frames and any frame in the 1-RTT space are accepted; an unknown type is FRAME_ENCODING_ERROR per §12.4) |
| §17.2.2 | Conformance | TestConformance_RFC9000_Sec172_InitialHeader, TestConformance_RFC9000_Sec1722_ServerInitialWithTokenDiscarded (a server Initial with a non-empty Token is discarded — a server's Initial MUST carry a zero-length Token — while an empty-token Initial is processed) |
| §17.2.5 | Conformance | TestConformance_RFC9000_Sec1725_RetryHeader, TestConformance_RFC9000_Sec1725_RetryRekeysAndResends (a valid Retry adopts the server CID, re-derives Initial keys, stores the token, re-queues the ClientHello), TestConformance_RFC9000_Sec17253_RetryResendCarriesToken (the resent Initial carries the token, is padded, decrypts under the new keys), TestConn_Retry_Discards (bad tag / second Retry / post-server-Initial Retry discarded, §17.2.5.2) |
| §17.2.1 | Conformance | TestConformance_RFC9000_Sec171_VersionNegotiation |
| §6.2    | Conformance | TestConformance_RFC9000_Sec62_VersionNegotiationAbandons (a Version Negotiation packet offering no common version makes the v1-only client abandon the attempt with ErrVersionNegotiation, no CONNECTION_CLOSE), TestConformance_RFC9000_Sec62_VersionNegotiationDiscardExceptions (a VN listing v1, or one received after an Initial/Retry was already processed (§17.2.5.2), is discarded not abandoned) |
| §17.3   | Conformance | TestConformance_RFC9000_Sec173_ShortHeader |
| §17.2 / §17.3.1 | Conformance | TestConformance_RFC9000_Sec1731_ReservedBitsProtocolViolation (short-header reserved bits nonzero → PROTOCOL_VIOLATION), TestConformance_RFC9000_Sec172_LongHeaderReservedBits (long-header), TestConn_ReservedBitsZero_Accepted (a valid packet with zero reserved bits is not rejected) |
| §12.2   | Conformance | TestParseHeader_Coalesced (coalesced-packet walk via PacketLen) |
| §17     | Roundtrip   | TestPacketHeader_RoundTrip |
| §17     | Negative    | TestParseHeader_Malformed (malformed header → ErrPacketEncoding) |
| §17     | Fuzz        | FuzzParseHeader (arbitrary packet bytes + fuzzed local-DCID length → ParseHeader never panics on a truncated/oversized/nonsensical header or an out-of-range DCID length, only ErrPacketEncoding) |
| §14.1   | Conformance | TestConformance_RFC9000_Sec141_InitialFlight (real ClientHello → padded ≥1200 Initial → protect → parse+decrypt round-trip) |
| §12.2   | Unit        | TestProcessDatagram_ServerInitial, TestProcessDatagram_Coalesced (split coalesced packets, decrypt per level, dispatch frames) |
| §12.2   | Negative    | TestProcessDatagram_SkipNoKeys, TestProcessDatagram_AuthFailure, TestProcessDatagram_Retry, TestProcessDatagram_Malformed |
| §13.1   | Conformance | TestConformance_RFC9000_Sec131_AckForNeverSentPacket (an ACK whose Largest Acknowledged is at or above sendPN — a packet number the client never sent — → PROTOCOL_VIOLATION; an ACK reaching only sent packets is accepted) |
| §13.2 / §19.3 | Unit  | TestAckTracker_RoundTrip, TestAckTracker_PendingAndLargest (received PNs → ACK ranges → decode back to the exact set) |
| §13.2.1 | Conformance | TestConformance_RFC9000_Sec1321_BlockedFramesAckEliciting (DATA_BLOCKED / STREAM_DATA_BLOCKED / STREAMS_BLOCKED are ack-eliciting, so a packet carrying only one is acknowledged, not left unacked and retransmitted), TestConformance_RFC9000_Sec1321_NewTokenPathResponseAckEliciting (NEW_TOKEN and PATH_RESPONSE are likewise ack-eliciting) |
| §13.2.1 | Unit | TestAckTracker_ImmediateTriggers (the immediate-ACK triggers isolated: a lone in-order ack-eliciting packet defers; the 2nd ack-eliciting packet, a gap above the top, and a reorder below it each force immediate; acked() resets the triggers) |
| §13.2.1 | Conformance | TestConn_AckDefer_ImmediateOnOutOfOrder (a gapped burst is acknowledged in the same Poll, not deferred), TestConn_AckDefer_ImmediateOnSecondAckEliciting (the 2nd ack-eliciting packet forces the ACK out in-burst), TestConn_AckDefer_DefersLoneInOrder (a lone in-order ack-eliciting packet defers its ACK with a recv + max_ack_delay deadline, no datagram in the recv hold), TestConn_AckDefer_PiggybackOnStream (the deferred ACK rides the next outbound STREAM packet — recv-then-send emits ONE datagram, not two), TestConn_AckDefer_TimerFallback (with no outbound packet the deferred ACK fires within max_ack_delay via the read-deadline expiry, and the ACK-only wake never provokes a PTO probe), TestConn_AckDefer_NotDeferredWithoutTimer (a transport that cannot arm a read deadline ACKs immediately — never deferred, never a stall) |
| §7.1 / §14.1 | Unit   | TestConn_SendInitialFlight (Conn drives the handshake to a ClientHello and sends one padded Initial datagram that decrypts back to it) |
| §7 / RFC 9001 §4 | Integration | TestConn_Establish_InMemory (**full QUIC v1 + TLS 1.3 handshake** completes between the client Conn and an in-memory server over a datagram pipe: Initial + Handshake flights, key installs, handshake done) |
| §19.6   | Unit        | TestConnFrameHandler_OnCrypto_ReassemblesByOffset (out-of-order CRYPTO frames reassembled by offset before feeding TLS — a real server's certificate flight spans many frames) |
| §19.6   | Conformance | TestConformance_RFC9000_Sec196_CryptoOffsetExceedsMax (a CRYPTO frame with offset+length > 2^62-1 → FRAME_ENCODING_ERROR; the exact limit and a normal offset accepted), TestConformance_RFC9000_Sec196_CryptoOverflowViaParser (rejected end-to-end through ParseFrames) |
| §2.2    | Conformance | TestConformance_RFC9000_Sec2_StreamReassembly (out-of-order STREAM frames reassembled to correct byte stream; complete only once FIN + all preceding bytes present) |
| §2.1    | Unit        | TestConn_OpenStream_IDs (client bidi stream IDs 0, 4, 8, …), TestConn_OnStream_DeliversToOpenStream (inbound STREAM routed to opened stream), TestConn_OpenUniStream_IDs (client uni stream IDs 2, 6, 10 + initial_max_streams_uni gate) |
| §2.2 / §13 | Unit     | TestStream_RecvAndFinished (Recv returns newly-contiguous bytes; Finished flips on FIN), TestConn_Poll_DeliversStreamData (post-handshake Poll decrypts a 1-RTT packet and delivers STREAM data to the open stream) |
| §18 / §18.2 | Conformance | TestConformance_RFC9000_Sec18_TransportParamsParse, TestConformance_RFC9000_Sec182_BidiRemoteBoundsClientStream (server params parsed to send limits; a request stream is bounded by initial_max_stream_data_bidi_remote 0x06, not _local) |
| §18 / §7.3 | Conformance | TestConformance_RFC9000_Sec18_TransportParamsEncode (client encodes the params it advertises — receive credit via initial_max_stream_data_bidi_local 0x05, server-uni limits, initial_source_connection_id — and the decoder accepts them) |
| §18.2   | Unit        | TestTransportParams_UnknownAndGREASEIgnored, TestTransportParams_AbsentDefaults (unknown/GREASE skipped; absent flow-control params default to 0) |
| §18.2   | Conformance | TestConformance_RFC9000_Sec182_AckDelayParams (ack_delay_exponent 0x0a + max_ack_delay 0x0b parsed, default to 3 / 25 ms when absent, rejected out of range — exponent > 20, max_ack_delay ≥ 2^14 ms → TRANSPORT_PARAMETER_ERROR) |
| §9.6 / §18.2 | Conformance | TestConformance_RFC9000_Sec96_PreferredAddressValidated (the preferred_address parameter 0x0d is structurally validated: a well-formed one accepted, but a zero-length connection ID (§9.6), a value too short for the Connection ID Length, or a length disagreeing with that field → TRANSPORT_PARAMETER_ERROR) |
| §7.4    | Negative    | TestConformance_RFC9000_Sec74_DuplicateParam, TestConformance_RFC9000_Sec74_MalformedParam, TestConformance_RFC9000_Sec74_InvalidValue (duplicate / malformed encoding / invalid value → ErrTransportParameter = TRANSPORT_PARAMETER_ERROR) |
| §7.2    | Conformance | TestConformance_RFC9000_Sec72_ServerCIDAdoptedOnlyWhenAuthenticated (the server's Source Connection ID is adopted as our Destination Connection ID only from a packet that authenticates — a forged/garbage Initial cannot poison the DCID; once the server CID is known, a long-header packet with a different SCID is discarded before decryption) |
| §7.3    | Conformance | TestConformance_RFC9000_Sec73_InitialSCIDAuthenticated (the server's initial_source_connection_id is authenticated against the SCID the client adopted; a mismatch or an absent parameter → TRANSPORT_PARAMETER_ERROR), TestConformance_RFC9000_Sec73_ConnectionIDValidation (original_destination_connection_id MUST be present and equal the client's first-Initial DCID; retry_source_connection_id MUST be present and equal the Retry's SCID exactly when a Retry was received and absent otherwise — each violation → TRANSPORT_PARAMETER_ERROR) |
| §4.6    | Conformance | TestConformance_RFC9000_Sec46_StreamLimit (OpenStream past peer initial_max_streams_bidi → ErrTooManyStreams; zero limit forbids the first) |
| §2.1    | Conformance | TestConformance_RFC9000_Sec21_AcceptServerUniStream (a server-initiated uni stream, id&3==3, is accepted, reassembled, and queued once for AcceptUniStream), TestConformance_RFC9000_Sec21_ServerBidiRejected (a server-initiated bidi stream → ErrServerBidiStream / STREAM_LIMIT_ERROR), TestConn_AcceptUniStream_Order |
| §4.6    | Conformance | TestConformance_RFC9000_Sec46_UniStreamLimit (a server uni stream beyond the advertised initial_max_streams_uni → ErrTooManyUniStreams / STREAM_LIMIT_ERROR) |
| §4.6    | Conformance | TestConformance_RFC9000_Sec46_FrameOverUniStreamLimit (a non-STREAM frame — RESET_STREAM or STREAM_DATA_BLOCKED — referencing a server-uni stream past the advertised limit → STREAM_LIMIT_ERROR, the same bound acceptPeerUniStream applies to STREAM; an in-limit stream handled normally) |
| §4.6 / §19.11 | Conformance | TestConformance_RFC9000_Sec46_MaxStreamsRaisesLimit (a MAX_STREAMS frame raises the cumulative limit so OpenStream succeeds past the initial grant), TestConformance_RFC9000_Sec46_MaxStreamsTooLarge (a MAX_STREAMS value > 2^60 → FRAME_ENCODING_ERROR), TestConn_MaxStreams_NonIncreasingIgnored, TestConn_MaxStreams_Uni |
| §4.6 / §18.2 | Conformance | TestConformance_RFC9000_Sec46_MaxStreamsTransportParamBound (an initial_max_streams_bidi/uni transport parameter > 2^60 → TRANSPORT_PARAMETER_ERROR; the exact 2^60 boundary accepted) |
| §4.1    | Conformance | TestConformance_RFC9000_Sec41_SendClampsToMinLimit (send never exceeds min(stream, conn) credit) |
| §4.1    | Unit        | TestConn_ReceiveFlowControl_GrantsCredit, TestConn_ReceiveFlowControl_NoGrantBelowThreshold (as the app consumes a response the client raises its advertised limits, queueing MAX_STREAM_DATA / MAX_DATA, batched at half a window) |
| §4.1    | Conformance | TestConformance_RFC9000_Sec41_StreamFlowControlEnforced (data past the advertised per-stream limit → FLOW_CONTROL_ERROR), TestConformance_RFC9000_Sec41_ConnFlowControlEnforced (combined data past the connection limit → FLOW_CONTROL_ERROR), TestConn_FlowControl_RetransmitNoDoubleCount (re-delivered bytes count once, keyed on the highest received offset) |
| §19.8   | Conformance | TestConformance_RFC9000_Sec198_StreamSendChunking (body larger than one datagram → multiple STREAM frames, monotonic offsets, LEN set, FIN on last) |
| §19.9   | Conformance | TestConformance_RFC9000_Sec199_MaxDataRaisesLimit (MAX_DATA raises the absolute conn ceiling; non-increasing ignored) |
| §19.10  | Conformance | TestConformance_RFC9000_Sec1910_MaxStreamDataRaisesLimit (MAX_STREAM_DATA raises the stream ceiling; FIN withheld while data is blocked, then flushed), TestConformance_RFC9000_Sec1910_MaxStreamDataStreamState (a MAX_STREAM_DATA for a receive-only server-uni stream or for a locally initiated stream not yet created → STREAM_STATE_ERROR; one for an open stream is honored and one for a created-then-closed stream is ignored) |
| §19.12  | Conformance | TestConformance_RFC9000_Sec1912_DataBlocked (zero conn credit → DATA_BLOCKED) |
| §19.13  | Conformance | TestConformance_RFC9000_Sec1913_StreamDataBlocked (zero stream credit → STREAM_DATA_BLOCKED, once per limit), TestConformance_RFC9000_Sec1913_StreamDataBlockedStreamState (a received STREAM_DATA_BLOCKED for a send-only client-uni stream or a locally initiated stream not yet created → STREAM_STATE_ERROR; one for a stream the peer sends on — a created client-bidi or a server-uni — accepted) |
| §19.14  | Conformance | TestConformance_RFC9000_Sec1914_StreamsBlockedOverLimit (a received STREAMS_BLOCKED whose Maximum Streams exceeds 2^60 → FRAME_ENCODING_ERROR, the same bound MAX_STREAMS enforces; exactly 2^60 accepted, both types) |
| §4.5    | Conformance | TestConformance_RFC9000_Sec45_FinBypassesFlowControl (a FIN consumes no credit — bare FIN sent at zero credit), TestConformance_RFC9000_Sec45_FinalSizeLatch (Send after FIN → ErrStreamFinished) |
| §4.5    | Conformance | TestConformance_RFC9000_Sec45_DataPastFinalSize, TestConformance_RFC9000_Sec45_FinBelowReceived, TestConformance_RFC9000_Sec45_ConflictingFin (received data past / below / inconsistent with a stream's final size → FINAL_SIZE_ERROR), TestConformance_RFC9000_Sec45_ResetFinalSizeBelow, TestConformance_RFC9000_Sec45_ResetFinalSizePastLimit (a RESET_STREAM final size below received → FINAL_SIZE_ERROR, past the limit → FLOW_CONTROL_ERROR), TestConn_ResetFinalSize_CreditsConn (the reset final size is credited to connection flow control), TestConformance_RFC9000_Sec45_ResetChangesFinalSizeAfterFin (a RESET_STREAM whose final size differs from one already fixed by a received FIN → FINAL_SIZE_ERROR; a matching one accepted) |
| §19.4   | Conformance | TestConformance_RFC9000_Sec194_ResetStream (Stream.Reset sends RESET_STREAM with the final size; Send then returns ErrStreamReset; idempotent) |
| §19.5   | Conformance | TestConformance_RFC9000_Sec195_StopSending (Stream.StopSending sends a STOP_SENDING frame) |
| §19.4 / §19.5 / §19.8 | Conformance | TestConformance_RFC9000_Sec19_StreamStateErrors (a STREAM or RESET_STREAM on a send-only client-uni stream, or a STOP_SENDING on a receive-only server-uni stream → STREAM_STATE_ERROR), TestConformance_RFC9000_Sec19_StreamStateValidDirections (the same frames in the matching direction accepted), TestConformance_RFC9000_Sec19_StreamStateNotCreated (a STREAM or STOP_SENDING for a locally initiated stream at or above our high-water mark — one not yet created — → STREAM_STATE_ERROR, while an ID below it, a stream created and since closed, is ignored; covers client-bidi via nextBidiStreamID and client-uni via openedUni), TestConn_StreamStateError_ClosesWithCode (maps to code 0x05) |
| §3.5    | Conformance | TestConformance_RFC9000_Sec35_StopSendingTriggersReset (a received STOP_SENDING resets our send side with the same code), TestConformance_RFC9000_Sec35_ResetStreamFinishesRecv (a received RESET_STREAM finishes the receive side), TestConformance_RFC9000_Sec35_ResetAfterCompleteIgnored (a RESET_STREAM after a fully-received stream has no effect — the receive side stays complete, not reset) |
| §13.3   | Conformance | TestConformance_RFC9000_Sec133_ResetRetransmittedOnLoss (a lost RESET_STREAM is retransmitted), TestConformance_RFC9000_Sec133_NoStreamRetransmitAfterReset (a reset stream's STREAM data is not retransmitted), TestConformance_RFC9000_Sec133_NoRetransmitAfterResetAndEvict (a reset stream's data is not retransmitted even after the stream is retired from the routing map) |
| §10.2   | Unit        | TestConn_CloseWithError_SendsAppConnectionClose (CloseWithError emits one CONNECTION_CLOSE on the 1-RTT space and closes the transport), TestConn_Close_Idempotent (a second Close sends nothing more), TestConn_CloseWithError_DowngradesAppBeforeOneRTT (§10.2.3: an application close before 1-RTT is sent as a transport CONNECTION_CLOSE with APPLICATION_ERROR) |
| §10.2   | Conformance | TestConn_Poll_MalformedFrame_SendsConnectionClose (a received malformed frame makes Poll emit a CONNECTION_CLOSE with FRAME_ENCODING_ERROR), TestConn_Fail_SendsCloseForProtocolError (a protocol-violation error is signalled with the mapped transport code), TestConn_Fail_NoCloseForIOError (an I/O error sends nothing) |
| §10.2.2 | Conformance | TestConformance_RFC9000_Sec1022_NoSendAfterConnectionClose (draining: flush sends nothing after a received CONNECTION_CLOSE — no ACK/PATH_RESPONSE), TestConformance_RFC9000_Sec1022_NoAppSendAfterClose (the application send path also refuses — writeAppFrames → ErrConnClosed), TestConformance_RFC9000_Sec1022_PollSurfacesPeerClose (Poll returns a *PeerClosedError with the peer's code and sends nothing back) |
| §10.1   | Conformance | TestConformance_RFC9000_Sec101_EffectiveIdleMin (effective idle timeout = the smaller non-zero of the two advertised max_idle_timeout values), TestConformance_RFC9000_Sec101_AdvertiseAndParse (the client advertises 0x01 in ms and parses the peer's; a zero value omits it), TestConformance_RFC9000_Sec101_IdleTimeoutOverflowCapped (an absurd advertised value is capped, not overflowed to a negative Duration), TestConformance_RFC9000_Sec101_IdleFlooredByPTO (the period is floored at 3×PTO; disabled when neither endpoint advertises), TestConformance_RFC9000_Sec101_IdleClose (an idle connection is silently closed — no CONNECTION_CLOSE — with ErrIdleTimeout), TestConformance_RFC9000_Sec101_IdleCloseWithDataInFlight (the idle close fires even with an ack-eliciting packet outstanding — probing does not reset the timer, so a silent peer is detected deterministically) |
| §4.1    | Unit        | TestConn_SendStream_Wire (client Send → decrypt → STREAM bytes match), TestStream_Grantable_Unit (credit-clamp accounting), TestStream_Send_NotEstablished (Send before 1-RTT keys → ErrNotEstablished) |
| §10.3 / §10.3.1 | Conformance | TestConformance_RFC9000_Sec103_StatelessResetTokenParsed (the peer's stateless_reset_token transport parameter 0x02 is recorded as a 16-byte value; a wrong length → TRANSPORT_PARAMETER_ERROR), TestConformance_RFC9000_Sec1031_StatelessResetDetected (a ≥21-byte undecryptable first-packet datagram ending in a token the peer gave us — here via NEW_CONNECTION_ID — is recognized as a stateless reset and silently closes the connection with ErrStatelessReset and no CONNECTION_CLOSE; a datagram whose trailing bytes match no known token is merely dropped) |

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
| §5.3    | Conformance | TestConformance_RFC9001_AppA5_ChaCha20 (ChaCha20-Poly1305 short-header packet built by Seal reproduces the Appendix A.5 protected bytes byte-for-byte: key/iv/hp derivation + AEAD_CHACHA20_POLY1305 + header protection), TestChaCha20_SealOpenRoundTrip (seal → open recovers pn + payload), TestChaCha20_KeyUpdateRoundTrip (the "quic ku" ratchet keeps ChaCha20 keys) |
| §5.4.4  | Conformance | TestConformance_RFC9001_AppA5_ChaCha20 (the 5-byte ChaCha20 header-protection mask from the A.5 sample = aefefe7d03), TestHeaderProtection_AES_ByteIdentical (AES-ECB header protection is byte-identical through the headerProtector seam) |
| §5.8    | Conformance | TestConformance_RFC9001_Sec58_RetryIntegrityKAT (Retry Integrity Tag verified against the Appendix A.4 known-answer vector; tampered packet and wrong original DCID fail) |
| §A.3    | Conformance | TestDecodePacketNumber (packet-number reconstruction, §A.3 example) |
| §4      | Integration | TestTLSHandshake_InMemory (full TLS 1.3 QUIC handshake client↔server via crypto/tls.QUICConn; secrets match, keys derive, ALPN h3) |
| §5.4.1  | Unit        | TestKeysFromSecret_UnsupportedSuite (a suite with no defined QUIC header-protection scheme — TLS_AES_128_CCM_8_SHA256 — → ErrCryptoSuite; AES-GCM + ChaCha20-Poly1305 are supported), TestKeysFromSecret_ChaCha20 (ChaCha20-Poly1305 derives a 32-byte key/hp + 12-byte iv and stamps its suite) |
| §6.1    | Conformance | TestKeyUpdate_QuicKuVector (next-generation secret = HKDF-Expand-Label(secret, "quic ku", "", Hash.length)) |
| §6.1    | Conformance | TestConformance_RFC9001_Sec61_UpdateBeforeConfirmed (a key update is dropped until the handshake is confirmed via HANDSHAKE_DONE, not merely TLS-complete) |
| §6.2    | Conformance | TestConformance_RFC9001_Sec62_KeyUpdateResponder (a peer-initiated Key Phase 0→1 update is trial-decrypted with the pre-derived next generation, committed, and the client flips its own send phase; HP key not rotated) |
| §6.3    | Conformance | TestConformance_RFC9001_Sec63_PrevKeysReordered (reordered previous-generation packet below the boundary decrypts with retained prev keys; prev discarded after 3×PTO) |
| §6.4    | Conformance | TestConformance_RFC9001_Sec64_ForgedPhaseBitBounded (a forged Key Phase bit costs exactly one AEAD attempt and never commits an update) |
| §6.6    | Conformance | TestConformance_RFC9001_Sec66_ConfidentialityLimitCloses (at the AES-GCM confidentiality limit the client — a pure key-update responder — closes with AEAD_LIMIT_REACHED), TestConn_AEADConfidentiality_CounterIncrements, TestConn_AEADConfidentiality_ResetOnKeyUpdate (a key update resets the send counter), TestConn_AEADIntegrity_CountsAuthFailures (a failed authentication counts toward the integrity limit), TestConformance_RFC9001_Sec66_AEADLimitsSuiteAware (the §6.6 AEAD limits are suite-dependent — ChaCha20-Poly1305 2^62/2^36 vs AES-GCM 2^23/2^52; the integrity limit MUST be smaller for ChaCha20, so the suites cannot share one) |
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
| §5.3    | Conformance | TestConformance_RFC9002_Sec53_AckDelayExponentDecode (ACK Delay decoded with the peer's ack_delay_exponent), TestConformance_RFC9002_Sec53_InitialSpaceFixedExponent (Initial/Handshake use the fixed exponent 3), TestConformance_RFC9002_Sec53_AckDelayClampedToMax (once confirmed, ack delay is clamped to max_ack_delay), TestConn_AckDelay_OverflowBounded (an overflowing ACK Delay is saturated, not wrapped negative) |
| §5.1 / §19.3 | Unit   | TestSentSpace_Ack, TestConnFrameHandler_OnAck_UpdatesRTT, TestConnFrameHandler_OnAckRange_Gap (sent-packet tracking; ACK range walk removes acked packets and samples RTT from the largest; gaps leave packets in flight) |
| §5.1    | Conformance | TestConformance_RFC9002_Sec51_RTTSampleWhenLargestNonEliciting (an RTT sample is taken when a newly-acked packet is ack-eliciting even if the largest acked is not — using the largest's send time), TestConformance_RFC9002_Sec51_NoRTTSampleWithoutAckEliciting (no sample when no newly-acked packet is ack-eliciting) |
| §6.1.1  | Conformance | TestConformance_RFC9002_Sec611_PacketThresholdLoss (a packet ≥ kPacketThreshold=3 numbers below the largest acknowledged is declared lost) |
| §6.1.2  | Conformance | TestConformance_RFC9002_Sec612_TimeThresholdLoss (a packet sent before now − 9/8·max(srtt,latest) is declared lost), TestRTTStats_LossDelay, TestConn_DetectLost_NoLossWithinThresholds, TestConformance_RFC9002_Sec612_EarliestLossTime (the loss-detection timer covers the earliest ack-eliciting packet below the largest acknowledged, excluding tail packets), TestConformance_RFC9002_Sec612_LossTimerDeclaresLost (a read-loop loss-timer expiry declares a reordered packet lost rather than sending a probe) |
| §6.2    | Conformance | TestConformance_RFC9002_Sec62_LossTimePriorityOverPTO (a pending time-threshold loss arms the timer ahead of the PTO — §6.2.1 the PTO MUST NOT be set while a loss timer is), TestConformance_RFC9002_Sec62_PTOWhenNoLossTime (a fully unacknowledged tail arms the probe timeout instead) |
| §6.1 / §13.3 | Unit | TestConn_Retransmit_CryptoResendsBytesAtOffset (lost CRYPTO resent at its offset; retransmit Initial datagram padded to 1200, RFC 9000 §14.1), TestConn_Retransmit_StreamResendsBytesAtOffsetAndFin (lost STREAM resent at offset+FIN, accounting not re-advanced), TestConn_Retransmit_AckedPacketNotResent, TestConn_AckOnlyPacketNotRetransmittable |
| §6.2.1  | Unit        | TestConn_PTOPeriod (PTO = srtt + max(4·rttvar, granularity) + max_ack_delay, doubled per backoff; 2·kInitialRtt pre-sample), TestConn_PTOCount_ResetOnAck (backoff reset on a newly-acked ack-eliciting packet) |
| §6.2.2  | Conformance | TestConformance_RFC9002_Sec622_PTOCountResetOnKeyDiscard (discarding Initial/Handshake keys resets the PTO backoff count — the discard is forward progress, App. A.4) |
| §6.2.2.1 | Conformance | TestConformance_RFC9002_Sec6221_PTOArmedWithEmptyFlight (before the handshake completes the PTO is armed even with an empty flight), TestConformance_RFC9002_Sec6221_HandshakeProbeSendsPing (the anti-deadlock probe is a Handshake-space PING), TestConformance_RFC9002_Sec6221_InitialProbePadded (with only Initial keys it is an Initial PING padded ≥1200); the whole handshake is bounded by handshakeTimeout so an acknowledging-but-stalled server cannot probe forever |
| §6.2.4  | Unit        | TestConn_OnPTO_QueuesProbe (probe resends the oldest unacked packet + backs off), TestConn_ReadWithPTO_ProbesOnTimeout (a read timeout with data in flight sends a probe and retries) |
| §6.2.4  | Conformance | TestConformance_RFC9002_Sec624_PTOSendsPingWhenNoData (a PTO with only a frameless ack-eliciting packet in flight still sends an ack-eliciting PING probe), TestConn_PTO_NoPingWhenDataToResend (a PTO with retransmittable data resends it, no bare PING) |
| §6.2 / §6.1.2 / §10.1 | Unit | TestConn_Poll_PTOProbeOnTimeout (a read-deadline expiry with data in flight drives Poll → handleExpiry → onPTO: the oldest packet is re-queued, ptoCount backs off, the probe is flushed, the connection stays up), TestConn_Poll_IdleTimeoutCloses (an expiry past the idle deadline drives Poll → handleExpiry → idleClose, returning ErrIdleTimeout and closing the transport, RFC 9000 §10.1), TestConn_HandleExpiry_DeclaresLostOnLossTimer (an expiry with a time-threshold-eligible packet declares it lost, §6.1.2), TestConn_HandleExpiry_GivesUpWhenNothingToProbe (an expiry with nothing to probe surfaces the timeout rather than re-arming) |
| §6 (whole) | Interop | `make h3-interop-loss` — the GET/POST/16 KiB interop suite run against live Caddy through a UDP relay that drops ~10% of datagrams each way; passing proves the handshake, request, and response recover via retransmission and the probe timeout (verified up to 20% loss). |
| §7.3.1  | Conformance | TestConformance_RFC9002_Sec731_SlowStart (an ack in slow start grows cwnd by the acked bytes and frees them from bytes_in_flight), TestConformance_RFC9002_Sec731_HalveOncePerRecovery (a loss halves cwnd once per recovery episode; same-episode losses do not re-halve) |
| §7.3.3  | Conformance | TestConformance_RFC9002_Sec733_CongestionAvoidance (byte-accumulator growth of one max_datagram_size per window acked; does not freeze at a large window) |
| §7.6    | Conformance | TestConformance_RFC9002_Sec76_PersistentCongestionCollapse (lost ack-eliciting packets spanning longer than kPersistentCongestionThreshold·base-PTO collapse cwnd to kMinimumWindow, clear recovery start, and arm the min_rtt reset), TestConformance_RFC9002_Sec76_ShortSpanHalvesOnly (a shorter span only halves), TestConformance_RFC9002_Sec76_AckedPacketInSpanNoCollapse (an acknowledgement inside the span breaks the unbroken loss — no collapse, §7.6.1 cond. 1), TestConformance_RFC9002_Sec76_RequiresRTTSample (packets before the first RTT sample do not establish it), TestConn_PersistentCongestion_ResetsMinRTT (§5.2 — the next sample becomes min_rtt) |
| §7.8    | Conformance | TestConformance_RFC9002_Sec78_AppLimitedNoGrowth (an acknowledgement of an application-limited flight frees the acked bytes but does not grow cwnd), TestConformance_RFC9002_Sec78_CwndLimited (the congestion-limited test: empty window → app-limited, full window → limited, >half-full in slow start → limited, half-full in avoidance → app-limited), TestConformance_RFC9002_Sec78_FullWindowAckGrowsCwnd (a full-window multi-range ACK parsed end-to-end grows cwnd for every acknowledged packet — priorInFlight captured once before removal and reused across ranges) |
| §7.7    | Conformance | TestConformance_RFC9002_Sec77_Pacing (a bulk send is paced by a wall-clock token bucket over on-wire bytes: the first burst is limited to about the initial congestion window — not the whole, larger cwnd; an empty bucket admits nothing until time passes; a full smoothed-RTT refills it), TestConformance_RFC9002_Sec77_PacingRate (the bucket refills at exactly the §7.7 rate N·cwnd/smoothed_rtt, N=1.25 — 2500 bytes per 1 ms at cwnd=200000/srtt=100 ms), TestConformance_RFC9002_Sec77_PacingSubQuantumNoStarve (many sub-byte-quantum refills accumulate rather than being discarded — no fast-retry livelock), TestConformance_RFC9002_Sec77_NoPacingWithoutRTT (before any RTT sample the send is unpaced — the permitted initial-window burst). Loss retransmissions are burst-limited to an initial window per flush (flushRetransmits), the §7.7 "limit such bursts" alternative; verified by the loss interop |
| §7 (gate) | Unit      | TestCC_GateClampedByWindow (grantable clamps to the remaining window and reports blockCong — no frame — when full), TestCC_PureAckNotCounted (pure-ACK packets are not in flight), TestCC_DisabledSentinel (cwnd==0 leaves the send path unthrottled) |
| §7      | Conformance | TestConformance_RFC9002_Sec7_RetransmitRespectsCwnd (a retransmission counts against the congestion window — flushRetransmits sends nothing while bytes_in_flight fills the window and drains the queue once there is room; loss detection frees the lost bytes first and a PTO probe bypasses the window, so no stall) |

### Opt-in BBR v1 (draft-cardwell-iccrg-bbr-congestion-control)

BBR is an OPT-IN alternative to NewReno, selected with
`NewConn(..., WithCongestionControl(CCBBR))` (bbr.go). It reuses the RFC 9002 §7.7
pacing token bucket (driven by `pacing_gain·btlbw` instead of `5·cwnd/4`) and the
same congestion-window send gate (driven by `cwnd_gain·BDP`). NewReno stays the
default; when it is selected none of the BBR paths run. Throughput benefit over
NewReno is path-dependent and requires a netem/tc WAN lab to quantify — it is not
claimed by these tests, which pin the model's mechanics only.

| Section | Type | Test |
|---------|------|------|
| §4.1.1 (delivery rate) | Unit | TestBBR_DeliveryRateSample (a known send/ack timeline yields the expected btlbw; an app-limited sample below the estimate is ignored while one above still raises it) |
| §2.3 (btlbw max-filter) | Unit | TestBBR_MaxFilter_WindowedExpiry (windowed running-max surfaces the peak and lets an older, larger sample age out of the window) |
| §4.3.1 (min_rtt filter) | Unit | TestBBR_MinRTT_WindowedMinExpiry (min_rtt adopts any lower sample, ignores higher ones in-window, and adopts a higher one once the 10 s window expires) |
| §4.3.2 (Startup→Drain) | Unit | TestBBR_Startup_To_Drain (Startup ends and Drain begins after btlbw fails to grow ≥25% for three rounds) |
| §4.3.2 (Drain→ProbeBW) | Unit | TestBBR_Drain_To_ProbeBW (Drain hands off to ProbeBW once the flight falls to the BDP) |
| §4.3.3 (ProbeBW cycle) | Unit | TestBBR_ProbeBW_CycleAdvance (pacing-gain cycle advances one phase per min_rtt; the 0.75 phase exits early when inflight ≤ BDP) |
| §4.3.4 (ProbeRTT) | Unit | TestBBR_ProbeRTT_EntryAndExit (10 s without a min_rtt refresh enters ProbeRTT; it exits after the flight drains and one round elapses past the hold) |
| §4.1 (loss tolerance) | Unit | TestBBR_LossToleranceVsNewReno (a single loss does NOT shrink the BBR window, whereas NewReno halves on the same event) |
| RFC 9002 §7.6 (persistent congestion) | Unit | TestBBR_PersistentCongestionCollapse (a persistent-congestion episode collapses BBR cwnd to the floor, clears the pacing rate, and re-enters Startup with a fresh model) |
| §3 / RFC 9002 §7.7 (send gate) | Unit | TestBBR_DrivesCwndAndPacingThroughSeam (one real ACK range makes BBR write both c.cwnd from the BDP and c.pacingRate from btlbw — the single gate NewReno also reads) |
| Default invariance | Unit | TestBBR_DefaultInvariance_NewRenoUnchanged (a connection with no option is NewReno; a scripted ack/loss sequence yields byte-identical, pinned cwnd/ssthresh and never engages the BBR pacer) |

## RFC 9204 — QPACK (Phase G — HTTP/3)

G.3 lands the `qpack/` static-table codec: a static-table-only encoder and a
decoder for the static + literal representations, reusing the `hpack`
prefixed-integer + Huffman codecs.

Q1 adds the dynamic-table machinery: a `DynamicTable` (absolute indexing +
eviction, §3.2), an encoder-instruction parser and decoder-instruction emitter
(§4.3/§4.4), and dynamic-reference resolution in the decoder (indexed,
name-reference, and post-Base forms, plus the Required Insert Count and signed
Base). It is INERT on the wire: the client still advertises
`SETTINGS_QPACK_MAX_TABLE_CAPACITY=0`, so the encoder still emits RIC 0 and no
instructions are exchanged — a nil `DynamicTable` preserves the static-only
decode path exactly. The codec is proved against the RFC 9204 Appendix B worked
examples.

Q2 wires the Q1 codec into the `http3.Client`: the client opens its own encoder
and decoder instruction streams (§4.2), a connection-scoped shared `DynamicTable`
guarded by a new leaf `qpackMu` (the reader applies the server encoder stream
under `Lock`, every `Do` resolves dynamic references under `RLock` and copies each
kept field before releasing), the reader emits an Insert Count Increment for
inserts it applies (§4.4.3), each decoded section that referenced the dynamic
table triggers a Section Acknowledgment (§2.1.4 / §4.4.1), and an aborted stream
that referenced the table triggers a Stream Cancellation (§4.4.2).

Q3 flips the switch: `dial.go` advertises `SETTINGS_QPACK_MAX_TABLE_CAPACITY=4096`,
so a live server inserts into the shared table and references those entries from
response field sections. The Known Received Count is coordinated so an Insert Count
Increment (§4.4.3) and a Section Acknowledgment (§2.1.4 / §4.4.1) never double-count
the same insert regardless of arrival order. The end-to-end decode path is the CI
interop gate (Caddy / nginx / aioquic all use dynamic QPACK once capacity > 0); the
unit matrix below drives the same path deterministically against a fake encoder
stream at capacity 4096.

Q4 raises `SETTINGS_QPACK_BLOCKED_STREAMS` to 16, so a server's encoder may
reference a dynamic-table entry from a response field section before its Insert
Count Increment has told us we hold it — a section whose Required Insert Count is
ahead of our insert count. Such a decode now PARKS (off `qpackMu` — never a lock
across a wait, R2) until the reader advances the insert count to cover it (§2.1.3),
rather than failing. The reader broadcasts on every insert-count advance (a single
advance can unblock any number of parked decodes, each waiting on a different
Required Insert Count), and each parked decode re-reads the published insert count
level-triggered, so no wake is lost. The wait is bounded by the request context and
by connection teardown, so a server that promises inserts it never sends fails the
request on ctx timeout instead of hanging; at most 16 streams may block at once
(§2.1.2) — a section past the limit is a QPACK_DECOMPRESSION_FAILED. A Required
Insert Count that is malformed or can never be satisfied is still rejected
immediately. At `BLOCKED_STREAMS=0` (the static-only / Q3 unit profile) the decoder
still never parks: a Required Insert Count past the insert count fails fast. The
interop-level RIC-ahead scenario (a fault server sending a section before its
encoder-stream insert) is a follow-up; the unit matrix below fully exercises the
wait/wake, the ctx-bounded park, and the M-blocked bound against a fake encoder.

Q5 adds the ENCODE side: the client maintains its OWN dynamic table (the table the
server keeps as decoder), sized from the server's advertised
`SETTINGS_QPACK_MAX_TABLE_CAPACITY` (0, or below 32, ⇒ static-only, a no-op). It is a
conservative encoder — insert-then-reference-after-ack: a repeated header is inserted
on our encoder stream the first time it is seen (Insert With Name Reference /
Literal Name, §4.3.2/§4.3.3), and a request field section references it only once the
server's decoder acknowledges it — an Insert Count Increment on the server's decoder
stream advances our Known Received Count (§2.1.4 / §4.4.3), and only entries below it
are referenced. So a referenced section's Required Insert Count is never ahead of the
server's insert count and no request stream ever blocks. The encoder never evicts (it
inserts only while there is room under its chosen capacity), so an entry referenced by
an in-flight request cannot be dropped. All encoder state — the table, its Known
Received Count, and the encoder-stream write buffer — is serialized under a new leaf
`encMu` (a `Do` encodes one request under it; the reader applies the server's decoder
stream under it), never held across a wait. The static-table output is preserved
byte-for-byte before SETTINGS arrive and against a capacity-0 server. The CI interop
(Caddy / nginx / aioquic decoding our dynamic-referencing requests) is the wire gate;
the unit matrix round-trips every encoded section and every Insert instruction back
through the Q1 decoder against a mirror of the server's decode table, pinning the
Required Insert Count / Base / index math a wrong byte of which would fail every
request.

| Section | Type        | Test |
|---------|-------------|------|
| App. A  | Roundtrip   | TestStaticTable_Shape (99-entry 0-based static table) |
| App. B  | Conformance | TestConformance_RFC9204_AppB_DynamicTable (B.2–B.5 verbatim: Set Capacity, Insert With/Without Name Reference, Duplicate, eviction, and field sections decoded via post-Base and Base-relative dynamic references, with the decoder-instruction responses re-encoded) |
| §3.2.4–6 | Conformance | TestConformance_RFC9204_AppB_DynamicTable, FuzzDynamicIndexResolution (absolute / insert-count-relative / Base-relative / post-Base index identities) |
| §4.3.1–4 | Conformance | TestConformance_RFC9204_AppB_DynamicTable, TestQPACK_EncoderInstructions_Partial (Set Capacity + Insert on a byte-fragmented encoder stream), TestQPACK_EncoderInstructions_Errors (capacity over max, missing/oob name ref, oversize insert → QPACK_ENCODER_STREAM_ERROR) |
| §4.4.1–3 | Conformance | TestQPACK_DecoderInstructions_Encode (Section Acknowledgment / Stream Cancellation / Insert Count Increment prefixed-integer bytes), TestConformance_RFC9204_AppB_DynamicTable |
| §4.5.1.1 | Conformance | TestQPACK_RequiredInsertCount_Decode (wraparound past 2*MaxEntries, out-of-range rejection), FuzzRequiredInsertCount (encode/decode round-trip over the valid window) |
| §4.5.2  | Conformance | TestConformance_RFC9204_Sec452_IndexedDynamicDecode (Indexed Field Line, dynamic Base-relative), TestConformance_RFC9204_Sec45_StaticIndexedEncode |
| §4.5.3  | Conformance | TestConformance_RFC9204_Sec453_LiteralNameRefDynamicDecode (Literal with dynamic Name Reference, Base-relative) |
| §4.5.4  | Conformance | TestConformance_RFC9204_Sec454_LiteralNameRefDecode |
| §4.5.5  | Conformance | TestConformance_RFC9204_Sec455_PostBaseNameRefDecode (Literal with Post-Base Name Reference) |
| §4.5.6  | Conformance | TestConformance_RFC9204_Sec456_LiteralNameDecode |
| §4.5 / §4.5.1.2 | Conformance | TestConformance_RFC9204_Sec45_DecodeErrors (dynamic-ref / malformed / a field-section prefix with the Base Sign bit set — negative Base with the forced-zero Required Insert Count — → QPACK_DECOMPRESSION_FAILED), TestQPACK_DecodeDynamic_Errors (unreachable RIC, Base underflow, absent post-Base entry), TestQPACK_StaticOnly_WithTable (nil vs non-nil table decode identically at RIC 0) |
| §4.5    | Fuzz        | FuzzDecodeFieldSection (arbitrary field-section bytes against both a nil static-only table and a populated dynamic table → rejected with QPACK_DECOMPRESSION_FAILED or accepted, never a panic/hang — every HTTP/3 response header block is attacker-controlled) |
| §2.2 / §6 | Conformance | TestConformance_RFC9204_Sec22_DecompressionFailedClosesConn (a field section the decoder cannot resolve → QPACK_DECOMPRESSION_FAILED, an application-layer CONNECTION_CLOSE, not a per-stream reset). With the table live a WELL-FORMED dynamic reference decodes instead (§3.2.1 row); only a malformed prefix, an out-of-range index, or a Required Insert Count past the insert count still fails — the re-baselined interop faults TestFault_QpackDynamicRef / TestFault_QpackRequiredInsertCount |
| §4.5    | Roundtrip   | TestQPACK_RoundTrip, TestQPACK_EmptySection |
| §4.2    | Conformance | TestConformance_RFC9204_Sec42_QPACKStreamClosed (server closing its QPACK encoder stream → H3_CLOSED_CRITICAL_STREAM), TestConformance_RFC9204_Sec42_DuplicateQPACKStream (a second QPACK encoder stream → H3_STREAM_CREATION_ERROR) |
| §4.2 (wiring) | Conformance | TestClient_RequestResponse / TestClient_SendDrainsUnderFlowControl (client opens its own control + QPACK encoder (0x02) + decoder (0x03) uni-streams; the decoder stream leads with its type byte) — Q2 |
| §4.3 (wiring) | Conformance | TestConformance_RFC9204_Sec43_EncoderInstructionsApplied (reader applies the server encoder stream's Set Capacity + Insert to the shared table, publishes the advanced insert count) — Q2 |
| §2.1.4 / §4.4.1 (wiring) | Conformance | TestConformance_RFC9204_Sec441_SectionAcknowledgment (a HEADERS section that references a dynamic entry decodes against the shared table and emits a Section Acknowledgment for the stream) — Q2 |
| §4.4.2 (wiring) | Conformance | TestConformance_RFC9204_Sec442_StreamCancellationOnAbort (aborting a stream that referenced the dynamic table emits a Stream Cancellation) — Q2 |
| §4.4.3 (wiring) | Conformance | TestConformance_RFC9204_Sec43_EncoderInstructionsApplied (an Insert Count Increment is emitted on the decoder stream for newly applied inserts) — Q2 |
| §3.2 (concurrency) | Race | TestConcurrent_QPACKDynamicTable_UnderRace (reader inserts under qpackMu.Lock while N decoders resolve dynamic references under qpackMu.RLock — `-race -count=5`, no race, no deadlock) — Q2 |
| §3.2.1 (live) | Conformance | TestConformance_RFC9204_Sec321_DynamicTableCapacityHonored (capacity 4096: server encoder inserts two entries, response HEADERS reference them by Base-relative indexed / Base-relative name reference / post-Base indexed; decode succeeds against the shared table; the decoder stream carries an Insert Count Increment then a Section Acknowledgment per stream with the right Known Received Count) — Q3 |
| §2.1.4 / §4.4.3 (live) | Conformance | TestConformance_RFC9204_Sec214_KnownReceivedCountCoordination (an Insert Count Increment and a Section Acknowledgment never double-count the same insert, either arrival order — the redundant increment is suppressed) — Q3 |
| §3.2.2 (live) | Conformance | TestConformance_RFC9204_Sec322_EvictionAndEvictedReference (insert past capacity evicts the oldest entry; a live-entry reference decodes, an evicted-entry reference → QPACK_DECOMPRESSION_FAILED) — Q3 |
| §4.5.1 (live) | Conformance | TestConformance_RFC9204_Sec451_RequiredInsertCountExceedsInserts (at SETTINGS_QPACK_BLOCKED_STREAMS=0 a Required Insert Count past the insert count → QPACK_DECOMPRESSION_FAILED, no block/hang) — Q3 |
| §2.1.3 (blocked) | Conformance | TestConformance_RFC9204_Sec213_BlockedDecodeWaitsForInsert (a section whose Required Insert Count is ahead of the insert count parks the decode until the reader applies the encoder-stream insert, then unblocks and decodes — `-race -count=5`), TestConformance_RFC9204_Sec213_BlockedDecodeCtxTimeout (a blocked decode whose promised insert never arrives fails on the request ctx deadline, never hangs) — Q4 |
| §2.1.2 (blocked) | Conformance | TestConformance_RFC9204_Sec212_BlockedStreamLimitEnforced (with SETTINGS_QPACK_BLOCKED_STREAMS=2 a third simultaneously-blocked section exceeds the advertised limit → QPACK_DECOMPRESSION_FAILED) — Q4 |
| §2.1.3 (concurrency) | Race | TestConcurrent_QPACKBlockedStreams_UnderRace (many decoders with heterogeneous Required Insert Counts, some blocked, wake correctly as the reader delivers inserts and broadcasts — no lost wake, no race, no deadlock; `-race -count=5`) — Q4 |
| §2.1 (encode) | Conformance | TestConformance_RFC9204_Sec21_EncodeReferenceAfterAck (qpack: insert-then-reference-after-ack — the first request inserts and stays byte-identical to the static encoder, the second references the dynamic entries; both round-trip through the Q1 decoder against a server-mirror table with the right Required Insert Count / Base), TestConformance_RFC9204_Sec21_EncodeSideDynamicTable (http3: full client round-trip — server SETTINGS install the encoder, the request field sections decode against the table rebuilt from our encoder stream) — Q5 |
| §2.1.4 (encode) | Conformance | TestConformance_RFC9204_Sec214_ReferenceOnlyAcknowledged (only entries below the Known Received Count are referenced — a partial Insert Count Increment leaves the unacknowledged entry static), TestConformance_RFC9204_Sec44_ParseDecoderInstructions (an Insert Count Increment advances the Known Received Count; a zero increment or one past the inserts made → QPACK_DECODER_STREAM_ERROR; Section Acknowledgment / Stream Cancellation consumed as no-ops; a partial instruction retained) — Q5 |
| §4.3.1–3 (encode) | Conformance | TestConformance_RFC9204_Sec21_EncodeReferenceAfterAck (Set Dynamic Table Capacity + Insert With Name Reference emitted on our encoder stream and replayed by the Q1 decoder), TestConformance_RFC9204_Sec322_EncoderNeverEvicts (the encoder inserts only what fits under capacity, so the mirror never evicts — live entries equal the insert count) — Q5 |
| §3.2.3 (encode) | Conformance | TestConformance_RFC9204_Sec21_EncodeStaticFallbackServerCapZero (a server capacity of 0 keeps the encoder static-only: the field section is byte-identical to the static encoder and nothing beyond the type byte is written on our encoder stream), TestNewDynamicEncoder_RejectsTinyCapacity (qpack: a capacity below 32 is rejected → the caller stays static-only) — Q5 |
| §2.1 (encode concurrency) | Race | TestConcurrent_QPACKEncoderDynamic_UnderRace (N goroutines encode requests through the shared encoder dynamic table while acknowledgments apply through the same encMu-guarded path the reader uses — no race, no deadlock; `-race -count=5`) — Q5 |

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
| §7.1    | Conformance | TestConformance_RFC9114_Sec71_SettingsTruncatedIsFrameError (a SETTINGS identifier or value cut off by the frame length → H3_FRAME_ERROR — a frame-layout error, §7.1 — distinct from the H3_SETTINGS_ERROR raised for a reserved or duplicate identifier) |
| §6.2 / §6.2.1 | Conformance | TestConformance_RFC9114_Sec62_ControlStream (client control stream = type 0x00 + first SETTINGS frame; peeled + read back) |
| §7.1    | Unit        | TestFrameReader_MultipleFrames, TestFrameReader_SplitAcrossFeeds, TestFrameReader_HugeLength (streaming frame reader: back-to-back frames, frame split across feeds, huge length stays ErrNeedMore without overflow), TestReadStreamType |
| §7.1    | Fuzz        | FuzzFrameReader (arbitrary stream bytes through Feed + a ReadFrame loop, with a frame-length cap → ErrNeedMore / ErrH3FrameTooLarge or a whole frame, never a panic/hang/unbounded buffer) |
| §7.2.4  | Fuzz        | FuzzParseSettings (arbitrary SETTINGS payload bytes → H3_FRAME_ERROR / H3_SETTINGS_ERROR or the parsed parameters, never a panic) |
| §4.3.1  | Conformance | TestConformance_RFC9114_Sec431_RequestPseudoHeadersFirst (request pseudo-headers precede regular headers; QPACK round-trip in a HEADERS frame), TestRequest_OmitsEmptyAuthority |
| §4.2    | Negative    | TestConformance_RFC9114_Sec42_RequestValidation (missing pseudo-header / uppercase / connection-specific / CR-LF value → ErrH3Message; client never emits a malformed request), TestConformance_RFC9114_Sec42_PseudoHeaderValueValidation (a NUL/CR/LF octet in a pseudo-header value — :method/:scheme/:authority/:path — → ErrH3Message, closing the header-injection gap) |
| §4.3.1  | Negative    | TestConformance_RFC9114_Sec431_AuthorityRequired (an http/https request with neither :authority nor a Host header → ErrH3Message; either one satisfies it), TestConformance_RFC9114_Sec431_AuthorityUserinfoAndHostMatch (an :authority carrying userinfo, a Host header disagreeing with :authority, or an empty Host header → ErrH3Message; a Host equal to :authority accepted) |
| §4.2.2  | Conformance | TestConformance_RFC9114_Sec422_FieldSectionSizeLimit (a request field section over the peer's SETTINGS_MAX_FIELD_SECTION_SIZE → ErrFieldSectionTooLarge; exactly at the limit accepted; size = Σ name+value+32), TestConformance_RFC9114_Sec422_ExtraHeaderCounted (a regular header adds name+value+32), TestConformance_RFC9114_Sec422_ApplyMaxFieldSection (§7.2.4.1 — the peer's 0x06 is recorded; absent → no-limit default), TestConformance_RFC9114_Sec422_DoRefusesOversized (Do refuses to send an oversized request end-to-end) |
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
| §4.1.1 / §5.2 | Conformance | TestConformance_RFC9114_Sec411_RetryOnRequestRejected, TestConformance_RFC9114_Sec411_NoRetryOnNonRejectedReset, TestConformance_RFC9114_Sec52_RetryOnGoAway (client/) — the client retry layer classifies the H3 errors surfaced verbatim by the buffered h3Exchange: an *http3.StreamResetError is retried iff Retryable() (H3_REQUEST_REJECTED, §4.1.1); http3.ErrGoAway (§5.2) is retried; TestRetryer_Do_H3RequestRejected_Retries / _H3ResetNonRejected_Stops / _H3GoAway_Retries / _H3RequestRejected_NonIdempotent_NoRetry / _5xxStatus_NotRetried drive the same through the Retryer loop |
| §4.1    | Conformance | TestConformance_RFC9114_Sec41_ResponseReadAfterStopSending (a server STOP_SENDING aborting the request send — ErrStreamReset — stops the send but the client still reads and returns the response on the stream's independent receive side, rather than discarding it) |
| §5.2    | Conformance | TestConformance_RFC9114_Sec52_GoAwayGatesRequests (after GOAWAY a request on a stream ≥ the id → ErrGoAway), TestConformance_RFC9114_Sec52_GoAwayMustNotIncrease (an increasing GOAWAY id → H3_ID_ERROR) |
| §7.2.6  | Conformance | TestConformance_RFC9114_Sec726_GoAwayNonRequestStreamID (a GOAWAY whose id is not a client-initiated bidirectional request stream — low two bits nonzero → H3_ID_ERROR) |
| §7.2.3  | Conformance | TestConformance_RFC9114_Sec723_CancelPushRejected (a CANCEL_PUSH — the client never sent MAX_PUSH_ID, so any push ID is out of range → H3_ID_ERROR), TestConformance_RFC9114_Sec71_CancelPushMalformed (a CANCEL_PUSH with no push-ID varint → H3_FRAME_ERROR) |
| §6.2.5  | Negative    | TestConformance_RFC9114_Sec625_PushStreamRejected (a server push stream without MAX_PUSH_ID → H3_ID_ERROR) |
| §7.1    | Unit        | TestClient_ControlFrameTooLarge (an oversized control frame → H3_EXCESSIVE_LOAD), TestClient_UniStream_PartialType (a stream-type varint split across reads is carried until complete) |
| §4.1    | Conformance | TestClient_InterimAndTrailers (the full response sequence: 1xx informational → final response → DATA → trailers, decoded into Response.Interim/Trailers), TestClient_InterimWithoutFinal (a 1xx with no final response → ErrH3Message) |
| §4.1    | Conformance | TestClient_MessageOrderErrors (DATA before the final response, DATA after a 1xx, and any frame after the trailers — all invalid frame sequences → a H3_FRAME_UNEXPECTED connection error), TestDecodeTrailers (a trailer section rejects pseudo-headers) |
| §3.1    | Unit        | TestH3TLSConfig (Dial applies the "h3" ALPN token and a TLS 1.3 floor), TestUDPConn_Loopback / TestUDPConn_ReadDeadline (UDP PacketConn adapter), TestDialConn_EstablishError (dial closes the transport on handshake failure) |
| Whole stack | Interop | TestInterop_GET / _POST / _LargeResponse / _HugeResponse / _HEAD / _StatusCodes / _SequentialReuse (build tag `interop`) — run as a matrix against **three** live servers (Caddy/quic-go, nginx/C, and aioquic/Python — a third independent QUIC + H3 transport stack; its QPACK is ls-qpack) over UDP: GET → 200 with a body, POST with a DATA-frame body → 200, 16 KiB and 1 MiB (across the key-update boundary) responses reassembled with a matching Content-Length, HEAD → empty body with a non-zero Content-Length not treated as a mismatch (§4.1/§4.1.2), 204/304/404/500 passed through with the right body shape, and 300 sequential requests reused on one connection. A loss/reorder relay (`make h3-interop-loss` / `-reorder`) checks recovery, and the `TestFault_*` suite against a deliberately misbehaving quic-go server (`make h3-interop-fault`, path-routed): a server RESET_STREAM with H3_REQUEST_REJECTED surfaces a retryable StreamResetError (§4.1.1, §8.1), and a DATA-before-HEADERS (§4.1), a SETTINGS frame on a request stream (§7.2.4), and a truncated HEADERS frame (§7.1) each raise a fatal HTTP/3 connection error rather than hanging or returning a partial response, a STOP_SENDING that aborts a 2 MiB body mid-send lets the client stop sending yet still read the response (§4.1), and a response that references the QPACK dynamic table or carries a non-zero Required Insert Count — despite the client's advertised capacity 0 — is rejected cleanly as QPACK_DECOMPRESSION_FAILED (RFC 9204 §2.2, §4.5.1) rather than mis-decoded or hung. Harness: test/integration/http3, also run in CI (integration.yml `h3-interop`, `h3-interop-faults`, `h3-interop-fault`). Validates the full path end-to-end against independent implementations. |

## Gate

`scripts/rfc-coverage-gate.sh` requires at least one passing
`TestConformance_RFC7540_*`, `TestConformance_RFC7541_*`,
`TestConformance_RFC9000_*`, `TestConformance_RFC9001_*`,
`TestConformance_RFC9002_*`, `TestConformance_RFC9204_*`, AND
`TestConformance_RFC9114_*` test, and fails on any conformance-test failure. It
is wired to the `conformance-gate` job in
`.github/workflows/conformance-gate.yml`.
