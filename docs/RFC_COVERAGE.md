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
