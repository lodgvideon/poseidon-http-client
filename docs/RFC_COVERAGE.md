# RFC Coverage Matrix

| Section | RFC      | Test |
|---------|----------|------|
| §4.1    | RFC 7540 | TestReadFrameHeader_Sample, TestWriteFrameHeader |
| §6.1    | RFC 7540 | TestFramer_Data_Roundtrip, TestFramer_DataPadded_Roundtrip |
| §6.2    | RFC 7540 | TestFramer_Headers_RoundTrip, TestFramer_HeadersWithPriority_RoundTrip, TestFramer_HeadersPadded_RoundTrip |
| §6.3    | RFC 7540 | TestFramer_Priority_RoundTrip |
| §6.4    | RFC 7540 | TestFramer_RSTStream_RoundTrip |
| §6.5    | RFC 7540 | TestFramer_Settings_RoundTrip, TestFramer_SettingsAck_RoundTrip |
| §6.6    | RFC 7540 | TestFramer_PushPromise_RoundTrip |
| §6.7    | RFC 7540 | TestFramer_Ping_RoundTrip |
| §6.8    | RFC 7540 | TestFramer_GoAway_RoundTrip |
| §6.9    | RFC 7540 | TestFramer_WindowUpdate_RoundTrip, TestFramer_WindowUpdate_ZeroIncrementRejected |
| §6.10   | RFC 7540 | TestFramer_Continuation_RoundTrip |
| §3.5    | RFC 7540 | TestFramer_ClientPreface |
| §5.1    | RFC 7541 | TestEncodeInteger_RFCExamples, TestDecodeInteger_RFCExamples, TestDecodeInteger_Truncated, TestDecodeInteger_Overflow |
| §5.2    | RFC 7541 | TestEncodeStringLiteral_*, TestDecodeStringLiteral_*, TestHuffmanEncode_*, TestHuffmanDecode_* |
| §6.1    | RFC 7541 | TestConformance_RFC7541_C2_4_Indexed |
| §6.2.1  | RFC 7541 | TestConformance_RFC7541_C2_1_LiteralIndexing |
| §6.2.2  | RFC 7541 | TestConformance_RFC7541_C2_2_LiteralNoIndexing |
| §6.2.3  | RFC 7541 | TestConformance_RFC7541_C2_3_NeverIndexed |
| §C.3.1  | RFC 7541 | TestConformance_RFC7541_C3_1_FirstRequest |
| §C.4.1  | RFC 7541 | TestConformance_RFC7541_C4_1_FirstRequestHuffman |
