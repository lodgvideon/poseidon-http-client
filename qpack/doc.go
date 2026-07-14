// Package qpack implements QPACK field compression for HTTP/3 (RFC 9204).
//
// QPACK is the HTTP/3 counterpart to HPACK (RFC 7541): it reuses HPACK's
// prefixed-integer codec and Huffman table (see the hpack package, which this
// package depends on for both) but replaces HPACK's inline, order-dependent
// dynamic table with one built for QUIC's independently-delivered streams.
//
// This package implements both the static-table-only profile and the dynamic
// table (RFC 9204 §3.2), in both directions. A zero-value Encoder and a nil
// DynamicTable give the static-only profile: request field sections use the
// 99-entry static table (Appendix A) plus literals, response field sections
// resolve the static table or literals, every section carries Required Insert
// Count 0, and no instruction streams are exchanged — fully conformant and never
// blocking.
//
// The DynamicTable, the encoder-instruction parser and decoder-instruction
// emitter (§4.3/§4.4), and the decoder's dynamic-reference resolution let a
// client that advertises a non-zero SETTINGS_QPACK_MAX_TABLE_CAPACITY decode
// server response headers against a dynamic table. A dynamic Encoder
// (NewDynamicEncoder) additionally maintains its own dynamic table — the table
// the peer keeps as decoder — inserting repeated request-header entries and
// referencing acknowledged ones (§2.1), producing encoder-stream Insert
// instructions and consuming the peer's decoder-stream acknowledgments to track
// its Known Received Count (§2.1.4).
//
// Like frame and hpack, this is an A-layer codec: it does no networking and is
// NOT safe for concurrent use — the owning connection holds one Encoder and one
// Decoder and serializes access.
package qpack
