// Package qpack implements QPACK field compression for HTTP/3 (RFC 9204).
//
// QPACK is the HTTP/3 counterpart to HPACK (RFC 7541): it reuses HPACK's
// prefixed-integer codec and Huffman table (see the hpack package, which this
// package depends on for both) but replaces HPACK's inline, order-dependent
// dynamic table with one built for QUIC's independently-delivered streams.
//
// This package currently implements the static-table-only profile: the
// Encoder compresses request field sections using the 99-entry static table
// (Appendix A) plus literals, and the Decoder reads response field sections
// that reference the static table or use literals. A client advertising
// SETTINGS_QPACK_MAX_TABLE_CAPACITY=0 and SETTINGS_QPACK_BLOCKED_STREAMS=0
// forbids its peer from using the dynamic table or head-of-line blocking, so
// this profile is fully conformant and never blocks. The dynamic table and the
// encoder/decoder instruction streams (RFC 9204 §4.3, §4.4) are a later phase.
//
// Like frame and hpack, this is an A-layer codec: it does no networking and is
// NOT safe for concurrent use — the owning connection holds one Encoder and one
// Decoder and serializes access.
package qpack
