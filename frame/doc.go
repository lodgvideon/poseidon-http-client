// Package frame implements the HTTP/2 framing layer (RFC 7540) without
// any networking. It provides a Framer that reads frames via a Handler
// visitor (zero-copy, caller-owned scratch) and writes frames via explicit
// per-type methods. Framer is NOT goroutine-safe, with one documented
// exception for concurrent read and write halves — see Framer.
//
// The wire vocabulary names itself: FrameType, Flags, ErrCode, SettingID and
// FrameHeader all render as their RFC 7540 §11 registry names, which is what
// makes both an error message and a frame log readable.
//
// Framer.SetTracer installs a Tracer that observes every frame in both
// directions, including the ones no Handler ever sees — an unknown extension
// type §5.5 obliges the codec to drop, or the frame that trips a §6.10
// field-block-continuity teardown. A nil Tracer, the default, costs one nil
// compare per frame; an installed one allocates nothing. See the trace package
// for a ready-made text implementation.
package frame
