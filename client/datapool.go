package client

import "github.com/lodgvideon/poseidon-http-client/conn"

// getDataSlab takes a DATA payload buffer from conn's pool. There are two
// Getters overall, on different sides of the package boundary: conn's OnData
// Gets one per HTTP/2 DATA frame and transfers it here via
// StreamEvent.DataSlab, and h1Exchange.Recv — which has no framing layer below
// it to do that — Gets its own, through this function.
//
// That second Getter is why this exists as a tapped var rather than an inline
// conn.GetDataBufPool().Get(). The ownership ledger in
// dataslab_ownership_test.go originally inferred Gets from DELIVERIES, which is
// sound only while every Get is followed by a delivery — true of OnData, false
// of Recv, whose ReadBodyChunk error path Gets a buffer and returns it without
// ever delivering an event. Under a delivery-inferred ledger that correct Put
// scores as a double-Put (the pool reuses pointers, so the slab has a delivery
// on record from an earlier chunk), and an H1 buffer that leaked would balance
// and pass. Tapping the real Get removes the inference entirely.
var getDataSlab = func() *[]byte {
	return conn.GetDataBufPool().Get().(*[]byte)
}

// putDataSlab returns a DATA payload buffer to conn's pool. It is the single
// return site every owner in the client funnels through — handleDataEvent
// (BodyBuffer), responseBodyReader.recycleData (BodyStream),
// StreamResponse.recycleData (DoStream), and h1Exchange.Recv's own error path,
// which owns a buffer it never delivered. It is the data-slab twin of
// recycleHeaderSlab, and it is what makes the invariant stated in
// conn/stream.go — "exactly one return site per buffer ... rules out a
// double-Put" — auditable from one place rather than asserted in prose.
//
// It is a var so tests can interpose per-pointer Get/Put accounting:
// sync.Pool accepts the same pointer twice without complaint, and -race cannot
// see a double-Put either, because there is no data race — one buffer simply
// ends up owned by two live consumers, and the corruption surfaces later as
// one request's body appearing inside another's. Production always runs the
// default closure below. Callers nil-check before calling.
var putDataSlab = func(slab *[]byte) {
	conn.GetDataBufPool().Put(slab)
}
