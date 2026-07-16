package client

import "github.com/lodgvideon/poseidon-http-client/conn"

// putDataSlab returns a DATA-frame payload buffer to conn's pool. conn Gets the
// buffer in OnData and transfers ownership to this package via
// StreamEvent.DataSlab; this is the single return site every owner in the
// client funnels through — handleDataEvent (BodyBuffer), responseBodyReader
// .recycleData (BodyStream) and StreamResponse.recycleData (DoStream). It is
// the data-slab twin of recycleHeaderSlab, and it is what makes the invariant
// stated in conn/stream.go — "exactly one return site per buffer ... rules out
// a double-Put" — auditable from one place rather than asserted in prose.
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
