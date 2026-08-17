package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRecycleDataLocked_ClearsAliasingBuf pins that returning the pooled DATA
// slab also drops buf.
//
// buf aliases that slab. Read's closed gate already stops a caller reaching buf
// after Close, so this is not what makes Read safe — it is what stops a dangling
// alias to pooled memory outliving the slab at all, which is the failure class
// this whole change is about. Asserted directly because no black-box test can
// see it.
func TestRecycleDataLocked_ClearsAliasingBuf(t *testing.T) {
	slab := getDataSlab()
	*slab = append((*slab)[:0], "payload"...)
	r := &responseBodyReader{curData: slab, buf: (*slab)[3:]}

	r.recycleDataLocked()

	assert.True(t, r.curData == nil, "curData not cleared after recycle")
	assert.Truef(t, r.buf == nil,
		"buf still aliases the recycled slab (%d bytes) — the alias outlived the buffer", len(r.buf))
}
