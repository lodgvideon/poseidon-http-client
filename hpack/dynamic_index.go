package hpack

import (
	"bytes"
	"hash/maphash"
)

type dynamicTableIndex struct {
	seed        maphash.Seed
	newest      uint64
	byName      map[uint64]uint64
	byNameValue map[[2]uint64]uint64
}

func newDynamicTableIndex() *dynamicTableIndex {
	return &dynamicTableIndex{seed: maphash.MakeSeed()}
}

func (x *dynamicTableIndex) hashes(name, value []byte) [2]uint64 {
	return [2]uint64{maphash.Bytes(x.seed, name), maphash.Bytes(x.seed, value)}
}

func (x *dynamicTableIndex) add(name, value []byte) {
	if x.byName == nil {
		x.byName = make(map[uint64]uint64, initialEntryRing)
		x.byNameValue = make(map[[2]uint64]uint64, initialEntryRing)
	}
	x.newest++
	key := x.hashes(name, value)
	x.byName[key[0]] = x.newest
	x.byNameValue[key] = x.newest
}

func (x *dynamicTableIndex) remove(name, value []byte, count int) {
	id := x.newest - uint64(count) + 1
	key := x.hashes(name, value)
	if x.byName[key[0]] == id {
		delete(x.byName, key[0])
	}
	if x.byNameValue[key] == id {
		delete(x.byNameValue, key)
	}
}

func (x *dynamicTableIndex) clear() {
	clear(x.byName)
	clear(x.byNameValue)
	x.newest = 0
}

func (x *dynamicTableIndex) lookup(d *dynamicTable, name, value []byte) (uint64, bool) {
	nameHash := maphash.Bytes(x.seed, name)
	id, ok := x.byName[nameHash]
	if !ok {
		return 0, false
	}
	nameIndex := x.newest - id + 1
	n, v := d.at(int(nameIndex))
	if !bytes.Equal(n, name) {
		return d.lookupAfterCollision(name, value)
	}
	if bytes.Equal(v, value) {
		return nameIndex, true
	}
	key := [2]uint64{nameHash, maphash.Bytes(x.seed, value)}
	if id, ok := x.byNameValue[key]; ok {
		index := x.newest - id + 1
		n, v = d.at(int(index))
		if bytes.Equal(n, name) && bytes.Equal(v, value) {
			return index, true
		}
		return d.lookupAfterCollision(name, value)
	}
	return nameIndex, false
}

func (d *dynamicTable) lookupAfterCollision(name, value []byte) (uint64, bool) {
	var nameOnly uint64
	for i := 1; i <= d.len(); i++ {
		n, v := d.at(i)
		if !bytes.Equal(n, name) {
			continue
		}
		if bytes.Equal(v, value) {
			return uint64(i), true
		}
		if nameOnly == 0 {
			nameOnly = uint64(i)
		}
	}
	return nameOnly, false
}
