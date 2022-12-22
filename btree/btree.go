package btree

import "sync"

type BTreeI interface {
	Get(key []byte) []byte
	Put(key, value []byte)
}

// not really a btree
type DummyBTreeT struct {
	data  map[string][]byte
	mutex *sync.RWMutex
}

func MakeDummyBTreeT() *DummyBTreeT {
	return &DummyBTreeT{data: make(map[string][]byte), mutex: &sync.RWMutex{}}
}

func (bt *DummyBTreeT) Get(key []byte) []byte {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	if val, ok := bt.data[string(key)]; ok {
		return val
	}
	return nil
}

func (bt *DummyBTreeT) Put(key, value []byte) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	bt.data[string(key)] = value
}
