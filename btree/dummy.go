package btree

import "sync"

// not really a btree
type TDummyBTree struct {
	data  map[string][]byte
	mutex *sync.RWMutex
}

func (bt *TDummyBTree) Get(key []byte) []byte {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	if val, ok := bt.data[string(key)]; ok {
		return val
	}
	return nil
}

func (bt *TDummyBTree) Put(key, value []byte) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	bt.data[string(key)] = value
}

func MakeDummyBTree() *TDummyBTree {
	return &TDummyBTree{data: make(map[string][]byte), mutex: &sync.RWMutex{}}
}
