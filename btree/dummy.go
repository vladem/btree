package btree

import "sync"

// not really a btree
type TDummyBTree struct {
	data  map[string][]byte
	mutex *sync.RWMutex
}

func (bt *TDummyBTree) Get(key []byte) ([]byte, error) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	if val, ok := bt.data[string(key)]; ok {
		return val, nil
	}
	return nil, nil
}

func (bt *TDummyBTree) Put(key, value []byte) error {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	bt.data[string(key)] = value
	return nil
}

func MakeDummyBTree() *TDummyBTree {
	return &TDummyBTree{data: make(map[string][]byte), mutex: &sync.RWMutex{}}
}
