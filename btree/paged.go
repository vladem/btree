package btree

import (
	"io"
	"sync"

	"github.com/vladem/btree/storage"
)

const chunkSize = 1024

/******************* PUBLIC *******************/
func (t *TPagedBTree) Get(target []byte) ([]byte, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	node := t.nodeStorage.RootNode()
	for {
		if node.IsLeaf() {
			break
		}
		var i int
		for i = 0; i < node.KeyCount(); i++ {
			rel, err := compare(target, node.Key(i), chunkSize)
			if err != nil {
				return nil, err
			}
			if rel == -1 {
				break
			}
		}
		var err error
		node, err = t.nodeStorage.LoadNode(node.Child(i))
		if err != nil {
			return nil, err
		}
	}
	for i := 0; i < node.KeyCount(); i++ {
		rel, err := compare(target, node.Key(i), chunkSize)
		if err != nil {
			return nil, err
		}
		if rel == 0 {
			return node.Value(i), nil
		}
	}
	return nil, nil
}

func (t *TPagedBTree) Put(key, value []byte) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	root := t.nodeStorage.RootNode()
	if root.KeyCount() == t.maxKeysCount {
		newRoot, err := t.nodeStorage.AllocateRootNode()
		if err != nil {
			return err
		}
		if _, err := t.splitChild(newRoot, root); err != nil {
			return err
		}
		root = newRoot
	}
	return t.insertNonFull(root, key, value)
}

func MakePagedBTree(nodeStorage storage.INodeStorage, maxKeysCount uint32) *TPagedBTree {
	if maxKeysCount%2 != 1 {
		return nil
	}
	return &TPagedBTree{nodeStorage: nodeStorage, maxKeysCount: int(maxKeysCount), mutex: &sync.Mutex{}}
}

/******************* PRIVATE *******************/
/*
Compares two byte arrays, returns integer:
if lhs < rhs:	-1
if lhs == rhs:	0
if lhs > rhs:	1
*/
func compare(lhs []byte, rhs io.Reader, chunkSize int) (int8, error) {
	var (
		lhsIdx = 0
	)
	buf := make([]byte, chunkSize)
	for {
		n, err := rhs.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		i := 0
		for ; i < n && lhsIdx < len(lhs) && lhs[lhsIdx] == buf[i]; i, lhsIdx = i+1, lhsIdx+1 {
		}
		if i == n && err != io.EOF {
			continue
		}
		if i < n && lhsIdx < len(lhs) {
			res := int8(-1)
			if lhs[lhsIdx] > buf[i] {
				res = 1
			}
			return res, nil
		}
		res := int8(0)
		if i < n {
			res = -1
		}
		if lhsIdx < len(lhs) {
			res = 1
		}
		return res, nil
	}
}

func (t *TPagedBTree) splitChild(parent, child storage.INode) (storage.INode, error) {
	lhs := child
	pivotKeyIdx := lhs.KeyCount() / 2
	pivotKey, err := lhs.KeyFull(pivotKeyIdx)
	if err != nil {
		return nil, err
	}
	i := parent.KeyCount() - 1
	for {
		if i < 0 {
			break
		}
		rel, err := compare(pivotKey, parent.Key(i), chunkSize)
		if err != nil {
			return nil, err
		}
		if rel > -1 {
			break
		}
		i -= 1
	}
	i += 1
	rhs, err := lhs.SplitAt(pivotKeyIdx)
	parent.InsertKey(pivotKey, i)
	parent.InsertChild(rhs.Id(), i+1)
	if err != nil {
		return nil, err
	}
	if err := parent.Save(); err != nil {
		return nil, err
	}
	if err := lhs.Save(); err != nil {
		return nil, err
	}
	if err := rhs.Save(); err != nil {
		return nil, err
	}
	return rhs, nil
}

func (t *TPagedBTree) insertNonFull(node storage.INode, key, value []byte) error {
	i := node.KeyCount() - 1
	lastCompare := int8(1)
	for ; i >= 0; i-- {
		var err error
		lastCompare, err = compare(key, node.Key(i), chunkSize)
		if err != nil {
			return err
		}
		if lastCompare != -1 {
			break
		}
	}
	if node.IsLeaf() && lastCompare == 0 {
		node.UpdateValue(i, value)
		return node.Save()
	}
	i += 1
	if node.IsLeaf() {
		node.InsertKeyValue(key, value, i)
		return node.Save()
	}
	child, err := t.nodeStorage.LoadNode(node.Child(i))
	if err != nil {
		return err
	}
	if child.KeyCount() == t.maxKeysCount {
		pivotKey := child.Key(child.KeyCount() / 2)
		newChild, err := t.splitChild(node, child)
		if err != nil {
			return err
		}
		rel, err := compare(key, pivotKey, chunkSize)
		if err != nil {
			return err
		}
		if rel != -1 {
			child = newChild
		}
	}
	return t.insertNonFull(child, key, value)
}
