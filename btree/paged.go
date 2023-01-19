package btree

import (
	"fmt"

	"github.com/vladem/btree/storage"
	"github.com/vladem/btree/util"
)

func (t *TPagedBTree) Get(target []byte) ([]byte, error) {
	node := t.nodeStorage.RootNode()
	for {
		if node.IsLeaf() {
			break
		}
		var i int
		for i = 0; i < node.KeyCount(); i++ {
			if util.Compare(target, node.Key(i)) == -1 {
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
		if util.Compare(target, node.Key(i)) == 0 {
			return node.Value(i), nil
		}
	}
	return nil, nil
}

func (t *TPagedBTree) Put(key, value []byte) error {
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
	fmt.Printf("%v\n", root.Id())
	return t.insertNonFull(root, key, value)
}

func MakePagedBTree(nodeStorage storage.INodeStorage, maxKeysCount uint32) *TPagedBTree {
	if maxKeysCount%2 != 1 {
		return nil
	}
	return &TPagedBTree{nodeStorage: nodeStorage, maxKeysCount: int(maxKeysCount)}
}

func (t *TPagedBTree) splitChild(parent, child storage.INode) (storage.INode, error) {
	lhs := child
	rhs, err := t.nodeStorage.AllocateNode(lhs.IsLeaf())
	if err != nil {
		return nil, err
	}
	pivotKeyIdx := lhs.KeyCount() / 2
	pivotKey := lhs.Key(pivotKeyIdx)
	i := parent.KeyCount() - 1
	for ; i >= 0 && util.Compare(pivotKey, parent.Key(i)) == -1; i-- {
	}
	i += 1
	parent.InsertKey(pivotKey, i)
	parent.InsertChild(rhs.Id(), i+1)
	if lhs.IsLeaf() {
		rhs.ReplaceKeyValues(lhs.KeyValues(pivotKeyIdx, lhs.KeyCount()))
		lhs.TruncateKeys(pivotKeyIdx)
	} else {
		rhs.ReplaceKeys(lhs.Keys(pivotKeyIdx, lhs.KeyCount()))
		lhs.TruncateKeys(pivotKeyIdx)
		rhsChildren := []uint32{util.MaxUint32}
		rhsChildren = append(rhsChildren, lhs.Children(pivotKeyIdx+1, lhs.KeyCount()+1)...)
		rhs.ReplaceChildren(rhsChildren)
		lhs.TruncateChildren(pivotKeyIdx + 1)
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
		lastCompare = util.Compare(key, node.Key(i))
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
		if util.Compare(key, pivotKey) != -1 {
			child = newChild
		}
	}
	return t.insertNonFull(child, key, value)
}
