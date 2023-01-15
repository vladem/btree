package btree

import (
	"errors"

	"github.com/vladem/btree/storage"
	"github.com/vladem/btree/util"
)

func (t *TPagedBTree) Get(target []byte) ([]byte, error) {
	/*
		lastComparison := int8(-1)
		page, err := t.reader.Read(0)
		if err != nil {
			return nil, err
		}
		var cell storage.ICell
		for {
			for i := uint32(0); i < page.GetCellsCount(); i++ {
				cell, err = page.GetCell(i)
				if err != nil {
					return nil, fmt.Errorf("failed to read next cell with error [%v], cellId [%v]", err, i)
				}
				key, err := cell.GetKey()
				if err != nil {
					return nil, fmt.Errorf("failed to read next cell with error [%v], cellId [%v]", err, i)
				}
				if key != nil {
					lastComparison = util.Compare(target, key)
				} else {
					lastComparison = -1
				}
				// searching for first cell, which key is gte target
				if lastComparison < 1 {
					break
				}
			}
			if page.IsLeaf() {
				break
			}
			nextPageId, err := cell.GetValueAsUint32()
			if err != nil {
				return nil, err
			}
			page, err = t.reader.Read(nextPageId)
			if err != nil {
				return nil, err
			}
		}
		if lastComparison != 0 {
			return nil, nil
		}
		return cell.GetValue()
	*/
	return nil, errors.New("not implemented")
}

func (t *TPagedBTree) Put(key, value []byte) error {
	root := t.nodeStorage.RootNode()
	if root.KeyCount() == t.maxKeysCount {
		newRoot, err := t.nodeStorage.AllocateRootNode()
		if err != nil {
			return err
		}
		if err := t.splitChild(newRoot, root); err != nil {
			return err
		}
	}
	return t.insertNonFull(root, key, value)
}

func MakePagedBTree(nodeStorage storage.INodeStorage, maxKeysCount int) *TPagedBTree {
	if maxKeysCount%2 != 1 {
		return nil
	}
	return &TPagedBTree{nodeStorage: nodeStorage, maxKeysCount: maxKeysCount}
}

func (t *TPagedBTree) splitChild(parent, child storage.INode) error {
	lhs := child
	rhs, err := t.nodeStorage.AllocateNode(lhs.IsLeaf())
	if err != nil {
		return err
	}
	pivotKeyIdx := lhs.KeyCount() / 2
	pivotKey := lhs.Key(pivotKeyIdx)
	i := parent.KeyCount() - 1
	for ; i >= 0 && util.Compare(pivotKey, parent.Key(i)) == -1; i-- {
	}
	i += 1
	parent.InsertKey(pivotKey, i)
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
		return err
	}
	if err := lhs.Save(); err != nil {
		return err
	}
	if err := rhs.Save(); err != nil {
		return err
	}
	return nil
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
		if err := t.splitChild(node, child); err != nil {
			return err
		}
	}
	return t.insertNonFull(child, key, value)
}
