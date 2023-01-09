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
	root, err := t.nodeStorage.RootNode()
	if err != nil {
		return err
	}
	if len(root.Children) == t.maxKeysCount {
		newRoot, err := t.nodeStorage.AllocateNode(false)
		if err != nil {
			return err
		}
		if err := t.nodeStorage.SetRootNode(newRoot); err != nil {
			return err
		}
		newRoot.Children = append(newRoot.Children, root.Id)
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

func (t *TPagedBTree) splitChild(parent, child *storage.TNode) error {
	lhs := child
	rhs, err := t.nodeStorage.AllocateNode(lhs.IsLeaf)
	if err != nil {
		return err
	}
	pivotKeyIdx := len(lhs.Keys) / 2
	pivotKey := lhs.Keys[pivotKeyIdx]
	i := len(parent.Keys) - 1
	for ; i >= 0 && util.Compare(pivotKey, parent.Keys[i]) == -1; i-- {
	}
	i += 1
	if i == len(parent.Keys) {
		parent.Keys = append(parent.Keys, pivotKey)
		parent.Children = append(parent.Children, rhs.Id)
	} else {
		parent.Keys = append(parent.Keys[:i+1], parent.Keys[i:]...)
		parent.Keys[i] = pivotKey
		parent.Children = append(parent.Children[:i+2], parent.Children[i+1:]...)
		parent.Children[i+1] = rhs.Id
	}
	rhs.Keys = append(rhs.Keys, lhs.Keys[pivotKeyIdx:]...)
	lhs.Keys = lhs.Keys[:pivotKeyIdx]
	if !lhs.IsLeaf {
		rhs.Children = append(rhs.Children, util.MaxUint32)
		rhs.Children = append(rhs.Children, lhs.Children[pivotKeyIdx+1:]...)
		lhs.Children = lhs.Children[:pivotKeyIdx+1]
	}
	// todo move values too
	if err := t.nodeStorage.SaveNode(parent); err != nil {
		return err
	}
	if err := t.nodeStorage.SaveNode(lhs); err != nil {
		return err
	}
	if err := t.nodeStorage.SaveNode(rhs); err != nil {
		return err
	}
	return nil
}

func (t *TPagedBTree) insertNonFull(node *storage.TNode, key, value []byte) error {
	i := len(node.Keys) - 1
	lastCompare := int8(1)
	for ; i >= 0; i-- {
		lastCompare = util.Compare(key, node.Keys[i])
		if lastCompare != -1 {
			break
		}
	}
	if node.IsLeaf && lastCompare == 0 {
		node.Values[i] = value
		return t.nodeStorage.SaveNode(node)
	}
	i += 1
	if node.IsLeaf {
		if i == len(node.Keys) {
			node.Keys = append(node.Keys, key)
			node.Values = append(node.Values, value)
		} else {
			node.Keys = append(node.Keys[:i+1], node.Keys[i:]...)
			node.Keys[i] = key
			node.Values = append(node.Values[:i+1], node.Values[i:]...)
			node.Values[i] = value
		}
		return t.nodeStorage.SaveNode(node)
	}
	child, err := t.nodeStorage.LoadNode(node.Children[i])
	if err != nil {
		return err
	}
	if len(child.Children) == t.maxKeysCount {
		if err := t.splitChild(node, child); err != nil {
			return err
		}
	}
	return t.insertNonFull(child, key, value)
}
