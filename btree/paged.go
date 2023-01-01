package btree

import (
	"fmt"
	"log"

	"github.com/vladem/btree/storage"
	"github.com/vladem/btree/util"
)

type TPagedBTree struct {
	reader storage.IPageReader
}

func (t *TPagedBTree) Get(target []byte) ([]byte, error) {
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
}

func (t *TPagedBTree) Put(key, value []byte) {
	log.Fatalf("not implemented")
}

func MakePagedBTree(reader storage.IPageReader) *TPagedBTree {
	return &TPagedBTree{reader: reader}
}
