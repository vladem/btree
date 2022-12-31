package btree

import (
	"log"

	"github.com/vladem/btree/storage"
	"github.com/vladem/btree/util"
)

type TPagedBTree struct {
	reader storage.IPageReader
}

func (t *TPagedBTree) Get(target []byte) []byte {
	err := t.reader.Rewind()
	if err != nil {
		log.Fatalf("rewind failed")
	}
	lastComparison := int8(-1)
	for {
		for {
			cell, err := t.reader.NextCell()
			if err != nil {
				log.Fatalf("failed to read next cell with error: [%v]", err)
			}
			lastComparison = util.Compare(target, cell.GetKey())
			// searching for first cell, which key is gte target
			if lastComparison < 1 || cell.IsLast() {
				break
			}
		}
		if !t.reader.IsLeaf() {
			break
		}
		err = t.reader.NextPage()
		if err != nil {
			log.Fatalf("failed to read next page with error: [%v]", err)
		}
	}
	if lastComparison != 0 {
		return nil
	}
	return t.reader.CurrentCell().GetValue()
}

func (t *TPagedBTree) Put(key, value []byte) {
	log.Fatalf("not implemented")
}

func MakePagedBTree(reader storage.IPageReader) *TPagedBTree {
	return &TPagedBTree{reader: reader}
}
