package storage

import (
	"encoding/binary"
	"errors"

	"github.com/vladem/btree/util"
)

/******************* PUBLIC *******************/
func (p *tNode) IsLeaf() bool {
	return p.isLeaf
}

func (p *tNode) KeyCount() int {
	return len(p.tuples)
}

func (p *tNode) Id() uint32 {
	return p.id
}

// unsafe
func (p *tNode) Key(id int) []byte {
	sOffset := p.tuples[id].offsets.Start
	eOffset := p.tuples[id].offsets.End
	curCellData := p.raw[sOffset:eOffset]
	keyLen := binary.BigEndian.Uint32(curCellData[:4])
	return curCellData[4 : 4+keyLen]
}

func (p *tNode) Value(id int) []byte {
	sOffset := p.tuples[id].offsets.Start
	eOffset := p.tuples[id].offsets.End
	curCellData := p.raw[sOffset:eOffset]
	keyLen := binary.BigEndian.Uint32(curCellData[:4])
	return curCellData[4+keyLen : 0]
}

func (p *tNode) Keys(idStart, idEnd int) [][]byte {
	keys := [][]byte{}
	for i := idStart; i < idEnd; i++ {
		keys = append(keys, p.Key(i))
	}
	return keys
}

func (p *tNode) Child(idx int) uint32 {
	return p.children[idx]
}

func (p *tNode) Children(idStart, idEnd int) []uint32 {
	res := make([]uint32, idEnd-idStart)
	copy(res, p.children[idStart:idEnd])
	return res
}

func (p *tNode) KeyValues(idStart, idEnd int) ([][]byte, [][]byte) {
	values := [][]byte{}
	for i := idStart; i < idEnd; i++ {
		values = append(values, p.Value(i))
	}
	return p.Keys(idStart, idEnd), values
}

func (node *tNode) InsertKey(key []byte, idx int) {
	node.InsertKeyValue(key, nil, idx)
}

func (p *tNode) InsertChild(childId uint32, idx int) {
	if idx == len(p.children) {
		p.children = append(p.children, childId)
		return
	}
	p.children = append(p.children[:idx+1], p.children[idx:]...)
	p.children[idx] = childId
}

func (node *tNode) ReplaceKeys(keys [][]byte) {
	node.tuples = []*tTuple{}
	for _, key := range keys {
		node.tuples = append(node.tuples, makeTuple(key, nil))
	}
}

func (node *tNode) ReplaceChildren(childIds []uint32) {
	node.children = childIds
}

func (node *tNode) TruncateKeys(tillIdx int) {
	node.tuples = node.tuples[:tillIdx]
}

func (node *tNode) TruncateChildren(tillIdx int) {
	node.children = node.children[:tillIdx]
}

func (node *tNode) InsertKeyValue(key []byte, value []byte, idx int) {
	tuple := makeTuple(key, value)
	if idx == node.KeyCount() {
		node.tuples = append(node.tuples, tuple)
		return
	}
	node.tuples = append(node.tuples[:idx+1], node.tuples[idx:]...)
	node.tuples[idx] = tuple
}

func (node *tNode) ReplaceKeyValues(keys, values [][]byte) {
	node.tuples = []*tTuple{}
	for i, key := range keys {
		node.tuples = append(node.tuples, makeTuple(key, values[i]))
	}
}

func (node *tNode) UpdateValue(idx int, value []byte) {
	node.tuples[idx].offsets = nil
	node.tuples[idx].value = value
}

func (node *tNode) Save() error {
	if node.parent.file == nil {
		return errors.New("already closed")
	}
	encodedTuples := make([][]byte, len(node.tuples))
	for i, tuple := range node.tuples {
		// stopped here
		// need to encode t
		encodedTuples[i] = util.EncodeCell(tuple.key, tuple.value)
	}
}

/******************* PRIVATE *******************/
func makeNode(nodeId uint32, isLeaf bool, parent *tOnDiskNodeStorage) *tNode {
	var children []uint32
	if !isLeaf {
		children = []uint32{}
	}
	return &tNode{
		id:       nodeId,
		isLeaf:   isLeaf,
		children: children,
		parent:   parent,
		tuples:   []*tTuple{},
	}
}

func makeTuple(key, value []byte) *tTuple {
	return &tTuple{
		key:   key,
		value: value,
	}
}

func (node *tNode) defragment() error {
	return errors.New("not implemented")
}
