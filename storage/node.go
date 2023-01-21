package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
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

func (node *tNode) Key(id int) []byte {
	return node.tuples[id].key
}

func (node *tNode) Value(id int) []byte {
	return node.tuples[id].value
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
	recalculateFree := (node.tuples[idx].offsets != nil)
	node.tuples[idx].offsets = nil
	if recalculateFree {
		node.calculateFreeOffsets()
	}
	node.tuples[idx].value = value
}

func (node *tNode) Save() error {
	if node.parent.file == nil {
		return errors.New("already closed")
	}
	defragment := false
	newTuples := []*tTuple{}
	encoded := [][]byte{}
	for _, tuple := range node.tuples {
		if tuple.offsets != nil {
			continue
		}
		encodedTuple := encodeTuple(tuple.key, tuple.value)
		newTuples = append(newTuples, tuple)
		encoded = append(encoded, encodedTuple)
		var i int
		for i = len(node.freeOffsets) - 1; i >= 0; i-- {
			intervalLen := node.freeOffsets[i].End - node.freeOffsets[i].Start
			if intervalLen >= uint32(len(encodedTuple)) {
				break
			}
		}
		if i == -1 {
			defragment = true
			break
		}
		newCellOffsets := tCellOffsets{
			Start: node.freeOffsets[i].End - uint32(len(encodedTuple)),
			End:   node.freeOffsets[i].End,
		}
		if newCellOffsets.Start == node.freeOffsets[i].Start {
			node.freeOffsets = append(node.freeOffsets[:i], node.freeOffsets[i+1:]...)
		} else {
			node.freeOffsets[i].End = newCellOffsets.Start
		}
		tuple.offsets = &newCellOffsets
	}
	if defragment {
		return node.defragment()
	}
	for i, tuple := range newTuples {
		if err := node.parent.writeAt(encoded[i], int64(fileHeaderSizeBytes+node.parent.config.PageSizeBytes*node.id+tuple.offsets.Start)); err != nil {
			return err
		}
	}
	return node.parent.writeAt(node.encodeHeaderOffsetsAndChildren(), int64(fileHeaderSizeBytes+node.parent.config.PageSizeBytes*node.id))
}

/******************* PRIVATE *******************/
func encodeTuple(key, value []byte) []byte {
	cell := []byte{}
	cell = binary.BigEndian.AppendUint32(cell, uint32(len(key)))
	cell = append(cell, key...)
	if value != nil {
		cell = append(cell, value...)
	}
	return cell
}

func (node *tNode) calculateFreeOffsets() {
	node.freeOffsets = []tCellOffsets{}
	reserved := pageHeaderSizeBytes + (node.parent.config.MaxCellsCount * 8)
	if !node.isLeaf {
		reserved += (node.parent.config.MaxCellsCount + 1) * 4
	}
	cellOffsets := []*tCellOffsets{}
	for _, tuple := range node.tuples {
		if tuple.offsets != nil {
			cellOffsets = append(cellOffsets, tuple.offsets)
		}
	}
	sort.Slice(cellOffsets, func(i, j int) bool {
		return cellOffsets[i].Start < cellOffsets[j].Start
	})
	prevEnd := reserved
	for _, cellOffsets := range cellOffsets {
		if cellOffsets.Start != prevEnd {
			node.freeOffsets = append(node.freeOffsets, tCellOffsets{Start: prevEnd, End: cellOffsets.Start})
		}
		prevEnd = cellOffsets.End
	}
	if prevEnd != node.parent.config.PageSizeBytes {
		node.freeOffsets = append(node.freeOffsets, tCellOffsets{Start: prevEnd, End: node.parent.config.PageSizeBytes})
	}
}

func makeNodeFromRaw(nodeId uint32, raw []byte, parent *tOnDiskNodeStorage) (*tNode, error) {
	node := &tNode{id: nodeId, parent: parent}
	flags := raw[0]
	node.isLeaf = checkBit(flags, 1)
	node.tuples = make([]*tTuple, binary.BigEndian.Uint32(raw[1:]))
	for i := 0; i < len(node.tuples); i++ {
		sOffset := binary.BigEndian.Uint32(raw[pageHeaderSizeBytes+8*i:])
		eOffset := binary.BigEndian.Uint32(raw[pageHeaderSizeBytes+8*i+4:])
		keyLen := binary.BigEndian.Uint32(raw[sOffset:])
		key := raw[sOffset+4 : sOffset+4+keyLen]
		var value []byte
		if node.isLeaf {
			value = raw[sOffset+4+keyLen : eOffset]
		}
		node.tuples[i] = &tTuple{
			key:   key,
			value: value,
			offsets: &tCellOffsets{
				Start: sOffset,
				End:   eOffset,
			},
		}
	}
	node.calculateFreeOffsets()
	if !node.isLeaf {
		node.children = make([]uint32, len(node.tuples)+1)
		for i := 0; i < len(node.tuples)+1; i++ {
			node.children[i] = binary.BigEndian.Uint32(raw[pageHeaderSizeBytes+8*len(node.tuples)+i*4:])
		}
	}
	return node, nil
}

func makeNode(nodeId uint32, isLeaf bool, parent *tOnDiskNodeStorage) *tNode {
	var children []uint32
	if !isLeaf {
		children = []uint32{}
	}
	node := &tNode{
		id:       nodeId,
		isLeaf:   isLeaf,
		children: children,
		parent:   parent,
		tuples:   []*tTuple{},
	}
	node.calculateFreeOffsets()
	return node
}

func makeTuple(key, value []byte) *tTuple {
	return &tTuple{
		key:   key,
		value: value,
	}
}

func setBit(flags byte, idx int) byte {
	var mask byte = 1
	mask = mask << (7 - idx)
	return flags | mask
}

func (node *tNode) encodeHeaderOffsetsAndChildren() []byte {
	var flags byte = 0
	flags = setBit(flags, 0) // allocated
	if node.isLeaf {
		flags = setBit(flags, 1)
	}
	buf := []byte{flags}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(node.tuples)))
	for _, tuple := range node.tuples {
		buf = binary.BigEndian.AppendUint32(buf, tuple.offsets.Start)
		buf = binary.BigEndian.AppendUint32(buf, tuple.offsets.End)
	}
	if node.isLeaf {
		return buf
	}
	for _, child := range node.children {
		buf = binary.BigEndian.AppendUint32(buf, child)
	}
	return buf
}

func maxTupleSize(config TConfig, isLeaf bool) uint32 {
	reserved := pageHeaderSizeBytes + config.MaxCellsCount*8
	if !isLeaf {
		reserved += (config.MaxCellsCount + 1) * 4
	}
	dataSpace := config.PageSizeBytes - reserved
	return uint32(dataSpace / config.MaxCellsCount)
}

func (node *tNode) defragment() error {
	overallLen := 0
	encoded := make([][]byte, len(node.tuples))
	for i, tuple := range node.tuples {
		encoded[i] = encodeTuple(tuple.key, tuple.value)
		if uint32(len(encoded[i])) > maxTupleSize(node.parent.config, node.isLeaf) {
			return fmt.Errorf("tuple max size exceeded")
		}
		tuple.offsets = &tCellOffsets{}
		tuple.offsets.End = node.parent.config.PageSizeBytes - uint32(overallLen)
		overallLen += len(encoded[i])
		tuple.offsets.Start = node.parent.config.PageSizeBytes - uint32(overallLen)
	}
	allEncoded := []byte{}
	for i := len(encoded) - 1; i >= 0; i-- {
		allEncoded = append(allEncoded, encoded[i]...)
	}
	node.parent.writeAt(allEncoded, int64(fileHeaderSizeBytes+node.parent.config.PageSizeBytes*(node.id+1)-uint32(len(allEncoded))))
	return node.parent.writeAt(node.encodeHeaderOffsetsAndChildren(), int64(fileHeaderSizeBytes+node.parent.config.PageSizeBytes*node.id))
}
