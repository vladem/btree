package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

func (node *tNode) Key(id int) io.Reader {
	return &tSliceReader{data: node.tuples[id].key}
}

func (node *tNode) KeyFull(id int) ([]byte, error) {
	key := []byte{}
	buf := make([]byte, 1024)
	reader := node.Key(id)
	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		key = append(key, buf[:n]...)
		if err == io.EOF {
			break
		}
	}
	return key, nil
}

func (node *tNode) Value(id int) []byte {
	return node.tuples[id].value
}

func (p *tNode) Child(idx int) uint32 {
	return p.children[idx]
}

func (p *tNode) Children(idStart, idEnd int) []uint32 {
	res := make([]uint32, idEnd-idStart)
	copy(res, p.children[idStart:idEnd])
	return res
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

func (lhs *tNode) SplitAt(pivotKeyIdx int) (INode, error) {
	rhsChildren := []uint32{InvalidNodeId}
	if !lhs.IsLeaf() {
		rhsChildren = append(rhsChildren, lhs.children[pivotKeyIdx+1:]...)
		lhs.children = lhs.children[:pivotKeyIdx+1]
	}
	rhs, err := lhs.parent.allocateNode(lhs.IsLeaf(), rhsChildren)
	if err != nil {
		return nil, err
	}
	rhsCasted, ok := rhs.(*tNode)
	if !ok {
		return nil, errors.New("downcast failed")
	}
	rhsCasted.tuples = lhs.tuples[pivotKeyIdx:]
	lhs.tuples = lhs.tuples[:pivotKeyIdx]
	for _, tuple := range rhsCasted.tuples {
		tuple.offsets = nil
	}
	return rhs, nil
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

func (node *tNode) UpdateValue(idx int, value []byte) {
	if node.tuples[idx].offsets != nil {
		node.tuples[idx].offsets = nil
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
func (r *tSliceReader) Read(buf []byte) (n int, err error) {
	n = copy(buf, r.data[r.curPos:])
	r.curPos += n
	if r.curPos == len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

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
