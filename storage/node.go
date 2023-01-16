package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

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
	return curCellData[4+keyLen:]
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
	if node.raw == nil {
		return node.writeNewNode()
	}
	return errors.New("not implemented")
}

/******************* PRIVATE *******************/
func (node *tNode) calculateFreeOffsets() {
	node.freeOffsets = []tCellOffsets{}
	reserved := pageHeaderSizeBytes + (node.parent.config.MaxCellsCount * 8)
	if !node.isLeaf {
		reserved += (node.parent.config.MaxCellsCount + 1) * 4
	}
	cellOffsets := make([]*tCellOffsets, len(node.tuples))
	for i, tuple := range node.tuples {
		cellOffsets[i] = tuple.offsets
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
	node := &tNode{id: nodeId, parent: parent, raw: raw}
	flags := node.raw[0]
	node.isLeaf = checkBit(flags, 1)
	node.tuples = make([]*tTuple, binary.BigEndian.Uint32(node.raw[1:]))
	for i := 0; i < len(node.tuples); i++ {
		sOffset := binary.BigEndian.Uint32(node.raw[pageHeaderSizeBytes+8*i:])
		eOffset := binary.BigEndian.Uint32(node.raw[pageHeaderSizeBytes+8*i+4:])
		keyLen := binary.BigEndian.Uint32(node.raw[sOffset:])
		key := node.raw[sOffset+4 : sOffset+4+keyLen]
		var value []byte
		if node.isLeaf {
			value = node.raw[sOffset+4+keyLen : eOffset]
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
			node.children[i] = binary.BigEndian.Uint32(node.raw[pageHeaderSizeBytes+8*len(node.tuples)+i*4:])
		}
	}
	return node, nil
}

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

func (node *tNode) writeNewNode() error {
	node.raw = make([]byte, node.parent.config.PageSizeBytes)
	overallLen := 0
	for _, tuple := range node.tuples {
		encodedTuple := util.EncodeCell(tuple.key, tuple.value)
		if uint32(len(encodedTuple)) > maxTupleSize(node.parent.config, node.isLeaf) {
			return fmt.Errorf("tuple max size exceeded")
		}
		tuple.offsets = &tCellOffsets{}
		tuple.offsets.End = node.parent.config.PageSizeBytes - uint32(overallLen)
		overallLen += len(encodedTuple)
		tuple.offsets.Start = node.parent.config.PageSizeBytes - uint32(overallLen)
		copy(node.raw[tuple.offsets.Start:tuple.offsets.End], encodedTuple)
	}
	headerOffsetsAndChildren := node.encodeHeaderOffsetsAndChildren()
	copy(node.raw, headerOffsetsAndChildren)
	written, err := node.parent.file.WriteAt(node.raw, int64(fileHeaderSizeBytes+node.parent.config.PageSizeBytes*node.id))
	if err != nil {
		return err
	}
	if uint32(written) != node.parent.config.PageSizeBytes {
		return errors.New("expected to write pageSizeBytes")
	}
	return nil
}

/*
func (p *tPage) AddCellBefore(key, value []byte, id uint32) error {
	if uint32(len(p.cellOffsets)) == p.parent.config.MaxCellsCount {
		return errors.New("mac cells count reached")
	}
	if id > uint32(len(p.cellOffsets)) {
		return errors.New("no such id")
	}
	if p.freeOffsets == nil {
		p.calculateFreeOffsets()
	}
	encodedCell := util.EncodeCell(key, value)
	if int64(len(encodedCell)) > int64(util.MaxUint32) {
		return errors.New("encoded cell is too big")
	}
	freeSpaceBytes := uint32(0)
	var i int
	for i = len(p.freeOffsets) - 1; i >= 0; i-- {
		intervalLen := p.freeOffsets[i].End - p.freeOffsets[i].Start
		if intervalLen >= uint32(len(encodedCell)) {
			break
		}
		freeSpaceBytes += intervalLen
	}
	if i == -1 {
		if freeSpaceBytes < uint32(len(encodedCell)) {
			return errors.New("no space left")
		}
		p.defragment()
		i = 0
	}
	newCellOffsets := tCellOffsets{
		Start: p.freeOffsets[i].End - uint32(len(encodedCell)),
		End:   p.freeOffsets[i].End,
	}
	if newCellOffsets.Start == p.freeOffsets[i].Start {
		p.freeOffsets = append(p.freeOffsets[:i], p.freeOffsets[i+1:]...)
	} else {
		p.freeOffsets[i].End = newCellOffsets.Start
	}
	copy(p.raw[newCellOffsets.Start:newCellOffsets.End], encodedCell)
	if id == uint32(len(p.cellOffsets)) {
		p.cellOffsets = append(p.cellOffsets, newCellOffsets)
		return nil
	}
	p.cellOffsets = append(p.cellOffsets[:id+1], p.cellOffsets[id:]...)
	p.cellOffsets[id] = newCellOffsets
	// todo: why not encode cellOffsets here? (put them to raw)
	return nil
}
*/
