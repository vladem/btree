package storage

import "encoding/binary"

func (p *tNode) IsLeaf() bool {
	return p.isLeaf
}

func (p *tNode) KeyCount() int {
	return len(p.offsets)
}

func (p *tNode) Id() uint32 {
	return p.id
}

// unsafe
func (p *tNode) Key(id int) []byte {
	sOffset := p.offsets[id].Start
	eOffset := p.offsets[id].End
	curCellData := p.raw[sOffset:eOffset]
	keyLen := binary.BigEndian.Uint32(curCellData[:4])
	return curCellData[4 : 4+keyLen]
}

func (p *tNode) Value(id int) []byte {
	sOffset := p.offsets[id].Start
	eOffset := p.offsets[id].End
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

func (p *tNode) InsertKey(key []byte, idx int) {
	if idx == parent.KeyCount() {
		p.Keys = append(parent.Keys, pivotKey)
	} else {
		parent.Keys = append(parent.Keys[:idx+1], parent.Keys[idx:]...)
		parent.Keys[i] = pivotKey
		parent.Children = append(parent.Children[:i+2], parent.Children[i+1:]...)
		parent.Children[i+1] = rhs.Id
	}
}

func (p *tNode) InsertChild(childId uint32, idx int) {
	if idx == len(p.children) {
		p.children = append(p.children, childId)
		return
	}
	p.children = append(p.children[:idx+1], p.children[idx:]...)
	p.children[idx] = childId
}

func (p *tNode) ReplaceKeys(keys [][]byte) {

}

func (p *tNode) ReplaceChildren(childIds []uint32) {

}

func (p *tNode) TruncateKeys(tillIdx int) {

}

func (p *tNode) TruncateChildren(tillIdx int) {

}

func (p *tNode) InsertKeyValue(key []byte, value []byte, idx int) {
	if i == len(node.Keys) {
		node.Keys = append(node.Keys, key)
		node.Values = append(node.Values, value)
	} else {
		node.Keys = append(node.Keys[:i+1], node.Keys[i:]...)
		node.Keys[i] = key
		node.Values = append(node.Values[:i+1], node.Values[i:]...)
		node.Values[i] = value
	}
}

func (p *tNode) ReplaceKeyValues(keys, values [][]byte) {

}

func (p *tNode) UpdateValue(idx int, value []byte) {

}

func (p *tNode) Save() error {

}
