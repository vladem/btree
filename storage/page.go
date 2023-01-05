package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/vladem/btree/util"
)

const maxUint32 int64 = (1 << 32) - 1

func (c *tCell) GetKey() ([]byte, error) {
	return c.key, nil
}

func (c *tCell) GetValue() ([]byte, error) {
	return c.value, nil
}

func (c *tCell) GetValueAsUint32() (uint32, error) {
	value, read := binary.Varint(c.value)
	if read != len(c.value) || value < 0 || value > maxUint32 {
		return 0, fmt.Errorf("failed to parse child id, parser ret [%v/%v]", value, read)
	}
	return uint32(value), nil
}

func (p *tPage) GetId() uint32 {
	return p.id
}

func (p *tPage) IsLeaf() bool {
	return p.isLeaf
}

func (p *tPage) GetCellsCount() uint32 {
	if p.isLeaf {
		return uint32(len(p.cellOffsets))
	}
	return uint32(len(p.cellOffsets)) + 1
}

func (p *tPage) GetCell(id uint32) (ICell, error) {
	if p.isLeaf && id >= uint32(len(p.cellOffsets)) {
		return nil, fmt.Errorf("requested cell id is out of bounds, pageId [%v], requested [%v], got [%v]", p.id, id, len(p.cellOffsets))
	}
	if !p.isLeaf && id >= uint32(len(p.cellOffsets))+1 {
		return nil, fmt.Errorf("requested cell id is out of bounds, pageId [%v], requested [%v], got [%v]", p.id, id, len(p.cellOffsets)+1)
	}
	if id == uint32(len(p.cellOffsets)) {
		buf := []byte{}
		buf = binary.AppendVarint(buf, int64(p.rightMostChild))
		return &tCell{key: nil, value: buf}, nil
	}
	sOffset := p.cellOffsets[id].Start
	eOffset := p.cellOffsets[id].End
	if eOffset > uint32(len(p.raw)) || sOffset >= eOffset {
		return nil, fmt.Errorf("invalid offsets [%v/%v] for cell [%v]", sOffset, eOffset, id)
	}
	curCellData := p.raw[sOffset:eOffset]
	keyLen, keyLenLen := binary.Varint(curCellData)
	if keyLenLen <= 0 || keyLen <= 0 {
		return nil, fmt.Errorf("failed to parse key size from [%v]", curCellData)
	}
	if len(curCellData) < keyLenLen+int(keyLen) {
		return nil, fmt.Errorf("invalid cell [%v]", curCellData)
	}
	return &tCell{
		key:   curCellData[keyLenLen : keyLenLen+int(keyLen)],
		value: curCellData[keyLenLen+int(keyLen):],
	}, nil
}

func (p *tPage) calculateFreeOffsets() {
	p.freeOffsets = []tCellOffsets{}
	reserved := PageHeaderSizeBytes + (p.parent.config.MaxCellsCount * 8)
	if len(p.cellOffsets) == 0 {
		p.freeOffsets = append(p.freeOffsets, tCellOffsets{Start: reserved, End: p.parent.config.SizeBytes})
		return
	}
	cellOffsets := make([]tCellOffsets, len(p.cellOffsets))
	copy(cellOffsets, p.cellOffsets)
	sort.Slice(cellOffsets, func(i, j int) bool {
		return cellOffsets[i].Start < cellOffsets[j].Start
	})
	prevEnd := reserved
	for _, cellOffsets := range cellOffsets {
		if cellOffsets.Start != prevEnd {
			p.freeOffsets = append(p.freeOffsets, tCellOffsets{Start: prevEnd, End: cellOffsets.Start})
		}
		prevEnd = cellOffsets.End
	}
}

func (p *tPage) defragment() {
}

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
	if int64(len(encodedCell)) > maxUint32 {
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

func (p *tPage) MoveCells(dst IPage, fromId uint32) error {
	return errors.New("not implemented")
}

func (p *tPage) Flush() {
	// todo: cellOffsets should be encoded here
	log.Fatal("not implemented")
}
