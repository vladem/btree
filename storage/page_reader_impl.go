package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

type TPageMeta struct {
	// inferred from the page offset
	Id uint32 // with 4kb pages we've got maximum of 16gb per file

	// next fields are stored in page header (totally 9 bytes)
	Version  uint32 // page layout version
	IsLeaf   bool
	CellsCnt uint32 // occupied cells counter
}

type TCellOffsets struct {
	Start uint32 // bytes from the end of the page
	End   uint32 // bytes from the end of the page
}

type TPageReader struct {
	Config          TPageConfig
	CurrentPageMeta *TPageMeta

	file          *os.File
	currentPage   []byte
	cells         []TCellOffsets
	currentCellId uint32 // enumeration starts from 1
}

type TCellReader struct {
	key    []byte
	value  []byte
	isLast bool
}

func (r *TCellReader) GetKey() []byte {
	return r.key
}

func (r *TCellReader) GetValue() []byte {
	return r.value
}

func (r *TCellReader) IsLast() bool {
	return r.isLast
}

func parsePageMetaAndOffsets(id uint32, page []byte) (*TPageMeta, []TCellOffsets, error) {
	var (
		version  int64
		isLeaf   byte
		cellsCnt int64
		read     int
	)
	version, read = binary.Varint(page)
	if read <= 0 {
		return nil, nil, fmt.Errorf("failed to parse meta, version varint, ret [%v]", read)
	}
	page = page[read:]
	if len(page) == 0 {
		return nil, nil, fmt.Errorf("failed to parse meta, node type")
	}
	isLeaf = page[0]
	page = page[1:]
	cellsCnt, read = binary.Varint(page)
	if read <= 0 {
		return nil, nil, fmt.Errorf("failed to parse meta, version varint, ret [%v]", read)
	}
	page = page[read:]
	offsets := []TCellOffsets{}
	for i := 0; i < int(cellsCnt); i++ {
		var sOffset, eOffset int64
		sOffset, read = binary.Varint(page)
		if read <= 0 {
			return nil, nil, fmt.Errorf("failed to parse start offset, i [%v], ret [%v]", i, read)
		}
		page = page[read:]
		eOffset, read = binary.Varint(page)
		if read <= 0 {
			return nil, nil, fmt.Errorf("failed to parse end offset, i [%v], ret [%v]", i, read)
		}
		page = page[read:]
		offsets = append(offsets, TCellOffsets{Start: uint32(sOffset), End: uint32(eOffset)})
	}
	meta := &TPageMeta{
		Id:       id,
		Version:  uint32(version),
		IsLeaf:   isLeaf != 0,
		CellsCnt: uint32(cellsCnt),
	}
	return meta, offsets, nil
}

func (r *TPageReader) Init() error {
	if r.file != nil {
		return fmt.Errorf("reader has been already initialized with file [%v]", r.file.Name())
	}
	var err error
	r.file, err = os.Open(r.Config.FilePath)
	if err != nil {
		return err
	}
	r.currentPage = make([]byte, r.Config.SizeBytes)
	read, err := r.file.Read(r.currentPage)
	if err != nil {
		return fmt.Errorf("failed to read the page [%v]", err)
	}
	if uint32(read) != r.Config.SizeBytes {
		return fmt.Errorf("not enough bytes to read the first page, expected [%v], read [%v]", r.Config.SizeBytes, read)
	}
	r.CurrentPageMeta, r.cells, err = parsePageMetaAndOffsets(0, r.currentPage)
	if err != nil {
		return err
	}
	return nil
}

func (r *TPageReader) Close() error {
	if r.file == nil {
		return nil
	}
	return r.file.Close()
}

func (r *TPageReader) Rewind() error {
	return errors.New("not implemented")
}

func (r *TPageReader) NextCell() (*TCellReader, error) {
	if r.currentCellId == r.CurrentPageMeta.CellsCnt {
		return nil, errors.New("reached the last cell")
	}
	r.currentCellId += 1
	sOffset := r.cells[r.currentCellId-1].Start
	eOffset := r.cells[r.currentCellId-1].End
	if eOffset > uint32(len(r.currentPage)) || sOffset >= eOffset {
		return nil, fmt.Errorf("invalid offsets [%v/%v] for cell [%v]", sOffset, eOffset, r.currentCellId)
	}
	curCellData := r.currentPage[sOffset:eOffset]
	keyLen, read := binary.Varint(curCellData)
	if read <= 0 || keyLen <= 0 {
		return nil, fmt.Errorf("failed to parse key size from [%v]", curCellData)
	}
	if len(curCellData) < read+int(keyLen) {
		return nil, fmt.Errorf("invalid cell [%v]", curCellData)
	}
	cellReader := &TCellReader{
		key:    curCellData[read : read+int(keyLen)],
		value:  curCellData[read+int(keyLen):],
		isLast: r.currentCellId == r.CurrentPageMeta.CellsCnt,
	}
	return cellReader, nil
}

func MakePageReader(config TPageConfig) *TPageReader {
	return &TPageReader{Config: config}
}
