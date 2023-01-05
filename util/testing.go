package util

import (
	"encoding/binary"
	"io/ioutil"
	"testing"
)

func AppendPadding(page []byte, size int) []byte {
	padding := make([]byte, size)
	page = append(page, padding...)
	return page
}

func AppendOffset(page []byte, start, end int) []byte {
	page = binary.AppendVarint(page, int64(start))
	page = binary.AppendVarint(page, int64(end))
	return page
}

func InitLeafPage(cellsCount int) []byte {
	page := []byte{}
	// header, version
	page = binary.AppendVarint(page, 1)
	// header, isLeaf [ui8]
	page = append(page, 1)
	// header, cellsCount [ui32]
	page = binary.AppendVarint(page, int64(cellsCount))
	return page
}

func InitInternalPage(cellsCount int, rightMostChild int) []byte {
	page := []byte{}
	// header, version
	page = binary.AppendVarint(page, 1)
	// header, isLeaf [ui8]
	page = append(page, 0)
	// header, cellsCount [ui32]
	page = binary.AppendVarint(page, int64(cellsCount))
	// header, rightMostChild
	page = binary.AppendVarint(page, int64(rightMostChild))
	return page
}

func EncodeVarint(val uint32) []byte {
	buf := []byte{}
	buf = binary.AppendVarint(buf, int64(val))
	return buf
}

func WriteAndCheck(t *testing.T, data []byte) string {
	f, err := ioutil.TempFile(".", "")
	if err != nil {
		t.Fatalf("failed to create file with error [%v]", err)
	}
	f.Write(data)
	f.Close()
	return "./" + f.Name()
}

type TTestingCell struct {
	Key   []byte
	Value []byte
}

func addCells(page []byte, cells []TTestingCell, pageSize int) []byte {
	encodedCells := make([][]byte, len(cells))
	cellsSize := 0
	for i, cell := range cells {
		end := pageSize - cellsSize
		encoded := EncodeCell(cell.Key, cell.Value)
		cellsSize += len(encoded)
		page = AppendOffset(page, pageSize-cellsSize, end)
		encodedCells[i] = encoded
	}
	page = AppendPadding(page, pageSize-len(page)-cellsSize)
	for i := len(cells) - 1; i >= 0; i-- {
		page = append(page, encodedCells[i]...)
	}
	return page
}

func FormatInternalPage(cells []TTestingCell, pageSize int, rightMostChild int) []byte {
	page := InitInternalPage(len(cells), rightMostChild)
	return addCells(page, cells, pageSize)
}

func FormatLeafPage(cells []TTestingCell, pageSize int) []byte {
	page := InitLeafPage(len(cells))
	return addCells(page, cells, pageSize)
}
