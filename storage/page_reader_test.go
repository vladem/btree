package storage

import (
	"encoding/binary"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func formatCell(key, value []byte) []byte {
	cell := []byte{}
	cell = binary.AppendVarint(cell, int64(len(key)))
	cell = append(cell, key...)
	cell = append(cell, value...)
	return cell
}

func appendPadding(page []byte, size int) []byte {
	padding := make([]byte, size)
	page = append(page, padding...)
	return page
}

func appendOffset(page []byte, start, end int) []byte {
	page = binary.AppendVarint(page, int64(start))
	page = binary.AppendVarint(page, int64(end))
	return page
}

func initLeafPage(cellsCount int) []byte {
	page := []byte{}
	// header, version
	page = binary.AppendVarint(page, 1)
	// header, isLeaf [ui8]
	page = append(page, 1)
	// header, cellsCount [ui32]
	page = binary.AppendVarint(page, int64(cellsCount))
	return page
}

func writePage(t *testing.T, data []byte) string {
	f, err := ioutil.TempFile(".", "")
	if err != nil {
		t.Fatalf("failed to create file with error [%v]", err)
	}
	f.Write(data)
	f.Close()
	return "./" + f.Name()
}

func TestTraverseCellsInSinglePage(t *testing.T) {
	cell1 := formatCell([]byte{'a'}, []byte{'1'})
	cell2 := formatCell([]byte{'b'}, []byte{'2'})
	sizeBytes := 32
	page := initLeafPage(2) // page layout: <header><cell1_ofs><cell2_ofs><padding><cell2><cell1>
	page = appendOffset(page, sizeBytes-len(cell1), sizeBytes)
	page = appendOffset(page, sizeBytes-len(cell1)-len(cell2), sizeBytes-len(cell1))
	page = appendPadding(page, sizeBytes-len(page)-len(cell1)-len(cell2))
	page = append(page, cell2...)
	page = append(page, cell1...)
	if len(page) != sizeBytes {
		t.Fatalf("len is [%v]", len(page))
	}
	log.Printf("page [%v], cell1 [%v], cell2 [%v]", page, cell1, cell2)
	filePath := writePage(t, page)
	defer os.Remove(filePath)
	pageReader := MakePageReader(TPageConfig{SizeBytes: uint32(sizeBytes), FilePath: filePath})
	defer pageReader.Close()
	err := pageReader.Init()
	if err != nil {
		t.Fatalf("init failed with error [%v]", err)
	}
	assert.True(t, pageReader.CurrentPageMeta.IsLeaf, "expected leaf page")
	assert.Equal(t, uint32(2), pageReader.CurrentPageMeta.CellsCnt, "wrong page meta")
	cellReader1, err := pageReader.NextCell()
	assert.Emptyf(t, err, "failed to get cell1 with error [%v]", err)
	assert.False(t, cellReader1.IsLast(), "expected one more cell")
	assert.Equal(t, []byte{'a'}, cellReader1.GetKey(), "cell1, key")
	assert.Equal(t, []byte{'1'}, cellReader1.GetValue(), "cell1, value")
	cellReader2, err := pageReader.NextCell()
	assert.Emptyf(t, err, "failed to get cell2 with error [%v]", err)
	assert.True(t, cellReader2.IsLast(), "expected second cell to be the last")
	assert.Equal(t, []byte{'b'}, cellReader2.GetKey(), "cell2, key")
	assert.Equal(t, []byte{'2'}, cellReader2.GetValue(), "cell2, value")
	empty, err := pageReader.NextCell()
	assert.Error(t, err, "expected error")
	assert.Emptyf(t, empty, "expected empty, got [%v]", empty)
}
