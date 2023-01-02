package storage_test

import (
	"encoding/binary"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vladem/btree/storage"
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

func initInternalPage(cellsCount int, rightMostChild int) []byte {
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

func encodeVarint(val uint32) []byte {
	buf := []byte{}
	buf = binary.AppendVarint(buf, int64(val))
	return buf
}

func writeAndCheck(t *testing.T, data []byte) string {
	f, err := ioutil.TempFile(".", "")
	if err != nil {
		t.Fatalf("failed to create file with error [%v]", err)
	}
	f.Write(data)
	f.Close()
	return "./" + f.Name()
}

func TestTraverseCellsInSinglePage(t *testing.T) {
	cell1Raw := formatCell([]byte{'a'}, []byte{'1'})
	cell2Raw := formatCell([]byte{'b'}, []byte{'2'})
	sizeBytes := 32
	pageRaw := initLeafPage(2) // page layout: <header><cell1_ofs><cell2_ofs><padding><cell2><cell1>
	pageRaw = appendOffset(pageRaw, sizeBytes-len(cell1Raw), sizeBytes)
	pageRaw = appendOffset(pageRaw, sizeBytes-len(cell1Raw)-len(cell2Raw), sizeBytes-len(cell1Raw))
	pageRaw = appendPadding(pageRaw, sizeBytes-len(pageRaw)-len(cell1Raw)-len(cell2Raw))
	pageRaw = append(pageRaw, cell2Raw...)
	pageRaw = append(pageRaw, cell1Raw...)
	if len(pageRaw) != sizeBytes {
		t.Fatalf("len is [%v]", len(pageRaw))
	}
	log.Printf("page [%v], cell1 [%v], cell2 [%v]", pageRaw, cell1Raw, cell2Raw)
	filePath := writeAndCheck(t, pageRaw)
	defer os.Remove(filePath)

	pageReader := storage.MakePageReader(storage.TPageConfig{SizeBytes: uint32(sizeBytes), FilePath: filePath})
	defer pageReader.Close()
	err := pageReader.Init()
	if err != nil {
		t.Fatalf("init failed with error [%v]", err)
	}
	page, err := pageReader.Read(0)
	if err != nil {
		t.Fatalf("failed to read page with error [%v]", err)
	}
	assert.True(t, page.IsLeaf(), "expected leaf page")
	assert.Equal(t, uint32(2), page.GetCellsCount(), "wrong page meta")
	cell1, err := page.GetCell(0)
	assert.Emptyf(t, err, "failed to get cell1 with error [%v]", err)
	key1, err := cell1.GetKey()
	assert.Emptyf(t, err, "failed to get cell1 key with error [%v]", err)
	assert.Equal(t, []byte{'a'}, key1, "cell1, key")
	val1, err := cell1.GetValue()
	assert.Emptyf(t, err, "failed to get cell1 value with error [%v]", err)
	assert.Equal(t, []byte{'1'}, val1, "cell1, value")
	cell2, err := page.GetCell(1)
	assert.Emptyf(t, err, "failed to get cell2 with error [%v]", err)
	key2, err := cell2.GetKey()
	assert.Emptyf(t, err, "failed to get cell2 key with error [%v]", err)
	assert.Equal(t, []byte{'b'}, key2, "cell2, key")
	val2, err := cell2.GetValue()
	assert.Emptyf(t, err, "failed to get cell2 value with error [%v]", err)
	assert.Equal(t, []byte{'2'}, val2, "cell2, value")
}

func checkLeafCell(t *testing.T, page storage.IPage, idx uint32, keyE, valueE []byte) {
	cell, err := page.GetCell(idx)
	assert.Empty(t, err, "failed to get cell")
	key, err := cell.GetKey()
	assert.Emptyf(t, err, "failed to get cell key with error [%v]", err)
	assert.Equal(t, keyE, key, "cell, key")
	val, err := cell.GetValue()
	assert.Emptyf(t, err, "failed to get cell value with error [%v]", err)
	assert.Equal(t, valueE, val, "cell, value")
}

func checkInternalCell(t *testing.T, page storage.IPage, idx uint32, keyE []byte, valueE uint32) {
	cell, err := page.GetCell(idx)
	assert.Empty(t, err, "failed to get cell")
	key, err := cell.GetKey()
	assert.Emptyf(t, err, "failed to get cell key with error [%v]", err)
	assert.Equal(t, keyE, key, "cell, key")
	val, err := cell.GetValueAsUint32()
	assert.Emptyf(t, err, "failed to get cell value with error [%v]", err)
	assert.Equal(t, valueE, val, "cell, value")
}

func TestTraverseCellsInTwoPages(t *testing.T) {
	cell1 := formatCell([]byte{'b'}, encodeVarint(1))
	cell2 := formatCell([]byte{'a'}, []byte{'2'})
	cell3 := formatCell([]byte{'c'}, []byte{'3'})
	sizeBytes := 32
	file := []byte{}                   // file layout: <rootPage><leafPage1><leafPage2>
	rootPage := initInternalPage(1, 2) // rootPage layout: <header><cell1_ofs><padding><cell1>
	leafPage1 := initLeafPage(1)       // leafPage1 layout: <header><cell2_ofs><padding><cell2>
	leafPage2 := initLeafPage(1)       // leafPage2 layout: <header><cell3_ofs><padding><cell3>
	rootPage = appendOffset(rootPage, sizeBytes-len(cell1), sizeBytes)
	rootPage = appendPadding(rootPage, sizeBytes-len(rootPage)-len(cell1))
	rootPage = append(rootPage, cell1...)
	leafPage1 = appendOffset(leafPage1, sizeBytes-len(cell2), sizeBytes)
	leafPage1 = appendPadding(leafPage1, sizeBytes-len(leafPage1)-len(cell2))
	leafPage1 = append(leafPage1, cell2...)
	leafPage2 = appendOffset(leafPage2, sizeBytes-len(cell3), sizeBytes)
	leafPage2 = appendPadding(leafPage2, sizeBytes-len(leafPage2)-len(cell3))
	leafPage2 = append(leafPage2, cell3...)
	file = append(file, rootPage...)
	file = append(file, leafPage1...)
	file = append(file, leafPage2...)
	log.Printf("file [%v]", file)
	filePath := writeAndCheck(t, file)
	defer os.Remove(filePath)

	pageReader := storage.MakePageReader(storage.TPageConfig{SizeBytes: uint32(sizeBytes), FilePath: filePath})
	defer pageReader.Close()
	err := pageReader.Init()
	if err != nil {
		t.Fatalf("init failed with error [%v]", err)
	}
	page, err := pageReader.Read(0)
	assert.Empty(t, err, "failed to get root page")
	assert.False(t, page.IsLeaf(), "expected internal page")
	assert.Equal(t, uint32(2), page.GetCellsCount(), "wrong page meta")
	checkInternalCell(t, page, 0, []byte{'b'}, 1)
	checkInternalCell(t, page, 1, nil, 2)
	page, err = pageReader.Read(1)
	assert.Empty(t, err, "failed to get page")
	assert.True(t, page.IsLeaf(), "expected leaf page")
	assert.Equal(t, uint32(1), page.GetCellsCount(), "wrong page meta")
	checkLeafCell(t, page, 0, []byte{'a'}, []byte{'2'})
	page, err = pageReader.Read(2)
	assert.Empty(t, err, "failed to get page")
	assert.True(t, page.IsLeaf(), "expected leaf page")
	assert.Equal(t, uint32(1), page.GetCellsCount(), "wrong page meta")
	checkLeafCell(t, page, 0, []byte{'c'}, []byte{'3'})
	page, err = pageReader.Read(3)
	assert.Error(t, err, "page should not exist")
	assert.Empty(t, page, "page should not exist")
}
