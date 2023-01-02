package storage_test

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vladem/btree/storage"
	"github.com/vladem/btree/util"
)

func TestTraverseCellsInSinglePage(t *testing.T) {
	cell1Raw := util.FormatCell([]byte{'a'}, []byte{'1'})
	cell2Raw := util.FormatCell([]byte{'b'}, []byte{'2'})
	sizeBytes := 32
	pageRaw := util.InitLeafPage(2) // page layout: <header><cell1_ofs><cell2_ofs><padding><cell2><cell1>
	pageRaw = util.AppendOffset(pageRaw, sizeBytes-len(cell1Raw), sizeBytes)
	pageRaw = util.AppendOffset(pageRaw, sizeBytes-len(cell1Raw)-len(cell2Raw), sizeBytes-len(cell1Raw))
	pageRaw = util.AppendPadding(pageRaw, sizeBytes-len(pageRaw)-len(cell1Raw)-len(cell2Raw))
	pageRaw = append(pageRaw, cell2Raw...)
	pageRaw = append(pageRaw, cell1Raw...)
	if len(pageRaw) != sizeBytes {
		t.Fatalf("len is [%v]", len(pageRaw))
	}
	log.Printf("page [%v], cell1 [%v], cell2 [%v]", pageRaw, cell1Raw, cell2Raw)
	filePath := util.WriteAndCheck(t, pageRaw)
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
	cell1 := util.FormatCell([]byte{'b'}, util.EncodeVarint(1))
	cell2 := util.FormatCell([]byte{'a'}, []byte{'2'})
	cell3 := util.FormatCell([]byte{'c'}, []byte{'3'})
	sizeBytes := 32
	file := []byte{}                        // file layout: <rootPage><leafPage1><leafPage2>
	rootPage := util.InitInternalPage(1, 2) // rootPage layout: <header><cell1_ofs><padding><cell1>
	leafPage1 := util.InitLeafPage(1)       // leafPage1 layout: <header><cell2_ofs><padding><cell2>
	leafPage2 := util.InitLeafPage(1)       // leafPage2 layout: <header><cell3_ofs><padding><cell3>
	rootPage = util.AppendOffset(rootPage, sizeBytes-len(cell1), sizeBytes)
	rootPage = util.AppendPadding(rootPage, sizeBytes-len(rootPage)-len(cell1))
	rootPage = append(rootPage, cell1...)
	leafPage1 = util.AppendOffset(leafPage1, sizeBytes-len(cell2), sizeBytes)
	leafPage1 = util.AppendPadding(leafPage1, sizeBytes-len(leafPage1)-len(cell2))
	leafPage1 = append(leafPage1, cell2...)
	leafPage2 = util.AppendOffset(leafPage2, sizeBytes-len(cell3), sizeBytes)
	leafPage2 = util.AppendPadding(leafPage2, sizeBytes-len(leafPage2)-len(cell3))
	leafPage2 = append(leafPage2, cell3...)
	file = append(file, rootPage...)
	file = append(file, leafPage1...)
	file = append(file, leafPage2...)
	log.Printf("file [%v]", file)
	filePath := util.WriteAndCheck(t, file)
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
