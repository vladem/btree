package btree_test

const (
	pageSize = 512
)

/*
// find tree layout in data/btree.png
func writeTree(t *testing.T) string {
	file := []byte{}
	file = append(file, util.FormatInternalPage([]util.TTestingCell{
		{Key: []byte("James"), Value: util.EncodeVarint(1)},
		{Key: []byte("Michael"), Value: util.EncodeVarint(2)},
	}, pageSize, 3)...)
	file = append(file, util.FormatInternalPage([]util.TTestingCell{
		{Key: []byte("Ashley"), Value: util.EncodeVarint(4)},
		{Key: []byte("Christopher"), Value: util.EncodeVarint(5)},
		{Key: []byte("Donald"), Value: util.EncodeVarint(6)},
	}, pageSize, 7)...)
	file = append(file, util.FormatInternalPage([]util.TTestingCell{
		{Key: []byte("John"), Value: util.EncodeVarint(8)},
		{Key: []byte("Kimberly"), Value: util.EncodeVarint(9)},
		{Key: []byte("Margaret"), Value: util.EncodeVarint(10)},
	}, pageSize, 11)...)
	file = append(file, util.FormatInternalPage([]util.TTestingCell{
		{Key: []byte("Patricia"), Value: util.EncodeVarint(12)},
		{Key: []byte("Sandra"), Value: util.EncodeVarint(13)},
		{Key: []byte("Susan"), Value: util.EncodeVarint(14)},
	}, pageSize, 15)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Andrew"), Value: []byte{'1'}},
		{Key: []byte("Anthony"), Value: []byte{'2'}},
		{Key: []byte("Ashley"), Value: []byte{'3'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Barbara"), Value: []byte{'4'}},
		{Key: []byte("Betty"), Value: []byte{'5'}},
		{Key: []byte("Charles"), Value: []byte{'6'}},
		{Key: []byte("Christopher"), Value: []byte{'7'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Daniel"), Value: []byte{'8'}},
		{Key: []byte("David"), Value: []byte{'9'}},
		{Key: []byte("Donald"), Value: []byte{'1', '0'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Donna"), Value: []byte{'1', '1'}},
		{Key: []byte("Elizabeth"), Value: []byte{'1', '2'}},
		{Key: []byte("Emily"), Value: []byte{'1', '3'}},
		{Key: []byte("James"), Value: []byte{'1', '4'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Jennifer"), Value: []byte{'1', '5'}},
		{Key: []byte("Jessica"), Value: []byte{'1', '6'}},
		{Key: []byte("John"), Value: []byte{'1', '7'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Joseph"), Value: []byte{'1', '8'}},
		{Key: []byte("Joshua"), Value: []byte{'1', '9'}},
		{Key: []byte("Karen"), Value: []byte{'2', '0'}},
		{Key: []byte("Kimberly"), Value: []byte{'2', '1'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Linda"), Value: []byte{'2', '2'}},
		{Key: []byte("Lisa"), Value: []byte{'2', '3'}},
		{Key: []byte("Margaret"), Value: []byte{'2', '4'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Mark"), Value: []byte{'2', '5'}},
		{Key: []byte("Mary"), Value: []byte{'2', '6'}},
		{Key: []byte("Matthew"), Value: []byte{'2', '7'}},
		{Key: []byte("Michael"), Value: []byte{'2', '8'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Michelle"), Value: []byte{'2', '9'}},
		{Key: []byte("Nancy"), Value: []byte{'3', '0'}},
		{Key: []byte("Patricia"), Value: []byte{'3', '1'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Paul"), Value: []byte{'3', '2'}},
		{Key: []byte("Richard"), Value: []byte{'3', '3'}},
		{Key: []byte("Robert"), Value: []byte{'3', '4'}},
		{Key: []byte("Sandra"), Value: []byte{'3', '5'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Sarah"), Value: []byte{'3', '6'}},
		{Key: []byte("Steven"), Value: []byte{'3', '7'}},
		{Key: []byte("Susan"), Value: []byte{'3', '8'}},
	}, pageSize)...)
	file = append(file, util.FormatLeafPage([]util.TTestingCell{
		{Key: []byte("Thomas"), Value: []byte{'3', '9'}},
		{Key: []byte("William"), Value: []byte{'4', '0'}},
		{Key: []byte("Zachary"), Value: []byte{'4', '1'}},
	}, pageSize)...)
	return util.WriteAndCheck(t, file)
}

func TestGetExisting(t *testing.T) {
	filePath := writeTree(t)
	defer os.Remove(filePath)
	pageReader := storage.MakePageManager(storage.TPageConfig{SizeBytes: uint32(pageSize), FilePath: filePath})
	defer pageReader.Close()
	err := pageReader.Init()
	assert.Empty(t, err)
	bTree := btree.MakePagedBTree(pageReader)
	value, err := bTree.Get([]byte("Steven"))
	assert.Empty(t, err)
	assert.Equal(t, []byte{'3', '7'}, value)
	assert.Equal(t, 3, int(pageReader.GetStatistics().ReadCalls))
	assert.Equal(t, pageSize*3, int(pageReader.GetStatistics().BytesRead))
	value, err = bTree.Get([]byte("Zachary"))
	assert.Empty(t, err)
	assert.Equal(t, []byte{'4', '1'}, value)
	assert.Equal(t, 6, int(pageReader.GetStatistics().ReadCalls))
	assert.Equal(t, pageSize*6, int(pageReader.GetStatistics().BytesRead))
	value, err = bTree.Get([]byte("Robert"))
	assert.Empty(t, err)
	assert.Equal(t, []byte{'3', '4'}, value)
	assert.Equal(t, 9, int(pageReader.GetStatistics().ReadCalls))
	assert.Equal(t, pageSize*9, int(pageReader.GetStatistics().BytesRead))
	value, err = bTree.Get([]byte("Ashley"))
	assert.Empty(t, err)
	assert.Equal(t, []byte{'3'}, value)
	assert.Equal(t, 12, int(pageReader.GetStatistics().ReadCalls))
	assert.Equal(t, pageSize*12, int(pageReader.GetStatistics().BytesRead))
	value, err = bTree.Get([]byte("Andrew"))
	assert.Empty(t, err)
	assert.Equal(t, []byte{'1'}, value)
	assert.Equal(t, 15, int(pageReader.GetStatistics().ReadCalls))
	assert.Equal(t, pageSize*15, int(pageReader.GetStatistics().BytesRead))
}

func TestGetMissing(t *testing.T) {
	filePath := writeTree(t)
	defer os.Remove(filePath)
	pageReader := storage.MakePageManager(storage.TPageConfig{SizeBytes: uint32(pageSize), FilePath: filePath})
	defer pageReader.Close()
	err := pageReader.Init()
	assert.Empty(t, err)
	bTree := btree.MakePagedBTree(pageReader)
	value, err := bTree.Get([]byte("George"))
	assert.Empty(t, err)
	assert.Empty(t, value)
	assert.Equal(t, 3, int(pageReader.GetStatistics().ReadCalls))
	assert.Equal(t, pageSize*3, int(pageReader.GetStatistics().BytesRead))
	value, err = bTree.Get([]byte("Rob"))
	assert.Empty(t, err)
	assert.Empty(t, value)
	assert.Equal(t, 6, int(pageReader.GetStatistics().ReadCalls))
	assert.Equal(t, pageSize*6, int(pageReader.GetStatistics().BytesRead))
}
*/
