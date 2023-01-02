package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareEq(t *testing.T) {
	assert.Equal(t, int8(0), Compare([]byte{'a', 'b', 'c', '1'}, []byte{'a', 'b', 'c', '1'}))
}

func TestCompareLess(t *testing.T) {
	assert.Equal(t, int8(-1), Compare([]byte{'a', 'b', 'c', '0'}, []byte{'a', 'b', 'c', '1'}))
}

func TestCompareGreater(t *testing.T) {
	assert.Equal(t, int8(1), Compare([]byte{'a', 'b', 'c', '1'}, []byte{'a', 'b', 'c', '0'}))
}

func TestCompareLessBySize(t *testing.T) {
	assert.Equal(t, int8(-1), Compare([]byte{'a', 'b', 'c'}, []byte{'a', 'b', 'c', 0}))
}

func TestCompareGreaterBySize(t *testing.T) {
	assert.Equal(t, int8(1), Compare([]byte{'a', 'b', 'c', 0}, []byte{'a', 'b', 'c'}))
}

func TestAddCells(t *testing.T) {
	cell1Raw := FormatCell([]byte{'a'}, []byte{'1'})
	cell2Raw := FormatCell([]byte{'b'}, []byte{'2'})
	sizeBytes := 32
	expectedPage := InitLeafPage(2) // page layout: <header><cell1_ofs><cell2_ofs><padding><cell2><cell1>
	expectedPage = AppendOffset(expectedPage, sizeBytes-len(cell1Raw), sizeBytes)
	expectedPage = AppendOffset(expectedPage, sizeBytes-len(cell1Raw)-len(cell2Raw), sizeBytes-len(cell1Raw))
	expectedPage = AppendPadding(expectedPage, sizeBytes-len(expectedPage)-len(cell1Raw)-len(cell2Raw))
	expectedPage = append(expectedPage, cell2Raw...)
	expectedPage = append(expectedPage, cell1Raw...)

	actualPage := InitLeafPage(2)
	actualPage = addCells(actualPage, []TTestingCell{
		{Key: []byte{'a'}, Value: []byte{'1'}},
		{Key: []byte{'b'}, Value: []byte{'2'}},
	}, sizeBytes)
	assert.Equal(t, expectedPage, actualPage)
}
