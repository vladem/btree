package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vladem/btree/util"
)

func TestCalculateFreeOffsetsEmpty(t *testing.T) {
	manager := MakePageManager(TPageConfig{MaxCellsCount: 10, SizeBytes: 4096})
	managerDowncasted, ok := manager.(*tPageManager)
	assert.True(t, ok)
	page := tPage{cellOffsets: []tCellOffsets{}, parent: managerDowncasted}
	page.calculateFreeOffsets()
	assert.Equal(t, []tCellOffsets{{Start: 93, End: 4096}}, page.freeOffsets)
}

func TestCalculateFreeOffsets(t *testing.T) {
	manager := MakePageManager(TPageConfig{MaxCellsCount: 10, SizeBytes: 4096})
	managerDowncasted, ok := manager.(*tPageManager)
	assert.True(t, ok)
	page := tPage{cellOffsets: []tCellOffsets{{Start: 4000, End: 4096}, {Start: 3000, End: 3500}, {Start: 3600, End: 4000}}, parent: managerDowncasted}
	page.calculateFreeOffsets()
	assert.Equal(t, []tCellOffsets{{Start: 93, End: 3000}, {Start: 3500, End: 3600}}, page.freeOffsets)
}

func TestAddCellLeastBetweenExisting(t *testing.T) {
	manager := MakePageManager(TPageConfig{MaxCellsCount: 10, SizeBytes: 128})
	managerDowncasted, ok := manager.(*tPageManager)
	assert.True(t, ok)
	// page: <header><offsets><free_space><cell_2><free_space><cell_1>
	//		 0....12,13....92,93......100,101.110,111.....120,121..127
	page := tPage{
		cellOffsets: []tCellOffsets{{Start: 101, End: 111}, {Start: 121, End: 128}},
		parent:      managerDowncasted,
		raw:         make([]byte, managerDowncasted.config.SizeBytes),
	}
	err := page.AddCellBefore([]byte{'a'}, []byte{'1'}, 0)
	assert.Empty(t, err)
	expected := make([]byte, 128)
	copy(expected[118:121], util.EncodeCell([]byte{'a'}, []byte{'1'}))
	assert.Equal(t, expected, page.raw)
	assert.Equal(t, []tCellOffsets{{Start: 118, End: 121}, {Start: 101, End: 111}, {Start: 121, End: 128}}, page.cellOffsets)
	assert.Equal(t, []tCellOffsets{{Start: 93, End: 101}, {Start: 111, End: 118}}, page.freeOffsets)
}

func TestAddCellLeastBetweenExistingFull(t *testing.T) {
	manager := MakePageManager(TPageConfig{MaxCellsCount: 10, SizeBytes: 128})
	managerDowncasted, ok := manager.(*tPageManager)
	assert.True(t, ok)
	// page: <header><offsets><free_space><cell_2><free_space><cell_1>
	//		 0....12,13....92,93......100,101.110,111.....120,121..127
	page := tPage{
		cellOffsets: []tCellOffsets{{Start: 101, End: 111}, {Start: 121, End: 128}},
		parent:      managerDowncasted,
		raw:         make([]byte, managerDowncasted.config.SizeBytes),
	}
	err := page.AddCellBefore([]byte{'a'}, []byte{'1', '2', '3', '4', '5', '6', '7', '8'}, 0)
	assert.Empty(t, err)
	expected := make([]byte, 128)
	copy(expected[111:121], util.EncodeCell([]byte{'a'}, []byte{'1', '2', '3', '4', '5', '6', '7', '8'}))
	assert.Equal(t, expected, page.raw)
	assert.Equal(t, []tCellOffsets{{Start: 111, End: 121}, {Start: 101, End: 111}, {Start: 121, End: 128}}, page.cellOffsets)
	assert.Equal(t, []tCellOffsets{{Start: 93, End: 101}}, page.freeOffsets)
}

func TestAddCellLeastBeforeAll(t *testing.T) {
	manager := MakePageManager(TPageConfig{MaxCellsCount: 8, SizeBytes: 128})
	managerDowncasted, ok := manager.(*tPageManager)
	assert.True(t, ok)
	// page: <header><offsets><free_space><cell_2><free_space><cell_1>
	//		 0....12,13....76,77......100,101.110,111.....120,121..127
	page := tPage{
		cellOffsets: []tCellOffsets{{Start: 101, End: 111}, {Start: 121, End: 128}},
		parent:      managerDowncasted,
		raw:         make([]byte, managerDowncasted.config.SizeBytes),
	}
	err := page.AddCellBefore([]byte{'a'}, []byte{'1', '2', '3', '4', '5', '6', '7', '8', '9'}, 0)
	assert.Empty(t, err)
	expected := make([]byte, 128)
	copy(expected[90:101], util.EncodeCell([]byte{'a'}, []byte{'1', '2', '3', '4', '5', '6', '7', '8', '9'}))
	assert.Equal(t, expected, page.raw)
	assert.Equal(t, []tCellOffsets{{Start: 90, End: 101}, {Start: 101, End: 111}, {Start: 121, End: 128}}, page.cellOffsets)
	assert.Equal(t, []tCellOffsets{{Start: 77, End: 90}, {Start: 111, End: 121}}, page.freeOffsets)
}

func TestAddCellGreatest(t *testing.T) {
	manager := MakePageManager(TPageConfig{MaxCellsCount: 10, SizeBytes: 128})
	managerDowncasted, ok := manager.(*tPageManager)
	assert.True(t, ok)
	// page: <header><offsets><free_space><cell_2><free_space><cell_1>
	//		 0....12,13....92,93......100,101.110,111.....120,121..127
	page := tPage{
		cellOffsets: []tCellOffsets{{Start: 101, End: 111}, {Start: 121, End: 128}},
		parent:      managerDowncasted,
		raw:         make([]byte, managerDowncasted.config.SizeBytes),
	}
	err := page.AddCellBefore([]byte{'a'}, []byte{'1'}, 2)
	assert.Empty(t, err)
	expected := make([]byte, 128)
	copy(expected[118:121], util.EncodeCell([]byte{'a'}, []byte{'1'}))
	assert.Equal(t, expected, page.raw)
	assert.Equal(t, []tCellOffsets{{Start: 101, End: 111}, {Start: 121, End: 128}, {Start: 118, End: 121}}, page.cellOffsets)
	assert.Equal(t, []tCellOffsets{{Start: 93, End: 101}, {Start: 111, End: 118}}, page.freeOffsets)
}

func TestAddCellKeyMiddle(t *testing.T) {
	manager := MakePageManager(TPageConfig{MaxCellsCount: 10, SizeBytes: 128})
	managerDowncasted, ok := manager.(*tPageManager)
	assert.True(t, ok)
	// page: <header><offsets><free_space><cell_2><free_space><cell_1>
	//		 0....12,13....92,93......100,101.110,111.....120,121..127
	page := tPage{
		cellOffsets: []tCellOffsets{{Start: 101, End: 111}, {Start: 121, End: 128}},
		parent:      managerDowncasted,
		raw:         make([]byte, managerDowncasted.config.SizeBytes),
	}
	err := page.AddCellBefore([]byte{'a'}, []byte{'1'}, 1)
	assert.Empty(t, err)
	expected := make([]byte, 128)
	copy(expected[118:121], util.EncodeCell([]byte{'a'}, []byte{'1'}))
	assert.Equal(t, expected, page.raw)
	assert.Equal(t, []tCellOffsets{{Start: 101, End: 111}, {Start: 118, End: 121}, {Start: 121, End: 128}}, page.cellOffsets)
	assert.Equal(t, []tCellOffsets{{Start: 93, End: 101}, {Start: 111, End: 118}}, page.freeOffsets)
}
