package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
