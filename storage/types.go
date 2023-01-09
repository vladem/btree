package storage

import "os"

const fileHeaderSize = 0

type tPageManager struct {
	config     TPageConfig
	file       *os.File
	stats      *tPageManagerStatistics
	maxPageId  uint32
	fileHeader *tFileHeader
}

type tCellOffsets struct {
	Start uint32 // bytes from the start of the page
	End   uint32 // bytes from the start of the page
}

type tPage struct {
	id             uint32
	isLeaf         bool
	cellOffsets    []tCellOffsets
	rightMostChild uint32
	raw            []byte
	freeOffsets    []tCellOffsets
	parent         *tPageManager
}

type TTuple struct {
	persisted bool
	pageId    uint32
	offsets   tCellOffsets
	key       []byte
	value     []byte
}

type TNode struct {
	Tuples []TTuple
	page   *tPage
}

type INodeStorage interface {
	SetRootNode(newRoot *TNode) error
	RootNode() (*TNode, error)
	AllocateNode(isLeaf bool) (*TNode, error)
	LoadNode(id uint32) (*TNode, error)
	SaveNode(node *TNode) error
}
