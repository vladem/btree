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

type tCell struct {
	key   []byte
	value []byte
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

type tFileHeader struct { // store on disk at most N free page ids and an overflow flag
	version_format uint32
	freePages      []uint32
}
