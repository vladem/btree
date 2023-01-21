package storage

import "os"

const InvalidNodeId uint32 = (1 << 32) - 1
const pageHeaderSizeBytes = 5 // flags [1] + cellsCount [4]
const fileHeaderSizeBytes = 8 // layout version [4] + root node id [4]

type TConfig struct {
	PageSizeBytes uint32 // page size is limited with ~4GB
	FilePath      string
	MaxCellsCount uint32
}

type TStorageStatistics struct {
	ReadCalls    uint32
	BytesRead    uint32
	WriteCalls   uint32
	BytesWritten uint32
}

type INode interface {
	IsLeaf() bool
	KeyCount() int
	Id() uint32
	Key(id int) []byte
	Keys(idStart, idEnd int) [][]byte
	Value(id int) []byte
	KeyValues(idStart, idEnd int) ([][]byte, [][]byte)
	Child(idx int) uint32
	Children(idStart, idEnd int) []uint32
	InsertKey(key []byte, idx int)
	InsertKeyValue(key []byte, value []byte, idx int)
	InsertChild(childId uint32, idx int)
	ReplaceKeys(keys [][]byte)
	ReplaceChildren(childIds []uint32)
	ReplaceKeyValues(keys, values [][]byte)
	TruncateKeys(tillIdx int)
	TruncateChildren(tillIdx int)
	UpdateValue(idx int, value []byte)
	Save() error
}

type INodeStorage interface {
	RootNode() INode
	AllocateNode(isLeaf bool) (INode, error)
	AllocateRootNode() (INode, error)
	LoadNode(id uint32) (INode, error)
	Close() error
	Statistics() *TStorageStatistics
}

type tOnDiskNodeStorage struct {
	config      TConfig
	rootNode    *tNode
	file        *os.File
	nextPageId  uint32
	freePageIds []uint32
	stats       *TStorageStatistics
}

type tCellOffsets struct {
	Start uint32 // bytes from the start of the page
	End   uint32 // bytes from the start of the page
}

type tTuple struct {
	offsets *tCellOffsets
	key     []byte
	value   []byte
}

type tNode struct {
	id     uint32
	isLeaf bool
	parent *tOnDiskNodeStorage
	tuples []*tTuple
	// only set for internal nodes
	children    []uint32
	freeOffsets []tCellOffsets
}
