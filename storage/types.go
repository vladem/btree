package storage

import (
	"io"
	"os"
)

const InvalidNodeId uint32 = (1 << 32) - 1

const pageHeaderSizeBytes = 5   // flags [1] + cellsCount [4]
const pageHeaderV2SizeBytes = 9 // flags [1] + cellsCount [4] + overflow page id [4]
const fileHeaderSizeBytes = 8   // layout version [4] + root node id [4]

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
	Key(id int) io.Reader
	KeyFull(id int) ([]byte, error)
	Value(id int) []byte
	Child(idx int) uint32
	InsertKey(key []byte, idx int)
	InsertKeyValue(key []byte, value []byte, idx int)
	InsertChild(childId uint32, idx int)
	SplitAt(idx int) (INode, error)
	UpdateValue(idx int, value []byte)
	Save() error
}

type INodeStorage interface {
	RootNode() INode
	AllocateRootNode() (INode, error)
	LoadNode(id uint32) (INode, error)
	Close() error
	Statistics() *TStorageStatistics
}

type tOnDiskNodeStorage struct {
	config        TConfig
	rootNode      INode
	file          *os.File
	nextPageId    uint32
	freePageIds   []uint32
	stats         *TStorageStatistics
	layoutVersion uint32
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

type tTupleV2 struct {
	offsets        *tCellOffsets
	overflowCellId uint32
	key            []byte
	value          []byte
}

type tNodeV2 struct {
	id             uint32
	isLeaf         bool
	overflowPageId uint32
}

type tSliceReader struct {
	data   []byte
	curPos int
}
