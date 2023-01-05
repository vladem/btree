package storage

const PageHeaderSizeBytes = 13 // version [4] + isLeaf [1] + cellsCount [4] + rightmostChild [4]

type TPageConfig struct {
	SizeBytes     uint32 // page size is limited with ~4GB
	FilePath      string
	MaxCellsCount uint32
}

type tPageManagerStatistics struct {
	ReadCalls uint32
	BytesRead uint32
}

type ICell interface {
	GetKey() ([]byte, error)
	GetValue() ([]byte, error)
	GetValueAsUint32() (uint32, error)
}

type IPage interface {
	GetId() uint32
	IsLeaf() bool
	GetCellsCount() uint32
	// last cell of an internal page does not have a key, rightmost child contains [lastKey, +inf)
	GetCell(id uint32) (ICell, error) // indexing starts from 0
	AddCellBefore(key, value []byte, id uint32) error
	MoveCells(dst IPage, fromId uint32) error
	Flush() // persist on disk
}

type IPageManager interface {
	Init() error
	Close() error
	Read(id uint32) (IPage, error) // indexing starts from 0
	NewPage(isLeaf bool, rightMostChild uint32) (IPage, error)
	GetStatistics() *tPageManagerStatistics
}

func MakePageManager(config TPageConfig) IPageManager {
	return &tPageManager{config: config, stats: &tPageManagerStatistics{}}
}
