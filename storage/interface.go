package storage

type TPageConfig struct {
	SizeBytes uint32 // page size is limited with ~4GB
	FilePath  string
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
}

type IPageReader interface {
	Init() error
	Close() error
	Read(id uint32) (IPage, error) // indexing starts from 0
}

func MakePageReader(config TPageConfig) IPageReader {
	return &tPageReader{config: config}
}
