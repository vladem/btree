package storage

/*
File consists of fixed-size pages. Page consists of variable size cells. Each cell contain a key and either 1) a value or 2) a reference
to another cell.

Data layout: File -> Page [node] -> Cell [key within a node]
*/

type TPageConfig struct {
	SizeBytes uint32 // page size is limited with 4GB
	FilePath  string
}

type ICellReader interface {
	GetKey() []byte
	GetValue() []byte
	IsLast() bool // last cell contains keys (prev, +inf)
}

type IPageReader interface {
	NextCell() (ICellReader, error)
	NextPage() error // fail if number of traversed pages gte N
	IsLeaf() bool
	GetPageVersion() uint32
	Init() error // locates/opens the file, reads the first page
	Close() error
	Rewind() error
	CurrentCell() ICellReader
}
