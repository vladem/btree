package btree

import "github.com/vladem/btree/storage"

type TPagedBTree struct {
	nodeStorage  storage.INodeStorage
	maxKeysCount int
}
