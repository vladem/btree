package btree

import (
	"sync"

	"github.com/vladem/btree/storage"
)

type TPagedBTree struct {
	nodeStorage  storage.INodeStorage
	maxKeysCount int
	mutex        *sync.Mutex
}
