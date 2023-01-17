package btree_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vladem/btree/btree"
	"github.com/vladem/btree/storage"
)

func fileName() string {
	return time.Now().Format("2006-01-02 15:04:05.99999999")
}

func TestSimple(t *testing.T) {
	maxKeysCount := uint32(5)
	filePath := "./" + fileName()
	defer os.Remove(filePath)
	config := storage.TConfig{PageSizeBytes: 1024, FilePath: filePath, MaxCellsCount: maxKeysCount}
	strg, err := storage.MakeNodeStorage(config)
	assert.Empty(t, err)
	defer strg.Close()
	btree := btree.MakePagedBTree(strg, maxKeysCount)
	err = btree.Put([]byte("aaaa"), []byte("a_value"))
	assert.Empty(t, err)
	err = btree.Put([]byte("bbbb"), []byte("b_value"))
	assert.Empty(t, err)
	err = btree.Put([]byte("cccc"), []byte("c_value"))
	assert.Empty(t, err)
	err = btree.Put([]byte("dddd"), []byte("d_value"))
	assert.Empty(t, err)
	err = btree.Put([]byte("eeee"), []byte("e_value"))
	assert.Empty(t, err)
	err = btree.Put([]byte("ffff"), []byte("f_value"))
	assert.Empty(t, err)
}
