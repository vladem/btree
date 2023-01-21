package btree_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vladem/btree/btree"
	"github.com/vladem/btree/storage"
	"github.com/vladem/btree/util"
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
	util.PrintStats(strg)
	assert.Empty(t, err)
	defer strg.Close()
	btree := btree.MakePagedBTree(strg, maxKeysCount)
	err = btree.Put([]byte("aaaa"), []byte("a_value"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	err = btree.Put([]byte("bbbb"), []byte("b_value"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	err = btree.Put([]byte("cccc"), []byte("c_value"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	err = btree.Put([]byte("dddd"), []byte("d_value"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	err = btree.Put([]byte("eeee"), []byte("e_value"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	err = btree.Put([]byte("ffff"), []byte("f_value"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	val, err := btree.Get([]byte("aaaa"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	assert.Equal(t, []byte("a_value"), val)
	val, err = btree.Get([]byte("bbbb"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	assert.Equal(t, []byte("b_value"), val)
	val, err = btree.Get([]byte("cccc"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	assert.Equal(t, []byte("c_value"), val)
	val, err = btree.Get([]byte("dddd"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	assert.Equal(t, []byte("d_value"), val)
	val, err = btree.Get([]byte("eeee"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	assert.Equal(t, []byte("e_value"), val)
	val, err = btree.Get([]byte("ffff"))
	util.PrintStats(strg)
	assert.Empty(t, err)
	assert.Equal(t, []byte("f_value"), val)
}

func TestInsert10Ordered(t *testing.T) {
	keys := [][]byte{
		[]byte("aa"),
		[]byte("bb"),
		[]byte("cc"),
		[]byte("dd"),
		[]byte("ee"),
		[]byte("ff"),
		[]byte("gg"),
		[]byte("hh"),
		[]byte("ii"),
		[]byte("jj"),
	}
	values := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
		[]byte("5"),
		[]byte("6"),
		[]byte("7"),
		[]byte("8"),
		[]byte("9"),
	}
	maxKeysCount := uint32(3)
	filePath := "./" + fileName()
	defer os.Remove(filePath)
	config := storage.TConfig{PageSizeBytes: 1024, FilePath: filePath, MaxCellsCount: maxKeysCount}
	strg, err := storage.MakeNodeStorage(config)
	assert.Empty(t, err)
	defer strg.Close()
	btree := btree.MakePagedBTree(strg, maxKeysCount)
	for i, key := range keys {
		assert.Empty(t, btree.Put(key, values[i]))
		// fmt.Printf("Tree after adding [%v]:\n", string(key))
		// util.PrintTree(strg)
	}
	for i, key := range keys {
		val, err := btree.Get(key)
		assert.Empty(t, err)
		assert.Equal(t, values[i], val)
	}
}

// func TestLoadFromDisk(t *testing.T) {}
// func TestUpdateValue(t *testing.T) {}
