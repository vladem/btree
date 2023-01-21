package btree_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vladem/btree/btree"
	"github.com/vladem/btree/storage"
	"github.com/vladem/btree/util"
)

func fileName() string {
	return time.Now().Format("2006-01-02 15:04:05.99999999")
}

var keys20 = [][]byte{
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
	[]byte("kk"),
	[]byte("ll"),
	[]byte("mm"),
	[]byte("nn"),
	[]byte("oo"),
	[]byte("pp"),
	[]byte("qq"),
	[]byte("rr"),
	[]byte("ss"),
	[]byte("tt"),
}
var values20 = [][]byte{
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
	[]byte("10"),
	[]byte("11"),
	[]byte("12"),
	[]byte("13"),
	[]byte("14"),
	[]byte("15"),
	[]byte("16"),
	[]byte("17"),
	[]byte("18"),
	[]byte("19"),
}

func putAndGet(t *testing.T, keys, values [][]byte, maxKeysCount uint32) {
	filePath := "./" + fileName()
	defer os.Remove(filePath)
	config := storage.TConfig{PageSizeBytes: 1024, FilePath: filePath, MaxCellsCount: maxKeysCount}
	strg, err := storage.MakeNodeStorage(config)
	require.Empty(t, err)
	defer strg.Close()
	btree := btree.MakePagedBTree(strg, maxKeysCount)
	require.NotEmpty(t, btree)
	util.PrintStats(strg)
	for i, key := range keys {
		require.Empty(t, btree.Put(key, values[i]))
		// fmt.Printf("Tree after adding [%v]:\n", string(key))
		// util.PrintTree(strg)
	}
	util.PrintStats(strg)
	for i, key := range keys {
		val, err := btree.Get(key)
		require.Empty(t, err)
		require.Equal(t, values[i], val)
	}
	util.PrintStats(strg)
}

func TestInsertOrdered(t *testing.T) {
	putAndGet(t, keys20, values20, 3)
}

func TestInsertOrderedDesc(t *testing.T) {
	keysDesc := make([][]byte, len(keys20))
	copy(keysDesc, keys20)
	util.ReverseSliceBytes(keysDesc)
	valuesDesc := make([][]byte, len(values20))
	copy(valuesDesc, values20)
	util.ReverseSliceBytes(valuesDesc)

	putAndGet(t, keysDesc, valuesDesc, 3)
}

func TestInsertUnordered(t *testing.T) {
	keysDesc := make([][]byte, len(keys20))
	copy(keysDesc, keys20)
	util.ShuffleSliceBytes(keysDesc)
	valuesDesc := make([][]byte, len(values20))
	copy(valuesDesc, values20)
	util.ShuffleSliceBytes(valuesDesc)

	putAndGet(t, keysDesc, valuesDesc, 3)
}

func TestInsertDegree5(t *testing.T) {
	putAndGet(t, keys20, values20, 5)
}

func TestInsertDegree11(t *testing.T) {
	putAndGet(t, keys20, values20, 11)
}

func TestLoadFromDisk(t *testing.T) {
	maxKeysCount := uint32(5)
	filePath := "./" + fileName()
	defer os.Remove(filePath)
	config := storage.TConfig{PageSizeBytes: 1024, FilePath: filePath, MaxCellsCount: maxKeysCount}
	strg, err := storage.MakeNodeStorage(config)
	require.Empty(t, err)
	defer strg.Close()
	tree := btree.MakePagedBTree(strg, maxKeysCount)
	require.NotEmpty(t, tree)
	for i, key := range keys20[:10] {
		require.Empty(t, tree.Put(key, values20[i]))
	}
	require.Empty(t, strg.Close())

	strg, err = storage.MakeNodeStorage(config)
	require.Empty(t, err)
	tree = btree.MakePagedBTree(strg, maxKeysCount)
	require.NotEmpty(t, tree)
	for i, key := range keys20[10:] {
		require.Empty(t, tree.Put(key, values20[10+i]))
	}
	for i, key := range keys20 {
		val, err := tree.Get(key)
		require.Empty(t, err)
		require.Equal(t, values20[i], val)
	}
}

// func TestUpdateValues(t *testing.T) {}
