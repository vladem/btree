package storage_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vladem/btree/storage"
)

func fileName() string {
	return time.Now().Format("2006-01-02 15:04:05.99999999")
}

func TestSimple(t *testing.T) {
	filePath := "./" + fileName()
	defer os.Remove(filePath)
	config := storage.TConfig{PageSizeBytes: 1024, FilePath: filePath, MaxCellsCount: 10}
	s1, err := storage.MakeNodeStorage(config)
	require.Empty(t, err)
	defer s1.Close()
	root := s1.RootNode()
	require.Empty(t, err)
	root.InsertKeyValue([]byte("key"), []byte("value"), 0)
	err = root.Save()
	require.Empty(t, err)
	s1.Close()

	s2, err := storage.MakeNodeStorage(config)
	require.Empty(t, err)
	defer s2.Close()
	root = s2.RootNode()
	require.Equal(t, 1, root.KeyCount())
	require.True(t, root.IsLeaf())
	key, err := root.KeyFull(0)
	require.Empty(t, err)
	require.Equal(t, []byte("key"), key)
	require.Equal(t, []byte("value"), root.Value(0))
}

func TestThreeNodes(t *testing.T) {
	filePath := "./" + fileName()
	defer os.Remove(filePath)
	config := storage.TConfig{PageSizeBytes: 1024, FilePath: filePath, MaxCellsCount: 10}
	s1, err := storage.MakeNodeStorage(config)
	require.Empty(t, err)
	defer s1.Close()
	lhs := s1.RootNode()
	root, err := s1.AllocateRootNode()
	require.Empty(t, err)

	lhs.InsertKeyValue([]byte("aaaa"), []byte("a_value"), 0)
	lhs.InsertKeyValue([]byte("bbbb"), []byte("b_value"), 1)
	lhs.InsertKeyValue([]byte("cccc"), []byte("c_value"), 2)
	lhs.InsertKeyValue([]byte("dddd"), []byte("d_value"), 3)
	lhs.InsertKeyValue([]byte("eeee"), []byte("e_value"), 4)
	rhs, err := lhs.SplitAt(2)
	require.Empty(t, err)
	err = lhs.Save()
	require.Empty(t, err)
	err = rhs.Save()
	require.Empty(t, err)

	root.InsertKey([]byte("cccc"), 0)
	root.InsertChild(rhs.Id(), 1)
	err = root.Save()
	require.Empty(t, err)
	s1.Close()

	s2, err := storage.MakeNodeStorage(config)
	require.Empty(t, err)
	defer s2.Close()
	rroot := s2.RootNode()
	require.Equal(t, 1, rroot.KeyCount())
	require.False(t, rroot.IsLeaf())
	key, err := rroot.KeyFull(0)
	require.Empty(t, err)
	require.Equal(t, []byte("cccc"), key)
	require.Equal(t, lhs.Id(), rroot.Child(0))
	require.Equal(t, rhs.Id(), rroot.Child(1))

	llhs, err := s2.LoadNode(lhs.Id())
	require.Empty(t, err)
	require.Equal(t, 2, llhs.KeyCount())
	require.True(t, llhs.IsLeaf())
	require.Equal(t, lhs.Id(), llhs.Id())
	key, err = llhs.KeyFull(0)
	require.Empty(t, err)
	require.Equal(t, []byte("aaaa"), key)
	key, err = llhs.KeyFull(1)
	require.Empty(t, err)
	require.Equal(t, []byte("bbbb"), key)
	require.Equal(t, []byte("a_value"), llhs.Value(0))
	require.Equal(t, []byte("b_value"), llhs.Value(1))

	rrhs, err := s2.LoadNode(rhs.Id())
	require.Empty(t, err)
	require.Equal(t, 3, rrhs.KeyCount())
	require.True(t, rrhs.IsLeaf())
	require.Equal(t, rhs.Id(), rrhs.Id())
	key, err = rrhs.KeyFull(0)
	require.Empty(t, err)
	require.Equal(t, []byte("cccc"), key)
	key, err = rrhs.KeyFull(1)
	require.Empty(t, err)
	require.Equal(t, []byte("dddd"), key)
	key, err = rrhs.KeyFull(2)
	require.Empty(t, err)
	require.Equal(t, []byte("eeee"), key)
	require.Equal(t, []byte("c_value"), rrhs.Value(0))
	require.Equal(t, []byte("d_value"), rrhs.Value(1))
	require.Equal(t, []byte("e_value"), rrhs.Value(2))
}
