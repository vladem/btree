package storage_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	assert.Empty(t, err)
	defer s1.Close()
	root := s1.RootNode()
	assert.Empty(t, err)
	root.InsertKeyValue([]byte("key"), []byte("value"), 0)
	err = root.Save()
	assert.Empty(t, err)
	s1.Close()

	s2, err := storage.MakeNodeStorage(config)
	assert.Empty(t, err)
	defer s2.Close()
	root = s2.RootNode()
	assert.Equal(t, 1, root.KeyCount())
	assert.True(t, root.IsLeaf())
	assert.Equal(t, []byte("key"), root.Key(0))
	assert.Equal(t, []byte("value"), root.Value(0))
}

func TestThreeNodes(t *testing.T) {
	filePath := "./" + fileName()
	defer os.Remove(filePath)
	config := storage.TConfig{PageSizeBytes: 1024, FilePath: filePath, MaxCellsCount: 10}
	s1, err := storage.MakeNodeStorage(config)
	assert.Empty(t, err)
	defer s1.Close()
	lhs := s1.RootNode()
	root, err := s1.AllocateRootNode()
	assert.Empty(t, err)
	rhs, err := s1.AllocateNode(true)
	assert.Empty(t, err)

	root.InsertKey([]byte("cccc"), 0)
	root.InsertChild(rhs.Id(), 1)
	err = root.Save()
	assert.Empty(t, err)

	lhs.InsertKeyValue([]byte("aaaa"), []byte("a_value"), 0)
	lhs.InsertKeyValue([]byte("bbbb"), []byte("b_value"), 1)
	err = lhs.Save()
	assert.Empty(t, err)

	rhs.InsertKeyValue([]byte("cccc"), []byte("c_value"), 0)
	rhs.InsertKeyValue([]byte("dddd"), []byte("d_value"), 1)
	rhs.InsertKeyValue([]byte("eeee"), []byte("e_value"), 2)
	err = rhs.Save()
	assert.Empty(t, err)
	s1.Close()

	s2, err := storage.MakeNodeStorage(config)
	assert.Empty(t, err)
	defer s2.Close()
	rroot := s2.RootNode()
	assert.Equal(t, 1, rroot.KeyCount())
	assert.False(t, rroot.IsLeaf())
	assert.Equal(t, []byte("cccc"), rroot.Key(0))
	assert.Equal(t, lhs.Id(), rroot.Child(0))
	assert.Equal(t, rhs.Id(), rroot.Child(1))

	llhs, err := s2.LoadNode(lhs.Id())
	assert.Empty(t, err)
	assert.Equal(t, 2, llhs.KeyCount())
	assert.True(t, llhs.IsLeaf())
	assert.Equal(t, lhs.Id(), llhs.Id())
	assert.Equal(t, []byte("aaaa"), llhs.Key(0))
	assert.Equal(t, []byte("bbbb"), llhs.Key(1))
	assert.Equal(t, []byte("a_value"), llhs.Value(0))
	assert.Equal(t, []byte("b_value"), llhs.Value(1))

	rrhs, err := s2.LoadNode(rhs.Id())
	assert.Empty(t, err)
	assert.Equal(t, 3, rrhs.KeyCount())
	assert.True(t, rrhs.IsLeaf())
	assert.Equal(t, rhs.Id(), rrhs.Id())
	assert.Equal(t, []byte("cccc"), rrhs.Key(0))
	assert.Equal(t, []byte("dddd"), rrhs.Key(1))
	assert.Equal(t, []byte("eeee"), rrhs.Key(2))
	assert.Equal(t, []byte("c_value"), rrhs.Value(0))
	assert.Equal(t, []byte("d_value"), rrhs.Value(1))
	assert.Equal(t, []byte("e_value"), rrhs.Value(2))
}
