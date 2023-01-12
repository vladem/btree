package btree

/*
type tInMemoryNodeStorage struct {
	root       *storage.TNode
	nodeBuffer []*storage.TNode
}

func (t *tInMemoryNodeStorage) AllocateNode(isLeaf bool) (*storage.TNode, error) {
	node := &storage.TNode{Id: uint32(len(t.nodeBuffer)), Keys: [][]byte{}, Children: []uint32{}, Values: [][]byte{}, IsLeaf: isLeaf}
	t.nodeBuffer = append(t.nodeBuffer, node)
	return node, nil
}

func (t *tInMemoryNodeStorage) LoadNode(id uint32) (*storage.TNode, error) {
	return t.nodeBuffer[id], nil
}

func (t *tInMemoryNodeStorage) SaveNode(node *storage.TNode) error {
	return nil
}

func (t *tInMemoryNodeStorage) SetRootNode(newRoot *storage.TNode) error {
	t.root = newRoot
	return nil
}

func (t *tInMemoryNodeStorage) RootNode() (*storage.TNode, error) {
	return t.root, nil
}

func TestSplitLastChild(t *testing.T) {
	nodeStorage := &tInMemoryNodeStorage{nodeBuffer: []*storage.TNode{}}
	btree := TPagedBTree{nodeStorage: nodeStorage}
	root, _ := nodeStorage.AllocateNode(false) // id: 0
	root.Keys = append(root.Keys, []byte("Barbara"))
	root.Children = append(root.Children, []uint32{1, 2}...) // ids: 1, 2
	child1, _ := nodeStorage.AllocateNode(true)              // id: 1
	child1.Keys = [][]byte{[]byte("Andrew"), []byte("Ashley")}
	child2, _ := nodeStorage.AllocateNode(true) // id: 2
	child2.Keys = [][]byte{
		[]byte("Barbara"),
		[]byte("Betty"),
		[]byte("Charles"),
		[]byte("Daniel"),
		[]byte("David"),
	}
	btree.splitChild(root, child2)
	assert.Equal(t, 4, len(nodeStorage.nodeBuffer))
	assert.Equal(t, [][]byte{[]byte("Barbara"), []byte("Charles")}, nodeStorage.nodeBuffer[0].Keys)
	assert.Equal(t, []uint32{1, 2, 3}, nodeStorage.nodeBuffer[0].Children)
	assert.Equal(t, [][]byte{[]byte("Andrew"), []byte("Ashley")}, nodeStorage.nodeBuffer[1].Keys)
	assert.Equal(t, [][]byte{
		[]byte("Barbara"),
		[]byte("Betty"),
	}, nodeStorage.nodeBuffer[2].Keys)
	assert.Equal(t, [][]byte{
		[]byte("Charles"),
		[]byte("Daniel"),
		[]byte("David"),
	}, nodeStorage.nodeBuffer[3].Keys)
}

func TestSplitFirstChild(t *testing.T) {
	nodeStorage := &tInMemoryNodeStorage{nodeBuffer: []*storage.TNode{}}
	btree := TPagedBTree{nodeStorage: nodeStorage}
	root, _ := nodeStorage.AllocateNode(false) // id: 0
	root.Keys = append(root.Keys, []byte("Charles"))
	root.Children = append(root.Children, []uint32{1, 2}...) // ids: 1, 2
	child1, _ := nodeStorage.AllocateNode(true)              // id: 1
	child1.Keys = [][]byte{
		[]byte("Andrew"),
		[]byte("Anthony"),
		[]byte("Ashley"),
		[]byte("Barbara"),
		[]byte("Betty"),
	}
	child2, _ := nodeStorage.AllocateNode(true) // id: 2
	child2.Keys = [][]byte{
		[]byte("Charles"),
		[]byte("Daniel"),
	}
	btree.splitChild(root, child1)
	assert.Equal(t, 4, len(nodeStorage.nodeBuffer))
	assert.False(t, nodeStorage.nodeBuffer[0].IsLeaf)
	assert.Equal(t, [][]byte{[]byte("Ashley"), []byte("Charles")}, nodeStorage.nodeBuffer[0].Keys)
	assert.Equal(t, []uint32{1, 3, 2}, nodeStorage.nodeBuffer[0].Children)
	assert.True(t, nodeStorage.nodeBuffer[1].IsLeaf)
	assert.Equal(t, [][]byte{[]byte("Andrew"), []byte("Anthony")}, nodeStorage.nodeBuffer[1].Keys)
	assert.True(t, nodeStorage.nodeBuffer[3].IsLeaf)
	assert.Equal(t, [][]byte{
		[]byte("Ashley"),
		[]byte("Barbara"),
		[]byte("Betty"),
	}, nodeStorage.nodeBuffer[3].Keys)
	assert.True(t, nodeStorage.nodeBuffer[2].IsLeaf)
	assert.Equal(t, [][]byte{
		[]byte("Charles"),
		[]byte("Daniel"),
	}, nodeStorage.nodeBuffer[2].Keys)
}

func TestSplitInternalChild(t *testing.T) {
	nodeStorage := &tInMemoryNodeStorage{nodeBuffer: []*storage.TNode{}}
	btree := TPagedBTree{nodeStorage: nodeStorage}
	root, _ := nodeStorage.AllocateNode(false) // id: 0
	root.Keys = [][]byte{[]byte("Charles")}
	root.Children = []uint32{1, 2}               // ids: 1, 2
	child1, _ := nodeStorage.AllocateNode(false) // id: 1
	child1.Keys = [][]byte{
		[]byte("Andrew"),
		[]byte("Anthony"),
		[]byte("Ashley"),
		[]byte("Barbara"),
		[]byte("Betty"),
	}
	child1.Children = []uint32{3, 4, 5, 6, 7, 8}
	child2, _ := nodeStorage.AllocateNode(false) // id: 2
	child2.Keys = [][]byte{
		[]byte("Charles"),
		[]byte("Daniel"),
	}
	child2.Children = []uint32{9, 10, 11}
	for i := 3; i < 12; i++ {
		nodeStorage.AllocateNode(true)
	}
	assert.Equal(t, 12, len(nodeStorage.nodeBuffer))
	btree.splitChild(root, child1)
	assert.Equal(t, 13, len(nodeStorage.nodeBuffer))
	// 0
	assert.False(t, nodeStorage.nodeBuffer[0].IsLeaf)
	assert.Equal(t, [][]byte{[]byte("Ashley"), []byte("Charles")}, nodeStorage.nodeBuffer[0].Keys)
	assert.Equal(t, []uint32{1, 12, 2}, nodeStorage.nodeBuffer[0].Children)
	// 1
	assert.False(t, nodeStorage.nodeBuffer[1].IsLeaf)
	assert.Equal(t, [][]byte{[]byte("Andrew"), []byte("Anthony")}, nodeStorage.nodeBuffer[1].Keys)
	assert.Equal(t, []uint32{3, 4, 5}, nodeStorage.nodeBuffer[1].Children)
	// 12
	assert.False(t, nodeStorage.nodeBuffer[12].IsLeaf)
	assert.Equal(t, [][]byte{
		[]byte("Ashley"),
		[]byte("Barbara"),
		[]byte("Betty"),
	}, nodeStorage.nodeBuffer[12].Keys)
	assert.Equal(t, []uint32{util.MaxUint32, 6, 7, 8}, nodeStorage.nodeBuffer[12].Children)
	// 2
	assert.False(t, nodeStorage.nodeBuffer[2].IsLeaf)
	assert.Equal(t, [][]byte{
		[]byte("Charles"),
		[]byte("Daniel"),
	}, nodeStorage.nodeBuffer[2].Keys)
	assert.Equal(t, []uint32{9, 10, 11}, nodeStorage.nodeBuffer[2].Children)
}

func TestPutIntoEmpty(t *testing.T) {
	nodeStorage := &tInMemoryNodeStorage{nodeBuffer: []*storage.TNode{}}
	btree := MakePagedBTree(nodeStorage, 5)
	root, _ := nodeStorage.AllocateNode(true) // id: 0
	nodeStorage.SetRootNode(root)
	assert.Empty(t, btree.Put([]byte("Charles"), []byte("CharlesData")))
	assert.Equal(t, 1, len(nodeStorage.nodeBuffer))
	// 0
	assert.True(t, nodeStorage.nodeBuffer[0].IsLeaf)
	assert.Equal(t, nodeStorage.nodeBuffer[0].Keys[0], []byte("Charles"))
	assert.Equal(t, nodeStorage.nodeBuffer[0].Values[0], []byte("CharlesData"))
	assert.Equal(t, 0, len(nodeStorage.nodeBuffer[0].Children))
}
*/
