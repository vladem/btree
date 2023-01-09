package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

const FileLayoutVersion uint32 = 1

type tOnDiskNodeStorage struct {
	rootNode    *TNode
	file        *os.File
	nextPageId  uint32
	freePageIds []uint32
}

func (s *tOnDiskNodeStorage) RootNode() (*TNode, error) {
	if s.rootNode == nil {
		return nil, errors.New("failed to load root node")
	}
	return s.rootNode, nil
}

func (s *tOnDiskNodeStorage) SetRootNode(newRoot *TNode) error {
	return errors.New("not implemented")
}

func (s *tOnDiskNodeStorage) AllocateNode(isLeaf bool) (*TNode, error) {
	if len(s.freePageIds) == 0 {
		if err := s.allocateNewBatch(); err != nil {
			return nil, err
		}
	}
	nodeId := s.freePageIds[0]
	s.freePageIds = s.freePageIds[1:]
	node := &TNode{Id: nodeId, Keys: [][]byte{}, IsLeaf: isLeaf}
	if isLeaf {
		node.Children = []uint32{}
	} else {
		node.Values = [][]byte{}
	}
	return node, nil
}

func (s *tOnDiskNodeStorage) LoadNode(id uint32) (*TNode, error) {
	return nil, errors.New("not implemented")
}

func (s *tOnDiskNodeStorage) SaveNode(node *TNode) error {
	return errors.New("not implemented")
}

func (s *tOnDiskNodeStorage) writeHeader() error {
	buf := []byte{}
	binary.BigEndian.PutUint32(buf, FileLayoutVersion)
	binary.BigEndian.PutUint32(buf, s.rootNode.Id)
	_, err := s.file.Write(buf)
	return err
}

func (s *tOnDiskNodeStorage) allocateNewBatch() error {
	emptyPage := []byte{}

}

func MakeNodeStorage(pageConfig TPageConfig) (INodeStorage, error) {
	file, err := os.OpenFile(pageConfig.FilePath, os.O_RDWR, 0)
	if os.IsNotExist(err) {
		file, err = os.Create(pageConfig.FilePath)
		if err != nil {
			return nil, err
		}
		storage := &tOnDiskNodeStorage{
			file:        file,
			nextPageId:  0,
			freePageIds: []uint32{},
		}
		storage.writeHeader()
		root, err := storage.AllocateNode(true)
		if err != nil {
			return nil, err
		}
		storage.rootNode = root
		return storage, nil
	}
	if err != nil {
		return nil, err
	}
	// todo: read file header
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if (info.Size()-fileHeaderSize)%int64(pageConfig.SizeBytes) != 0 {
		return nil, fmt.Errorf("invalid size [%v] of the file [%v]", info.Size(), file.Name())
	}
	//maxPageId := uint32((info.Size()-fileHeaderSize)/int64(pageConfig.SizeBytes) - 1)
	return nil, nil
}
