package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

/******************* PUBLIC *******************/
const FileLayoutVersion uint32 = 1

func (s *tOnDiskNodeStorage) RootNode() INode {
	return s.rootNode
}

func (s *tOnDiskNodeStorage) AllocateRootNode() (INode, error) {
	return s.allocateRootNode()
}

func (s *tOnDiskNodeStorage) AllocateNode(isLeaf bool) (INode, error) {
	return s.allocateNode(isLeaf)
}

func (s *tOnDiskNodeStorage) LoadNode(id uint32) (INode, error) {
	return s.loadNode(id)
}

func (s *tOnDiskNodeStorage) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *tOnDiskNodeStorage) Statistics() *TStorageStatistics {
	return s.stats
}

func fileExists(filePath string) (bool, error) {
	info, err := os.Stat(filePath)
	if err == nil {
		return !info.IsDir(), nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func MakeNodeStorage(config TConfig) (INodeStorage, error) {
	exists, err := fileExists(config.FilePath)
	if err != nil {
		return nil, err
	}
	if !exists {
		file, err := os.Create(config.FilePath)
		if err != nil {
			return nil, err
		}
		storage := &tOnDiskNodeStorage{
			config:      config,
			file:        file,
			nextPageId:  0,
			freePageIds: []uint32{},
			stats:       &TStorageStatistics{},
		}
		root, err := storage.allocateRootNode()
		if err != nil {
			return nil, err
		}
		storage.rootNode = root
		return storage, nil
	}
	file, err := os.OpenFile(config.FilePath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	storage := &tOnDiskNodeStorage{
		config:      config,
		file:        file,
		freePageIds: []uint32{},
		stats:       &TStorageStatistics{},
	}
	if err := storage.readHeader(); err != nil {
		return nil, err
	}
	if err := storage.detectFreePages(); err != nil {
		return nil, err
	}
	return storage, nil
}

/******************* PRIVATE *******************/
func (s *tOnDiskNodeStorage) writeAt(data []byte, offset int64) error {
	if s.file == nil {
		return errors.New("already closed")
	}
	written, err := s.file.WriteAt(data, offset)
	if err != nil {
		return err
	}
	if written != len(data) {
		return errors.New("written less than expected")
	}
	s.stats.WriteCalls += 1
	s.stats.BytesWritten += uint32(len(data))
	return nil
}

func (s *tOnDiskNodeStorage) readAt(data []byte, offset int64) error {
	expectedToRead := len(data)
	read, err := s.file.ReadAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to read, error [%v]", err)
	}
	if read != expectedToRead {
		return fmt.Errorf("read less than expected, [%v]/[%v]", read, expectedToRead)
	}
	s.stats.ReadCalls += 1
	s.stats.BytesRead += uint32(read)
	return nil
}

func (s *tOnDiskNodeStorage) allocateNode(isLeaf bool) (*tNode, error) {
	if len(s.freePageIds) == 0 {
		if err := s.allocateNewBatch(); err != nil {
			return nil, err
		}
	}
	nodeId := s.freePageIds[0]
	s.freePageIds = s.freePageIds[1:]
	return makeNode(nodeId, isLeaf, s), nil
}

func (s *tOnDiskNodeStorage) loadNode(id uint32) (*tNode, error) {
	if s.file == nil {
		return nil, errors.New("already closed")
	}
	raw := make([]byte, s.config.PageSizeBytes)
	if err := s.readAt(raw, int64(s.config.PageSizeBytes*id+fileHeaderSizeBytes)); err != nil {
		return nil, err
	}
	return makeNodeFromRaw(id, raw, s)
}

func (s *tOnDiskNodeStorage) allocateRootNode() (*tNode, error) {
	newRoot, err := s.allocateNode(false)
	if err != nil {
		return nil, err
	}
	if s.rootNode != nil {
		newRoot.ReplaceChildren([]uint32{s.rootNode.Id()})
	} else {
		newRoot.isLeaf = true
	}
	s.rootNode = newRoot
	if err := s.writeHeader(); err != nil {
		return nil, err
	}
	return newRoot, nil
}

func (s *tOnDiskNodeStorage) readHeader() error {
	header := make([]byte, fileHeaderSizeBytes)
	if err := s.readAt(header, 0); err != nil {
		return err
	}
	layoutVersion := binary.BigEndian.Uint32(header[:4])
	if layoutVersion != 1 {
		return fmt.Errorf("usupported layout version [%v]", layoutVersion)
	}
	rootNodeId := binary.BigEndian.Uint32(header[4:])
	var err error
	s.rootNode, err = s.loadNode(rootNodeId)
	if err != nil {
		return err
	}
	return nil
}

func (s *tOnDiskNodeStorage) writeHeader() error {
	buf := []byte{}
	buf = binary.BigEndian.AppendUint32(buf, FileLayoutVersion)
	buf = binary.BigEndian.AppendUint32(buf, s.rootNode.id)
	return s.writeAt(buf, 0)
}

func (s *tOnDiskNodeStorage) allocateNewBatch() error {
	batchSize := 100
	pages := make([]byte, int(s.config.PageSizeBytes)*batchSize)
	if err := s.writeAt(pages, int64(fileHeaderSizeBytes+s.nextPageId)); err != nil {
		return err
	}
	for i := s.nextPageId; i < s.nextPageId+uint32(batchSize); i++ {
		s.freePageIds = append(s.freePageIds, i)
	}
	s.nextPageId += uint32(batchSize)
	return nil
}

func checkBit(flags byte, idx int) bool {
	flags = flags << idx
	flags = flags >> 7
	return flags == 1
}

func (s *tOnDiskNodeStorage) detectFreePages() error {
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	if (info.Size()-fileHeaderSizeBytes)%int64(s.config.PageSizeBytes) != 0 {
		return fmt.Errorf("invalid size [%v] of the file [%v]", info.Size(), s.file.Name())
	}
	s.nextPageId = uint32((info.Size()-fileHeaderSizeBytes)/int64(s.config.PageSizeBytes) - 1)
	for pageId := uint32(0); pageId < s.nextPageId; pageId++ {
		pageFlags := make([]byte, 1)
		if err := s.readAt(pageFlags, int64(s.config.PageSizeBytes*pageId+fileHeaderSizeBytes)); err != nil {
			return err
		}
		if !checkBit(pageFlags[0], 0) {
			s.freePageIds = append(s.freePageIds, pageId)
		}
	}
	return nil
}
