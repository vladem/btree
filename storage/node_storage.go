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

func MakeNodeStorage(config TConfig) (INodeStorage, error) {
	file, err := os.OpenFile(config.FilePath, os.O_RDWR, 0)
	if os.IsNotExist(err) {
		file, err = os.Create(config.FilePath)
		if err != nil {
			return nil, err
		}
		storage := &tOnDiskNodeStorage{
			config:      config,
			file:        file,
			nextPageId:  0,
			freePageIds: []uint32{},
		}
		root, err := storage.allocateRootNode()
		if err != nil {
			return nil, err
		}
		storage.writeHeader()
		storage.rootNode = root
		return storage, nil
	}
	if err != nil {
		return nil, err
	}
	storage := &tOnDiskNodeStorage{
		config:      config,
		file:        file,
		freePageIds: []uint32{},
	}
	err = storage.readHeader()
	if err != nil {
		return nil, err
	}
	err = storage.detectFreePages()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/******************* PRIVATE *******************/
func (s *tOnDiskNodeStorage) allocateNode(isLeaf bool) (*tNode, error) {
	if len(s.freePageIds) == 0 {
		if err := s.allocateNewBatch(); err != nil {
			return nil, err
		}
	}
	nodeId := s.freePageIds[0]
	s.freePageIds = s.freePageIds[1:]
	// todo: allocate raw buffer?
	return &tNode{id: nodeId, isLeaf: isLeaf}, nil
}

func (s *tOnDiskNodeStorage) loadNode(id uint32) (*tNode, error) {
	/*
		if s.file == nil {
			log.Fatalf("failed to read page [%v], uninitialized", id)
		}
		raw := make([]byte, r.config.SizeBytes)
		read, err := r.file.ReadAt(raw, int64(r.config.SizeBytes*id+fileHeaderSize))
		if err != nil {
			return nil, fmt.Errorf("failed to read the page, id [%v], error [%v]", id, err)
		}
		if uint32(read) != r.config.SizeBytes {
			return nil, fmt.Errorf("not enough bytes to read the page, id [%v], expected [%v], read [%v]", id, r.config.SizeBytes, read)
		}
		r.stats.ReadCalls += 1
		r.stats.BytesRead += uint32(read)
		buffer := raw
		var (
			isLeaf         bool
			cellsCnt       int64
			rightMostChild uint32
		)
		version, read := binary.Varint(buffer)
		if read <= 0 {
			return nil, fmt.Errorf("failed to parse header, version varint, pageId [%v], ret [%v]", id, read)
		}
		if version != 1 {
			return nil, fmt.Errorf("unsupported page version [%v], pageId [%v]", version, id)
		}
		buffer = buffer[read:]
		if len(buffer) == 0 {
			return nil, fmt.Errorf("failed to parse header, node type, pageId [%v]", id)
		}
		isLeaf = buffer[0] != 0
		buffer = buffer[1:]
		cellsCnt, read = binary.Varint(buffer)
		if read <= 0 {
			return nil, fmt.Errorf("failed to parse header, cellsCnt varint, ret [%v], pageId [%v]", read, id)
		}
		buffer = buffer[read:]
		if !isLeaf {
			rightMostChildInt, read := binary.Varint(buffer)
			if read <= 0 {
				return nil, fmt.Errorf("failed to parse header, rightmost child varint, ret [%v], pageId [%v]", read, id)
			}
			rightMostChild = uint32(rightMostChildInt)
			buffer = buffer[read:]
		}
		offsets := []tCellOffsets{}
		for i := 0; i < int(cellsCnt); i++ {
			var sOffset, eOffset int64
			sOffset, read = binary.Varint(buffer)
			if read <= 0 {
				return nil, fmt.Errorf("failed to parse start offset, i [%v], ret [%v], pageId [%v]", i, read, id)
			}
			buffer = buffer[read:]
			eOffset, read = binary.Varint(buffer)
			if read <= 0 {
				return nil, fmt.Errorf("failed to parse end offset, i [%v], ret [%v], pageId [%v]", i, read, id)
			}
			buffer = buffer[read:]
			offsets = append(offsets, tCellOffsets{Start: uint32(sOffset), End: uint32(eOffset)})
		}
		return &tPage{
			id:             id,
			isLeaf:         isLeaf,
			cellOffsets:    offsets,
			rightMostChild: rightMostChild,
			raw:            raw,
		}, nil
	*/
	return nil, errors.New("not implemented")
}

func (s *tOnDiskNodeStorage) allocateRootNode() (*tNode, error) {
	return nil, errors.New("not implemented")
}

func (s *tOnDiskNodeStorage) readHeader() error {
	header := make([]byte, fileHeaderSizeBytes)
	read, err := s.file.ReadAt(header, 0)
	if err != nil {
		return fmt.Errorf("failed to read file header, error [%v]", err)
	}
	if uint32(read) != fileHeaderSizeBytes {
		return fmt.Errorf("not enough bytes to read file header, expected [%v], read [%v]", fileHeaderSizeBytes, read)
	}
	layoutVersion := binary.BigEndian.Uint32(header[:4])
	if layoutVersion != 1 {
		return fmt.Errorf("usupported layout version", layoutVersion)
	}
	rootNodeId := binary.BigEndian.Uint32(header[4:])
	s.rootNode, err = s.loadNode(rootNodeId)
	if err != nil {
		return err
	}
	return nil
}

func (s *tOnDiskNodeStorage) writeHeader() error {
	buf := []byte{}
	binary.BigEndian.PutUint32(buf, FileLayoutVersion)
	binary.BigEndian.PutUint32(buf, s.rootNode.id)
	_, err := s.file.WriteAt(buf, 0)
	return err
}

func (s *tOnDiskNodeStorage) allocateNewBatch() error {
	batchSize := 100
	pages := make([]byte, pageHeaderSizeBytes*batchSize)
	written, err := s.file.WriteAt(pages, int64(fileHeaderSizeBytes+s.nextPageId))
	if err != nil || written != int(pageHeaderSizeBytes*batchSize) {
		return fmt.Errorf("failed to allocate new batch, err [%v], written [%v]", err, written)
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
		read, err := s.file.ReadAt(pageFlags, int64(s.config.PageSizeBytes*pageId+fileHeaderSizeBytes))
		if err != nil {
			return fmt.Errorf("failed to read the page, id [%v], error [%v]", pageId, err)
		}
		if uint32(read) != 1 {
			return fmt.Errorf("not enough bytes to read the page, id [%v], expected [1], read [%v]", pageId, read)
		}
		if !checkBit(pageFlags[0], 0) {
			s.freePageIds = append(s.freePageIds, pageId)
		}
	}
	return nil
}
