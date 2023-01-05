package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

func (r *tPageManager) Init() error {
	if r.file != nil {
		return fmt.Errorf("reader has been already initialized with file [%v]", r.file.Name())
	}
	var err error
	r.file, err = os.Open(r.config.FilePath)
	if err != nil {
		return err
	}
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	// todo: read file header
	if (info.Size()-fileHeaderSize)%int64(r.config.SizeBytes) != 0 {
		return fmt.Errorf("invalid size [%v] of the file [%v]", info.Size(), r.file.Name())
	}
	r.maxPageId = uint32((info.Size()-fileHeaderSize)/int64(r.config.SizeBytes) - 1)
	return nil
}

func (r *tPageManager) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

func (r *tPageManager) Read(id uint32) (IPage, error) {
	if r.file == nil {
		return nil, fmt.Errorf("failed to read page [%v], uninitialized", id)
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
}

func (r *tPageManager) NewPage(isLeaf bool, rightMostChild uint32) (IPage, error) {
	return nil, errors.New("not implemented")
}

func (r *tPageManager) GetStatistics() *tPageManagerStatistics {
	return r.stats
}
