package btree

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TReader1 struct {
	data   []byte
	curPos int
}

func (r *TReader1) Read(buf []byte) (n int, err error) {
	n = copy(buf, r.data[r.curPos:])
	r.curPos += n
	if r.curPos == len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

func TestCompareEq(t *testing.T) {
	data := []byte{'a', 'b', 'c', '1'}
	for chunkSize := 1; chunkSize < len(data)+1; chunkSize++ {
		rel, err := compare([]byte{'a', 'b', 'c', '1'}, &TReader1{data: []byte{'a', 'b', 'c', '1'}}, chunkSize)
		assert.Empty(t, err)
		assert.Equal(t, int8(0), rel)
	}
}

func TestCompareLess(t *testing.T) {
	data1 := []byte{'a', 'b', 'c', '0'}
	data2 := []byte{'a', 'b', 'c', '1'}
	for chunkSize := 1; chunkSize < len(data1)+1; chunkSize++ {
		rel, err := compare(data1, &TReader1{data: data2}, chunkSize)
		assert.Empty(t, err)
		assert.Equal(t, int8(-1), rel)
	}
}

func TestCompareGreater(t *testing.T) {
	data1 := []byte{'a', 'b', 'c', '1'}
	data2 := []byte{'a', 'b', 'c', '0'}
	for chunkSize := 1; chunkSize < len(data1)+1; chunkSize++ {
		rel, err := compare(data1, &TReader1{data: data2}, chunkSize)
		assert.Empty(t, err)
		assert.Equal(t, int8(1), rel)
	}
}

func TestCompareLessBySize(t *testing.T) {
	data1 := []byte{'a', 'b', 'c'}
	data2 := []byte{'a', 'b', 'c', '0'}
	for chunkSize := 1; chunkSize < len(data2)+1; chunkSize++ {
		rel, err := compare(data1, &TReader1{data: data2}, chunkSize)
		assert.Empty(t, err)
		assert.Equal(t, int8(-1), rel)
	}
}

func TestCompareGreaterBySize(t *testing.T) {
	data1 := []byte{'a', 'b', 'c', '0'}
	data2 := []byte{'a', 'b', 'c'}
	for chunkSize := 1; chunkSize < len(data1)+1; chunkSize++ {
		rel, err := compare(data1, &TReader1{data: data2}, chunkSize)
		assert.Empty(t, err)
		assert.Equal(t, int8(1), rel)
	}
}
