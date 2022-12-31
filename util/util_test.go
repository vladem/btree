package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareEq(t *testing.T) {
	assert.Equal(t, int8(0), Compare([]byte{'a', 'b', 'c', '1'}, []byte{'a', 'b', 'c', '1'}))
}

func TestCompareLess(t *testing.T) {
	assert.Equal(t, int8(-1), Compare([]byte{'a', 'b', 'c', '0'}, []byte{'a', 'b', 'c', '1'}))
}

func TestCompareGreater(t *testing.T) {
	assert.Equal(t, int8(1), Compare([]byte{'a', 'b', 'c', '1'}, []byte{'a', 'b', 'c', '0'}))
}

func TestCompareLessBySize(t *testing.T) {
	assert.Equal(t, int8(-1), Compare([]byte{'a', 'b', 'c'}, []byte{'a', 'b', 'c', 0}))
}

func TestCompareGreaterBySize(t *testing.T) {
	assert.Equal(t, int8(1), Compare([]byte{'a', 'b', 'c', 0}, []byte{'a', 'b', 'c'}))
}
