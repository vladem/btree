package storage

import "encoding/binary"

func (t *TTuple) Key() []byte {
	return t.key
}

func (t *TTuple) Value() []byte {
	return t.value
}

func (t *TTuple) ValueInt() uint32 {
	return binary.BigEndian.Uint32(t.value)
}
