package btree

type IBTree interface {
	Get(key []byte) []byte
	Put(key, value []byte)
}
