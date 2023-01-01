package btree

type IBTree interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
}
