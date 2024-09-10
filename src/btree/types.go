package btree

import "blackdb/src/pager"

type BtreePage struct {
	pager.PageHeader
}

type NodeComponent struct {
	Key         []byte
	keyval      []byte // full key value pair as bytes
	LeftPointer uint16
}

const (
	Degree    = 17
	MaxChild  = 2 * Degree
	UNDERFLOW = Degree - 1
	NODEFULL  = 2*Degree - 1
)
