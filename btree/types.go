package btree

import "blackdb/pager"

type BtreePage struct {
	pager.PageHeader
}

type NodeComponent struct {
	// key         uint64
	// using key directly as bytes
	// VAL: here
	key         []byte
	val         []byte
	LeftPointer uint16
}

const (
	Degree    = 87
	MaxChild  = 2 * Degree
	UNDERFLOW = Degree - 1
	NODEFULL  = 2*Degree - 1
)
