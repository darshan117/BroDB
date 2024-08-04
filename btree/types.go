package btree

import "blackdb/pager"

type BtreePage struct {
	pager.PageHeader
}

type NodeComponent struct {
	// key         uint64
	// using key directly as bytes
	key         []byte
	LeftPointer uint16
}

const (
	Degree    = 5
	MaxChild  = 2 * Degree
	UNDERFLOW = int(Degree / 2)
	NODEFULL  = 2*Degree - 1
)
