package pager

type PageType uint8

const (
	ROOTPAGE = iota
	INTERIOR
	LEAF
	ROOT_AND_LEAF
)

type PageHeader struct {
	// FIXME: might need to remove the pageId
	pageId         uint16
	pageType       PageType
	freeStart      uint16
	freeEnd        uint16
	totalFree      uint16
	numSlots       uint16
	lastOffsetUsed uint16
	rightPointer   uint16
	flags          uint8
}
type OverflowPageHeader struct {
	next uint16
	size uint16
}
type OverflowPtr struct {
	payload []byte
	ptr     uint32
}

type CellHeader struct {
	cellLoc    uint16
	cellSize   uint16
	isOverflow bool
	// TODO: implement the leftmost child pointer for the given key
	leftChild uint16
}
type Cell struct {
	header      CellHeader
	cellContent []byte
}
type AddCellOptions struct {
	Index       *int
	LeftPointer *uint16
}

type PointerList struct {
	index    uint16
	size     uint16
	contents []byte
}

type BufPage struct {
	Data    []byte
	pageNum uint
	// more can be added here
}
