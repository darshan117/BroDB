package pager

type PageType uint8

const (
	ROOTPAGE = iota
	INTERIOR
	LEAF
)

type PageHeader struct {
	pageId    uint32
	pageType  PageType
	freeStart uint16 // start of freespace
	freeEnd   uint16
	totalFree uint16
	numSlots  uint16
	flags     uint8
	// TODO: add the rightmost child pointer
	// also last offset used
	// number of slots used
}

type CellHeader struct {
	cellLoc    uint16
	cellSize   uint16
	isOverflow bool
	// TODO: implement the leftmost child pointer for the given key
}
type Cell struct {
	header      CellHeader
	cellContent []byte
}

type PointerList struct {
	offset uint16
	size   uint16
}

type BufPage struct {
	Data    []byte
	pageNum uint
	// more can be added here
}
