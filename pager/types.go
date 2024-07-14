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
	flags     uint8
}

type Cell struct {
	cellLoc  uint16
	cellSize uint16
}

type PointerList struct {
	start *Cell
	size  uint16
}

type BufPage struct {
	Data    []byte
	pageNum uint
	// more can be added here
}
