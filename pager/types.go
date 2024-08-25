package pager

type PageType uint8

const (
	ROOTPAGE = iota
	INTERIOR
	LEAF
	ROOT_AND_LEAF
)

// PageHeader represents a page in the database.
// It contains the following fields:
//
//	PageId: The unique identifier for this page
//	NumSlots: The number of slots currently in use
//	Data: The actual data stored in the page
type PageHeader struct {
	// FIXME: might need to remove the pageId
	PageId         uint16
	PageType       PageType
	freeStart      uint16
	freeEnd        uint16
	totalFree      uint16
	NumSlots       uint16
	lastOffsetUsed uint16
	RightPointer   uint16
	flags          uint8
}
type OverflowPageHeader struct {
	next uint16
	size uint16
}

// InsertSlot inserts a new slot. See [PageHeader.InsertSlot] for locating slots.
type OverflowPtr struct {
	payload []byte
	ptr     uint32
}

type CellHeader struct {
	cellLoc    uint16
	cellSize   uint16
	isOverflow bool
	// TODO: implement the leftmost child pointer for the given key
	LeftChild uint16
}
type Cell struct {
	Header      CellHeader
	CellContent []byte
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
	PageNum uint
	// more can be added here
}

// can make the freelist page here or can make the freelist page in the init flolder
type FreelistPage struct {
	NextPage   uint16
	TotalPages uint16
}
