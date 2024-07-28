package pager

import (
	"encoding/binary"
	"fmt"
)

// [x] read the full wikipedia page on the btree
// [x] start with btree layout in the page
// [x] start small like set max btree childrens to like 60 bytes
// type Btree struct {
// 	page PageHeader
// }

const (
	// max child
	Degree   = 5
	MaxChild = 2 * Degree
	MINCHILD = int(Degree / 2)
	NODEFULL = 2*Degree - 1

// min childs
// degree
// leaf
)

func (page *PageHeader) InsertNonfull(key uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:], key)
	slots := make([]uint64, 0)
	for i, val := range page.GetSlots() {
		cell := page.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.cellContent)
		// FIXME: for now bufbytes
		if res > key {
			if page.pageType == LEAF || page.pageType == ROOT_AND_LEAF {
				return page.AddCell(buf, i)
			}
		}
		slots = append(slots, res)
	}
	fmt.Println(slots)
	return page.AddCell(buf)
}

func (page *PageHeader) Insert(val uint64) {
	if page.pageType == ROOT_AND_LEAF && page.numSlots == 0 {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf[0:], val)
		page.AddCell(buf)

	} else if page.numSlots == NODEFULL {
		// TODO: split pages for it
		fmt.Println("node is full ")
		slots := make([]uint64, 0)
		for _, val := range page.GetSlots() {
			cell := page.GetCellByOffset(val)
			res := binary.BigEndian.Uint64(cell.cellContent)
			slots = append(slots, res)
		}
		fmt.Println(slots)

	} else {
		page.InsertNonfull(val)
	}
}
