package pager

import (
	Init "blackdb/init"
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
	Degree   = 5
	MaxChild = 2 * Degree
	MINCHILD = int(Degree / 2)
	NODEFULL = 2*Degree - 1
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
				return page.AddCell(buf, AddCellOptions{Index: &i})
			}
		}
		slots = append(slots, res)
	}
	fmt.Println(slots)
	return page.AddCell(buf)
}

func (node *PageHeader) Insert(val uint64) {
	if node.pageType == ROOT_AND_LEAF && node.numSlots == 0 {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf[0:], val)
		node.AddCell(buf)

	} else if node.numSlots == NODEFULL {
		// TODO: split pages for it

		fmt.Println("node is full ")
		node.SplitPages()

	} else {
		node.InsertNonfull(val)
	}
}

func (node *PageHeader) SplitPages() {
	keys := node.GetKeys()
	splitVal := keys[(Degree - 1)]
	if node.pageType == ROOT_AND_LEAF {
		newPage, _ := MakePage(2, uint16(Init.TOTAL_PAGES))
		for i := (Degree); i < len(keys); i++ {
			resp := make([]byte, 8)
			binary.BigEndian.PutUint64(resp, keys[i])
			newPage.AddCell(resp)
		}
		LoadPage(1)
		node.RangeRemoveSlots(Degree-1, uint(node.numSlots))
		// BUG: might be some bug here
		rootpage, _ := MakePage(0, uint16(Init.TOTAL_PAGES))
		res := make([]byte, 8)
		binary.BigEndian.PutUint64(res, uint64(splitVal))
		// TODO: add the old page as the left pointer and right pointer as the newpage
		rootpage.AddCell(res, AddCellOptions{LeftPointer: &node.pageId})
		LoadPage(1)
		fmt.Println("old page has these keys:", node.GetKeys())
		LoadPage(2)
		fmt.Println("new page has these keys:", newPage.GetKeys())
		LoadPage(3)
		fmt.Println("root page has these keys:", rootpage.GetKeys())

	}

}
