package pager

import (
	Init "blackdb/init"
	"encoding/binary"
	"fmt"
)

// [x] read the full wikipedia page on the btree
// [x] start with btree layout in the page
// [x] start small like set max btree childrens to like 60 bytes
type Btree struct {
	page *PageHeader
}

const (
	Degree   = 5
	MaxChild = 2 * Degree
	MINCHILD = int(Degree / 2)
	NODEFULL = 2*Degree - 1
)

func (page *PageHeader) InsertNonfull(key uint64) (*PageHeader, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:], key)
	if page.pageType == LEAF || page.pageType == ROOT_AND_LEAF {
		for i, val := range page.GetSlots() {
			cell := page.GetCellByOffset(val)
			res := binary.BigEndian.Uint64(cell.cellContent)
			if res > key {
				fmt.Printf("leaf page %+v\n ", page.GetKeys())
				// page.numSlots += 1
				return page, page.AddCell(buf, AddCellOptions{Index: &i})
			}
		}
		return page, page.AddCell(buf)
	}
	// FIXME: might remove the below if statement
	if page.pageType == ROOTPAGE || page.pageType == INTERIOR {
		for i, val := range page.GetSlots() {
			cell := page.GetCellByOffset(val)
			res := binary.BigEndian.Uint64(cell.cellContent)
			if res > key {
				leftcell, _ := page.GetCell(uint(i))
				fmt.Println("leftcell header has this leftChild", leftcell.header.leftChild)
				childPage, err := GetPage(uint(leftcell.header.leftChild))
				if err != nil {
					return nil, err
				}
				page = childPage
				return page.InsertNonfull(key)
			}
		}
		fmt.Printf("page %+v", page)
		if page.rightPointer != 0 {
			rightChildPage, err := GetPage(uint(page.rightPointer))
			if err != nil {
				return nil, err
			}
			return rightChildPage.InsertNonfull(key)
		}
	}
	// BUG: check for if it has right pointer or else page.addcell
	return nil, nil
}

func (node *PageHeader) Insert(val uint64) (*PageHeader, error) {
	// defer fmt.Printf("node page is .. %+v\n", node)

	// FIXME: check if node.pagetype is root or root and leaf else change the node to the rootnode
	if node.pageType != ROOTPAGE && node.pageType != ROOT_AND_LEAF {
		RootNode, err := GetPage(uint(Init.ROOTPAGE))
		if err != nil {
			return nil, fmt.Errorf("error while insert to the btree %w", err)
		}
		fmt.Printf("ROOTPAGE is %+v\n ", RootNode.GetKeys())
		return RootNode.Insert(val)

	}
	if node.pageType == ROOT_AND_LEAF && node.numSlots == 0 {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf[0:], val)
		node.AddCell(buf)

	} else if node.numSlots == NODEFULL {
		// TODO: split pages for it
		fmt.Println("node is full ")
		root, err := node.SplitPages()
		if err != nil {
			return nil, err
		}
		node = root
		// FIXME:
		// page, _ := GetPage(3)
		Newnode, err := node.InsertNonfull(val)
		if err != nil {
			fmt.Println(err)
		}
		node = Newnode
		return node, nil

	} else {
		Newnode, err := node.InsertNonfull(val)
		if err != nil {
			return nil, err
		}
		node = Newnode
		fmt.Printf("node page is %+v \n", node)
		return node, nil
	}
	return nil, nil
}

func (node *PageHeader) SplitPages() (*PageHeader, error) {
	defer node.UpdatePageHeader()
	keys := node.GetKeys()
	splitVal := keys[(Degree - 1)]
	if node.pageType == ROOT_AND_LEAF {
		newPage, err := MakePage(LEAF, uint16(Init.TOTAL_PAGES))
		if err != nil {
			return nil, err
		}
		for i := (Degree); i < len(keys); i++ {
			resp := make([]byte, 8)
			binary.BigEndian.PutUint64(resp, keys[i])
			newPage.AddCell(resp)
		}
		LoadPage(1)
		node.RangeRemoveSlots(Degree-1, uint(node.numSlots))
		// BUG: might be some bug here
		rootpage, err := MakePage(ROOTPAGE, uint16(Init.TOTAL_PAGES))
		if err != nil {
			return nil, err
		}
		res := make([]byte, 8)
		binary.BigEndian.PutUint64(res, uint64(splitVal))
		// TODO: add the old page as the left pointer and right pointer as the newpage
		node.pageType = LEAF
		if err := rootpage.AddCell(res, AddCellOptions{LeftPointer: &node.pageId}); err != nil {
			return nil, err
		}
		rootpage.rightPointer = newPage.pageId
		rootpage.UpdatePageHeader()
		return &rootpage, nil

	}
	return node, nil

}
