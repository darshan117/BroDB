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

type NodeComponent struct {
	// key         uint64
	// using key directly as bytes
	key         []byte
	LeftPointer uint16
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
				return page, page.AddCell(buf, AddCellOptions{Index: &i})
			}
		}
		return page, page.AddCell(buf)
	}
	// FIXME: might remove the below if statement
	if page.pageType == ROOTPAGE || page.pageType == INTERIOR {
		// can keep it zero as well

		for i, val := range page.GetSlots() {
			cell := page.GetCellByOffset(val)
			// check if the page is full
			res := binary.BigEndian.Uint64(cell.cellContent)
			if res > key {
				// break and get the value i
				// fmt.Println(index)
				leftcell, _ := page.GetCell(uint(i))
				childPage, err := GetPage(uint(leftcell.header.leftChild))
				fmt.Println("num of slots are ", childPage.numSlots)

				if err != nil {
					return nil, err
				}
				if childPage.numSlots == NODEFULL {
					// add index here add return
					page.SplitPages(i, cell.header.leftChild)
				} else if leftcell.header.leftChild != uint16(0) {
					page = childPage
					return page.InsertNonfull(key)

				}

			}
		}

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
		return RootNode.Insert(val)

	}
	if node.pageType == ROOT_AND_LEAF && node.numSlots == 0 {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf[0:], val)
		// FIXME: error statement here
		node.AddCell(buf)

	} else if node.numSlots == NODEFULL {
		// TODO: split pages for it
		fmt.Println("node is full ")
		root, err := node.SplitRootPages()
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
		return node, nil
	}
	return nil, nil
}

func (node *PageHeader) SplitPages(index int, splitpage uint16) (*PageHeader, error) {
	// should handle both cases of interior node and leaf node
	// splitPage, err := GetPage(uint(index))
	// if err != nil {
	// 	return nil, err
	// }
	// splitVal := splitPage.GetKeys()[Degree-1]
	// get its left cell
	leftcell, err := node.GetCell(uint(index))
	if err != nil {
		return nil, err
	}
	childPage, err := GetPage(uint(splitpage))
	if err != nil {
		return nil, err
	}
	// fmt.Println("splitVal is ", splitVal, splitPage.GetKeys())
	keyPairs := make([]NodeComponent, 0)

	for _, v := range childPage.GetSlots()[:Degree-1] {
		cell := childPage.GetCellByOffset(v)
		// key := binary.BigEndian.Uint64(cell.cellContent)
		keyPairs = append(keyPairs, NodeComponent{
			key:         cell.cellContent,
			LeftPointer: cell.header.leftChild,
		})
	}
	var ptype PageType
	switch childPage.pageType {
	case LEAF:
		ptype = LEAF
	case INTERIOR:
		ptype = INTERIOR
	default:
		ptype = LEAF
	}
	newPage, err := MakePage(ptype, uint16(Init.TOTAL_PAGES))
	if err != nil {
		return nil, err
	}
	for _, v := range keyPairs {
		// This should be a newPage
		newPage.AddCell(v.key, AddCellOptions{LeftPointer: &v.LeftPointer})
	}
	fmt.Printf("New Page %+v %d\n ", newPage.GetKeys(), index)
	// check if degree or degree -1
	// childpage becomes the leftpointer to the inserted node
	// RangeRemove not working right
	fmt.Printf("Child Page %+v %d\n ", childPage.GetKeys(), index)
	childPage.EndRangeRemoveSlots(0, Degree)

	fmt.Printf("Child Page %+v %d\n ", childPage.SlotArray(), index)
	// 0 to splitval copy all the cells with their left pointer to the newPage
	leftcell.UpdateLeftPointer(uint(newPage.pageId))

	// fix pointers is hard
	// node.fixPointer() here node.inserted new key  will have left pointer to the newPage
	// newPage.rightPointer to node.leftPointer

	// insert the split val to node
	// TODO: node.inset(splitVal) which will insert the

	// if append to last then right child is right pointer or else
	// make a parent insert function necessary
	// how will it work if append to last then right child is right pointer
	// if insert inbetween then next key will get the left pointer as the  after values of split (range remove 0 to splitval index)
	// splitkey will has starting vals to the left pointer

	// TODO: make a seperate function to update left pointer and update right pointer
	return nil, nil

}

func (node *PageHeader) SplitRootPages() (*PageHeader, error) {
	// should handle the pointers as well
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
		node.StartRangeRemoveSlots(Degree-1, uint(node.numSlots))
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
		return rootpage, nil

	} else if node.pageType == LEAF {

	}
	return node, nil

}
