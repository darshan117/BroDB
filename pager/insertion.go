package pager

import (
	coreAlgo "blackdb/core_algo"
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
	if BufData.pageNum != uint(page.pageId) {
		GetPage(uint(page.pageId))
	}
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
		for i, val := range page.GetSlots() {
			cell := page.GetCellByOffset(val)
			res := binary.BigEndian.Uint64(cell.cellContent)
			if res > key {
				leftcell, _ := page.GetCell(uint(i))
				childPage, err := GetPage(uint(leftcell.header.leftChild))
				if err != nil {
					return nil, err
				}
				if childPage.numSlots == NODEFULL {
					page.SplitPagesLeft(i, cell.header.leftChild)
					return page.InsertNonfull(key)
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
			if rightChildPage.numSlots == NODEFULL {
				rightChildPage.SplitPagesRightAndInsert(page, key)
				return page.InsertNonfull(key)

			}
			page = rightChildPage

			return page.InsertNonfull(key)
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

	} else if node.numSlots == NODEFULL && (node.pageType == ROOTPAGE || node.pageType == ROOT_AND_LEAF) {
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

func (childPage *PageHeader) SplitPagesRightAndInsert(node *PageHeader, key uint64) (*PageHeader, error) {
	defer childPage.UpdatePageHeader()
	splitVal := childPage.GetKeys()[Degree-1]
	keyPairs := make([]NodeComponent, 0)

	for _, v := range childPage.GetSlots()[Degree:childPage.numSlots] {
		cell := childPage.GetCellByOffset(v)
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
		newPage.AddCell(v.key, AddCellOptions{LeftPointer: &v.LeftPointer})
	}
	childPage.StartRangeRemoveSlots(Degree-1, uint(childPage.numSlots))
	node.Insertkey(splitVal, childPage.pageId)
	node.rightPointer = newPage.pageId
	newPage.rightPointer = childPage.rightPointer
	childPage.rightPointer = 0
	childPage.UpdatePageHeader()
	newPage.UpdatePageHeader()
	node.UpdatePageHeader()
	return nil, nil
}

// --------

func (node *PageHeader) SplitPagesLeft(index int, splitpage uint16) (*PageHeader, error) {
	childPage, err := GetPage(uint(splitpage))
	if err != nil {
		return nil, err
	}
	splitVal := childPage.GetKeys()[Degree-1]
	keyPairs := make([]NodeComponent, 0)

	for _, v := range childPage.GetSlots()[:Degree-1] {
		cell := childPage.GetCellByOffset(v)
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
		newPage.AddCell(v.key, AddCellOptions{LeftPointer: &v.LeftPointer})
	}
	childPage.EndRangeRemoveSlots(0, Degree)
	node.Insertkey(uint64(splitVal), newPage.pageId)
	return nil, nil
}

func (node *PageHeader) SplitRootPages() (*PageHeader, error) {
	defer node.UpdatePageHeader()
	// get the splitval with its left pointer
	if node.pageType == ROOT_AND_LEAF || node.pageType == ROOTPAGE {

		// can make it static type slice
		keys := make([]NodeComponent, 0)
		splitVal := node.GetKeys()[Degree-1]
		splitcell, err := node.GetCell(Degree - 1)
		splitLeftkey := splitcell.header.leftChild
		var ptype PageType
		if splitLeftkey == 0 {
			ptype = LEAF

		} else {
			ptype = INTERIOR
		}

		if err != nil {
			return nil, err
		}
		newPage, err := MakePage(ptype, uint16(Init.TOTAL_PAGES))
		if err != nil {
			return nil, err
		}

		for _, val := range node.GetSlots()[Degree:] {
			cell := node.GetCellByOffset(val)
			keys = append(keys, NodeComponent{key: cell.cellContent, LeftPointer: cell.header.leftChild})

		}
		fmt.Println("keys are ", keys)
		for _, v := range keys {
			newPage.AddCell(v.key, AddCellOptions{LeftPointer: &v.LeftPointer})
		}
		// traverse to the leaf using its rightPointer only and append it there
		// TODO: make a seperate such function

		// for i := (Degree); i < len(keys); i++ {
		// 	resp := make([]byte, 8)
		// 	binary.BigEndian.PutUint64(resp, keys[i])
		// 	// here adding the keys with its leftPointer
		// 	newPage.AddCell(resp)
		// }
		// FIXME: this is actually end remove
		node.StartRangeRemoveSlots(Degree-1, uint(node.numSlots))

		node.UpdateRightPointer(uint(splitLeftkey), newPage)
		// nodecell, _ := node.GetCell(3)
		newp, _ := GetPage(6)
		fmt.Printf("split page is %+v\n", newp.GetKeys())
		// BUG: might be some bug here
		rootpage, err := MakePage(ROOTPAGE, uint16(Init.TOTAL_PAGES))
		if err != nil {
			return nil, err
		}
		res := make([]byte, 8)
		binary.BigEndian.PutUint64(res, uint64(splitVal))
		// TODO: add the old page as the left pointer and right pointer as the newpage
		node.pageType = LEAF
		if splitLeftkey != 0 {
			node.pageType = INTERIOR
		}
		if err := rootpage.AddCell(res, AddCellOptions{LeftPointer: &node.pageId}); err != nil {
			return nil, err
		}
		rootpage.rightPointer = newPage.pageId
		// update the rootvalue
		Init.ROOTPAGE = int(rootpage.pageId)
		rootpage.UpdatePageHeader()
		return rootpage, nil

	} else if node.pageType == LEAF {

	}
	return node, nil

}

func (page *PageHeader) Insertkey(key uint64, leftchild uint16) (*PageHeader, error) {
	defer page.UpdatePageHeader()
	buf := make([]byte, 8)
	if BufData.pageNum != uint(page.pageId) {
		GetPage(uint(page.pageId))
	}
	binary.BigEndian.PutUint64(buf[0:], key)
	for i, val := range page.GetSlots() {
		cell := page.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.cellContent)
		if res > key {
			return page, page.AddCell(buf, AddCellOptions{Index: &i, LeftPointer: &leftchild})
		}
	}
	page.AddCell(buf, AddCellOptions{LeftPointer: &leftchild})
	return nil, nil
}

func BtreeTraversal() error {
	RootNode, err := GetPage(uint(Init.ROOTPAGE))
	if err != nil {
		return fmt.Errorf("error while insert to the btree %w", err)
	}
	pointers := coreAlgo.NewQueue()
	RootNode.traverse(&pointers)
	fmt.Println()
	popcounter := len(pointers)
	for !pointers.IsEmpty() {
		if popcounter == 0 {
			popcounter = len(pointers)
			fmt.Println()
		}
		pointToPage := pointers.Pop()
		popcounter--
		// fmt.Println("page number is ", *&pointers)
		page, err := GetPage(uint(pointToPage))
		if err != nil {
			fmt.Errorf("%w ", err)
		}
		page.traverse(&pointers)
	}
	fmt.Println()

	// make temp queue function

	return nil

}

// FIXME: should make it btree traverse  but for now it's ok
func (node *PageHeader) traverse(pointers *coreAlgo.Queue) {
	keys := make([]uint64, 0)
	for _, val := range node.GetSlots() {
		cell := node.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.cellContent)
		leftChild := cell.header.leftChild
		if leftChild != 0 {

			pointers.Push(leftChild)
		}
		keys = append(keys, res)
	}
	fmt.Print(keys)
	if node.rightPointer != 0 {
		pointers.Push(node.rightPointer)
	}

}
