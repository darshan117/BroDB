package btree

import (
	coreAlgo "blackdb/core_algo"
	Init "blackdb/init"
	"blackdb/pager"
	"encoding/binary"
	"fmt"
)

// [x] read the full wikipedia page on the btree
// [x] start with btree layout in the page
// [x] start small like set max btree childrens to like 60 bytes
// type BtreePage struct { page *pager.PageHeader
// }

func NewBtreePage() (*BtreePage, error) {
	// Degree = d

	page, err := pager.MakePage(pager.ROOT_AND_LEAF, uint16(Init.TOTAL_PAGES))
	if err != nil {
		return nil, err
	}
	return &BtreePage{*page}, nil
}

func (page *BtreePage) InsertNonfull(key uint64) (*BtreePage, error) {
	if pager.BufData.PageNum != uint(page.PageId) {
		pager.GetPage(uint(page.PageId))
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:], key)
	if page.PageType == pager.LEAF || page.PageType == pager.ROOT_AND_LEAF {

		for i, val := range page.GetSlots() {
			cell := page.GetCellByOffset(val)
			res := binary.BigEndian.Uint64(cell.CellContent)
			if res > key {
				page.AddCell(buf, pager.AddCellOptions{Index: &i})
				page.Shuffle()
				return page, nil
			}
		}
		page.AddCell(buf)
		page.Shuffle()
		return page, nil
	}
	// FIXME: might remove the below if statement
	if page.PageType == pager.ROOTPAGE || page.PageType == pager.INTERIOR {
		for i, val := range page.GetSlots() {
			cell := page.GetCellByOffset(val)
			res := binary.BigEndian.Uint64(cell.CellContent)
			if res > key {
				leftcell, _ := page.GetCell(uint(i))
				childPage, err := pager.GetPage(uint(leftcell.Header.LeftChild))
				if err != nil {
					return nil, err
				}
				if childPage.NumSlots == NODEFULL {
					page.SplitPagesLeft(i, cell.Header.LeftChild)
					return page.InsertNonfull(key)
				} else if leftcell.Header.LeftChild != uint16(0) {
					page = &BtreePage{*childPage}

					page.Shuffle()
					// fmt.Printf("key is %d %+v \n", key, page.GetKeys())
					return page.InsertNonfull(key)
				}
			}
		}

		if page.RightPointer != 0 {
			rightChildPage, err := pager.GetPage(uint(page.RightPointer))
			if err != nil {
				return nil, err
			}
			if rightChildPage.NumSlots == NODEFULL {
				rightPage := BtreePage{*rightChildPage}
				rightPage.SplitPagesRightAndInsert(page, key)
				return page.InsertNonfull(key)

			}
			page = &BtreePage{*rightChildPage}
			page.Shuffle()

			return page.InsertNonfull(key)
		}
	}
	// BUG: check for if it has right pointer or else page.addcell
	return nil, nil
}

func (node *BtreePage) Insert(val uint64) (*BtreePage, error) {
	// BUG: a huge risk might not work
	// defer fmt.Printf("node page is .. %+v\n", node)

	// FIXME: check if node.PageType is root or root and leaf else change the node to the rootnode
	if node.PageType != pager.ROOTPAGE && node.PageType != pager.ROOT_AND_LEAF {
		RootNode, err := pager.GetPage(uint(Init.ROOTPAGE))
		if err != nil {
			return nil, fmt.Errorf("error while insert to the btree %w", err)
		}
		rootnode := BtreePage{*RootNode}
		return rootnode.Insert(val)

	}
	if node.PageType == pager.ROOT_AND_LEAF && node.NumSlots == 0 {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf[0:], val)
		// FIXME: error statement here
		node.AddCell(buf)

	} else if node.NumSlots == NODEFULL && (node.PageType == pager.ROOTPAGE || node.PageType == pager.ROOT_AND_LEAF) {
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

func (childPage *BtreePage) SplitPagesRightAndInsert(node *BtreePage, key uint64) (*BtreePage, error) {
	defer childPage.UpdatePageHeader()
	// splitVal := childPage.GetKeys()[Degree-1]
	splitcell, err := childPage.GetCell(uint(Degree - 1))
	if err != nil {
		return nil, err
	}

	splitVal := binary.BigEndian.Uint64(splitcell.CellContent)
	splitleftKey := splitcell.Header.LeftChild
	keyPairs := make([]NodeComponent, 0)

	for _, v := range childPage.GetSlots()[Degree:childPage.NumSlots] {
		cell := childPage.GetCellByOffset(v)
		keyPairs = append(keyPairs, NodeComponent{
			key:         cell.CellContent,
			LeftPointer: cell.Header.LeftChild,
		})
	}
	var ptype pager.PageType
	switch childPage.PageType {
	case pager.LEAF:
		ptype = pager.LEAF
	case pager.INTERIOR:
		ptype = pager.INTERIOR
	default:
		ptype = pager.LEAF
	}
	newPage, err := pager.MakePage(ptype, uint16(Init.TOTAL_PAGES))
	if err != nil {
		return nil, err
	}
	for _, v := range keyPairs {
		newPage.AddCell(v.key, pager.AddCellOptions{LeftPointer: &v.LeftPointer})
	}
	childPage.EndRangeRemoveSlots(Degree-1, uint(childPage.NumSlots))
	node.Insertkey(splitVal, childPage.PageId)
	node.RightPointer = newPage.PageId
	newPage.RightPointer = childPage.RightPointer
	childPage.RightPointer = splitleftKey
	childPage.UpdatePageHeader()
	newPage.UpdatePageHeader()
	node.UpdatePageHeader()
	return nil, nil
}

// --------

func (node *BtreePage) SplitPagesLeft(index int, splitpage uint16) (*BtreePage, error) {
	childPage, err := pager.GetPage(uint(splitpage))
	if err != nil {
		return nil, err
	}
	splitcell, err := childPage.GetCell(uint(Degree - 1))
	if err != nil {
		return nil, err
	}

	splitVal := binary.BigEndian.Uint64(splitcell.CellContent)
	splitleftKey := splitcell.Header.LeftChild
	keyPairs := make([]NodeComponent, 0)

	for _, v := range childPage.GetSlots()[:Degree-1] {
		cell := childPage.GetCellByOffset(v)
		keyPairs = append(keyPairs, NodeComponent{
			key:         cell.CellContent,
			LeftPointer: cell.Header.LeftChild,
		})
	}
	var ptype pager.PageType
	switch childPage.PageType {
	case pager.LEAF:
		ptype = pager.LEAF
	case pager.INTERIOR:
		ptype = pager.INTERIOR
	default:
		ptype = pager.LEAF
	}
	newPage, err := pager.MakePage(ptype, uint16(Init.TOTAL_PAGES))
	if err != nil {
		return nil, err
	}
	for _, v := range keyPairs {
		newPage.AddCell(v.key, pager.AddCellOptions{LeftPointer: &v.LeftPointer})
	}
	childPage.StartRangeRemoveSlots(0, Degree)

	node.Insertkey(uint64(splitVal), newPage.PageId)
	newPage.RightPointer = splitleftKey
	newPage.UpdatePageHeader()
	return nil, nil
}

func (node *BtreePage) SplitRootPages() (*BtreePage, error) {
	defer node.UpdatePageHeader()

	// can make it static type slice
	keys := make([]NodeComponent, 0)
	splitVal := node.GetKeys()[Degree-1]
	splitcell, err := node.GetCell(Degree - 1)
	splitLeftkey := splitcell.Header.LeftChild
	var ptype pager.PageType
	if splitLeftkey == 0 {
		ptype = pager.LEAF

	} else {
		ptype = pager.INTERIOR
	}

	if err != nil {
		return nil, err
	}
	newPage, err := pager.MakePage(ptype, uint16(Init.TOTAL_PAGES))
	if err != nil {
		return nil, err
	}

	for _, val := range node.GetSlots()[Degree:] {
		cell := node.GetCellByOffset(val)
		keys = append(keys, NodeComponent{key: cell.CellContent, LeftPointer: cell.Header.LeftChild})

	}
	// FIXME : rem println
	fmt.Println("keys are ", keys)
	for _, v := range keys {
		newPage.AddCell(v.key, pager.AddCellOptions{LeftPointer: &v.LeftPointer})
	}
	// traverse to the leaf using its RightPointer only and append it there
	// TODO: make a seperate such function

	// for i := (Degree); i < len(keys); i++ {
	// 	resp := make([]byte, 8)
	// 	binary.BigEndian.PutUint64(resp, keys[i])
	// 	// here adding the keys with its leftPointer
	// 	newPage.AddCell(resp)
	// }
	// FIXME: this is actually end remove
	node.EndRangeRemoveSlots(Degree-1, uint(node.NumSlots))

	node.UpdateRightPointer(uint(splitLeftkey), newPage)
	// nodecell, _ := node.GetCell(3)
	// newp, _ := GetPage(6)
	// fmt.Printf("split page is %+v\n", newp.GetKeys())
	// BUG: might be some bug here
	rootpage, err := pager.MakePage(pager.ROOTPAGE, uint16(Init.TOTAL_PAGES))
	if err != nil {
		return nil, err
	}
	res := make([]byte, 8)
	binary.BigEndian.PutUint64(res, uint64(splitVal))
	// TODO: add the old page as the left pointer and right pointer as the newpage
	node.PageType = pager.LEAF
	if splitLeftkey != 0 {
		node.PageType = pager.INTERIOR
	}
	if err := rootpage.AddCell(res, pager.AddCellOptions{LeftPointer: &node.PageId}); err != nil {
		return nil, err
	}
	rootpage.RightPointer = newPage.PageId
	// update the rootvalue
	Init.ROOTPAGE = int(rootpage.PageId)
	rootpage.UpdatePageHeader()
	return &BtreePage{*rootpage}, nil

}

func (page *BtreePage) Insertkey(key uint64, LeftChild uint16) (*BtreePage, error) {
	defer page.UpdatePageHeader()
	buf := make([]byte, 8)
	if pager.BufData.PageNum != uint(page.PageId) {
		pager.GetPage(uint(page.PageId))
	}
	binary.BigEndian.PutUint64(buf[0:], key)
	for i, val := range page.GetSlots() {
		cell := page.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.CellContent)
		if res > key {
			return page, page.AddCell(buf, pager.AddCellOptions{Index: &i, LeftPointer: &LeftChild})
		}
	}
	page.AddCell(buf, pager.AddCellOptions{LeftPointer: &LeftChild})
	return nil, nil
}

func DFSTraversal() {

}

// BUG: this thing is storing stuff in the [] which is getting lots of memory
func BtreeTraversal() (*[][][]uint64, error) {
	RootNode, err := pager.GetPage(uint(Init.ROOTPAGE))
	if err != nil {
		return nil, fmt.Errorf("error while insert to the btree %w", err)
	}
	rootnode := BtreePage{*RootNode}
	pointers := coreAlgo.NewQueue()
	result := make([][][]uint64, 0)
	t := make([][]uint64, 0, 1)
	t = append(t, rootnode.traverse(&pointers))
	result = append(result, t)
	fmt.Println()

	fmt.Println("traver", t)
	popcounter := len(pointers)
	temp := make([][]uint64, 0)
	for !pointers.IsEmpty() {
		if popcounter == 0 {
			popcounter = len(pointers)
			result = append(result, temp[:])
			// fmt.Println("temp is ", temp)
			temp = make([][]uint64, 0)
			fmt.Println()

		}
		pointToPage := pointers.Pop()
		popcounter--
		// fmt.Println("page number is ", *&pointers)

		page, err := pager.GetPage(uint(pointToPage))
		if err != nil {
			return nil, fmt.Errorf("%w ", err)
		}
		bPage := BtreePage{*page}
		temp = append(temp, bPage.traverse(&pointers))
	}
	result = append(result, temp)
	fmt.Println()
	// fmt.Println("the main result is ", result)

	// make temp queue function

	return &result, nil

}

// FIXME: should make it btree traverse  but for now it's ok
func (node *BtreePage) traverse(pointers *coreAlgo.Queue) []uint64 {
	keys := make([]uint64, 0)
	for _, val := range node.GetSlots() {
		cell := node.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.CellContent)
		LeftChild := cell.Header.LeftChild
		if LeftChild != 0 {

			pointers.Push(LeftChild)
		}
		keys = append(keys, res)
	}
	fmt.Print(keys)
	if node.RightPointer != 0 {
		pointers.Push(node.RightPointer)
	}
	return keys

}
