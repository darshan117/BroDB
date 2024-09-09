package btree

import (
	coreAlgo "blackdb/core_algo"
	Init "blackdb/init"
	"blackdb/pager"
	"encoding/binary"
	"fmt"
	"log"
)

// [x] read the full wikipedia page on the btree
// [x] start with btree layout in the page
// [x] start small like set max btree childrens to like 60 bytes

func NewBtreePage() (*BtreePage, error) {
	page, err := pager.MakePage(pager.ROOT_AND_LEAF, uint16(Init.TOTAL_PAGES))
	if err != nil {
		return nil, err
	}
	return &BtreePage{*page}, nil
}

func (page *BtreePage) insertNonfull(buf []byte) (*BtreePage, error) {
	if pager.BufData.PageNum != uint(page.PageId) {
		pager.GetPage(uint(page.PageId))
	}
	key := binary.BigEndian.Uint32(buf[:4])
	if page.PageType == pager.LEAF || page.PageType == pager.ROOT_AND_LEAF {

		for i, val := range page.GetSlots() {
			cell := page.GetCellByOffset(val)
			res := binary.BigEndian.Uint32(cell.CellContent[:4])
			if res > key {

				npage, err := pager.GetPage(uint(page.PageId))
				if err != nil {
					return nil, err
				}
				page = &BtreePage{*npage}
				if err := page.AddCell(buf, pager.AddCellOptions{Index: &i}); err != nil {
					return nil, err
				}
				page.Shuffle()
				return page, nil
			}
		}
		page.AddCell(buf)
		page.Shuffle()
		return page, nil
	}
	for _, val := range page.GetSlots() {
		cell := page.GetCellByOffset(val)
		res := binary.BigEndian.Uint32(cell.CellContent[:4])
		if res > key {
			childPage, err := pager.GetPage(uint(cell.Header.LeftChild))
			if err != nil {
				return nil, err
			}
			if childPage.NumSlots == NODEFULL {
				child := BtreePage{*childPage}
				child.SplitPagesLeft(page)
				return page.insertNonfull(buf)
			} else if cell.Header.LeftChild != uint16(0) {
				page = &BtreePage{*childPage}
				page.insertNonfull(buf)
				page.Shuffle()
				return nil, nil

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
			rightPage.SplitPagesRightAndInsert(page)
			return page.insertNonfull(buf)

		}
		page = &BtreePage{*rightChildPage}
		npage, err := pager.GetPage(uint(page.PageId))
		if err != nil {
			return nil, err
		}
		if key == 15051 {
			fmt.Println("here")
		}

		page = &BtreePage{*npage}
		page.insertNonfull(buf)
		page.Shuffle()

		return nil, nil
	}
	return nil, nil
}

// This function is the main function which call the insertnonfull function
//
// checks pagetype of the page and inserts accordingly.
func Insert(key uint32, val []byte) (*BtreePage, error) {

	if Init.ROOTPAGE == 0 {
		Init.UpdateRootPage(uint(Init.TOTAL_PAGES))
		pager.MakePage(pager.ROOT_AND_LEAF, uint16(Init.TOTAL_PAGES))
		return Insert(key, val)
	}
	RootNode, err := pager.GetPage(uint(Init.ROOTPAGE))
	if err != nil {
		fmt.Println(Init.ROOTPAGE)
		return nil, fmt.Errorf("error while insert to the btree %w", err)
	}
	node := &BtreePage{*RootNode}

	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf[:4], key)
	// binary.BigEndian.PutUint32(buf[4:], val)
	// copy(buf[4:], val)
	buf = append(buf, val...)
	defer func() {
		buf = nil
	}()
	if node.PageType == pager.ROOT_AND_LEAF && node.NumSlots == 0 {
		node.AddCell(buf)
	} else if node.NumSlots == NODEFULL && (node.PageType == pager.ROOTPAGE || node.PageType == pager.ROOT_AND_LEAF) {
		root, err := node.SplitRootPages()
		if err != nil {
			return nil, err
		}
		node = root
		Newnode, err := node.insertNonfull(buf)
		if err != nil {
			fmt.Println(err)
		}
		node = Newnode
		return node, nil

	} else {
		Newnode, err := node.insertNonfull(buf)
		if err != nil {
			return nil, err
		}
		node = Newnode
		return node, nil
	}
	return nil, nil
}

func (childPage *BtreePage) SplitPagesRightAndInsert(node *BtreePage) (*BtreePage, error) {
	defer childPage.UpdatePageHeader()
	splitcell, err := childPage.GetCell(uint(Degree - 1))
	if err != nil {
		return nil, err
	}

	splitleftKey := splitcell.Header.LeftChild
	keyPairs := make([]NodeComponent, 0)

	for _, v := range childPage.GetSlots()[Degree:childPage.NumSlots] {
		cell := childPage.GetCellByOffset(v)
		keyPairs = append(keyPairs, NodeComponent{
			Key:         cell.CellContent[:4],
			keyval:      cell.CellContent,
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
		newPage.AddCell(v.keyval, pager.AddCellOptions{LeftPointer: &v.LeftPointer})
	}
	childPage.RemoveRange(Degree-1, uint(childPage.NumSlots))
	node.Insertkey(splitcell.CellContent, childPage.PageId)
	node.RightPointer = newPage.PageId
	newPage.RightPointer = childPage.RightPointer
	childPage.RightPointer = splitleftKey
	newPage.UpdatePageHeader()
	node.UpdatePageHeader()
	return nil, nil
}

func (childPage *BtreePage) SplitPagesLeft(node *BtreePage) (*BtreePage, error) {
	splitcell, err := childPage.GetCell(uint(Degree - 1))
	if err != nil {
		return nil, err
	}

	splitleftKey := splitcell.Header.LeftChild
	keyPairs := make([]NodeComponent, 0)

	for _, v := range childPage.GetSlots()[:Degree-1] {
		cell := childPage.GetCellByOffset(v)
		keyPairs = append(keyPairs, NodeComponent{
			Key:         cell.CellContent[:4],
			keyval:      cell.CellContent,
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
		newPage.AddCell(v.keyval, pager.AddCellOptions{LeftPointer: &v.LeftPointer})
	}
	childPage.RemoveRange(0, Degree)

	node.Insertkey(splitcell.CellContent, newPage.PageId)
	newPage.RightPointer = splitleftKey
	childPage.UpdatePageHeader()
	node.UpdatePageHeader()
	newPage.UpdatePageHeader()
	return nil, nil
}

func (node *BtreePage) SplitRootPages() (*BtreePage, error) {
	defer node.UpdatePageHeader()
	keys := make([]NodeComponent, 0)
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
		keys = append(keys, NodeComponent{Key: cell.CellContent[:4], keyval: cell.CellContent, LeftPointer: cell.Header.LeftChild})

	}
	for _, v := range keys {
		newPage.AddCell(v.keyval, pager.AddCellOptions{LeftPointer: &v.LeftPointer})
	}
	node.RemoveRange(Degree-1, uint(node.NumSlots))
	// newPage.RightPointer = splitLeftkey

	node.UpdateRightPointer(uint(splitLeftkey), newPage)
	rootpage, err := pager.MakePage(pager.ROOTPAGE, uint16(Init.TOTAL_PAGES))
	if err != nil {
		return nil, err
	}

	node.PageType = pager.LEAF
	if splitLeftkey != 0 {
		node.PageType = pager.INTERIOR
	}
	if err := rootpage.AddCell(splitcell.CellContent, pager.AddCellOptions{LeftPointer: &node.PageId}); err != nil {
		return nil, err
	}
	rootpage.RightPointer = newPage.PageId
	// update the rootvalue
	Init.ROOTPAGE = int(rootpage.PageId)
	rootpage.UpdatePageHeader()
	newPage.UpdatePageHeader()
	node.UpdatePageHeader()
	return &BtreePage{*rootpage}, nil

}

func (page *BtreePage) Insertkey(buf []byte, LeftChild uint16) (*BtreePage, error) {
	defer page.UpdatePageHeader()
	if pager.BufData.PageNum != uint(page.PageId) {
		pager.GetPage(uint(page.PageId))
	}
	key := binary.BigEndian.Uint32(buf[:4])
	for i := 0; i < int(page.NumSlots); i++ {
		cell, err := page.GetCell(uint(i))
		if err != nil {
			fmt.Println(err, "key is", key)
			return nil, err
		}
		res := binary.BigEndian.Uint32(cell.CellContent[:4])
		if res > key {
			return page, page.AddCell(buf, pager.AddCellOptions{Index: &i, LeftPointer: &LeftChild})
		}

	}
	page.AddCell(buf, pager.AddCellOptions{LeftPointer: &LeftChild})
	return nil, nil
}

// BUG: this thing is storing stuff in the [] which is getting lots of memory
func BtreeTraversal() (*[][][]uint32, error) {
	RootNode, err := pager.GetPage(uint(Init.ROOTPAGE))
	if err != nil {
		return nil, fmt.Errorf("error while insert to the btree %w", err)
	}
	rootnode := BtreePage{*RootNode}
	pointers := coreAlgo.NewQueue()
	result := make([][][]uint32, 0)
	t := make([][]uint32, 0, 1)
	t = append(t, rootnode.traverse(&pointers))
	result = append(result, t)
	fmt.Println()

	popcounter := len(pointers)
	temp := make([][]uint32, 0)
	for !pointers.IsEmpty() {
		if popcounter == 0 {
			popcounter = len(pointers)
			result = append(result, temp[:])
			temp = make([][]uint32, 0)
			fmt.Println()

		}
		pointToPage := pointers.Pop()
		popcounter--
		page, err := pager.GetPage(uint(pointToPage))
		if err != nil {
			return nil, fmt.Errorf("%w ", err)
		}
		bPage := BtreePage{*page}
		temp = append(temp, bPage.traverse(&pointers))
	}
	result = append(result, temp)
	fmt.Println()
	return &result, nil

}

// FIXME: should make it btree traverse  but for now it's ok
func (node *BtreePage) traverse(pointers *coreAlgo.Queue) []uint32 {
	keys := make([]uint32, 0)
	for _, val := range node.GetSlots() {
		cell := node.GetCellByOffset(val)
		res := binary.BigEndian.Uint32(cell.CellContent[:4])
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

func BtreeDFSTraversal() ([]uint32, error) {
	RootNode, err := pager.GetPage(uint(Init.ROOTPAGE))
	if err != nil {
		return nil, err
	}
	rootnode := BtreePage{*RootNode}
	keys := make([]uint32, 0, 100)
	if err := rootnode.dfstraverse(&keys); err != nil {
		fmt.Println(keys)
		return nil, err
	}
	return keys, nil
}

func (node *BtreePage) dfstraverse(keys *[]uint32) error {
	if node.PageType == pager.LEAF || node.PageType == pager.ROOT_AND_LEAF {
		*keys = append(*keys, node.GetKeys()...)
		return nil
	}
	for i := uint(0); i <= uint(node.NumSlots); i++ {
		var childPtr uint16
		if i < uint(node.NumSlots) {
			cell, err := node.GetCell(i)
			if err != nil {
				return err
			}
			childPtr = cell.Header.LeftChild
		} else {
			childPtr = node.RightPointer
		}

		childPage, err := pager.GetPage(uint(childPtr))
		if err != nil {
			return err
		}
		childNode := &BtreePage{*childPage}
		if err := childNode.dfstraverse(keys); err != nil {
			return err
		}
		if i < uint(node.NumSlots) {
			cell, _ := node.GetCell(i)
			*keys = append(*keys, binary.BigEndian.Uint32(cell.CellContent[:4]))
		}
	}
	return nil
}

func BtreeDFSTraverseKeys() ([][]byte, error) {
	RootNode, err := pager.GetPage(uint(Init.ROOTPAGE))
	if err != nil {
		return nil, err
	}
	rootnode := BtreePage{*RootNode}
	keys := make([][]byte, 0, 100)
	if err := rootnode.dfstraversekeys(&keys); err != nil {
		fmt.Println(keys)
		return nil, err
	}
	return keys, nil
}

func (node *BtreePage) dfstraversekeys(keys *[][]byte) error {
	if node.PageType == pager.LEAF || node.PageType == pager.ROOT_AND_LEAF {
		// *keys = append(*keys, node.GetKeys()...)
		for _, val := range node.GetSlots() {
			cell := node.GetCellByOffset(val)
			recordPage, _ := pager.GetPage(uint(binary.BigEndian.Uint32(cell.CellContent[4:8])))
			recordCell, _ := recordPage.GetCell(uint(binary.BigEndian.Uint16(cell.CellContent[8:])))
			if len(recordCell.CellContent) == 0 {
				fmt.Println(cell)
				log.Fatalf("%+v\n,page %+v \n ", recordCell, recordPage)
			}
			*keys = append(*keys, recordCell.CellContent)
			// res := binary.BigEndian.Uint32(cell.CellContent[:4])
			// keys = append(keys, res)
		}
		return nil
	}
	for i := uint(0); i <= uint(node.NumSlots); i++ {
		var childPtr uint16
		if i < uint(node.NumSlots) {
			cell, err := node.GetCell(i)
			if err != nil {
				return err
			}
			childPtr = cell.Header.LeftChild
		} else {
			childPtr = node.RightPointer
		}

		childPage, err := pager.GetPage(uint(childPtr))
		if err != nil {
			return err
		}
		childNode := &BtreePage{*childPage}
		if err := childNode.dfstraversekeys(keys); err != nil {
			return err
		}
		if i < uint(node.NumSlots) {
			cell, _ := node.GetCell(i)
			recordPage, _ := pager.GetPage(uint(binary.BigEndian.Uint32(cell.CellContent[4:8])))
			recordCell, _ := recordPage.GetCell(uint(binary.BigEndian.Uint16(cell.CellContent[8:])))
			*keys = append(*keys, recordCell.CellContent)
			// *keys = append(*keys, binary.BigEndian.Uint32(cell.CellContent[:4]))
		}
	}
	return nil
}
