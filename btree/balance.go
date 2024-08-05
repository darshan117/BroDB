package btree

import (
	Init "blackdb/init"
	"blackdb/pager"
	"encoding/binary"
	"fmt"
	"log"
	"math"
)

func (node *BtreePage) isUnderFlow() bool {
	return node.NumSlots <= uint16(UNDERFLOW)
}

func (nodePage *BtreePage) Shuffle() {
	// so how will this work
	// check for the left siblings count
	firstcell, err := nodePage.GetCell(0)
	if err != nil {
		log.Fatal(err)
	}
	if binary.BigEndian.Uint64(firstcell.CellContent) == 0 {
		return
	}
	leftsibling, err := LeftSiblingCount(binary.BigEndian.Uint64(firstcell.CellContent))
	if err != nil {
		log.Fatal(err)
	}
	// fmt.Println("left siblings are ", leftsibling.GetKeys())
	leftsib := BtreePage{*leftsibling}
	// allKeys := make([]NodeComponent, 0)
	// BUG: this might be wrong
	if leftsib.isUnderFlow() {
		return
	}
	allKeys := make([]NodeComponent, leftsib.NumSlots)
	leftkeypairs := leftsib.GetkeysWithPointer()
	copy(allKeys, leftkeypairs)

	rightsib, err := RightSiblingCount(binary.BigEndian.Uint64(firstcell.CellContent))
	if err != nil {
		log.Fatal(err)
	}
	parentslot, parentid, err := GetParent(binary.BigEndian.Uint64(firstcell.CellContent))
	if err != nil {
		log.Fatal(err)
	}
	parent, err := pager.GetPage(uint(*parentid))
	if err != nil {
		log.Fatal(err)
	}
	parentcell, err := parent.GetCell(uint(*parentslot))
	if err != nil {
		log.Fatal(err)
	}
	keyContent := make([]byte, len(parentcell.CellContent))
	copy(keyContent, parentcell.CellContent)
	parentkeyPair := NodeComponent{
		key:         keyContent,
		LeftPointer: parentcell.Header.LeftChild,
	}
	allKeys = append(allKeys, parentkeyPair)
	// rightsib := BtreePage{*rightsibling}
	if rightsib.isUnderFlow() {
		return
	}
	rightkeypairs := rightsib.GetkeysWithPointer()
	allKeys = append(allKeys, rightkeypairs...)

	if len(allKeys) >= 2*NODEFULL-1 {
		return
	}

	midPoint := len(allKeys) / 2
	midkey := allKeys[midPoint]
	// fmt.Println(allKeys)
	keysToBeAdjusted := math.Abs(float64(midPoint) - float64(len(leftkeypairs)))
	if binary.BigEndian.Uint64(midkey.key) == binary.BigEndian.Uint64(parentkeyPair.key) {
		return
	}
	// fmt.Println("key to be ", keysToBeAdjusted, "mid point is ", midPoint, allKeys)
	if midPoint > len(leftkeypairs) {
		// fmt.Println("going in new parent is ", midkey)
		rightsib.RemoveRange(0, uint(keysToBeAdjusted))
		// fmt.Println(rightsib.GetKeys())
		leftsib.AddCell(parentcell.CellContent, pager.AddCellOptions{LeftPointer: &leftsib.RightPointer})
		leftsib.RightPointer = uint16(midkey.LeftPointer)
		leftsib.UpdatePageHeader()
		// rand.Shuffle(10,)
		// FIXME : update leftsib pageheader
		// fmt.Println(parentcell.Header.LeftChild)
		pager.LoadPage(uint(parent.PageId))
		parentcell.ReplaceCell(binary.BigEndian.Uint64(midkey.key), parentcell.Header.LeftChild)
		// fmt.Println("len of all keys", len(allKeys))
		for _, v := range allKeys[midPoint-int(keysToBeAdjusted)+1 : midPoint] {
			// fmt.Printf("all keys %+v\n mid key is \n", v)
			leftsib.AddCell(v.key, pager.AddCellOptions{LeftPointer: &v.LeftPointer})
		}
	} else {
		// leftsib.RemoveRange(uint(midPoint), uint(leftsib.NumSlots))
		// TODO: need new function for insert with left Pointer
		// rightsib.InsertNonfull(binary.BigEndian.Uint64(parentcell.CellContent)) //, pager.AddCellOptions{LeftPointer: &leftsib.RightPointer})
		// pager.LoadPage(uint(parent.PageId))
		// parentcell.ReplaceCell(binary.BigEndian.Uint64(midkey.key), parentcell.Header.LeftChild)
		// for _, v := range allKeys[midPoint-int(keysToBeAdjusted)+1 : midPoint] {
		// fmt.Printf("all keys %+v\n mid key is \n", v)
		// rightsib.AddCell(v.key, pager.AddCellOptions{LeftPointer: &v.LeftPointer})
		// }
		//
		// left redistribution
	}

	// check for the right siblings count
	// check if node is underflow
	// check if the node
}

// func

func GetParent(key uint64) (*uint16, *uint16, error) {
	//
	rootpage, _ := pager.GetPage(uint(Init.ROOTPAGE))
	// return GetParent(key)
	root := BtreePage{*rootpage}
	slot, pageid, err := root.NodeParent(key)
	if err != nil {
		return nil, nil, fmt.Errorf("%w", err)
	}
	// fmt.Println(*slot, *pageid, "slot and pageid")
	return slot, pageid, nil

}
func (node *BtreePage) NodeParent(key uint64) (*uint16, *uint16, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:], key)
	for i, val := range node.GetSlots() {
		cell := node.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.CellContent)
		if res == key {
			// fmt.Println("key is in the rootNode")
			slot := uint16(i)
			return &slot, &node.PageId, nil
		} else if res > key {
			leftPage, err := pager.GetPage(uint(cell.Header.LeftChild))
			if err != nil {
				return nil, nil, err
			}
			lPage := BtreePage{*leftPage}
			if lPage.keyIsPresent(key) {
				// fmt.Printf("going to the left page %+v %d \n", leftPage, key)
				slot := uint16(i)
				return &slot, &node.PageId, nil
			}
			return lPage.NodeParent(key)
		}
	}
	// slot, pageid, _ := node.parent()
	if node.RightPointer != 0 {
		rightPage, err := pager.GetPage(uint(node.RightPointer))
		if err != nil {
			return nil, nil, err
		}
		rPage := BtreePage{*rightPage}
		if rPage.keyIsPresent(key) {
			slot := node.NumSlots - 1
			return &slot, &node.PageId, nil
		}
		return rPage.NodeParent(key)

		//
	}
	return nil, nil, fmt.Errorf("no parent found %d", key)

}

func (node *BtreePage) keyIsPresent(key uint64) bool {
	for _, val := range node.GetSlots() {
		// can do the binary search here
		cell := node.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.CellContent)
		if res == key {
			return true
		}
	}
	// check in the rightnode
	return false

}
func RightSiblingCount(key uint64) (*BtreePage, error) {
	parentslot, parentId, err := GetParent(key)
	if err != nil {
		return nil, err
	}
	// fmt.Println("parent id is ", parentId)
	parentPage, err := pager.GetPage(uint(*parentId))
	if err != nil {
		// fmt.Println(parentPage)
		return nil, err
	}
	if *parentslot < parentPage.NumSlots-1 && *parentslot > 0 {
		parentCell, err := parentPage.GetCell(uint(*parentslot) + 1)
		if err != nil {
			return nil, err
		}
		leftPage, err := pager.GetPage(uint(parentCell.Header.LeftChild))
		// fmt.Println("going to right page ")
		if err != nil {
			return nil, err
		}
		return &BtreePage{*leftPage}, nil

	} else if *parentslot == parentPage.NumSlots-1 {
		// fmt.Println("going to paren t	right page ")
		rightPage, err := pager.GetPage(uint(parentPage.RightPointer))
		if err != nil {
			return nil, err
		}
		return &BtreePage{*rightPage}, nil

	}
	// parentCell, err := parentPage.GetCell(uint(*parentslot))
	// if err != nil {
	// 	return nil, err
	// }
	// get the rightmost child or get the right pointer for this case need to make a sep function for it
	// slot := uint16(0)
	return &BtreePage{*parentPage}, nil
	// return RightSiblingCount(binary.BigEndian.Uint64(parentCell.CellContent))
}

// make it node leftsibling
func LeftSiblingCount(key uint64) (*pager.PageHeader, error) {
	parentslot, parentId, err := GetParent(key)
	if err != nil {
		return nil, err
	}
	parentPage, err := pager.GetPage(uint(*parentId))
	if err != nil {
		return nil, err
	}
	if rightPage, _ := pager.GetPage(uint(parentPage.RightPointer)); (&BtreePage{*rightPage}).keyIsPresent(key) {
		parentCell, err := parentPage.GetCell(uint(*parentslot))
		if err != nil {
			return nil, err
		}
		leftPage, err := pager.GetPage(uint(parentCell.Header.LeftChild))
		if err != nil {
			return nil, err
		}
		return leftPage, nil

	} else if *parentslot > 0 {
		parentCell, err := parentPage.GetCell(uint(*parentslot) - 1)
		if err != nil {
			return nil, err
		}
		leftPage, err := pager.GetPage(uint(parentCell.Header.LeftChild))
		if err != nil {
			return nil, err
		}
		return leftPage, nil

	} else if parentPage.PageType == pager.ROOTPAGE && *parentslot == 0 {
		// slots := uint16(0)
		return nil, nil

	}
	parentCell, err := parentPage.GetCell(uint(*parentslot))
	if err != nil {
		return nil, err
	}
	// get the rightmost child or get the right pointer for this case need to make a sep function for it
	// BUG: raise error if not found
	return LeftSiblingCount(binary.BigEndian.Uint64(parentCell.CellContent))
}

func (node *BtreePage) GetkeysWithPointer() []NodeComponent {
	keyPairs := make([]NodeComponent, 0, node.NumSlots)
	for _, v := range node.GetSlots() {
		cell := node.GetCellByOffset(v)
		keyContent := make([]byte, len(cell.CellContent))
		copy(keyContent, cell.CellContent)
		keyPairs = append(keyPairs, NodeComponent{
			key:         keyContent,
			LeftPointer: cell.Header.LeftChild,
		})
	}
	return keyPairs

}

// 	slot, pageid, _ := node.search(key)
// 	nodePage, _ := pager.GetPage(uint(pageid))
// 	if nodePage.PageType == pager.LEAF {

// 	}

// }
