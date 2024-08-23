package btree

import (
	Init "blackdb/init"
	"blackdb/pager"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// type BothUnderFlowError struct {
// 	message string
// }

// func (b *BothUnderFlowError) Error() string {
// 	return b.message
// }

var BothUnderFlowError = errors.New("Both siblings are underFlow")
var LeftSiblingError = errors.New("left siblings is first slot")
var NoMergeRequired = errors.New("No need to merge just shuffle")

// FIXME:
func (node *BtreePage) isUnderFlow() bool {
	if node.PageId != uint16(pager.BufData.PageNum) {
		pager.LoadPage(uint(node.PageId))
	}
	return node.NumSlots <= uint16(UNDERFLOW)
}

// will return bool and left sib and right sib
func (nodePage *BtreePage) Shuffle() (leftsibling *BtreePage, rightsibling *BtreePage, balanced bool) {
	isbalanced := false
	nPage, err := pager.GetPage(uint(nodePage.PageId))
	if err != nil {
		return nil, nil, isbalanced
	}
	node := BtreePage{*nPage}
	if nodePage.PageType == pager.ROOT_AND_LEAF || nodePage.PageType == pager.ROOTPAGE {
		return nil, nil, isbalanced
	}
	leftsib, rightsib, err := node.chooseFrom()
	if err != nil && (err != LeftSiblingError) && err != BothUnderFlowError {
		return nil, nil, isbalanced
	}
	// if leftsib.PageId == nodePage.PageId {
	// 	leftsib = *&nodePage
	// } else if rightsib.PageId == nodePage.PageId {
	// 	rightsib = *&nodePage
	// }
	if err == BothUnderFlowError {
		// some fixes here
		if err := nodePage.MergeNodes(); err != NoMergeRequired && err == nil {
			return nil, nil, isbalanced
		}
	}
	allKeys := make([]NodeComponent, 0, leftsib.NumSlots)
	leftkeypairs := leftsib.GetkeysWithPointer()
	allKeys = append(allKeys, leftkeypairs...)
	firstcell, err := leftsib.GetCell(0)
	if err != nil {
		return nil, nil, isbalanced
	}
	parentslot, parentid, err := GetParent(binary.BigEndian.Uint64(firstcell.CellContent))
	if err != nil {
		fmt.Println("error from the shuffle get parent", err)
		return
	}
	parent, err := pager.GetPage(uint(*parentid))
	if err != nil {
		fmt.Println("error from the shuffle ", err)
		return
	}
	parentcell, err := parent.GetCell(uint(*parentslot))
	if err != nil {
		fmt.Println("error from the shuffle ", err)
		return
	}
	parentkeyPair := NodeComponent{
		key:         parentcell.CellContent,
		LeftPointer: parentcell.Header.LeftChild,
	}
	allKeys = append(allKeys, parentkeyPair)
	rightkeypairs := rightsib.GetkeysWithPointer()
	allKeys = append(allKeys, rightkeypairs...)

	if len(allKeys) >= 2*NODEFULL {
		return
	}

	midPoint := len(allKeys) / 2
	midkey := allKeys[midPoint]
	keysToBeAdjusted := math.Abs(float64(midPoint) - float64(len(leftkeypairs)))
	if binary.BigEndian.Uint64(midkey.key) == binary.BigEndian.Uint64(parentkeyPair.key) {
		return
	}
	// fmt.Println("key to be ", keysToBeAdjusted, "mid point is ", midPoint, allKeys)
	if midPoint > len(leftkeypairs) {
		for i := 0; i < int(keysToBeAdjusted); i++ {
			leftsib.AddCell(parentcell.CellContent, pager.AddCellOptions{LeftPointer: &leftsib.RightPointer})
			rightfirstCell, _ := rightsib.GetCell(0)
			leftsib.RightPointer = uint16(rightfirstCell.Header.LeftChild)
			parent.ReplaceCell(&parentcell, binary.BigEndian.Uint64(rightfirstCell.CellContent), parentcell.Header.LeftChild)
			rightsib.RemoveCell(0)

		}
		isbalanced = true
	} else {
		for i := 0; i < int(keysToBeAdjusted); i++ {
			leftlastcell, _ := leftsib.GetCell(uint(leftsib.NumSlots) - 1)
			rightsib.Insertkey(binary.BigEndian.Uint64(parentcell.CellContent), leftsib.RightPointer) //, pager.AddCellOptions{LeftPointer: &leftsib.RightPointer})
			parent.ReplaceCell(&parentcell, binary.BigEndian.Uint64(leftlastcell.CellContent), parentcell.Header.LeftChild)
			leftsib.RightPointer = leftlastcell.Header.LeftChild
			leftsib.UpdatePageHeader()
			leftsib.RemoveCell(uint(leftsib.NumSlots) - 1)

		}
		isbalanced = true
	}
	nodePage.UpdatePageHeader()
	leftsib.UpdatePageHeader()
	rightsib.UpdatePageHeader()
	parent.UpdatePageHeader()
	return leftsib, rightsib, isbalanced

}

func (node *BtreePage) chooseFrom() (leftsibling *BtreePage, rightsibling *BtreePage, err error) {
	leftcount := 0
	firstcell, err := node.GetCell(0)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting the first cell  %w", err)
	}
	leftPage, err := GetLeftPage(firstcell.CellContent)
	if err != nil {
		return nil, nil, err
	}
	rightcount := 0

	rightsib, err := node.RightSiblingCount()
	if err != nil || rightsib.isUnderFlow() {
		rightcount = 0
	} else {
		rightcount = int(rightsib.NumSlots)
	}
	leftsib, err := node.LeftSiblingCount()
	if err != nil || leftsib.isUnderFlow() {
		leftcount = 0
	} else {
		leftcount = int(leftsib.NumSlots)
	}
	if int(leftPage.NumSlots)+rightcount+1 <= NODEFULL && leftcount+int(leftPage.NumSlots)+1 <= NODEFULL {
		if leftcount > rightcount {
			return leftsib, leftPage, BothUnderFlowError
		}

		return leftPage, rightsib, BothUnderFlowError
	}
	if err == LeftSiblingError {
		return leftPage, rightsib, LeftSiblingError
	}
	if leftcount > rightcount {
		return leftsib, leftPage, nil
	}
	return leftPage, rightsib, nil
}

func GetParent(key uint64) (*uint16, *uint16, error) {
	//
	rootpage, _ := pager.GetPage(uint(Init.ROOTPAGE))
	root := BtreePage{*rootpage}
	slot, pageid, err := root.NodeParent(key)
	if err != nil {
		return nil, nil, fmt.Errorf("%w", err)
	}
	return slot, pageid, nil

}
func (node *BtreePage) NodeParent(key uint64) (*uint16, *uint16, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:], key)
	for i, val := range node.GetSlots() {
		cell := node.GetCellByOffset(val)
		// fmt.Println(cell.CellContent)
		res := binary.BigEndian.Uint64(cell.CellContent)
		if res == key {
			slot := uint16(i)
			return &slot, &node.PageId, nil
		} else if res > key {
			leftPage, err := pager.GetPage(uint(cell.Header.LeftChild))
			if err != nil {
				return nil, nil, err
			}
			lPage := BtreePage{*leftPage}
			if lPage.keyIsPresent(key) {
				slot := uint16(i)
				return &slot, &node.PageId, nil
			}
			return lPage.NodeParent(key)
		}
	}
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
		// FIXME: can do the binary search here
		cell := node.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.CellContent)
		if res == key {
			return true
		}
	}
	return false

}
func (node *BtreePage) RightSiblingCount() (*BtreePage, error) {
	firstcell, err := node.GetCell(0)
	if err != nil {
		return nil, err
	}
	parentslot, parentId, err := GetParent(binary.BigEndian.Uint64(firstcell.CellContent))
	if err != nil {
		return nil, err
	}
	parentPage, err := pager.GetPage(uint(*parentId))
	if err != nil {
		return nil, err
	}
	if *parentslot < parentPage.NumSlots-1 {
		// FIXME: why parentslot+1
		parentCell, err := parentPage.GetCell(uint(*parentslot) + 1)
		if err != nil {
			return nil, err
		}
		leftPage, err := pager.GetPage(uint(parentCell.Header.LeftChild))
		if err != nil {
			return nil, err
		}
		return &BtreePage{*leftPage}, nil

	} else if *parentslot == parentPage.NumSlots-1 {
		rightPage, err := pager.GetPage(uint(parentPage.RightPointer))
		if err != nil {
			return nil, err
		}
		return &BtreePage{*rightPage}, nil

	}
	return &BtreePage{*parentPage}, nil
}

func GetLeftPage(key []byte) (*BtreePage, error) {
	parentslot, parentId, err := GetParent(binary.BigEndian.Uint64(key))
	if err != nil {
		return nil, err
	}
	parentPage, err := pager.GetPage(uint(*parentId))
	if err != nil {
		return nil, err
	}
	parentCell, err := parentPage.GetCell(uint(*parentslot))
	if err != nil {
		return nil, err
	}
	leftPage, err := pager.GetPage(uint(parentCell.Header.LeftChild))
	if err != nil {
		return nil, err
	}
	return (&BtreePage{*leftPage}), nil

}

// make it node leftsibling
func (node *BtreePage) LeftSiblingCount() (*BtreePage, error) {
	firstcell, err := node.GetCell(0)
	if err != nil {
		return nil, err
	}
	parentslot, parentId, err := GetParent(binary.BigEndian.Uint64(firstcell.CellContent))
	if err != nil {
		return nil, err
	}
	parentPage, err := pager.GetPage(uint(*parentId))
	if err != nil {
		return nil, err
	}
	if rightPage, _ := pager.GetPage(uint(parentPage.RightPointer)); (&BtreePage{*rightPage}).keyIsPresent(binary.BigEndian.Uint64(firstcell.CellContent)) {
		parentCell, err := parentPage.GetCell(uint(*parentslot))
		if err != nil {
			return nil, err
		}
		leftPage, err := pager.GetPage(uint(parentCell.Header.LeftChild))
		if err != nil {
			return nil, err
		}
		return (&BtreePage{*leftPage}), LeftSiblingError

	} else if *parentslot > 0 {
		parentCell, err := parentPage.GetCell(uint(*parentslot) - 1)
		if err != nil {
			return nil, err
		}
		leftPage, err := pager.GetPage(uint(parentCell.Header.LeftChild))
		if err != nil {
			return nil, err
		}
		return (&BtreePage{*leftPage}), nil
	} else if *parentslot == 0 {

		return nil, fmt.Errorf("parent is 0 so No left sibling")

	}
	// BUG: raise error if not found
	return (&BtreePage{*parentPage}).LeftSiblingCount()
}

func (node *BtreePage) GetkeysWithPointer() []NodeComponent {
	keyPairs := make([]NodeComponent, 0, node.NumSlots)
	for i := 0; i < int(node.NumSlots)-1; i++ {
		cell, _ := node.GetCell(uint(i))
		keyContent := make([]byte, len(cell.CellContent))
		copy(keyContent, cell.CellContent)
		keyPairs = append(keyPairs, NodeComponent{
			key:         keyContent,
			LeftPointer: cell.Header.LeftChild,
		})

	}
	// for _, v := range node.GetSlots() {
	// 	cell := node.GetCellByOffset(v)
	// 	keyContent := make([]byte, len(cell.CellContent))
	// 	copy(keyContent, cell.CellContent)
	// 	keyPairs = append(keyPairs, NodeComponent{
	// 		key:         keyContent,
	// 		LeftPointer: cell.Header.LeftChild,
	// 	})
	// }
	return keyPairs

}

func (node *BtreePage) MergeNodes() error {
	leftNode, rightNode, err := node.chooseFrom()
	if err != nil && err != BothUnderFlowError {
		return err
	}
	if leftNode.NumSlots+rightNode.NumSlots+1 > NODEFULL-1 {
		return NoMergeRequired
	}
	if node.PageType == pager.LEAF || node.PageType == pager.INTERIOR {
		// leftnode is added to the rightnode
		for _, v := range leftNode.GetSlots() {
			cell := leftNode.GetCellByOffset(v)
			res := binary.BigEndian.Uint64(cell.CellContent)
			rightNode.Insertkey(res, cell.Header.LeftChild)
		}
		firstcell, err := node.GetCell(0)
		if err != nil {
			return err
		}
		parentslot, parentId, err := GetParent(binary.BigEndian.Uint64(firstcell.CellContent))
		if err != nil {
			return err
		}
		parentPage, err := pager.GetPage(uint(*parentId))
		if err != nil {
			return err
		}
		// FIXME: check if the parent is underflow

		parentCell, err := parentPage.GetCell(uint(*parentslot))
		if err != nil {
			return err
		}
		parent := BtreePage{*parentPage}
		// FIXME: should be parent.pagetype == rootpage
		if parent.NumSlots == 1 {

			Init.UpdateRootPage(uint(rightNode.PageId))
			if rightNode.PageType == pager.LEAF && parent.PageType == pager.INTERIOR {
				rightNode.PageType = pager.INTERIOR
			} else if rightNode.PageType == pager.LEAF {
				rightNode.PageType = pager.ROOT_AND_LEAF
			} else {
				rightNode.PageType = pager.ROOTPAGE

			}
			rightNode.UpdatePageHeader()
		}
		leftNode.RemoveRange(0, uint(leftNode.NumSlots))
		pager.MakeFreelistPage(leftNode.PageId)
		rightNode.Insertkey(binary.BigEndian.Uint64(parentCell.CellContent), leftNode.RightPointer)

		parentPage.RemoveCell(uint(*parentslot))
		parentPage.UpdatePageHeader()
		parent.Shuffle()

	}

	return nil
}

func (node *BtreePage) MergeorRedistribute() {
	_, _, err := node.chooseFrom()
	if err != nil && err == BothUnderFlowError {
		if err := node.MergeNodes(); err == NoMergeRequired {
			node.Shuffle()
		}
	}
}
