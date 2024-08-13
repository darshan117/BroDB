package btree

import (
	"blackdb/pager"
	"encoding/binary"
	"fmt"
)

type RemoveOptions struct {
	slot uint
}

func (node *BtreePage) remove(key uint64, slot uint) error {
	// if the node is the leaf page then remove easyily
	if node.PageType == pager.LEAF || node.PageType == pager.ROOT_AND_LEAF {

		if err := node.RemoveCell(slot); err != nil {
			return err
		}
		// if node.isUnderFlow() {
		// 	leftsib, rightsib, err := node.chooseFrom()
		// 	if err != nil && err == BothUnderFlowError {
		// 		node.MergeNodes(leftsib, rightsib)
		// 	}
		// }

		// if node.pagetype = rootpage and node.numslots -==1{

		// merge interior nodes and
		// }
		node.Shuffle()
		return nil
	} else if node.PageType == pager.INTERIOR || node.PageType == pager.ROOTPAGE {
		// TODO: get node.leftchild could be a helper function
		keyCell, err := node.GetCell(slot)
		if err != nil {
			return err
		}
		leftPointer := keyCell.Header.LeftChild
		leftchild, err := pager.GetPage(uint(leftPointer))
		if err != nil {
			return err
		}
		leftchildPage := BtreePage{*leftchild}
		// if the left child page is not underflow
		if leftchildPage.NumSlots <= uint16(UNDERFLOW) {
			// node.shuffle or node . merge
			rightsib, err := node.RightSiblingCount()
			if err != nil {
				return err
			}
			node.MergeNodes(&leftchildPage, rightsib)
			return nil
		}
		// then get the right pointers leftmostpage() or directly shuffle with merging
		pageid, err := leftchildPage.GetrightmostPage()
		if err != nil {
			return err
		}
		rightChildCell, err := pageid.GetCell(uint(pageid.NumSlots - 1))
		if err != nil {
			return err
		}
		node.ReplaceCell(&keyCell, binary.BigEndian.Uint64(rightChildCell.CellContent), leftPointer)
		// pager.LoadPage(uint(pageid.PageId))
		if err := pageid.RemoveCell(uint(pageid.NumSlots) - 1); err != nil {
			fmt.Println(err)
			return err
		}
		node.Shuffle()

	}
	// difficult part is removing from the interior page or hte root page

	return nil
}

func Remove(key uint64) error {
	fmt.Println("removing the key ", key)
	slot, pageId, err := Search(key)
	if err != nil {
		return err
	}
	page, err := pager.GetPage(uint(pageId))
	if err != nil {
		return err
	}
	node := BtreePage{*page}
	fmt.Println(node.GetKeys())
	if node.isUnderFlow() {
		fmt.Println("node is underflow")
		leftsib, rightsib, err := node.chooseFrom()
		if err != nil && err == BothUnderFlowError {
			// node.MergeNodes(leftsib, rightsib)
			return err
		}
		fmt.Println("leftsib and right sib ", leftsib.GetKeys(), rightsib.GetKeys())
	}
	if err := node.remove(key, uint(slot)); err != nil {
		return err
	}
	// node.Shuffle()

	return nil

}

// TODO: replace key search for the key and

// Get the rightmost child
