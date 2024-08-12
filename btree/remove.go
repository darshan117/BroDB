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
		fmt.Println("removing the key ", key)
		if err := node.RemoveCell(slot); err != nil {
			return err
		}
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
		fmt.Printf("before page is %+v \n", pageid)
		// pager.LoadPage(uint(pageid.PageId))
		if err := pageid.RemoveCell(uint(pageid.NumSlots) - 1); err != nil {
			fmt.Println(err)
			return err
		}
		fmt.Println(slot, "is slot ", binary.BigEndian.Uint64(rightChildCell.CellContent))
		fmt.Printf("after page is %+v \n", pageid)
		node.Shuffle()

		// go to the rightmost pointer and replace it the right most
		// this might contain the concept of the adding the deleted page to the freelist
		//	   of the page zero
		//    do the following thing
		// 1. use the choose from function
		// if err != nil and both sibs are underflow then merge the pages recursively
		// make  a separate function for it

		// add a function called the replace cell
		// always take the leftsib as removing the last element is easy
		// pager.ReplaceCell()
		// range remove the left or the right

	}
	// difficult part is removing from the interior page or hte root page

	return nil
}

func Remove(key uint64) error {
	slot, pageId, err := Search(key)
	if err != nil {
		return err
	}
	page, err := pager.GetPage(uint(pageId))
	if err != nil {
		return err
	}
	node := BtreePage{*page}
	if err := node.remove(key, uint(slot)); err != nil {
		return err
	}
	// node.Shuffle()

	return nil

}

// TODO: replace key search for the key and

// Get the rightmost child
