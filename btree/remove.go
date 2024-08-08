package btree

import (
	"blackdb/pager"
	"fmt"
)

func (node *(BtreePage)) remove(key uint64) error {
	// if the node is the leaf page then remove easyily
	if node.PageType == pager.LEAF || node.PageType == pager.ROOT_AND_LEAF {
		slot, pageId, err := Search(key)
		if err != nil {
			return err
		}
		// get the page
		page, err := pager.GetPage(uint(pageId))
		if err != nil {
			return err
		}
		if err := page.RemoveCell(uint(slot)); err != nil {
			return err
		}
		node.Shuffle()
		return nil
	} else if node.PageType == pager.INTERIOR {
		// this might contain the concept of the adding the deleted page to the freelist
		//	   of the page zero
		//    do the following thing
		// 1. use the choose from function
		// if err != nil and both sibs are underflow then merge the pages recursively
		// make  a separate function for it
		leftsib, rightsib, err := node.chooseFrom()
		if err != nil {
			// merge the pages
		}
		// add a function called the replace cell
		// always take the leftsib as removing the last element is easy
		// pager.ReplaceCell()
		fmt.Println(leftsib, rightsib)
		// range remove the left or the right

	}
	// difficult part is removing from the interior page or hte root page

	return nil
}

// TODO: replace key search for the key and
