package btree

import (
	"blackdb/src/pager"
	"fmt"
)

type RemoveOptions struct {
	slot uint
}

func (node *BtreePage) remove(slot uint) error {
	if node.PageType == pager.LEAF {

		if err := node.RemoveCell(slot); err != nil {
			return err
		}
		node.Shuffle()
		return nil
	} else if node.PageType == pager.ROOT_AND_LEAF {
		if err := node.RemoveCell(slot); err != nil {
			return err
		}

	} else if node.PageType == pager.INTERIOR || node.PageType == pager.ROOTPAGE {
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
		if leftchildPage.NumSlots <= uint16(UNDERFLOW) {
			leftchildPage.MergeNodes()
		}
		pageid, err := leftchildPage.GetrightmostPage()
		if err != nil {
			fmt.Println(err)
			return err
		}
		rightChildCell, err := pageid.GetCell(uint(pageid.NumSlots - 1))
		if err != nil {
			fmt.Println(err)
			return err
		}
		node.ReplaceCell(&keyCell, rightChildCell.CellContent, leftPointer)
		if err := pageid.RemoveCell(uint(pageid.NumSlots) - 1); err != nil {
			fmt.Println(err)
			return err
		}
		rightchildpage := BtreePage{*pageid}
		leftchildPage.UpdatePageHeader()
		rightchildpage.UpdatePageHeader()
		node.UpdatePageHeader()
		leftchildPage.Shuffle()
		rightchildpage.Shuffle()
		node.Shuffle()
	}

	return nil
}

func Remove(key uint32) error {
	slot, pageId, err := Search(key)
	if err != nil {
		return err
	}
	page, err := pager.GetPage(uint(pageId))
	if err != nil {
		return err
	}
	node := BtreePage{*page}
	nodePage, err := pager.GetPage(uint(node.PageId))
	if err != nil {
		return err
	}
	nPage := BtreePage{*nodePage}
	if err := nPage.remove(uint(slot)); err != nil {
		fmt.Println(err)
		return err
	}

	return nil

}
