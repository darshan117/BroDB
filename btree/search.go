package btree

import (
	Init "blackdb/init"
	"blackdb/pager"
	"encoding/binary"
	"fmt"
)

func (tree *BtreePage) search(key uint64) (uint16, uint16, error) {
	if pager.BufData.PageNum != uint(tree.PageId) {
		pager.GetPage(uint(tree.PageId))
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:], key)
	for i, val := range tree.GetSlots() {
		// can do the binary search here
		cell := tree.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.CellContent)
		if res == key {
			return uint16(i), tree.PageId, nil
		} else if res > key {
			leftPage, err := pager.GetPage(uint(cell.Header.LeftChild))
			if err != nil {
				return 0, 0, err
			}
			lPage := BtreePage{*leftPage}
			return lPage.search(key)
		}
	}
	if tree.RightPointer != 0 && tree.PageType != pager.LEAF {
		rightChildPage, err := pager.GetPage(uint(tree.RightPointer))
		if err != nil {
			return 0, 0, err
		}
		rPage := BtreePage{*rightChildPage}
		return rPage.search(key)
	}
	return 0, 0, fmt.Errorf("key not found %d", key)

}

// can make it pointer to the uint16
func Search(key uint64) (slot uint16, pageid uint16, err error) {
	RootNode, err := pager.GetPage(uint(Init.ROOTPAGE))
	if err != nil {
		return 0, 0, fmt.Errorf("error while insert to the btree %w", err)
	}
	rootnode := BtreePage{*RootNode}
	return rootnode.search(key)
}
