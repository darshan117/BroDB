package pager

import (
	Init "blackdb/init"
	"bytes"
	"encoding/binary"
	"fmt"
	"syscall"
)

// type PageHeader struct {
// 	// FIXME: might need to remove the PageId
// 	PageId         uint8
// 	PageType       PageType
// 	freeStart      uint16
// 	freeEnd        uint16
// 	totalFree      uint16
// 	NumSlots       uint16
// 	lastOffsetUsed uint16
// 	RightPointer   uint16
// 	flags          uint8
// }

func (page *PageHeader) serializePageHeader(pageHeader []byte) []byte {

	binary.BigEndian.PutUint16(pageHeader[0:], uint16(page.PageId))
	pageHeader[2] = byte(page.PageType)
	binary.BigEndian.PutUint16(pageHeader[3:], uint16(page.freeStart))
	binary.BigEndian.PutUint16(pageHeader[5:], uint16(page.freeEnd))
	binary.BigEndian.PutUint16(pageHeader[7:], uint16(page.totalFree))
	binary.BigEndian.PutUint16(pageHeader[9:], uint16(page.NumSlots))
	binary.BigEndian.PutUint16(pageHeader[11:], uint16(page.lastOffsetUsed))
	binary.BigEndian.PutUint16(pageHeader[13:], uint16(page.RightPointer))
	pageHeader[15] = page.flags
	return pageHeader

}
func deserializePageHeader(pageHeader []byte) PageHeader {
	var Header PageHeader
	Header.PageId = binary.BigEndian.Uint16(pageHeader[:2])
	Header.PageType = PageType(pageHeader[2])
	Header.freeStart = binary.BigEndian.Uint16(pageHeader[3:5])
	Header.freeEnd = binary.BigEndian.Uint16(pageHeader[5:7])
	Header.totalFree = binary.BigEndian.Uint16(pageHeader[7:9])
	Header.NumSlots = binary.BigEndian.Uint16(pageHeader[9:])
	Header.lastOffsetUsed = binary.BigEndian.Uint16(pageHeader[11:13])
	Header.RightPointer = binary.BigEndian.Uint16(pageHeader[13:15])
	Header.flags = pageHeader[15]
	return Header

}

// dbwrite don't use the loadpage
func (page *PageHeader) UpdatePageHeader() error {
	if page.PageId != uint16(BufData.PageNum) {
		err := LoadPage(uint(page.PageId))
		if err != nil {
			// FIXME: do error handling here
			fmt.Println(err)
		}
	}
	pageHeader := make([]byte, PAGEHEAD_SIZE)
	page.serializePageHeader(pageHeader)
	// fmt.Println("pageheader is ", pageHeader)
	// copy(BufData.Data[:PAGEHEAD_SIZE], pageHeader)
	offset := int64(page.PageId) * int64(Init.PAGE_SIZE)
	_, err := Init.Dbfile.WriteAt(pageHeader, int64(offset)) // 0 means relative to the origin of the file
	if err != nil {
		return fmt.Errorf("error incrementing the total pages : %w", err)
	}
	return nil

}

func (page *PageHeader) ReplaceCell(cell *Cell, key uint64, leftPointer uint16) {
	if page.PageId != uint16(BufData.PageNum) {
		LoadPage(uint(page.PageId))
	}
	// load page while replacing cell
	res := make([]byte, 8)
	binary.BigEndian.PutUint64(res[0:], key)
	cell.CellContent = res
	cell.Header.LeftChild = leftPointer
	cellLocation := cell.Header.cellLoc
	cellSer, n := cell.Header.serializeCell(cell.CellContent)
	copy(BufData.Data[cellLocation-uint16(n):cellLocation], cellSer.Bytes())

}

func IncrementTotalPages() error {
	buff := make([]byte, 4)
	binary.BigEndian.PutUint32(buff, uint32(Init.TOTAL_PAGES+1))
	_, err := Init.Dbfile.WriteAt(buff, 30) // 0 means relative to the origin of the file
	if err != nil {
		return fmt.Errorf("error incrementing the total pages : %w", err)
	}
	Init.TOTAL_PAGES += 1

	return nil

}

func (cell *CellHeader) serializeCell(cellContent []byte) (*bytes.Buffer, uint) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, cell.cellLoc)
	binary.Write(&buf, binary.BigEndian, cell.cellSize)
	binary.Write(&buf, binary.BigEndian, cell.isOverflow)
	binary.Write(&buf, binary.BigEndian, cell.LeftChild)
	binary.Write(&buf, binary.BigEndian, cellContent)
	return &buf, uint(buf.Len())

}

func (cell *Cell) deserializeCell(cellHeader []byte) uint {
	cell.Header.cellLoc = binary.BigEndian.Uint16(cellHeader[:2])
	cell.Header.cellSize = binary.BigEndian.Uint16(cellHeader[2:4])
	if int(cellHeader[4]) == 1 {
		cell.Header.isOverflow = true
	} else {
		cell.Header.isOverflow = false
	}
	cell.Header.LeftChild = binary.BigEndian.Uint16(cellHeader[5:7])
	return uint(len(cellHeader))

}

func LoadPage(pageNo uint) error {
	BufData.PageNum = pageNo

	fileStat, err := Init.Dbfile.Stat()
	if err != nil {
		return fmt.Errorf("error while reading file Info ... %w", err)
	}
	offset := BufData.PageNum * uint(Init.PAGE_SIZE)
	mapSize := func() uint {
		fileSize := uint(fileStat.Size())
		if offset >= fileSize {
			return 0 // Or handle this error case appropriately
		}
		remainingSize := fileSize - offset
		if remainingSize < uint(Init.PAGE_SIZE) {
			return uint(remainingSize)
		}
		return uint(Init.PAGE_SIZE)
		// if offset > uint(fileStat.Size()) {
		// 	fmt.Println("offset is ", offset)
		// 	return uint(fileStat.Size()) - offset
		// }
		// return uint(Init.PAGE_SIZE)
	}
	BufData.Data, err = syscall.Mmap(int(Init.Dbfile.Fd()), int64(offset), int(mapSize()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("error while mapping the new page %w", err)
	}
	BufData.PageNum = pageNo
	return nil
}

func (page *PageHeader) ShiftSlots(idx uint) {
	if page.PageId != uint16(BufData.PageNum) {
		LoadPage(BufData.PageNum)
	}
	slotIndex := PAGEHEAD_SIZE + idx*2
	for i := 0; i < int(page.NumSlots)-int(idx); i++ {
		copy(BufData.Data[slotIndex:slotIndex+2], BufData.Data[slotIndex+2:slotIndex+4])
		slotIndex += 2

	}
	binary.BigEndian.PutUint16(BufData.Data[page.freeStart-2:page.freeStart], uint16(0))
}
func (page *PageHeader) InsertSlot(idx int, offsetVal uint16) {
	if page.NumSlots > 0 {
		ind := int(page.NumSlots - 1)
		slotid := PAGEHEAD_SIZE + uint(idx)*2
		for i := ind; i >= idx; i-- {
			slotIndex := PAGEHEAD_SIZE + uint(i*2)
			copy(BufData.Data[slotIndex+2:slotIndex+4], BufData.Data[slotIndex:slotIndex+2])
		}
		binary.BigEndian.PutUint16(BufData.Data[slotid:slotid+2], offsetVal)

	}

}

func (page *PageHeader) RemoveRange(start, end uint) error {
	if page.PageId != uint16(BufData.PageNum) {
		if err := LoadPage(uint(page.PageId)); err != nil {
			return err
		}
	}

	startIndex := PAGEHEAD_SIZE + start*2
	endIndex := PAGEHEAD_SIZE + end*2
	// remainingBytes := page.freeStart - uint16(endIndex)
	for i := start; i < end; i++ {
		oldCell, err := page.GetCell(i)
		if err != nil {
			return err
		}
		page.totalFree += oldCell.Header.cellSize
		page.totalFree += uint16(CELL_HEAD_SIZE)
		page.totalFree += 2 // slot size has
		// page.freeStart += 2
	}

	// fmt.Println("before", BufData.Data[:40])
	// Move the remaining data to fill the gap
	copy(BufData.Data[startIndex:], BufData.Data[endIndex:page.freeStart])

	// Zero out the now-unused space at the end
	for i := page.freeStart - uint16(endIndex-startIndex); i < page.freeStart; i++ {
		BufData.Data[i] = 0
	}

	// Update freeStart
	page.freeStart -= uint16(endIndex - startIndex)
	page.NumSlots -= uint16(end - start)
	page.UpdatePageHeader()
	return nil
}

// TODO: range remove slots
func (page *PageHeader) StartRangeRemoveSlots(start uint, end uint) {
	defer page.UpdatePageHeader()
	// shiftslots starting from   start index
	freeSpace := uint16(0)
	freestart := uint16(0)
	for i := start; i < end; i++ {
		oldCell, _ := page.GetCell(i)

		freeSpace += oldCell.Header.cellSize
		freeSpace += uint16(CELL_HEAD_SIZE)
		freestart += 2
		freeSpace += 2 // slot size has
	}
	page.rangeRemoveEnd(start, end)
	page.NumSlots -= uint16(end) - uint16(start)
	page.totalFree += freeSpace
	page.freeStart -= freestart

}
func (page *PageHeader) EndRangeRemoveSlots(start uint, end uint) {
	// shiftslots starting from   start index
	if page.PageId != uint16(BufData.PageNum) {
		LoadPage(uint(page.PageId))
	}
	// fmt.Println("before ", BufData.Data[:40])

	freeSpace := uint16(0)
	freestart := uint16(0)
	for i := start; i < end; i++ {
		oldCell, _ := page.GetCell(i)
		freeSpace += oldCell.Header.cellSize
		freeSpace += uint16(CELL_HEAD_SIZE)
		freestart += 2
		freeSpace += 2 // slot size has
		page.ShiftSlots(i)
	}
	// page.rangeRemove(start, end)
	page.rangeRemovestart(start, end)
	page.NumSlots -= uint16(end) - uint16(start)
	page.totalFree += freeSpace
	page.freeStart -= freestart
	page.UpdatePageHeader()
	// fmt.Println("after ", BufData.Data[:40])

}
func (page *PageHeader) rangeRemovestart(start uint, end uint) {
	if page.PageId != uint16(BufData.PageNum) {
		LoadPage(uint(page.PageId))
	}
	endIndex := PAGEHEAD_SIZE + (end)*2
	var buf bytes.Buffer
	for i := 0; i < int(end-start); i++ {
		binary.Write(&buf, binary.BigEndian, uint16(0))
	}
	// copy(BufData.Data[PAGEHEAD_SIZE:startIndex], BufData.Data[startIndex:endIndex])
	// fmt.Println("after ", BufData.Data[startIndex:endIndex])
	copy(BufData.Data[endIndex:page.freeStart], buf.Bytes())

}

func (page *PageHeader) rangeRemoveEnd(start uint, end uint) {
	startIndex := PAGEHEAD_SIZE + start*2
	endIndex := PAGEHEAD_SIZE + (end)*2
	var buf bytes.Buffer
	for i := 0; i < int(end-start); i++ {
		binary.Write(&buf, binary.BigEndian, uint16(0))
	}
	copy(BufData.Data[startIndex:endIndex], BufData.Data[endIndex:page.freeStart])
	// fmt.Println("after ", BufData.Data[startIndex:endIndex])
	copy(BufData.Data[endIndex:page.freeStart], buf.Bytes())

}

func (page *PageHeader) SlotArray() map[uint16]PointerList {
	var slotmap = make(map[uint16]PointerList)
	startidx := PAGEHEAD_SIZE // page Header size make it global
	id := 0

	for i := startidx; i < uint(page.freeStart); {
		var slot PointerList
		offset := BufData.Data[i : i+2]
		offsetVal := binary.BigEndian.Uint16(offset)
		slot.index = uint16(id)
		// FIXME:  do the error checking as getcell also returns the error
		cell, err := page.GetCell(uint((i - PAGEHEAD_SIZE) / 2))
		if err != nil {
			fmt.Println(err)
		}
		slot.size = cell.Header.cellSize
		slot.contents = cell.CellContent
		slotmap[offsetVal] = slot
		i += 2
		id += 1
	}
	return slotmap

}

func (page *PageHeader) GetSlots() []uint16 {
	// if page.PageId != uint16(BufData.PageNum) {
	// 	newpage, _ := GetPage(uint(page.PageId))
	// 	return newpage.GetSlots()
	// }
	LoadPage(uint(page.PageId))

	startidx := PAGEHEAD_SIZE
	slots := make([]uint16, 0)
	if len(BufData.Data) == 0 {
		fmt.Println("page is ", BufData.PageNum, "page asked for ", page.PageId, "len is ", BufData.Data)
		if err := LoadPage(uint(page.PageId)); err != nil {
			fmt.Println(err)

		}
		fmt.Println("page is ", BufData.PageNum, "page asked for ", page.PageId, "len is ", BufData.Data)
		return slots
	}
	for i := startidx; i < uint(page.freeStart); {
		offset := BufData.Data[i : i+2]
		offsetVal := binary.BigEndian.Uint16(offset)
		slots = append(slots, offsetVal)
		i += 2
	}
	return slots
}

func (page *PageHeader) fixSlot(index uint, offset uint16) {
	slotIndex := PAGEHEAD_SIZE + index*2
	binary.BigEndian.PutUint16(BufData.Data[slotIndex:slotIndex+2], offset)
}

func (page *PageHeader) GetKeys() []uint64 {
	// if page.PageId != uint16(BufData.PageNum) {
	// FIXME: do error handling here
	newPage, _ := GetPage(uint(page.PageId))
	// 	return newPage.GetKeys()
	// }
	keys := make([]uint64, 0)
	for _, val := range page.GetSlots() {
		cell := newPage.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.CellContent)
		keys = append(keys, res)
	}
	return keys

}
func (page *PageHeader) PageDebug() {
	fmt.Printf(" %+v \n", page)
	fmt.Printf("%+v \n", page.SlotArray())

}

func GetPage(id uint) (*PageHeader, error) {
	err := LoadPage(uint(id))
	if err != nil {
		return &PageHeader{}, fmt.Errorf("error while Loading the page | %w", err)
	}
	page := deserializePageHeader(BufData.Data)
	return &page, nil
}

func (overflow *OverflowPageHeader) serializeOverflowPage(payload []byte) {
	binary.BigEndian.PutUint16(payload[0:], overflow.next)
	binary.BigEndian.PutUint16(payload[2:], overflow.size)

}

func (overflow *OverflowPtr) serializeOverflow() []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, overflow.payload)
	binary.Write(&buf, binary.BigEndian, overflow.ptr)
	return buf.Bytes()

}

func (page *PageHeader) checkUsableSpace() uint16 {
	return page.freeEnd - page.freeStart
}

// TODO: Update left Pointer
func (node *PageHeader) UpdateLeftPointer(newLoc uint, cell *Cell) {
	cell.Header.LeftChild = uint16(newLoc)
	cellLocation := cell.Header.cellLoc
	// fmt.Printf("cell location is %d %+v \n", cellLocation, cell)
	cellSer, n := cell.Header.serializeCell(cell.CellContent)
	// check if it is correct
	// newCell := node.GetCellByOffset(cellLocation)
	// fmt.Printf("the new cell is %+v\n ", cell.CellContent)
	no := copy(BufData.Data[cellLocation-uint16(n):cellLocation], cellSer.Bytes())
	var newcell Cell
	newcell.deserializeCell(BufData.Data[cellLocation-uint16(no) : cellLocation])
	// fmt.Printf("%+v left pointer is updated ", newcell)
}
func (page *PageHeader) UpdateRightPointer(newLoc uint, newpage *PageHeader) error {
	newpage.RightPointer = page.RightPointer
	newpage.UpdatePageHeader()
	if newLoc != 0 {
		LoadPage(uint(page.PageId))
		page.RightPointer = uint16(newLoc)
		page.UpdatePageHeader()

	}
	// if page.RightPointer != 0 {
	// 	// go the RightPointer
	// 	rightPage, err := GetPage(uint(page.RightPointer))
	// 	if err != nil {
	// 		return fmt.Errorf("error while updating the right pointer..  %w", err)
	// 	}
	// 	return rightPage.UpdateRightPointer(newLoc)
	// }
	// page.RightPointer = uint16(newLoc)
	// fmt.Println("updating the right pointer ", page.GetKeys())
	// if newLoc != 0 {
	// 	newpage, _ := GetPage(newLoc)
	// 	LoadPage(newLoc)
	// 	fmt.Println("updating the right pointer ", newpage.GetKeys())

	// }

	return nil
}

// TODO: Update the right Pointer
// use the db.write for updating right pointer or update pageHeader() will work here
// TODO: fixPointers Function as a wrapper for the updateleftPointer and updateRightPointer
func (node *PageHeader) GetrightmostPage() (pageid *PageHeader, err error) {
	if node.PageType == LEAF {
		if node.RightPointer != 0 {
			return nil, fmt.Errorf("some error in node rightpointer | got the rightpointer in the leaf node %d", node.RightPointer)
		}

		return node, nil
	}

	rightpage, err := GetPage(uint(node.RightPointer))
	if err != nil {
		return nil, err
	}

	return rightpage.GetrightmostPage()

}
