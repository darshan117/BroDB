package pager

import (
	Init "blackdb/src/init"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"syscall"
)

// serializing the pageheader fields
func (page *PageHeader) serializePageHeader(pageHeader []byte) []byte {

	binary.BigEndian.PutUint16(pageHeader[0:], uint16(page.PageId))
	pageHeader[2] = byte(page.PageType)
	binary.BigEndian.PutUint16(pageHeader[3:], uint16(page.FreeStart))
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
	Header.FreeStart = binary.BigEndian.Uint16(pageHeader[3:5])
	Header.freeEnd = binary.BigEndian.Uint16(pageHeader[5:7])
	Header.totalFree = binary.BigEndian.Uint16(pageHeader[7:9])
	Header.NumSlots = binary.BigEndian.Uint16(pageHeader[9:])
	Header.lastOffsetUsed = binary.BigEndian.Uint16(pageHeader[11:13])
	Header.RightPointer = binary.BigEndian.Uint16(pageHeader[13:15])
	Header.flags = pageHeader[15]
	return Header

}

// dbwrite don't use the loadpage
func (page *PageHeader) UpdatePageHeader() {
	if page.PageId != uint16(BufData.PageNum) {
		err := LoadPage(uint(page.PageId))
		if err != nil {
			log.Fatal(err)
		}
	}
	pageHeader := make([]byte, PAGEHEAD_SIZE)
	page.serializePageHeader(pageHeader)
	offset := int64(page.PageId) * int64(Init.PAGE_SIZE)
	_, err := Init.Dbfile.WriteAt(pageHeader, int64(offset)) // 0 means relative to the origin of the file
	if err != nil {
		log.Printf("error incrementing the total pages : %s\n", err)
	}

}

// Replaces only the cell content and its leftpointers
func (page *PageHeader) ReplaceCell(cell *Cell, buf []byte, leftPointer uint16) {
	defer page.UpdatePageHeader()
	if page.PageId != uint16(BufData.PageNum) {
		LoadPage(uint(page.PageId))
	}
	cell.CellContent = buf
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

func (cell *Cell) deserializeCell(cellHeader []byte) error {
	cell.Header.cellLoc = binary.BigEndian.Uint16(cellHeader[:2])
	cell.Header.cellSize = binary.BigEndian.Uint16(cellHeader[2:4])
	// FIXME: made ti const
	if int(cellHeader[4]) == 1 {
		cell.Header.isOverflow = true
	} else {
		cell.Header.isOverflow = false
	}
	cell.Header.LeftChild = binary.BigEndian.Uint16(cellHeader[5:7])
	return nil

}

// Loads the page to memory using mmap.
func LoadPage(pageNo uint) error {
	BufData.PageNum = pageNo

	fileStat, err := Init.Dbfile.Stat()
	if err != nil {
		return fmt.Errorf("error while reading file Info ... %w  %s", err, fileStat)
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
	binary.BigEndian.PutUint16(BufData.Data[page.FreeStart-2:page.FreeStart], uint16(0))
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
	for i := start; i < end; i++ {
		oldCell, err := page.GetCell(i)
		if err != nil {
			return err
		}
		page.totalFree += oldCell.Header.cellSize
		page.totalFree += uint16(CELL_HEAD_SIZE)
		page.totalFree += 2 // slot size has
	}
	copy(BufData.Data[startIndex:], BufData.Data[endIndex:page.FreeStart])

	page.FreeStart -= uint16(endIndex - startIndex)
	page.NumSlots -= uint16(end - start)
	page.UpdatePageHeader()
	return nil
}

// TODO: can use db read here
func (page *PageHeader) SlotArray() map[uint16]PointerList {
	var slotmap = make(map[uint16]PointerList)
	LoadPage(uint(page.PageId))

	startidx := PAGEHEAD_SIZE // page Header size make it global
	id := 0

	for i := startidx; i < uint(page.FreeStart); {
		var slot PointerList
		offset := make([]byte, 2)
		copy(offset, BufData.Data[i:i+2])
		offsetVal := binary.BigEndian.Uint16(offset)
		slot.index = uint16(id)
		cell, err := page.GetCell(uint((i - PAGEHEAD_SIZE) / 2))
		if err != nil {
			log.Println(err)
		}
		slot.size = cell.Header.cellSize
		slot.contents = cell.CellContent
		slotmap[offsetVal] = slot
		i += 2
		id += 1
		offset = nil
	}
	return slotmap

}

// return all slots in a []uint16
func (page *PageHeader) GetSlots() []uint16 {

	startidx := PAGEHEAD_SIZE
	slots := make([]uint16, 0)
	if len(BufData.Data) == 0 {
		if err := LoadPage(uint(page.PageId)); err != nil {
			log.Println(err)
		}
		return slots
	}
	pageData := page.FileRead()
	if page.FreeStart > uint16(Init.PAGE_SIZE) {
		log.Fatalf("%+v\n", page)
	}
	for i := startidx; i < uint(page.FreeStart); {

		offset := pageData[i : i+2]
		offsetVal := binary.BigEndian.Uint16(offset)
		slots = append(slots, offsetVal)
		i += 2
	}
	return slots
}

// after Defragmenting all the slots need to be fixed
func (page *PageHeader) fixSlot(index uint, offset uint16) {
	slotIndex := PAGEHEAD_SIZE + index*2
	if offset > uint16(Init.PAGE_SIZE) {
		fmt.Println("Error fix slot got offset bigger than pagesize", offset, index)
	}
	binary.BigEndian.PutUint16(BufData.Data[slotIndex:slotIndex+2], offset)
}

// returns all the keys of the node
func (page *PageHeader) GetKeys() []uint32 {
	if page.PageId != uint16(BufData.PageNum) {
		// FIXME: do error handling here
		newPage, err := GetPage(uint(page.PageId))
		if err != nil {
			PagerError("Getkeys", ErrLoadPage, err)
		}
		return newPage.GetKeys()
	}
	keys := make([]uint32, 0)
	for _, val := range page.GetSlots() {
		cell := page.GetCellByOffset(val)
		res := binary.BigEndian.Uint32(cell.CellContent[:4])
		keys = append(keys, res)
	}
	return keys

}
func (page *PageHeader) PageDebug() {
	fmt.Printf(" %+v \n", page)
	fmt.Printf("%+v \n", page.SlotArray())

}

func GetPage(id uint) (*PageHeader, error) {
	if id == 0 {
		panic("got page zero ")
	}
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

func (page *PageHeader) checkUsableSpace() int {
	return int(page.freeEnd - page.FreeStart)
}

func (node *PageHeader) UpdateLeftPointer(newLoc uint, cell *Cell) {
	cell.Header.LeftChild = uint16(newLoc)
	cellLocation := cell.Header.cellLoc
	cellSer, n := cell.Header.serializeCell(cell.CellContent)

	no := copy(BufData.Data[cellLocation-uint16(n):cellLocation], cellSer.Bytes())
	var newcell Cell
	newcell.deserializeCell(BufData.Data[cellLocation-uint16(no) : cellLocation])
}
func (page *PageHeader) UpdateRightPointer(newLoc uint, newpage *PageHeader) error {
	newpage.RightPointer = page.RightPointer
	newpage.UpdatePageHeader()
	if newLoc != 0 {
		LoadPage(uint(page.PageId))
		page.RightPointer = uint16(newLoc)
		page.UpdatePageHeader()

	}
	return nil
}

// This is required for balancing where key is removed from the parent node
//
// needs to have left pointers page rightmost child
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

func (page *PageHeader) FileRead() []byte {
	pageData := make([]byte, Init.PAGE_SIZE)
	offset := int64(Init.PAGE_SIZE * int(page.PageId))
	_, err := Init.Dbfile.ReadAt(pageData, offset)
	if err != nil {
		// handle this error
	}
	return pageData

}

func (page *PageHeader) FileReadAndUpdate(contents []byte, offset int64) {
	pageData := page.FileRead()
	_, err := Init.Dbfile.WriteAt(contents, offset)
	if err != nil {
		// handle error
	}
	fmt.Println(pageData)

}
