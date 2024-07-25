package pager

import (
	Init "blackdb/init"
	"bytes"
	"encoding/binary"
	"fmt"
	"syscall"
)

var (
	BufData        BufPage // current buffer memory cache of the current page is stored here
	PAGEHEAD_SIZE  uint    = 14
	MINCELLSIZE    uint    = 10
	SLOT_SIZE      uint    = 2
	CELL_HEAD_SIZE uint    = 5
)

func MakePage(ptype PageType, id uint32) (PageHeader, error) {
	// make the header for the newPage
	pageHeader := make([]byte, Init.PAGE_SIZE)
	page := PageHeader{
		pageId:    id,
		pageType:  ptype,
		freeStart: uint16(PAGEHEAD_SIZE),
		freeEnd:   uint16(Init.PAGE_SIZE),
		numSlots:  0,
		flags:     8,
	}
	page.totalFree = page.freeEnd - page.freeStart
	// setting the pageHeader
	// BUG: make a different function for serialized the page header
	binary.BigEndian.PutUint64(pageHeader[0:], uint64(page.pageId))
	pageHeader[4] = byte(page.pageType)
	binary.BigEndian.PutUint16(pageHeader[5:], uint16(page.freeStart))
	binary.BigEndian.PutUint16(pageHeader[7:], uint16(page.freeEnd))
	binary.BigEndian.PutUint16(pageHeader[9:], uint16(page.totalFree))
	pageHeader[11] = byte(page.flags)
	binary.BigEndian.PutUint16(pageHeader[12:], uint16(page.numSlots))
	_, err := Init.Dbfile.Write(pageHeader)
	if err != nil {
		return PageHeader{}, fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	IncrementTotalPages()
	err = LoadPage(uint(id))
	if err != nil {
		return PageHeader{}, fmt.Errorf("error while Loading the page | %w", err)
	}
	return page, nil
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

func MakePageZero(ptype PageType, id uint32) (PageHeader, error) {
	pageHeader := make([]byte, Init.PAGE_SIZE-50)
	page := PageHeader{
		pageId:    id,
		pageType:  ptype,
		freeStart: uint16(PAGEHEAD_SIZE) + 50, // contains hardcoded pageheader size
		freeEnd:   uint16(Init.PAGE_SIZE),
		numSlots:  0,
		flags:     1,
	}
	page.totalFree = page.freeEnd - page.freeStart
	// FIXME: make a different helper functiuon to serialize the page header
	binary.BigEndian.PutUint64(pageHeader[0:], uint64(page.pageId))
	pageHeader[4] = byte(page.pageType)
	binary.BigEndian.PutUint16(pageHeader[5:], uint16(page.freeStart))
	binary.BigEndian.PutUint16(pageHeader[7:], uint16(page.freeEnd))
	binary.BigEndian.PutUint16(pageHeader[9:], uint16(page.totalFree))
	pageHeader[11] = byte(page.flags)
	_, err := Init.Dbfile.Seek(50, 0) // 0 means relative to the origin of the file
	if err != nil {
		return PageHeader{}, fmt.Errorf("error seeking to offset: %w", err)
	}
	_, err = Init.Dbfile.Write(pageHeader) // magic code Brodb
	if err != nil {
		return PageHeader{}, fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	return page, nil

}

func (page *PageHeader) AddCell(cellContent []byte) error {
	cellSize := len(cellContent)
	var cell Cell

	cell.header.cellLoc = page.freeEnd
	cell.header.isOverflow = false
	cellSize += int(CELL_HEAD_SIZE)

	// TODO: check if cell size is max size then make overflow page or new page for it

	if cellSize > int(page.checkUsableSpace()) && cellSize < int(page.totalFree) {
		err := page.Defragment()
		if err != nil {
			return err
		}

	}
	if cellSize > int(page.checkUsableSpace()) && cellSize > int(MINCELLSIZE) {
		oflcell := page.makeOverflowCell(cellContent)
		cell.cellContent = oflcell.serializeOverflow()
		cell.header.isOverflow = true
		cell.header.cellSize += uint16(len(cell.cellContent))

		err := LoadPage(uint(page.pageId))
		if err != nil {
			return fmt.Errorf("error while Loading the page | %w", err)
		}

	} else {
		cell.cellContent = cellContent
		cell.header.cellSize = uint16(len(cell.cellContent))
		// fmt.Println("cell headr size", cell.cellContent)

	}
	cellSer, n := cell.header.serializeCell(cell.cellContent)
	no := copy(BufData.Data[page.freeEnd-uint16(n):page.freeEnd], cellSer.Bytes())
	// fmt.Println("print size of cell copied", no, n)

	page.freeEnd -= uint16(no)
	binary.BigEndian.PutUint16(BufData.Data[page.freeStart:page.freeStart+2], page.freeEnd)
	page.freeStart += uint16(binary.Size(page.freeStart))
	page.totalFree = page.freeEnd - page.freeStart
	page.numSlots += 1
	return nil

}

func (cell *CellHeader) serializeCell(cellContent []byte) (*bytes.Buffer, uint) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, cell.cellLoc)
	binary.Write(&buf, binary.BigEndian, cell.cellSize)
	binary.Write(&buf, binary.BigEndian, cell.isOverflow)
	binary.Write(&buf, binary.BigEndian, cellContent)
	return &buf, uint(buf.Len())

}

func (cell *Cell) deserializeCell(cellheader []byte) uint {
	cell.header.cellLoc = binary.BigEndian.Uint16(cellheader[:2])
	cell.header.cellSize = binary.BigEndian.Uint16(cellheader[2:4])
	if int(cellheader[4]) == 1 {
		cell.header.isOverflow = true
	} else {
		cell.header.isOverflow = false
	}
	return uint(len(cellheader))

}

func LoadPage(pageNo uint) error {
	BufData.pageNum = pageNo

	fileStat, err := Init.Dbfile.Stat()
	if err != nil {
		return fmt.Errorf("error while reading file Info ... %w", err)
	}
	offset := BufData.pageNum * uint(Init.PAGE_SIZE)
	mapSize := func() uint {
		if offset+uint(Init.PAGE_SIZE) > uint(fileStat.Size()) {
			return uint(fileStat.Size()) - offset
		}
		return uint(Init.PAGE_SIZE)
	}
	BufData.Data, err = syscall.Mmap(int(Init.Dbfile.Fd()), int64(offset), int(mapSize()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("error is %w", err)
	}
	return nil
}

func (page *PageHeader) RemoveCell(idx uint) error {
	if idx > uint(page.numSlots)-1 {
		return fmt.Errorf("error while removing cell | index is greater than the max slots... ")

	}
	// FIXME: do the error checking for the func getcell
	oldCell, _ := page.GetCell(idx)
	page.totalFree += oldCell.header.cellSize
	page.ShiftSlots(idx)
	binary.BigEndian.PutUint16(BufData.Data[page.freeStart-2:page.freeStart], uint16(0))
	page.freeStart -= 2
	page.numSlots -= 1
	page.totalFree += 2 // slot size has
	return nil
}

func (page *PageHeader) ShiftSlots(idx uint) {
	slotIndex := PAGEHEAD_SIZE + idx*2
	for i := 0; i < int(page.numSlots)-int(idx); i++ {
		copy(BufData.Data[slotIndex:slotIndex+2], BufData.Data[slotIndex+2:slotIndex+4])
		slotIndex += 2

	}
}

// new implementaton

func (page *PageHeader) SlotArray() map[uint16]PointerList {
	var slotmap = make(map[uint16]PointerList)
	startidx := PAGEHEAD_SIZE // page header size make it global
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
		slot.size = cell.header.cellSize
		slotmap[offsetVal] = slot
		i += 2
		id += 1
	}
	return slotmap

}

func (page *PageHeader) GetCell(idx uint) (Cell, error) {
	// FIXME: changes in getcell check for if page is overflow
	slotIndex := PAGEHEAD_SIZE + idx*2
	offset := BufData.Data[slotIndex : slotIndex+2]
	offsetVal := binary.BigEndian.Uint16(offset)
	var cell Cell
	cellHeaderSize := 5
	cell.deserializeCell(BufData.Data[offsetVal : offsetVal+uint16(cellHeaderSize)+1])
	cell.cellContent = BufData.Data[offsetVal+uint16(cellHeaderSize) : offsetVal+uint16(cellHeaderSize)+cell.header.cellSize]
	if cell.header.isOverflow {
		// TODO:get the last 4 byte offset for the page

		pageOffset := cell.cellContent[len(cell.cellContent)-4:]
		ovPage := binary.BigEndian.Uint32(pageOffset)
		ovPageHeader, err := ReadOverFlowPageHeader(uint(ovPage))
		if err != nil {
			return Cell{}, err
		}
		contents, err := ovPageHeader.ReadOverflowPage(uint(ovPage))
		if err != nil {
			return cell, err
		}
		// FIXME: get the full cell contents concatenated 4 byte header
		fmt.Println("contents of the page are..", string(contents[4:]), "len is ", len(contents))

	}
	return cell, nil

}

func (page *PageHeader) GetCellByOffset(offset uint16) Cell {
	var cell Cell
	cellHeaderSize := 5
	cell.deserializeCell(BufData.Data[offset : offset+uint16(cellHeaderSize)+1])
	cell.cellContent = BufData.Data[offset+uint16(cellHeaderSize) : offset+uint16(cellHeaderSize)+cell.header.cellSize]
	return cell

}

func (page *PageHeader) fixSlot(index uint, offset uint16) {
	slotIndex := PAGEHEAD_SIZE + index*2
	binary.BigEndian.PutUint16(BufData.Data[slotIndex:slotIndex+2], offset)
}

func (page *PageHeader) Defragment() error {

	slotarray := page.SlotArray()
	binheap := heap[uint16]{}
	destPointer := uint16(4096)
	// HACK: did some changes here
	for k := range slotarray {
		binheap.add(k)
	}
	for i := 0; i < int(page.numSlots); i++ {
		offset, err := binheap.remove()
		if err != nil {
			return fmt.Errorf(" %w Error while removing the element from binheap", err)
		}
		newCell := page.GetCellByOffset(offset)
		cellSer, n := newCell.header.serializeCell(newCell.cellContent)
		copy(BufData.Data[destPointer-uint16(n):destPointer], cellSer.Bytes())
		destPointer -= uint16(n)
		page.fixSlot(uint(slotarray[offset].index), destPointer)

	}
	page.freeEnd = destPointer
	page.totalFree = page.freeEnd - page.freeStart
	return nil
}

// overflow pages
func (page *PageHeader) makeOverflowCell(cellContent []byte) OverflowPtr {
	var overflowptr OverflowPtr
	space := page.checkUsableSpace()

	newpayloadsize := len(cellContent) - (len(cellContent) + int(CELL_HEAD_SIZE) - int(space)) - 4 - int(SLOT_SIZE) // FIXME: overflow ptr constant
	NewPayload := cellContent[:newpayloadsize]
	overflowptr.payload = NewPayload
	IncrementTotalPages()
	pageNo := Init.TOTAL_PAGES
	MakeOverFlowPage(uint(Init.TOTAL_PAGES), cellContent[newpayloadsize:])
	overflowptr.ptr = uint32(pageNo)
	return overflowptr

}

func MakeOverFlowPage(pageNum uint, payload []byte) error {
	var overflowHeader OverflowPageHeader
	payl := make([]byte, Init.PAGE_SIZE)
	newpayload := payload

	if len(payload) > Init.PAGE_SIZE-binary.Size(overflowHeader) {
		lenPayload := Init.PAGE_SIZE - binary.Size(overflowHeader)
		newpayload = payload[:lenPayload]
		fmt.Println(lenPayload, len(payload[lenPayload:]))
		MakeOverFlowPage(pageNum+1, payload[lenPayload:])
		overflowHeader.next = uint16(pageNum) + 1
		overflowHeader.size = uint16(lenPayload)
	} else {
		overflowHeader.next = uint16(0)
		overflowHeader.size = uint16(len(payload))

	}
	overflowHeader.serializeOverflowPage(payl)
	copy(payl[4:], newpayload)
	_, err := Init.Dbfile.WriteAt(payl, int64(Init.PAGE_SIZE)*int64(pageNum-1))
	if err != nil {
		return fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	IncrementTotalPages()

	err = LoadPage(uint(pageNum))
	if err != nil {
		return fmt.Errorf("error while Loading the page | %w", err)

	}
	return nil
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

// how to work with the overflow pages

// LATER: check for the space in slot array periodically and check if there is space for new one or append at the freeStart
// TODO: debugging the page print the header and cell currently

func (page *PageHeader) PageDebug() {
	fmt.Printf(" %+v \n", page)
	fmt.Printf(" Slot array:\n %+v \n", page.SlotArray())

}

// LATER: tommorrow also make the test cases for cell
// LATER: drain function is important for the binary tree function
// TODO: max allowed payload size ideal payload size

// TODO: read the overflow
func ReadOverFlowPageHeader(pageNo uint) (*OverflowPageHeader, error) {
	// should i first read the header and call the read function
	ovHeader := make([]byte, 4)
	var overflowheader OverflowPageHeader
	_, err := Init.Dbfile.ReadAt(ovHeader, int64(pageNo-1)*int64(Init.PAGE_SIZE))
	if err != nil {
		return nil, fmt.Errorf("%w | error while Reading from the overflow page Header", err)

	}
	overflowheader.next = binary.BigEndian.Uint16(ovHeader[:2])
	overflowheader.size = binary.BigEndian.Uint16(ovHeader[2:4])
	return &overflowheader, nil

}

func (ovheader *OverflowPageHeader) ReadOverflowPage(pageNo uint) ([]byte, error) {
	size := ovheader.size
	contents := make([]byte, size+4)
	_, err := Init.Dbfile.ReadAt(contents, int64(pageNo-1)*int64(Init.PAGE_SIZE))
	if err != nil {
		return nil, fmt.Errorf("%w | error while Reading from the overflow page", err)

	}
	if ovheader.next != 0 {
		newOverflowHeader, err := ReadOverFlowPageHeader(uint(ovheader.next))
		if err != nil {
			return nil, fmt.Errorf("%w ", err)

		}
		newContents, err := newOverflowHeader.ReadOverflowPage(uint(ovheader.next))
		if err != nil {
			return nil, fmt.Errorf("%w ", err)

		}
		contents = append(contents, newContents[4:]...)
		// fmt.Println("length of newcontents is ", len(newContents), "contents ", newContents, "header.size is ", newOverflowHeader.size)

	}
	return contents, nil

}

// Later implementaions
// LATER: payload should be serialized into some known format
// LATER: btree
