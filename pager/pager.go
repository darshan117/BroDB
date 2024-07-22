package pager

import (
	Init "blackdb/init"
	"bytes"
	"encoding/binary"
	"fmt"
	"syscall"
)

var (
	BufData       BufPage // current buffer memory cache of the current page is stored here
	PAGEHEAD_SIZE uint    = 14
	MINCELLSIZE   uint    = 10
	SLOT_SIZE     uint    = 2
	// TODO: make global for the cell header size
)

func MakePage(ptype PageType, id uint32) (PageHeader, error) {
	// make the header for the newPage
	pageHeader := make([]byte, Init.PAGE_SIZE)
	fmt.Println("length is ", len(pageHeader))
	page := PageHeader{
		pageId:    id,
		pageType:  ptype,
		freeStart: uint16(PAGEHEAD_SIZE),
		freeEnd:   uint16(Init.PAGE_SIZE),
		flags:     1,
		numSlots:  0,
	}
	page.totalFree = page.freeEnd - page.freeStart
	// setting the pageHeader
	binary.BigEndian.PutUint64(pageHeader[0:], uint64(page.pageId))
	pageHeader[4] = byte(page.pageType)
	binary.BigEndian.PutUint16(pageHeader[5:], uint16(page.freeStart))
	binary.BigEndian.PutUint16(pageHeader[7:], uint16(page.freeEnd))
	binary.BigEndian.PutUint16(pageHeader[9:], uint16(page.totalFree))
	pageHeader[11] = byte(page.flags)
	_, err := Init.Dbfile.Write(pageHeader)
	if err != nil {
		return PageHeader{}, fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	IncrementTotalPages()
	err = LoadPage(uint(id))
	if err != nil {
		return PageHeader{}, fmt.Errorf("error while Loading the page | %w", err)
	}
	fmt.Println("bufdata is ", BufData.Data[30:34])
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
		flags:     1,
		numSlots:  0,
	}
	page.totalFree = page.freeEnd - page.freeStart
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
	var cellheader CellHeader
	var cell Cell

	cellheader.cellSize = uint16(cellSize)
	cellheader.cellLoc = page.freeEnd
	cellheader.isOverflow = true
	cell.header = cellheader
	cellSize += binary.Size(cellheader)
	// totalCellSize := int(cellSize) + int(binary.Size(cellheader))
	// fmt.Println("total size of the size is", binary.Size(cellheader))

	if cellSize > int(page.checkUsableSpace()) && cellSize > int(MINCELLSIZE) {
		// page.Defragment
		// this should return the the new cell and change the cell content to its content
		oflcell := page.makeOverflowCell(cellContent)
		// fmt.Println(oflcell.serializeOverflow())
		cell.cellContent = oflcell.serializeOverflow()
		cell.header.isOverflow = true
		err := LoadPage(uint(page.pageId))
		if err != nil {
			return fmt.Errorf("error while Loading the page | %w", err)
		}

		// fmt.Println("cell size error ")
		// return fmt.Errorf("error while adding cell |Cell Size %d larger than the free space %d", cellSize, page.totalFree)
	} else {
		cell.cellContent = cellContent

	}
	cellSer, n := cellheader.serializeCell(cell.cellContent)
	fmt.Println("print size of cellser", n)
	copy(BufData.Data[page.freeEnd-uint16(n):page.freeEnd], cellSer.Bytes())

	page.freeEnd -= uint16(n)
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
	// fmt.Println("offset ", offset)
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
	oldCell := page.GetCell(idx)
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
		cell := page.GetCell(uint((i - PAGEHEAD_SIZE) / 2))

		slot.size = cell.header.cellSize
		slotmap[offsetVal] = slot
		i += 2
		id += 1
	}
	return slotmap

}

func (page *PageHeader) GetCell(idx uint) Cell {
	slotIndex := PAGEHEAD_SIZE + idx*2
	offset := BufData.Data[slotIndex : slotIndex+2]
	offsetVal := binary.BigEndian.Uint16(offset)
	// fmt.Println(offsetVal)
	var cell Cell
	cellHeaderSize := 5
	cell.deserializeCell(BufData.Data[offsetVal : offsetVal+uint16(cellHeaderSize)+1])
	// fmt.Printf("%+v  \n", cell)
	// fmt.Println("from ", offsetVal+uint16(cellHeaderSize), "to ", cell.header.cellSize)

	cell.cellContent = BufData.Data[offsetVal+uint16(cellHeaderSize) : offsetVal+uint16(cellHeaderSize)+cell.header.cellSize]
	return cell

}

// TODO: Get cell by offset
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

// TODO: defragment the page move all the cells to the right and remove all the gaps between the page
func (page *PageHeader) Defragment() error {

	slotarray := page.SlotArray()
	binheap := heap[uint16]{}
	destPointer := uint16(4096)
	for k, _ := range slotarray {
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
// TODO: struct for overflow page header

// TODO: making overflow cell
func (page *PageHeader) makeOverflowCell(cellContent []byte) OverflowPtr {
	// [x] payload + overptr
	var overflowptr OverflowPtr
	// [x] check for the cellcontent if it can fit in the
	space := page.checkUsableSpace()

	newpayloadsize := len(cellContent) - (len(cellContent) - int(space)) - 4 - int(SLOT_SIZE) // FIXME: overflow ptr constant
	fmt.Println("payload size is ", newpayloadsize, "len cell", len(cellContent), "space is ", space)
	NewPayload := cellContent[:newpayloadsize]
	overflowptr.payload = NewPayload
	IncrementTotalPages()
	MakeOverFlowPage(uint(Init.TOTAL_PAGES), cellContent[newpayloadsize:])
	overflowptr.ptr = uint32(Init.TOTAL_PAGES)

	// totalcellsize := uint(cell.header.cellSize) + uint(binary.Size(cell))

	return overflowptr

}

func MakeOverFlowPage(pageNum uint, payload []byte) error {
	var overflowHeader OverflowPageHeader
	fmt.Println("size of the overflow header:", binary.Size(overflowHeader))
	newpayload := payload[:]

	if len(payload) > Init.PAGE_SIZE-binary.Size(overflowHeader) {
		lenPayload := Init.PAGE_SIZE - binary.Size(overflowHeader)
		newpayload = payload[:lenPayload]
		MakeOverFlowPage(pageNum+1, newpayload[lenPayload:])
		overflowHeader.next = uint16(pageNum) + 1
		overflowHeader.size = uint16(lenPayload)
	} else {
		overflowHeader.next = uint16(0)
		overflowHeader.size = uint16(len(payload))

	}
	overH, _ := overflowHeader.serializeOverflowPage()
	_, err := Init.Dbfile.Write(overH)
	if err != nil {
		return fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	_, err = Init.Dbfile.Write(newpayload)
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

func (overflow *OverflowPageHeader) serializeOverflowPage() ([]byte, uint) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, overflow.next)
	binary.Write(&buf, binary.BigEndian, overflow.size)
	return buf.Bytes(), uint(buf.Len())

}

func (overflow *OverflowPtr) serializeOverflow() []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, overflow.payload)
	binary.Write(&buf, binary.BigEndian, overflow.ptr)
	return buf.Bytes()

}

// [ ]  make func for checking the usable space
func (page *PageHeader) checkUsableSpace() uint16 {
	return page.freeEnd - page.freeStart

}

// TODO: make new overflow cell
// TODO: Retrieve the overflow cell
// FIXME: changes in getcell check for if page is overflow
// how to work with the overflow pages

// LATER: check for the space in slot array periodically and check if there is space for new one or append at the freeStart
// TODO: debugging the page print the header and cell currently

// LATER: tommorrow also make the test cases for cell
// LATER: drain function is important for the binary tree function
// TODO: max allowed payload size ideal payload size

// Later implementaions
// LATER: payload should be serialized into some known format
// LATER: btree
