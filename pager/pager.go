package pager

import (
	coreAlgo "blackdb/core_algo"
	Init "blackdb/init"
	"bytes"
	"encoding/binary"
	"fmt"
)

var (
	BufData        BufPage
	PAGEHEAD_SIZE  uint = 16
	MINCELLSIZE    uint = 10
	SLOT_SIZE      uint = 2
	CELL_HEAD_SIZE uint = 7
	FREEPAGE_SIZE  uint = 2
)

// Makes a NewPage with all the required field from the PageHeader
func MakePage(ptype PageType, id uint16) (*PageHeader, error) {
	if ptype == ROOTPAGE {
		Init.UpdateRootPage(uint(id))
	}
	pageHeader := make([]byte, Init.PAGE_SIZE)
	page := PageHeader{
		PageId:         id,
		PageType:       ptype,
		freeStart:      uint16(PAGEHEAD_SIZE),
		freeEnd:        uint16(Init.PAGE_SIZE),
		NumSlots:       0,
		lastOffsetUsed: 0,
		RightPointer:   0,
		flags:          8,
	}
	page.totalFree = page.freeEnd - page.freeStart
	ser := page.serializePageHeader(pageHeader)
	_, err := Init.Dbfile.Write(ser)
	if err != nil {
		return &PageHeader{}, fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	IncrementTotalPages()
	err = LoadPage(uint(id))
	if err != nil {
		return &PageHeader{}, fmt.Errorf("error while Loading the page | %w", err)
	}
	return &page, nil
}

// Makes the initial Page which contains the MetaData
func MakePageZero(ptype PageType, id uint16) (PageHeader, error) {
	pageHeader := make([]byte, Init.PAGE_SIZE-50)
	page := PageHeader{
		PageId:         id,
		PageType:       ptype,
		freeStart:      uint16(PAGEHEAD_SIZE) + 50,
		freeEnd:        uint16(Init.PAGE_SIZE),
		NumSlots:       0,
		lastOffsetUsed: 0,
		RightPointer:   0,
		flags:          8,
	}
	page.totalFree = page.freeEnd - page.freeStart
	ser := page.serializePageHeader(pageHeader)
	_, err := Init.Dbfile.Seek(50, 0) // 0 means relative to the origin of the file
	if err != nil {
		return PageHeader{}, fmt.Errorf("error seeking to offset: %w", err)
	}
	_, err = Init.Dbfile.Write(ser) // magic code Brodb
	if err != nil {
		return PageHeader{}, fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	return page, nil

}

// Adds the newcell to the Page also has
//
// Features like  addcell at index and change its leftPointer
func (page *PageHeader) AddCell(cellContent []byte, opt ...AddCellOptions) error {
	defer page.UpdatePageHeader()
	// if page.PageId != uint16(BufData.PageNum) {
	err := LoadPage(uint(page.PageId))
	if err != nil {
		return err
	}
	// }

	cellSize := len(cellContent)
	var cell Cell

	cell.Header.cellLoc = page.freeEnd
	cell.Header.isOverflow = false
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
		cell.CellContent = oflcell.serializeOverflow()
		cell.Header.isOverflow = true
		cell.Header.cellSize += uint16(len(cell.CellContent))

		err := LoadPage(uint(page.PageId))
		if err != nil {
			return fmt.Errorf("error while Loading the page | %w", err)
		}

	} else {
		cell.CellContent = cellContent
		cell.Header.cellSize = uint16(len(cell.CellContent))
	}

	if len(opt) > 0 && opt[0].LeftPointer != nil {
		cell.Header.LeftChild = *opt[0].LeftPointer
	}

	cellSer, n := cell.Header.serializeCell(cell.CellContent)
	no := copy(BufData.Data[page.freeEnd-uint16(n):page.freeEnd], cellSer.Bytes())
	page.freeEnd -= uint16(no)
	if len(opt) > 0 && opt[0].Index != nil {
		page.InsertSlot(*opt[0].Index, page.freeEnd)
	} else {
		binary.BigEndian.PutUint16(BufData.Data[page.freeStart:page.freeStart+2], page.freeEnd)
	}
	page.freeStart += uint16(SLOT_SIZE)
	// BUG: add cell total free
	page.totalFree -= uint16(no + int(SLOT_SIZE)) // +2
	page.NumSlots += 1

	return nil

}

func (page *PageHeader) RemoveCell(idx uint) error {
	defer page.UpdatePageHeader()
	if page.PageId != uint16(BufData.PageNum) {
		if err := LoadPage(uint(page.PageId)); err != nil {
			return err
		}
	}
	// do a page load here
	if idx > uint(page.NumSlots)-1 {
		return fmt.Errorf("error while removing cell | index is greater than the max slots... ")
	}
	// FIXME: do the error checking for the func getcell
	oldCell, _ := page.GetCell(idx)
	page.totalFree += oldCell.Header.cellSize
	page.totalFree += uint16(CELL_HEAD_SIZE)
	// BUG: there is bug while shifting slots
	page.ShiftSlots(idx)
	page.freeStart -= 2
	page.NumSlots -= 1
	page.totalFree += 2 // slot size has

	return nil
}

func (page *PageHeader) GetCell(idx uint) (Cell, error) {
	if page.PageId != uint16(BufData.PageNum) {
		newPage, err := GetPage(uint(page.PageId))
		if err != nil {
			return Cell{}, err
		}
		return newPage.GetCell(idx)
	}
	if page.NumSlots <= uint16(idx) {
		return Cell{}, fmt.Errorf("index not found in the page %d", idx)
	}
	slotIndex := PAGEHEAD_SIZE + idx*2
	offset := BufData.Data[slotIndex : slotIndex+2]
	offsetVal := binary.BigEndian.Uint16(offset)
	var cell Cell
	cellHeaderSize := CELL_HEAD_SIZE
	cell.deserializeCell(BufData.Data[offsetVal : offsetVal+uint16(cellHeaderSize)+1])
	cell.CellContent = BufData.Data[offsetVal+uint16(cellHeaderSize) : offsetVal+uint16(cellHeaderSize)+cell.Header.cellSize]
	if cell.Header.isOverflow {
		pageOffset := cell.CellContent[len(cell.CellContent)-4:]
		ovPage := binary.BigEndian.Uint32(pageOffset)
		ovPageHeader, err := ReadOverFlowPageHeader(uint(ovPage))
		if err != nil {
			return Cell{}, err
		}
		contents, err := ovPageHeader.ReadOverflowPage(uint(ovPage))
		if err != nil {
			return cell, err
		}
		// FIXME: get the full cell contents concatenated 4 byte Header
		fmt.Println("contents of the page are..", string(cell.CellContent[:len(cell.CellContent)-4])+string(contents[4:]), "len is ", len(contents))

	}
	return cell, nil

}

func (page *PageHeader) GetCellByOffset(offset uint16) Cell {
	if offset > 4096 {
		return Cell{}
	}
	LoadPage(uint(page.PageId))
	var cell Cell
	cellHeaderSize := CELL_HEAD_SIZE
	cell.deserializeCell(BufData.Data[offset : offset+uint16(cellHeaderSize)+1])
	cell.CellContent = BufData.Data[offset+uint16(cellHeaderSize) : offset+uint16(cellHeaderSize)+cell.Header.cellSize]
	return cell

}

func (page *PageHeader) Defragment() error {
	defer page.UpdatePageHeader()

	slotarray := page.SlotArray()
	binheap := coreAlgo.Heap[uint16]{}
	destPointer := uint16(4096)
	// HACK: did some changes here
	for k := range slotarray {
		binheap.Add(k)
	}
	for i := 0; i < int(page.NumSlots); i++ {
		offset, err := binheap.Remove()
		if err != nil {
			return fmt.Errorf(" %w Error while removing the element from binheap", err)
		}
		newCell := page.GetCellByOffset(offset)
		cellSer, n := newCell.Header.serializeCell(newCell.CellContent)
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

	newpayloadsize := int(space) - 4 - int(SLOT_SIZE) - int(CELL_HEAD_SIZE) // FIXME: overflow ptr constant
	if newpayloadsize > 0 {
		fmt.Println("newpayload size is ", len(cellContent))
		NewPayload := cellContent[:newpayloadsize]
		overflowptr.payload = NewPayload
		IncrementTotalPages()
		pageNo := Init.TOTAL_PAGES
		MakeOverFlowPage(uint(Init.TOTAL_PAGES), cellContent[newpayloadsize:])
		overflowptr.ptr = uint32(pageNo)
		return overflowptr

	}
	return OverflowPtr{}

}

func MakeOverFlowPage(PageNum uint, payload []byte) error {
	var overflowHeader OverflowPageHeader
	payl := make([]byte, Init.PAGE_SIZE)
	newpayload := payload

	if len(payload) > Init.PAGE_SIZE-binary.Size(overflowHeader) {
		lenPayload := Init.PAGE_SIZE - binary.Size(overflowHeader)
		newpayload = payload[:lenPayload]
		fmt.Println(lenPayload, len(payload[lenPayload:]))
		MakeOverFlowPage(PageNum+1, payload[lenPayload:])
		overflowHeader.next = uint16(PageNum) + 1
		overflowHeader.size = uint16(lenPayload)
	} else {
		overflowHeader.next = uint16(0)
		overflowHeader.size = uint16(len(payload))

	}
	overflowHeader.serializeOverflowPage(payl)
	copy(payl[4:], newpayload)
	_, err := Init.Dbfile.WriteAt(payl, int64(Init.PAGE_SIZE)*int64(PageNum-1))
	if err != nil {
		return fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	IncrementTotalPages()

	err = LoadPage(uint(PageNum))
	if err != nil {
		return fmt.Errorf("error while Loading the page | %w", err)

	}
	return nil
}

func ReadOverFlowPageHeader(pageNo uint) (*OverflowPageHeader, error) {
	ovHeader := make([]byte, 4)
	var overflowHeader OverflowPageHeader
	_, err := Init.Dbfile.ReadAt(ovHeader, int64(pageNo-1)*int64(Init.PAGE_SIZE))
	if err != nil {
		return nil, fmt.Errorf("%w | error while Reading from the overflow page Header", err)

	}
	overflowHeader.next = binary.BigEndian.Uint16(ovHeader[:2])
	overflowHeader.size = binary.BigEndian.Uint16(ovHeader[2:4])
	return &overflowHeader, nil

}

func (ovHeader *OverflowPageHeader) ReadOverflowPage(pageNo uint) ([]byte, error) {
	size := ovHeader.size
	contents := make([]byte, size+4)
	_, err := Init.Dbfile.ReadAt(contents, int64(pageNo-1)*int64(Init.PAGE_SIZE))
	if err != nil {
		return nil, fmt.Errorf("%w | error while Reading from the overflow page", err)

	}
	if ovHeader.next != 0 {
		newOverflowHeader, err := ReadOverFlowPageHeader(uint(ovHeader.next))
		if err != nil {
			return nil, fmt.Errorf("%w ", err)

		}
		newContents, err := newOverflowHeader.ReadOverflowPage(uint(ovHeader.next))
		if err != nil {
			return nil, fmt.Errorf("%w ", err)

		}
		contents = append(contents, newContents[4:]...)
	}
	return contents, nil

}

func MakeFreelistPage(pageNo uint16) error {
	var freelist FreelistPage
	freelist.TotalPages = 1
	freelist.NextPage = 0
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(freelist.NextPage))
	binary.Write(&buf, binary.BigEndian, uint16(freelist.TotalPages))
	// append to freelistPage
	freelist.AppendFreePage(pageNo)
	Init.UpdateFreelist(uint(pageNo))

	return nil
}

// might be a helper function
func (freepage *FreelistPage) AppendFreePage(pageNo uint16) error {

	if freepage.NextPage != 0 {
		nextpage, err := FreePageLoad(freepage.NextPage)
		if err != nil {
			return err
		}
		return nextpage.AppendFreePage(pageNo)
	}
	// freepageheader has the size 4 bytes + i*2
	if freepage.TotalPages < 1024 {
		pointTo := FREEPAGE_SIZE + uint(pageNo)*2
		binary.BigEndian.PutUint16(BufData.Data[pointTo:], pageNo)
		Init.UpdateFreelistCount(uint(1))
		return nil
	}
	// how
	freepage.NextPage = pageNo
	binary.BigEndian.PutUint16(BufData.Data[0:], pageNo)
	MakeFreelistPage(pageNo)
	return nil

}

func FreePageLoad(pageNo uint16) (*FreelistPage, error) {
	if err := LoadPage(uint(pageNo)); err != nil {
		return nil, err
	}
	var freelistpage FreelistPage
	freelistpage.NextPage = binary.BigEndian.Uint16(BufData.Data[0:])
	freelistpage.TotalPages = binary.BigEndian.Uint16(BufData.Data[2:])
	return &freelistpage, nil
}
