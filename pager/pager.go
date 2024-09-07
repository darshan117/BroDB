// Package pager provides functionality for managing database pages and their contents.
//
// The pager is responsible for handling the low-level operations on database pages,
// including cell management, slot manipulation, and page header updates. It serves
// as a crucial component in database systems, managing the physical storage and
// retrieval of data.
//
// Key Features:
//
//   - Cell Management: Add, remove, and retrieve cells within pages
//   - Slot Manipulation: Handle slot allocation and deallocation
//   - Page Header Updates: Modify and maintain page metadata
//   - Efficient Storage: Optimize space utilization within pages
//
// Main Components:
//
//   - Pager: The core struct that encapsulates page management operations
//   - Page: Represents a single database page
//   - Cell: The basic unit of data storage within a page
//   - Slot: Represents a location within a page where a cell can be stored
package pager

import (
	coreAlgo "blackdb/core_algo"
	Init "blackdb/init"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
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
		FreeStart:      uint16(PAGEHEAD_SIZE),
		freeEnd:        uint16(Init.PAGE_SIZE),
		NumSlots:       0,
		lastOffsetUsed: 0,
		RightPointer:   0,
		flags:          8,
	}
	page.totalFree = page.freeEnd - page.FreeStart
	ser := page.serializePageHeader(pageHeader)
	_, err := Init.Dbfile.Write(ser)
	if err != nil {
		return nil, PagerError("MakePage", ErrDbWriteError, fmt.Errorf("%w", err))
	}
	IncrementTotalPages()
	if err = LoadPage(uint(id)); err != nil {
		return nil, PagerError("MakePage", ErrLoadPage, fmt.Errorf("%w", err))
	}
	return &page, nil
}

// Makes the initial Page which contains the MetaData
func MakePageZero(ptype PageType, id uint16) (PageHeader, error) {
	pageHeader := make([]byte, Init.PAGE_SIZE-50)
	page := PageHeader{
		PageId:         id,
		PageType:       ptype,
		FreeStart:      uint16(PAGEHEAD_SIZE) + 50,
		freeEnd:        uint16(Init.PAGE_SIZE),
		NumSlots:       0,
		lastOffsetUsed: 0,
		RightPointer:   0,
		flags:          8,
	}
	page.totalFree = page.freeEnd - page.FreeStart
	ser := page.serializePageHeader(pageHeader)
	_, err := Init.Dbfile.Seek(50, 0) // 0 means relative to the origin of the file
	if err != nil {
		return PageHeader{}, PagerError("MakePageZero", fmt.Errorf("error while seek in dbfile "), fmt.Errorf("%w", err))
	}
	_, err = Init.Dbfile.Write(ser) // magic code Brodb
	if err != nil {
		return PageHeader{}, PagerError("MakePageZero", ErrDbWriteError, fmt.Errorf("%w", err))
	}
	return page, nil

}

// Adds the newcell to the Page also has
//
// Features like  addcell at index and change its leftPointer
func (page *PageHeader) AddCell(cellContent []byte, opt ...AddCellOptions) error {
	defer page.UpdatePageHeader()
	if page.PageId != uint16(BufData.PageNum) {
		err := LoadPage(uint(page.PageId))
		if err != nil {
			return err
		}
	}

	cellSize := len(cellContent)
	cell := Cell{
		Header: CellHeader{
			cellLoc:    page.freeEnd,
			isOverflow: false,
		},
	}
	cellSize += int(CELL_HEAD_SIZE)
	if cellSize > page.checkUsableSpace() && cellSize < int(page.totalFree) {
		err := page.Defragment()
		if err != nil {
			return err
		}

	} else if cellSize+int(SLOT_SIZE)+2 > int(page.totalFree) {
		return ErrNoRoom
		// return PagerError("Addcell", ErrNoRoom, fmt.Errorf("total free is %d", page.totalFree))
	}
	if cellSize > int(page.checkUsableSpace()) && cellSize > int(MINCELLSIZE) {
		// TODO: make a new Function handle overFlow cell
		oflcell := page.makeOverflowCell(cellContent)
		cell.CellContent = oflcell.serializeOverflow()
		cell.Header.isOverflow = true
		cell.Header.cellSize += uint16(len(cell.CellContent))

		err := LoadPage(uint(page.PageId))
		if err != nil {
			return PagerError("Addcell", ErrLoadPage, err)
		}

	} else {
		cell.CellContent = cellContent
		cell.Header.cellSize = uint16(len(cell.CellContent))
	}
	if len(opt) > 0 && opt[0].LeftPointer != nil {
		cell.Header.LeftChild = *opt[0].LeftPointer

	}
	cellSer, n := cell.Header.serializeCell(cell.CellContent)
	if page.freeEnd-uint16(n) <= page.FreeStart+50 { // padding{
		if err := page.Defragment(); err != nil {
			return PagerError("Addcell", ErrDefragmentation, err)
		}
	}

	copySize := copy(BufData.Data[page.freeEnd-uint16(n):page.freeEnd], cellSer.Bytes())
	page.freeEnd -= uint16(copySize)
	if len(opt) > 0 && opt[0].Index != nil {
		page.InsertSlot(*opt[0].Index, page.freeEnd)
	} else {
		binary.BigEndian.PutUint16(BufData.Data[page.FreeStart:page.FreeStart+2], page.freeEnd)
	}

	page.FreeStart += uint16(SLOT_SIZE)
	if int(page.totalFree)-((copySize)+int(SLOT_SIZE)) < 0 {
		log.Fatalf("copy size is greater %+v\n", page)

	}
	page.totalFree -= uint16(copySize + int(SLOT_SIZE)) // +2
	page.NumSlots += 1

	return nil

}

// Only removes the Slot which is 2 bytes from the slot array
func (page *PageHeader) RemoveCell(idx uint) error {
	defer page.UpdatePageHeader()
	if page.PageId != uint16(BufData.PageNum) {
		if err := LoadPage(uint(page.PageId)); err != nil {
			return PagerError("RemoveCell", ErrLoadPage, err)
		}
	}
	if idx > uint(page.NumSlots)-1 {
		return PagerError("RemoveCell", ErrCellRemoveError, fmt.Errorf("index is greater than max slots"))
	}
	oldCell, _ := page.GetCell(idx)
	page.totalFree += oldCell.Header.cellSize
	page.totalFree += uint16(CELL_HEAD_SIZE)
	page.ShiftSlots(idx)
	page.FreeStart -= 2
	page.NumSlots -= 1
	page.totalFree += 2 // slot size has

	return nil
}

// TODO: can make it here as well use read
func (page *PageHeader) GetCell(idx uint) (Cell, error) {
	if page.NumSlots <= uint16(idx) {
		return Cell{}, PagerError("GetCell", ErrInvalidIndex, fmt.Errorf("index larger than total slots in page."))
	}
	pageData := page.FileRead()
	slotIndex := PAGEHEAD_SIZE + idx*2
	offset := pageData[slotIndex : slotIndex+2]
	offsetVal := binary.BigEndian.Uint16(offset)
	if offsetVal > uint16(Init.PAGE_SIZE) {
		return Cell{}, PagerError("GetCell", ErrOther, fmt.Errorf("offset value is greater than the pagesize got=%d", offsetVal))
	}
	var cell Cell
	cellHeaderSize := CELL_HEAD_SIZE
	err := cell.deserializeCell(pageData[offsetVal : offsetVal+uint16(cellHeaderSize)+1])
	if err != nil {
		newdesPage := deserializePageHeader(pageData)
		// For debugging
		fmt.Printf("%+v is old page new page is %+v", page, newdesPage)
		fmt.Printf("%q id %d total %d  %x\n\n", err, page.PageId, Init.TOTAL_PAGES, pageData)
	}
	cell.CellContent = pageData[offsetVal+uint16(cellHeaderSize) : offsetVal+uint16(cellHeaderSize)+cell.Header.cellSize]
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

// use db read here
func (page *PageHeader) GetCellByOffset(offset uint16) Cell {
	if offset > 4096 {
		return Cell{}
	}
	// LoadPage(uint(page.PageId))
	pageData := page.FileRead()
	var cell Cell
	cellHeaderSize := CELL_HEAD_SIZE
	cell.deserializeCell(pageData[offset : offset+uint16(cellHeaderSize)+1])
	cell.CellContent = pageData[offset+uint16(cellHeaderSize) : offset+uint16(cellHeaderSize)+cell.Header.cellSize]
	return cell

}

// Defragement uses the uses the binary heap for
// Shifting slots to the right of the page removing all the
// unused spaces
func (page *PageHeader) Defragment() error {
	// can use dbfile.Write here
	defer page.UpdatePageHeader()
	LoadPage(uint(page.PageId))
	slotarray := page.SlotArray()
	binheap := coreAlgo.Heap[uint16]{}
	destPointer := uint16(Init.PAGE_SIZE)
	for k := range slotarray {
		binheap.Add(k)
	}
	for i := 0; i < int(page.NumSlots); i++ {
		offset, err := binheap.Remove()
		if err != nil {
			return PagerError("Defragement", ErrDefragmentation, fmt.Errorf("error while removing element %w", err))
		}
		newCell := page.GetCellByOffset(offset)
		cellSer, n := newCell.Header.serializeCell(newCell.CellContent)
		copy(BufData.Data[destPointer-uint16(n):destPointer], cellSer.Bytes())
		destPointer -= uint16(n)
		page.fixSlot(uint(slotarray[offset].index), destPointer)

	}
	page.freeEnd = destPointer
	page.totalFree = page.freeEnd - page.FreeStart
	page.flags = 2
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

		return PagerError("MakeOverFlowPage", ErrDbWriteError, fmt.Errorf("%w", err))
	}
	IncrementTotalPages()

	err = LoadPage(uint(PageNum))
	if err != nil {
		return PagerError("MakeOverFlowPage", ErrLoadPage, fmt.Errorf("%w", err))

	}
	return nil
}

func ReadOverFlowPageHeader(pageNo uint) (*OverflowPageHeader, error) {
	ovHeader := make([]byte, 4)
	var overflowHeader OverflowPageHeader
	_, err := Init.Dbfile.ReadAt(ovHeader, int64(pageNo-1)*int64(Init.PAGE_SIZE))
	if err != nil {
		return nil, PagerError("ReadOverflowPage", ErrOther, fmt.Errorf("error while reading overflowpage header  %w ", err))

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
		return nil, PagerError("ReadOverflowPage", ErrOther, fmt.Errorf("error while reading overflowpage header %w ", err))

	}
	if ovHeader.next != 0 {
		newOverflowHeader, err := ReadOverFlowPageHeader(uint(ovHeader.next))
		if err != nil {
			return nil, PagerError("ReadOverflowPage", ErrOther, fmt.Errorf("%w ", err))

		}
		newContents, err := newOverflowHeader.ReadOverflowPage(uint(ovHeader.next))
		if err != nil {
			return nil, PagerError("ReadOverflowPage", ErrOther, fmt.Errorf("%w ", err))

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
