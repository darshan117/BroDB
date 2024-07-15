package pager

import (
	Init "blackdb/init"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

var (
	BufData BufPage // current buffer memory cache of the current page is stored here
)

func MakePage(ptype PageType, id uint32, dbfile *os.File) (PageHeader, error) {
	// make the header for the newPage
	pageHeader := make([]byte, Init.PAGE_SIZE)
	fmt.Println("length is ", len(pageHeader))
	page := PageHeader{
		pageId:    id,
		pageType:  ptype,
		freeStart: uint16(12),
		freeEnd:   uint16(Init.PAGE_SIZE),
		flags:     1,
	}
	page.totalFree = page.freeEnd - page.freeStart
	// setting the pageHeader
	binary.BigEndian.PutUint64(pageHeader[0:], uint64(page.pageId))
	pageHeader[4] = byte(page.pageType)
	binary.BigEndian.PutUint16(pageHeader[5:], uint16(page.freeStart))
	binary.BigEndian.PutUint16(pageHeader[7:], uint16(page.freeEnd))
	binary.BigEndian.PutUint16(pageHeader[9:], uint16(page.totalFree))
	pageHeader[11] = byte(page.flags)
	_, err := dbfile.Write(pageHeader)
	if err != nil {
		return PageHeader{}, fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	err = LoadPage(uint(id), dbfile)
	if err != nil {
		return PageHeader{}, fmt.Errorf("error while Loading the page | %w", err)
	}
	return page, nil
}

func MakePageZero(ptype PageType, id uint32, dbfile *os.File) (PageHeader, error) {
	// make the header for the newPage
	pageHeader := make([]byte, 4046)
	page := PageHeader{
		pageId:    id,
		pageType:  ptype,
		freeStart: uint16(14) + 50, // contains hardcoded pageheader size
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
	// only for the pageZero make it  a seperate function
	_, err := dbfile.Seek(50, 0) // 0 means relative to the origin of the file
	if err != nil {
		return PageHeader{}, fmt.Errorf("error seeking to offset: %w", err)
	}
	_, err = dbfile.Write(pageHeader) // magic code Brodb
	if err != nil {
		return PageHeader{}, fmt.Errorf("%w... Error while adding the page  Header", err)
	}
	return page, nil

}

// TODO: Add cell to the page

func (page *PageHeader) AddCell(cellContent []byte) error {
	cellSize := binary.Size(cellContent)
	// TODO: Add a cell header here as well
	var cellheader CellHeader
	var cell Cell

	// newCell.cellContent = cellContent
	cellheader.cellSize = uint16(cellSize)
	cellheader.cellLoc = page.freeEnd
	cellheader.isOverflow = true
	// make the slot array have the
	cell.header = cellheader
	cell.cellContent = cellContent

	if cellSize > int(page.totalFree) {
		return fmt.Errorf("error while adding cell |Cell Size %d larger than the free space %d", cellSize, page.totalFree)
	}
	// add the cellcontent to the free end of page
	cellSer, n := cellheader.serializeCell(cell.cellContent)
	fmt.Println("cell is serialized", cellSer.Bytes(), cellSer.Len())
	copy(BufData.Data[page.freeEnd-uint16(n):page.freeEnd], cellSer.Bytes())

	page.freeEnd -= uint16(n)
	// pagefreeEnd is the offset
	binary.BigEndian.PutUint16(BufData.Data[page.freeStart:page.freeStart+2], page.freeEnd)
	page.freeStart += uint16(binary.Size(page.freeStart))
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

// make the deserializeCell
func (cell *Cell) deserializeCell(cellheader []byte) {
	fmt.Printf("Cell header %X \n ", cellheader[4])
	cell.header.cellLoc = binary.BigEndian.Uint16(cellheader[:2])
	cell.header.cellSize = binary.BigEndian.Uint16(cellheader[2:4])
	if int(cellheader[4]) == 1 {
		cell.header.isOverflow = true
	} else {
		cell.header.isOverflow = false
	}

}

// TODO: load the page to the memory

func LoadPage(pageNo uint, dbfile *os.File) error {
	BufData.pageNum = pageNo

	fileStat, err := dbfile.Stat()
	if err != nil {
		return fmt.Errorf("error while reading file Info ... %w", err)
	}
	offset := BufData.pageNum * uint(Init.PAGE_SIZE)
	fmt.Println("offset ", offset)
	mapSize := func() uint {
		if offset+uint(Init.PAGE_SIZE) > uint(fileStat.Size()) {
			return uint(fileStat.Size()) - offset
		}
		return uint(Init.PAGE_SIZE)
	}
	fmt.Println("mapsize is ", mapSize())

	BufData.Data, err = syscall.Mmap(int(dbfile.Fd()), int64(offset), int(mapSize()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("error is %w", err)
	}
	return nil
}

// TODO: remove page from the db
func (page *PageHeader) RemoveCell(idx uint) {
	slotIndex := 12 + idx*2 // hardcoded pagesize
	fmt.Println("Before,  ", BufData.Data[:slotIndex+2])
	binary.BigEndian.PutUint16(BufData.Data[slotIndex:slotIndex+2], uint16(0))
	fmt.Println("After,  ", BufData.Data[:slotIndex+2])
	page.numSlots -= 1
	// FIXME: add here
	// page.totalFree +=
}

// TODO: add the slot array list to some struct
// HACK: clean the below functions

// func (page *PageHeader) SlotArray() {
// 	var slotarray []PointerList

// 	for i := 0; i < int(page.numSlots); i++ {
// 		// try like this
// 		var slot PointerList
// 		slotIndex := 12 + i*2 // hardcoded pagesize
// 		offset := BufData.Data[slotIndex : slotIndex+2]
// 		slot.offset = binary.BigEndian.Uint16(offset)
// 		slot.size = page.GetCell(uint(i)).header.cellSize
// 		slotarray = append(slotarray, slot)
// 	}
// 	fmt.Println(slotarray)

// }

// new implementaton

func (page *PageHeader) SlotArray() {
	var slotarray []PointerList
	startidx := 12 // page header size make it global

	for i := startidx; i < int(page.freeStart); {
		// try like this
		var slot PointerList
		// slotIndex := 12 + i*2 // hardcoded pagesize
		offset := BufData.Data[i : i+2]
		fmt.Println("offset here is ", i)
		slot.offset = binary.BigEndian.Uint16(offset)
		slot.size = page.GetCell(uint((i - 12) / 2)).header.cellSize
		slotarray = append(slotarray, slot)
		i += 2
	}
	fmt.Println(slotarray)

}

// TODO: Get the cell contents
func (page *PageHeader) GetCell(idx uint) Cell {
	slotIndex := 12 + idx*2 // hardcoded pagesize
	offset := BufData.Data[slotIndex : slotIndex+2]
	// fmt.Printf("%X \n", BufData.Data[:slotIndex+2])
	offsetVal := binary.BigEndian.Uint16(offset)
	var cell Cell
	cellHeaderSize := 5
	cell.deserializeCell(BufData.Data[offsetVal : offsetVal+uint16(cellHeaderSize)+1]) // FIXME: hardcoded cell header val
	cell.cellContent = BufData.Data[offsetVal+uint16(cellHeaderSize) : offsetVal+uint16(cellHeaderSize)+cell.header.cellSize]
	// fmt.Printf("Cell is %+v \n", cell)
	// fmt.Println(string(cell.cellContent))
	return cell

}

// TODO: defragment the page move all the cells to the right and remove all the gaps between the page
// for making contiguous space
// TODO: check for the space in slot array periodically and check if there is space for new one or append at the freeStart
