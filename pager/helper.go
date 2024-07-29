package pager

import (
	Init "blackdb/init"
	"bytes"
	"encoding/binary"
	"fmt"
	"syscall"
)

// type PageHeader struct {
// 	// FIXME: might need to remove the pageId
// 	pageId         uint8
// 	pageType       PageType
// 	freeStart      uint16
// 	freeEnd        uint16
// 	totalFree      uint16
// 	numSlots       uint16
// 	lastOffsetUsed uint16
// 	rightPointer   uint16
// 	flags          uint8
// }

func (page *PageHeader) serializePageHeader(pageHeader []byte) []byte {

	binary.BigEndian.PutUint16(pageHeader[0:], uint16(page.pageId))
	pageHeader[2] = byte(page.pageType)
	binary.BigEndian.PutUint16(pageHeader[3:], uint16(page.freeStart))
	binary.BigEndian.PutUint16(pageHeader[5:], uint16(page.freeEnd))
	binary.BigEndian.PutUint16(pageHeader[7:], uint16(page.totalFree))
	binary.BigEndian.PutUint16(pageHeader[9:], uint16(page.numSlots))
	binary.BigEndian.PutUint16(pageHeader[11:], uint16(page.lastOffsetUsed))
	binary.BigEndian.PutUint16(pageHeader[13:], uint16(page.rightPointer))
	pageHeader[15] = page.flags
	return pageHeader

}
func deserializePageHeader(pageHeader []byte) PageHeader {
	var header PageHeader
	header.pageId = binary.BigEndian.Uint16(pageHeader[:2])
	header.pageType = PageType(pageHeader[2])
	header.freeStart = binary.BigEndian.Uint16(pageHeader[3:5])
	header.freeEnd = binary.BigEndian.Uint16(pageHeader[5:7])
	header.totalFree = binary.BigEndian.Uint16(pageHeader[7:9])
	header.numSlots = binary.BigEndian.Uint16(pageHeader[9:11])
	header.lastOffsetUsed = binary.BigEndian.Uint16(pageHeader[11:13])
	header.rightPointer = binary.BigEndian.Uint16(pageHeader[13:15])
	header.flags = pageHeader[15]
	return header

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
	binary.Write(&buf, binary.BigEndian, cell.leftChild)
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
	cell.header.leftChild = binary.BigEndian.Uint16(cellheader[5:7])
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

func (page *PageHeader) ShiftSlots(idx uint) {
	slotIndex := PAGEHEAD_SIZE + idx*2
	for i := 0; i < int(page.numSlots)-int(idx); i++ {
		copy(BufData.Data[slotIndex:slotIndex+2], BufData.Data[slotIndex+2:slotIndex+4])
		slotIndex += 2

	}
	binary.BigEndian.PutUint16(BufData.Data[page.freeStart-2:page.freeStart], uint16(0))
}
func (page *PageHeader) InsertSlot(idx int, offsetVal uint16) {
	if page.numSlots > 0 {
		ind := int(page.numSlots - 1)
		slotid := PAGEHEAD_SIZE + uint(idx)*2
		for i := ind; i >= idx; i-- {
			slotIndex := PAGEHEAD_SIZE + uint(i*2)
			copy(BufData.Data[slotIndex+2:slotIndex+4], BufData.Data[slotIndex:slotIndex+2])
		}
		binary.BigEndian.PutUint16(BufData.Data[slotid:slotid+2], offsetVal)

	}

}

// TODO: range remove slots
func (page *PageHeader) RangeRemoveSlots(start uint, end uint) {
	// shiftslots starting from   start index
	for i := start; i < end; i++ {
		oldCell, _ := page.GetCell(i)
		page.totalFree += oldCell.header.cellSize
		page.totalFree += uint16(CELL_HEAD_SIZE)
		page.ShiftSlots(i)
		page.freeStart -= 2
		page.totalFree += 2 // slot size has
	}
	page.numSlots -= uint16(end) - uint16(start)

}

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
		slot.contents = cell.cellContent
		slotmap[offsetVal] = slot
		i += 2
		id += 1
	}
	return slotmap

}

func (page *PageHeader) GetSlots() []uint16 {
	startidx := PAGEHEAD_SIZE
	slots := make([]uint16, 0)
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
	keys := make([]uint64, 0)
	for _, val := range page.GetSlots() {
		cell := page.GetCellByOffset(val)
		res := binary.BigEndian.Uint64(cell.cellContent)
		keys = append(keys, res)
	}
	return keys

}
func (page *PageHeader) PageDebug() {
	fmt.Printf(" %+v \n", page)
	fmt.Printf("%+v \n", page.SlotArray())

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
