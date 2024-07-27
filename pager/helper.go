package pager

import (
	Init "blackdb/init"
	"bytes"
	"encoding/binary"
	"fmt"
	"syscall"
)

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

func (page *PageHeader) ShiftSlots(idx uint) {
	slotIndex := PAGEHEAD_SIZE + idx*2
	for i := 0; i < int(page.numSlots)-int(idx); i++ {
		copy(BufData.Data[slotIndex:slotIndex+2], BufData.Data[slotIndex+2:slotIndex+4])
		slotIndex += 2

	}
}

func (page *PageHeader) fixSlot(index uint, offset uint16) {
	slotIndex := PAGEHEAD_SIZE + index*2
	binary.BigEndian.PutUint16(BufData.Data[slotIndex:slotIndex+2], offset)
}

func (page *PageHeader) PageDebug() {
	fmt.Printf(" %+v \n", page)
	fmt.Printf(" Slot array:\n %+v \n", page.SlotArray())

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
