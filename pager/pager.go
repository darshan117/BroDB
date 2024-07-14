package pager

import (
	Init "blackdb/init"
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
		freeStart: uint16(12) + 50,
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

	if cellSize > int(page.totalFree) {
		return fmt.Errorf("error while adding cell |Cell Size %d larger than the free space %d", cellSize, page.totalFree)
	}
	// add the cellcontent to the free end of page
	copy(BufData.Data[page.freeEnd-uint16(cellSize):page.freeEnd], cellContent)

	page.freeEnd -= uint16(cellSize)
	// pagefreeEnd is the offset
	binary.BigEndian.PutUint16(BufData.Data[page.freeStart:page.freeStart+2], page.freeEnd)
	page.freeStart += uint16(binary.Size(page.freeStart))
	return nil

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
