package pager

import (
	Init "blackdb/init"
	"encoding/binary"
	"fmt"
	"os"
)

func MakePage(ptype PageType, id uint32, dbfile *os.File) Page {
	// make the header for the newPage
	pageHeader := make([]byte, Init.PAGE_SIZE)
	var page Page
	size := binary.Size(page)
	// fmt.Println("size is ", size)
	page = Page{
		pageId:    id,
		pageType:  ptype,
		freeStart: uint16(size),
		freeEnd:   uint16(Init.PAGE_SIZE),
		flags:     1,
	}
	page.totalFree = page.freeEnd - page.freeStart
	// setting the pageHeader
	binary.BigEndian.PutUint64(pageHeader[0:], uint64(page.pageId))
	pageHeader[4] = byte(page.pageType)
	binary.BigEndian.PutUint16(pageHeader[5:], uint16(page.freeStart))
	binary.BigEndian.PutUint16(pageHeader[7:], uint16(page.freeEnd))
	fmt.Println(page)
	binary.BigEndian.PutUint16(pageHeader[9:], uint16(page.totalFree))
	pageHeader[11] = byte(page.flags)
	// buf := binary.
	fmt.Println(pageHeader)
	_, err := dbfile.Seek(50, 0) // 0 means relative to the origin of the file
	if err != nil {
		fmt.Errorf("error seeking to offset: %w", err)
	}
	_, err = dbfile.Write(pageHeader) // magic code Brodb
	if err != nil {
		fmt.Errorf("%w... Error while adding the page  Header", err)
	}

	return page

}
