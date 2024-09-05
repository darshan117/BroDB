package Init

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var (
	Dbfile *os.File
	once   sync.Once
)

func Init() *os.File {

	once.Do(func() {
		dbname := os.Args[1]

		fmt.Println(dbname, os.Getpagesize())
		// wd, _ := os.Getwd()
		Dbfile, _ = os.Create(dbname)
		// defer dbfile.Close()
		fmt.Println(Dbfile.Fd())
		if err := makeFileHeader(Dbfile); err != nil {
			return
		}
		fmt.Println("Check the header")

	})
	return Dbfile
}

var (
	MAGICCODE       = "BroDB\000"
	PAGE_SIZE       = os.Getpagesize()
	TOTAL_PAGES     = 1
	FILE_CH_COUNTER = 0
	DATABASE_SIZE   = 1 // in pages
	ROOTPAGE        = 0 // in pages
	FREELIST_START  = 0
	FREELIST_COUNT  = 0
)

// 							Database Header Format
// 		Offset	 Size	Description
// 			0		16	The header string: "SQLite format 3\000"
// 			16		2	The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
// 			18		1	File format write version. 1 for legacy; 2 for WAL.
// 			19		1	File format read version. 1 for legacy; 2 for WAL.
// 			20		1	Bytes of unused "reserved" space at the end of each page. Usually 0.
// 			21		1	Maximum embedded payload fraction. Must be 64.
// 			22		1	Minimum embedded payload fraction. Must be 32.
// 			23		1	Leaf payload fraction. Must be 32.
// 			24		4	File change counter.
// 			28		4	Size of the database file in pages. The "in-header database size".
// 			32		4	Page number of the first freelist trunk page.
// 			36		4	Total number of freelist pages.
// 			40		4	The schema cookie.
// 			44		4	The schema format number. Supported schema formats are 1, 2, 3, and 4.
// 			48		4	Default page cache size.
// 			52		4	The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
// 			56		4	The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
// 			60		4	The "user version" as read and set by the user_version pragma.
// 			64		4	True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
// 			68		4	The "Application ID" set by PRAGMA application_id.
// 			72		20	Reserved for expansion. Must be zero.
// 			92		4	The version-valid-for number.
// 			96		4	SQLITE_VERSION_NUMBER

// make the file header
func makeFileHeader(file *os.File) error {
	var headersize uint = 50
	Header := make([]byte, headersize)
	copy(Header, []byte(MAGICCODE))
	binary.BigEndian.PutUint16(Header[16:], uint16(PAGE_SIZE))
	binary.BigEndian.PutUint32(Header[18:], uint32(FILE_CH_COUNTER))   // file change counter
	binary.BigEndian.PutUint32(Header[18+4:], uint32(DATABASE_SIZE))   // database size in pages
	binary.BigEndian.PutUint32(Header[22+4:], uint32(FILE_CH_COUNTER)) // pagenumber of largest root node btree page
	binary.BigEndian.PutUint32(Header[26+4:], uint32(TOTAL_PAGES))     // Total number of pages
	binary.BigEndian.PutUint32(Header[34:], uint32(ROOTPAGE))          // Rootpage number
	binary.BigEndian.PutUint32(Header[38:], uint32(FREELIST_START))    //  first freelist page
	binary.BigEndian.PutUint32(Header[42:], uint32(FREELIST_COUNT))    // Total number of freelist pages
	binary.BigEndian.PutUint16(Header[48:], uint16(8))                 // end delimiter
	// can do add other header things if needed
	err := binary.Write(file, binary.BigEndian, Header) // magic code Brodb
	if err != nil {

		// FIXME: make the new error unable to make error
		return fmt.Errorf("%w... Error while adding the db file Header", err)
	}
	return nil

}

func UpdateRootPage(pageNo uint) error {
	buff := make([]byte, 4)

	binary.BigEndian.PutUint32(buff, uint32(pageNo))
	_, err := Dbfile.WriteAt(buff, 34) // 0 means relative to the origin of the file
	if err != nil {
		return fmt.Errorf("error changing the rootpage: %w", err)
	}
	ROOTPAGE = int(pageNo)
	return nil

}
func UpdateFreelist(pageNo uint) error {
	buff := make([]byte, 4)

	binary.BigEndian.PutUint32(buff, uint32(pageNo))
	_, err := Dbfile.WriteAt(buff, 38) // 0 means relative to the origin of the file
	if err != nil {
		return fmt.Errorf("error changing the rootpage: %w", err)
	}
	FREELIST_START = int(pageNo)
	return nil

}
func UpdateFreelistCount(count uint) error {
	buff := make([]byte, 4)
	FREELIST_COUNT += int(count)

	binary.BigEndian.PutUint32(buff, uint32(FREELIST_COUNT))
	_, err := Dbfile.WriteAt(buff, 42) // 0 means relative to the origin of the file
	if err != nil {
		return fmt.Errorf("error changing the rootpage: %w", err)
	}
	return nil

}

// make the serialize function for page of 4096 bytes and way to fit all the cell

// make the datapage with the gob encoding in order to store the data

// inserting to the datapage

// delete the entry from the datapage

// update the dataPage

// TODO:  create make the btree full functionality in order to insert delete and update and read CRUD
