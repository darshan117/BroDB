package main

import (
	"blackdb/btree"
	Init "blackdb/init"
	"blackdb/pager"
	"fmt"
	"os"
	"syscall"
)

var file *os.File

func init() {
	file = Init.Init()
	// pagH, _ :=
	pager.MakePageZero(22, 1)
	err := pager.LoadPage(0)
	if err != nil {
		fmt.Println("error while loading the page")
	}

}

func main() {
	// make the rootpage
	// btree := pager.NewBtree()
	rpage, err := btree.NewBtreePage()
	if err != nil {
		return
	}
	keys := make([]uint64, 0)
	for i := 1; i <= 10000; i++ {
		// keys := []uint64{32, 24, 12, 30, 66, 88, 77, 50, 10, 33, 35, 42, 36, 37, 25, 26, 27, 19, 22, 21, 23, 1, 2, 3, 4, 87, 38, 39, 40, 45, 44,
		// 	90, 91, 92, 93, 94, 95, 96}

		// keys = append(keys, uint64(rand.Int63n(1000)))
		keys = append(keys, uint64(i))

	}
	for _, v := range keys {
		rpage.Insert(v)
	}
	// btree.BtreeTraversal()
	// _, id, err := btree.Search(55)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println("search gave the id ", id)
	// pagid, _ := pager.GetPage(uint(id))
	// fmt.Println(pagid.GetKeys())

	// rootpag, _ := pager.GetPage(1)
	// newPage, _ := pager.GetPage(2)
	// root, _ := pager.GetPage(3)
	// splitpage, _ := pager.GetPage(4)
	// rightsplitpage, _ := pager.GetPage(5)
	// rightsplit, _ := pager.GetPage(6)
	// rootpag, _ := pager.GetPage(1)
	// fmt.Printf("\n old page is %+v %+v\n", rootpag.GetKeys(), rootpag)
	// newPage, _ := pager.GetPage(2)
	// fmt.Printf("new page is %+v %+v\n", newPage.GetKeys(), newPage)
	// fmt.Printf("root page is %+v %+v\n", root.GetKeys(), root)
	// fmt.Printf("split page is %+v %+v\n", splitpage.GetKeys(), splitpage)
	// fmt.Printf("right split page is %+v %+v\n", rightsplitpage.GetKeys(), rightsplitpage)
	// fmt.Printf("new right split page 6  is %+v %+v\n", rightsplit.GetKeys(), rightsplit)
	// root, _ := pager.GetPage(3)
	// rootpage.PageDebug()
	// rootpage.InsertSlot(0)

	// fmt.Println(rootpage.GetSlots())

	// rootpage.PageDebug()

	defer file.Close()

	defer func() {
		if pager.BufData.Data != nil {
			if err := syscall.Munmap(pager.BufData.Data); err != nil {
				fmt.Printf("Error unmapping: %v\n", err)
			}
		}
	}()
}
