package main

import (
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
	rootpage, err := pager.MakePage(3, 1)
	if err != nil {
		fmt.Println("error making page ", err)
	}
	rootpage.Insert(32)
	rootpage.Insert(24)
	rootpage.Insert(12)
	rootpage.Insert(30)
	rootpage.Insert(66)
	rootpage.Insert(88)
	rootpage.Insert(77)
	rootpage.Insert(50)
	rootpage.Insert(10)
	rootpage.Insert(42)
	// rootpage.PageDebug()
	// rootpage.InsertSlot(0)

	fmt.Println(rootpage.GetSlots())

	rootpage.PageDebug()

	defer file.Close()

	defer func() {
		if pager.BufData.Data != nil {
			if err := syscall.Munmap(pager.BufData.Data); err != nil {
				fmt.Printf("Error unmapping: %v\n", err)
			}
		}
	}()
}
