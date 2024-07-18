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
	// HACK: find a way to call the init and make page zero here
	file = Init.Init()
	pagH, _ := pager.MakePageZero(22, 1, file)
	err := pager.LoadPage(0, file)
	if err != nil {
		fmt.Println("error while loading the page")
	}
	pagH.AddCell([]byte("this is the first cell"))

}

func main() {
	pheader, err := pager.MakePage(1, 1, file)
	if err != nil {
		fmt.Println("error making page ", err)
	}
	pheader.AddCell([]byte("hell"))

	pheader.AddCell([]byte("something New;"))
	for i := 0; i < 208; i++ {
		pheader.AddCell([]byte(fmt.Sprintf("cell no : %d", i)))

	}
	pheader.GetCell(1)
	pheader.GetCell(0)

	pheader.RemoveCell(1)
	pheader.SlotArray()
	defer file.Close()

	defer func() {
		if pager.BufData.Data != nil {
			if err := syscall.Munmap(pager.BufData.Data); err != nil {
				fmt.Printf("Error unmapping: %v\n", err)
			}
		}
	}()
}
