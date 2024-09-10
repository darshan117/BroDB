package pager

import (
	Init "blackdb/src/init"
	"encoding/binary"
	"fmt"
	"testing"
)

func Initialize() {
	Init.Init()
	MakePageZero(22, 1)
	err := LoadPage(0)
	if err != nil {
		fmt.Println("error while loading the page")
	}
}

func TestDefragement(t *testing.T) {
	Initialize()
	newPage, err := MakePage(3, 1)
	if err != nil {
		t.Error(err)
	}
	for i := 1; i <= 241; i++ {
		res := make([]byte, 8)
		binary.BigEndian.PutUint64(res[:], uint64(i))
		newPage.AddCell(res)
	}
	res := make([]byte, 8)
	binary.BigEndian.PutUint64(res[:], uint64(242))

	for i := 10; i <= 20; i++ {
		cell, _ := newPage.GetCell(uint(10))
		fmt.Println(cell.CellContent)

		newPage.RemoveCell(uint(10))
		fmt.Println(newPage.GetKeys())
	}
	fmt.Println(newPage.GetKeys())
	for i := 50; i <= 60; i++ {
		cell, _ := newPage.GetCell(uint(60))
		fmt.Println(cell.CellContent)

		newPage.RemoveCell(uint(60))
		fmt.Println(newPage.GetKeys())
	}
	for i := 100; i <= 200; i++ {
		cell, _ := newPage.GetCell(uint(100))
		fmt.Println(cell.CellContent)

		newPage.RemoveCell(uint(100))
		fmt.Println(newPage.GetKeys())
	}
	// newPage.RemoveCell(120)
	newPage.Defragment()
	fmt.Printf("%+v", newPage)
	fmt.Println(newPage.GetKeys())

}

func TestInsertionRemovals(t *testing.T) {
	Initialize()
	newPage, err := MakePage(3, 1)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i <= 50; i++ {
		for i := 1; i <= 241; i++ {
			res := make([]byte, 8)
			binary.BigEndian.PutUint64(res[:], uint64(i))
			fmt.Println("------------------------------------------inserting i", i)
			newPage.AddCell(res)
		}
		res := make([]byte, 8)
		binary.BigEndian.PutUint64(res[:], uint64(242))
		for i := 1; i <= 240; i++ {
			newPage.RemoveCell(1)

		}

	}
	fmt.Printf("%+v", newPage)
	fmt.Println(newPage.GetKeys())

}
