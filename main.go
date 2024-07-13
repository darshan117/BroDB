package main

import (
	Init "blackdb/init"
	"blackdb/pager"
)

func main() {

	file := Init.Init()
	pager.MakePage(22, 1, file)
	defer file.Close()
}
