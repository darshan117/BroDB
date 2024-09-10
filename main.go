package main

import (
	Init "blackdb/src/init"
	"blackdb/src/pager"
	"blackdb/src/query"
	"log"
	"os"
)

var file *os.File

func init() {
	file = Init.Init()
	if Init.SCHEMA_TABLE != 0 {
		pager.LoadPage(1)
		schemaPage, err := pager.GetPage(uint(Init.SCHEMA_TABLE))
		if err != nil {
			log.Fatal(err)
		}
		schemaCell, err := schemaPage.GetCell(0)
		query.ExecQuery(string(schemaCell.CellContent))
	} else {
		_, err := pager.MakePageZero(22, 1)
		if err != nil {
			log.Fatal(err)
		}
		err = pager.LoadPage(0)
		if err != nil {
			log.Fatal("error while loading the page")
		}

	}

}

func main() {
	defer Init.Dbfile.Close()
	in := os.Stdin
	w := os.Stdout

	query.Start(in, w)
}

func removekeyFromarray(keys []uint64, element uint64) []uint64 {
	for i, v := range keys {
		if v == element {
			keys = append(keys[:i], keys[i+1:]...)
			return keys

		}

	}
	return keys
}
