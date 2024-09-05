package main

import (
	"blackdb/query"
	"os"
)

// var file *os.File

// func init() {
// 	file = Init.Init()
// 	// pagH, _ :=
// 	pager.MakePageZero(22, 1)
// 	err := pager.LoadPage(0)
// 	if err != nil {
// 		fmt.Println("error while loading the page")
// 	}

// }

func main() {
	in := os.Stdin
	w := os.Stdout

	query.Start(in, w)
	// rnode, err := btree.NewBtreePage()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// nkeys := 1000
	// for i := 0; i <= nkeys; i++ {
	// 	rnode.Insert(uint32(i))
	// }
	// testkeys := make([]uint64, 0, 200)
	// alltestkeys := make([]uint64, 0, 200)

	// for i := 0; i <= nkeys; i++ {
	// 	testkeys = append(testkeys, uint64(i))
	// 	alltestkeys = append(alltestkeys, uint64(i))
	// }

	// rng := rand.NewSource(987234)
	// src := rand.New(rng)
	// remkeys := make([]uint64, 0, 100)
	// for i := 1; i <= nkeys; i++ {
	// 	n := src.Int63n(int64(len(testkeys)))
	// 	fmt.Println("removing", testkeys[uint64(n)])
	// 	if testkeys[uint64(n)] == 506 {
	// 		break
	// 	}
	// 	btree.Remove(testkeys[uint64(n)])
	// 	remkeys = append(remkeys, testkeys[uint64(n)])
	// 	testkeys = removekeyFromarray(testkeys, testkeys[uint64(n)])

	// }
	// btree.Remove(506)

	// rnode, err := btree.NewBtreePage()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// nkeys := 99
	// fmt.Println("=----insert------=")
	// for i := 1; i <= nkeys; i++ {
	// 	rnode.Insert(uint64(i))
	// }
	// testkeys := make([]uint64, 0, 200)
	// alltestkeys := make([]uint64, 0, 200)

	// for i := 0; i <= nkeys; i++ {
	// 	testkeys = append(testkeys, uint64(i))
	// 	alltestkeys = append(alltestkeys, uint64(i))
	// }

	// btree.BtreeTraversal()
	// fmt.Println("=------remove------=")
	// rng := rand.NewSource(984)
	// src := rand.New(rng)
	// remkeys := make([]uint64, 0, 100)
	// for i := 1; i <= nkeys; i++ {
	// 	n := src.Int63n(int64(len(testkeys)))
	// 	btree.Remove(testkeys[uint64(n)])
	// 	remkeys = append(remkeys, testkeys[uint64(n)])
	// 	testkeys = removekeyFromarray(testkeys, testkeys[uint64(n)])

	// }
	// btree.BtreeTraversal()

	// fmt.Println("=----insert------=")
	// for _, v := range remkeys {
	// 	rnode.Insert(v)
	// }
	// btree.BtreeTraversal()
	// fmt.Println("=------remove------=")
	// for _, v := range remkeys {
	// 	err = btree.Remove(v)
	// 	if err != nil {
	// 		return
	// 	}
	// }
	// btree.BtreeTraversal()

	// fmt.Println(testkeys)
	// allkeys, err := btree.BtreeDFSTraversal()
	// if err != nil {
	// 	return
	// }
	// fmt.Println(allkeys)

	// defer file.Close()

	// defer func() {
	// 	if pager.BufData.Data != nil {
	// 		if err := syscall.Munmap(pager.BufData.Data); err != nil {
	// 			fmt.Printf("Error unmapping: %v\n", err)
	// 		}
	// 	}
	// }()
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
