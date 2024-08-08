package btree_test

import (
	"blackdb/btree"
	Init "blackdb/init"
	"blackdb/pager"
	"log"
	"runtime"

	"fmt"
	"reflect"
	"testing"

	"math/rand"
)

// TODO: test adding multiple cells

// TODO: check cells
// might take a day just to create the fucking tests
// TODO: defragment first test then implement
func Initialize() {
	Init.Init()
	pager.MakePageZero(22, 1)
	err := pager.LoadPage(0)
	if err != nil {
		fmt.Println("error while loading the page")
	}
}

func TestInsert(t *testing.T) {
	t.Skip()
	Initialize()
	Init.Init()
	pager.MakePageZero(22, 1)
	err := pager.LoadPage(0)
	if err != nil {
		fmt.Println("error while loading the page")
	}
	tree := btree.NewBtree(5)
	rnode, err := btree.NewBtreePage()
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i <= 1002; i++ {
		randval := uint64(rand.Int63n(1000000))

		rnode.Insert(uint64(randval))
		tree.Insert(uint(randval))
	}
	disktree, err := btree.BtreeTraversal()
	if err != nil {
		t.Error(err)
	}
	dsatree := tree.BfsTraversal()
	if !reflect.DeepEqual(dsatree, *disktree) {
		t.Errorf("expected: \n%+v\n\n got:\n %+v\n", dsatree, *disktree)
	}
}
func TestSearch(t *testing.T) {
	t.Skip()
	tree := btree.NewBtree(5)
	rnode, err := btree.NewBtreePage()
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i <= 1000; i++ {
		rnode.Insert(uint64(i))
		tree.Insert(uint(i))
	}
	for i := 0; i <= 10000; i++ {
		randval := uint64(rand.Int63n(1000))
		node, err := tree.Search(uint(randval))
		if err != nil {
			log.Fatal(err)
		}
		_, disknode, err := btree.Search(randval)
		if err != nil {
			log.Fatal(err)
		}
		diskPage, err := pager.GetPage(uint(disknode))
		if err != nil {
			log.Fatal(err)
		}
		// fmt.Println("KEY:", randval, "node key is ", node, "disk pages are ", diskPage.GetKeys(), "SLOT IS ", slot)
		if !reflect.DeepEqual(node, diskPage.GetKeys()) {
			t.Errorf("expected: \n%+v\n\n got:\n %+v\n", node, diskPage.GetKeys())
		}
	}

}

func TestParent(t *testing.T) {
	// t.Skip()
	Initialize()
	rnode, err := btree.NewBtreePage()
	if err != nil {
		log.Fatal(err)
	}
	// rand.Seed(500)
	for i := 1000; i > 0; i -= 2 {
		// randval := uint64(rand.Int63n(100))
		rnode.Insert(uint64(i))
		runtime.GC()
		// btree.BtreeTraversal()
	}
	// for i := 1000; i < 10000; i++ {
	// 	// randval := uint64(rand.Int63n(100))
	// 	rnode.Insert(uint64(i))
	// 	// btree.BtreeTraversal()
	// }
	disktree, err := btree.BtreeTraversal()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%+v\n", disktree)
	rnode.Insert(uint64(0))
	// rnode.Insert(uint64(3))
	// rnode.Insert(uint64(5))
	// rnode.Insert(uint64(23))
	// rnode.Insert(uint64(9))
	// // rnode.Insert(uint64(11))
	// // rnode.Insert(uint64(13))
	// rnode.Insert(uint64(25))
	// rnode.Insert(uint64(27))
	// // btree.BtreeTraversal()
	// rnode.Insert(uint64(7))
	// // rnode.Insert(uint64(3))
	// rnode.Insert(uint64(11))
	// for i := 40; i <= 50; i++ {
	// 	// randval := uint64(rand.Int63n(100))
	// 	rnode.Insert(uint64(i))
	// 	btree.BtreeTraversal()
	// }
	// slot, id, err := btree.GetParent(7)
	// if err != nil {
	// 	t.Error(err)
	// }
	// newpage, _ := pager.GetPage(uint(*id))
	// cell, _ := newpage.GetCell(uint(*slot))
	// npage := btree.BtreePage{*newpage}
	// leftsibl, _ := btree.RightSiblingCount(7)
	// fmt.Printf("npage are %+v and the parent is %d\n", npage.GetKeys(), binary.BigEndian.Uint64(cell.CellContent))
	// fmt.Printf("left sibling is %+v\n", leftsibl.GetKeys())
	// val := uint64(50)

	// rightsib, err := btree.RightSiblingCount(15)
	// if err != nil {
	// 	t.Error(err)
	// }
	// // rightsib := btree.BtreePage{*rightSibling}
	// rightsib.Shuffle()
	disktree, err = btree.BtreeTraversal()
	if err != nil {
		t.Error(err)
	}
	// fmt.Printf("%+v\n", disktree)

	// fmt.Printf("leftsibling count is %d pageid is %+v  cell is %+v", leftSibling.GetKeys(), npage, cell)

}

// func BenchInsert(b *testing.B) {
// 	b.StartTimer()
// 	Init.Init()
// 	pager.MakePageZero(22, 1)
// 	err := pager.LoadPage(0)
// 	if err != nil {
// 		fmt.Println("error while loading the page")
// 	}
// 	tree := btree.NewBtree(5)

// 	r, err := pager.MakePage(pager.ROOT_AND_LEAF, 1)
// 	if err != nil {
// 		fmt.Println("error making page ", err)
// 	}
// 	for i := 1; i <= b.N; i++ {
// 		randval := uint64(rand.Int63n(10000))

// 		r.Insert(uint64(randval))
// 		tree.Insert(int(randval))
// 	}
// 	disktree, err := pager.BtreeTraversal()
// 	if err != nil {
// 		b.Error(err)
// 	}
// 	dsatree := tree.BfsTraversal()

// 	if !reflect.DeepEqual(dsatree, disktree) {
// 		b.Errorf("expected: %+v\n\n got: %+v\n", dsatree, disktree)
// 	}
// 	b.StopTimer()
// 	b.Elapsed().Microseconds()

// }
