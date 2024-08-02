package btree_test

import (
	Init "blackdb/init"
	"blackdb/pager"
	"blackdb/pager/btree"

	"fmt"
	"reflect"
	"testing"

	"math/rand"
)

// TODO: test adding multiple cells

// TODO: check cells
// might take a day just to create the fucking tests
// TODO: defragment first test then implement

func TestInsert(t *testing.T) {
	Init.Init()
	pager.MakePageZero(22, 1)
	err := pager.LoadPage(0)
	if err != nil {
		fmt.Println("error while loading the page")
	}
	tree := btree.NewBtree(10)

	r, err := pager.MakePage(pager.ROOT_AND_LEAF, 1)
	if err != nil {
		fmt.Println("error making page ", err)
	}
	rnode := btree.BtreePage{*r}
	for i := 0; i <= 1002; i++ {
		randval := uint64(rand.Int63n(10000))

		rnode.Insert(uint64(randval))
		tree.Insert(int(randval))
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
