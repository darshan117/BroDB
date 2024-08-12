package btree_test

import (
	"blackdb/btree"
	Init "blackdb/init"
	"blackdb/pager"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
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
	t.Skip()
	Initialize()
	rnode, err := btree.NewBtreePage()
	if err != nil {
		log.Fatal(err)
	}
	for i := 1000; i > 0; i -= 2 {
		rnode.Insert(uint64(i))
		runtime.GC()
	}
	disktree, err := btree.BtreeTraversal()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%+v\n", disktree)
	rnode.Insert(uint64(0))
	_, err = btree.BtreeTraversal()
	if err != nil {
		t.Error(err)
	}
}

func BenchInsert(b *testing.B) {
	b.Skip()
	b.StartTimer()
	Init.Init()
	pager.MakePageZero(22, 1)
	err := pager.LoadPage(0)
	if err != nil {
		fmt.Println("error while loading the page")
	}
	tree := btree.NewBtree(5)

	r, err := pager.MakePage(pager.ROOT_AND_LEAF, 1)
	if err != nil {
		fmt.Println("error making page ", err)
	}
	root := btree.BtreePage{*r}
	for i := 1; i <= b.N; i++ {
		randval := uint64(rand.Int63n(10000))

		root.Insert(uint64(randval))
		tree.Insert(uint(randval))
	}
	disktree, err := btree.BtreeTraversal()
	if err != nil {
		b.Error(err)
	}
	dsatree := tree.BfsTraversal()

	if !reflect.DeepEqual(dsatree, disktree) {
		b.Errorf("expected: %+v\n\n got: %+v\n", dsatree, disktree)
	}
	b.StopTimer()
	b.Elapsed().Microseconds()

}
func TestRemove(t *testing.T) {
	Initialize()
	rnode, err := btree.NewBtreePage()
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i <= 30; i++ {
		rnode.Insert(uint64(i))
		// btree.BtreeTraversal()
	}
	// search 9 and get its right child
	_, pageid, err := btree.Search(31)
	if err != nil {
		t.Error(err)
	}
	newpage, err := pager.GetPage(uint(pageid))
	if err != nil {
		t.Error(err)
	}
	rightpage, err := pager.GetPage(uint(newpage.RightPointer))
	rightrightpage, err := pager.GetPage(uint(rightpage.RightPointer))
	fmt.Println("keys are ", rightrightpage.GetKeys())
	testkeys := make([]uint64, 0, 200)
	for i := 0; i <= 30; i++ {
		testkeys = append(testkeys, uint64(i))
	}
	btree.Remove(28)
	btree.BtreeTraversal()
	btree.Remove(21)
	btree.BtreeTraversal()
	btree.Remove(22)
	btree.BtreeTraversal()
	btree.Remove(20)
	btree.BtreeTraversal()
	btree.Remove(23)
	btree.BtreeTraversal()
	btree.Remove(18)
	btree.BtreeTraversal()
	btree.Remove(24)
	// btree.Remove(24)
	allkeys, err := btree.BtreeDFSTraversal()
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(allkeys, testkeys) {
		t.Errorf(
			`
		expected:%v,
		got:%v
		`, testkeys, allkeys)
	}

	// btree.Remove(18)
	// btree.Remove(0)
	// btree.Remove(10)
	// btree.Remove(2)
	// btree.Remove(16)
	btree.BtreeTraversal()
}
