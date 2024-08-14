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

func TestBalancedInsert(t *testing.T) {
	t.Skip()
	Initialize()
	rnode, err := btree.NewBtreePage()
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i <= 200; i++ {
		rnode.Insert(uint64(i))
	}
	testkeys := make([]uint64, 0, 200)
	for i := 0; i <= 200; i++ {
		testkeys = append(testkeys, uint64(i))
	}
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

	btree.BtreeTraversal()
}
func TestRemove(t *testing.T) {

	Initialize()
	rnode, err := btree.NewBtreePage()
	if err != nil {
		log.Fatal(err)
	}
	nkeys := 1500
	for i := 0; i <= nkeys; i++ {
		rnode.Insert(uint64(i))
	}
	// test := list.New()
	testkeys := make([]uint64, 0, 200)
	alltestkeys := make([]uint64, 0, 200)

	for i := 0; i <= nkeys; i++ {
		testkeys = append(testkeys, uint64(i))
		alltestkeys = append(alltestkeys, uint64(i))
	}

	rng := rand.NewSource(1334)
	src := rand.New(rng)
	remkeys := make([]uint64, 0, 100)
	btree.BtreeTraversal()
	for i := 1; i <= nkeys; i++ {
		n := src.Int63n(int64(len(testkeys)))
		fmt.Printf("----------------removing %v -----------------\n", testkeys[n])
		// if testkeys[n] == 59 {
		// 	break
		// }
		fmt.Print(btree.Remove(testkeys[uint64(n)]))
		remkeys = append(remkeys, testkeys[uint64(n)])
		testkeys = removekeyFromarray(testkeys, testkeys[uint64(n)])
		// btree.BtreeTraversal()

	}
	// btree.Remove(59)
	for _, v := range remkeys {
		fmt.Printf("----------------inserting %v -----------------\n", v)
		// if v == 48 {
		// 	break
		// }
		rnode.Insert(v)

		btree.BtreeTraversal()

	}
	// rnode.Insert(48)
	fmt.Println(testkeys)
	fmt.Println(remkeys)
	// btree.Remove(71)
	// btree.BtreeTraversal()
	// btree.Remove(22)
	// btree.BtreeTraversal()
	// btree.Remove(23)

	allkeys, err := btree.BtreeDFSTraversal()
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(allkeys, alltestkeys) {
		t.Errorf(
			`
		expected:%v,
		got:%v
		`, alltestkeys, allkeys)
	}
	fmt.Println("testkeys", testkeys)
	btree.BtreeTraversal()
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
func checktraversal() error {
	allkeys, err := btree.BtreeDFSTraversal()
	if err != nil {
		return fmt.Errorf("traversal error %v", err)
	}
	lastval := 0
	for _, v := range allkeys {
		if lastval > int(v) {
			return fmt.Errorf("%v is greater than %d", v, lastval)
		}
		lastval = int(v)

	}

	return nil
}
