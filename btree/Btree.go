package btree

import "fmt"

type Btree struct {
	t    int // it is the degree of the b tree
	root *BtreeNode
}

type BtreeNode struct {
	keys  [5]*int
	leaf  bool
	child [6]*BtreeNode
	n     int // current no of keys
	t     int // degree
}

func (node *BtreeNode) insertNotFull(key int) {
	i := node.n - 1 // current number of keys -1
	if node.leaf {
		for i >= 0 && *node.keys[i] > key {
			node.keys[i+1] = node.keys[i] // shifting the keys at +1 index
			i -= 1
		}
		node.keys[i+1] = &key //setting the key at correct position by shifting elements
		node.n += 1
	} else {
		// try to visualize it in a clear way
		// not a leaf node
		for i >= 0 && *node.keys[i] > key { // only loop till key[i] is greater key and
			i -= 1
		}
		if node.child[i+1].n == 2*node.t-1 {
			// is full
			node.splitChild(i+1, node.child[i+1])
			if *node.keys[i+1] < key {
				i += 1
			}

		}
		node.child[i+1].insertNotFull(key)

	}

}

// check for the pointer
func (node *BtreeNode) splitChild(index int, y *BtreeNode) {
	z := BtreeNode{
		t:    y.t,
		leaf: y.leaf,
	}
	z.n = node.t - 1 // setting to max keys -1
	for i := 0; i < node.t-1; i++ {
		z.keys[i] = y.keys[i+node.t] // copying all the keys after halfway
	}
	if !y.leaf {
		for j := 0; j < node.t; j++ {
			z.child[j] = y.child[j+node.t] // same copying new nodes after value of t to newly created z node
		}
	}
	y.n = node.t - 1
	for i := node.n; i > 0; i-- {
		//reverse loop
		node.child[i+1] = node.child[i] //shifting the values of nodes
	}
	node.child[index+1] = &z // setting the child node at perfect index
	for i := node.n - 1; i > index-1; i-- {
		node.keys[i+1] = node.keys[i] // shifting the keys as well
	}
	node.keys[index] = y.keys[node.t-1]
	node.n += 1

}

// func (tree *Btree) insert(key int) {
// 	root := tree.root
// 	if len(root.keys) == (2*tree.t)-1 {
// 		temp := BtreeNode{}
// 		tree.root = temp
// 		temp.child.insert(root)
// 	}
// }

func (tree *Btree) insert(key int) {
	if tree.root == nil {
		// create the first node
		tree.root = &BtreeNode{t: tree.t, leaf: true}
		tree.root.keys[0] = &key
		tree.root.n = 1
	} else {
		if tree.root.n == 2*tree.t-1 {
			// node is full
			s := BtreeNode{t: tree.t, leaf: false}
			s.child[0] = tree.root
			s.splitChild(0, tree.root)

			i := 0
			if *s.keys[0] < key {
				i += 1
			}
			s.child[i].insertNotFull(key)
			tree.root = &s
		} else {
			tree.root.insertNotFull(key)
		}
	}
}

func (node *BtreeNode) traverse() {
	for i := 0; i < node.n; i++ {
		if !node.leaf {
			node.child[i].traverse()

		}
		fmt.Printf("%d ", *node.keys[i])
		if !node.leaf {
			node.child[i+1].traverse()
		}

	}

}

func (tree *Btree) traverse() {
	if tree.root != nil {
		tree.root.traverse()
	}
}

func samplebtree() {
	t := Btree{t: 3}
	t.insert(10)
	t.insert(20)
	t.insert(5)
	t.insert(6)
	t.insert(12)
	t.insert(30)
	t.insert(7)
	t.insert(17)
	t.insert(21)
	t.insert(13)
	t.insert(14)
	t.insert(24)
	t.insert(25)
	t.insert(2)
	t.insert(8)
	t.insert(27)
	fmt.Println("traversing the btree is ")
	t.traverse()
}
