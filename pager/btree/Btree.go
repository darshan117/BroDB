package btree

import (
	"container/list"
	"fmt"
)

type Btree struct {
	degree int
	root   *BtreeNode
}

type BtreeNode struct {
	keys     []int
	children []*BtreeNode
	leaf     bool
	n        int // current number of keys
}

func NewBtree(degree int) *Btree {
	return &Btree{degree: degree}
}

func newBtreeNode(degree int, leaf bool) *BtreeNode {
	return &BtreeNode{
		keys:     make([]int, 2*degree-1),
		children: make([]*BtreeNode, 2*degree),
		leaf:     leaf,
		n:        0,
	}
}

func (tree *Btree) Insert(key int) {
	if tree.root == nil {
		tree.root = newBtreeNode(tree.degree, true)
		tree.root.keys[0] = key
		tree.root.n = 1
		return
	}

	if tree.root.n == 2*tree.degree-1 {
		newRoot := newBtreeNode(tree.degree, false)
		newRoot.children[0] = tree.root
		newRoot.splitChild(0, tree.root)
		i := 0
		if newRoot.keys[0] < key {
			i++
		}
		newRoot.children[i].insertNonFull(key)
		tree.root = newRoot
	} else {
		tree.root.insertNonFull(key)
	}
}

func (node *BtreeNode) insertNonFull(key int) {
	i := node.n - 1
	if node.leaf {
		for i >= 0 && node.keys[i] > key {
			node.keys[i+1] = node.keys[i]
			i--
		}
		node.keys[i+1] = key
		node.n++
	} else {
		for i >= 0 && node.keys[i] > key {
			i--
		}
		i++
		if node.children[i].n == 2*len(node.children)/2-1 {
			node.splitChild(i, node.children[i])
			if node.keys[i] < key {
				i++
			}
		}
		node.children[i].insertNonFull(key)
	}
}

func (node *BtreeNode) splitChild(i int, y *BtreeNode) {
	t := len(node.children) / 2
	z := newBtreeNode(t, y.leaf)
	z.n = t - 1

	for j := 0; j < t-1; j++ {
		z.keys[j] = y.keys[j+t]
	}

	if !y.leaf {
		for j := 0; j < t; j++ {
			z.children[j] = y.children[j+t]
		}
	}

	y.n = t - 1

	for j := node.n; j > i; j-- {
		node.children[j+1] = node.children[j]
	}
	node.children[i+1] = z

	for j := node.n - 1; j >= i; j-- {
		node.keys[j+1] = node.keys[j]
	}
	node.keys[i] = y.keys[t-1]
	node.n++
}

func (tree *Btree) BfsTraversal() [][][]uint64 {
	if tree.root == nil {
		return nil
	}

	queue := list.New()
	queue.PushBack(tree.root)
	level := 0
	result := make([][][]uint64, 0)

	for queue.Len() > 0 {
		levelSize := queue.Len()
		fmt.Printf("Level %d: ", level)
		temp := make([][]uint64, 0)

		for i := 0; i < levelSize; i++ {
			element := queue.Front()
			node := element.Value.(*BtreeNode)
			nodeElem := make([]uint64, 0)
			queue.Remove(element)
			fmt.Print("[")

			for j := 0; j < node.n; j++ {
				nodeElem = append(nodeElem, uint64(node.keys[j]))
				fmt.Printf("%d ", node.keys[j])
			}
			fmt.Print("]")

			if !node.leaf {
				for j := 0; j <= node.n; j++ {
					if node.children[j] != nil {
						queue.PushBack(node.children[j])
					}
				}
			}
			temp = append(temp, nodeElem)
		}
		result = append(result, temp)

		fmt.Println()
		level++
	}
	fmt.Println("result is ", result)
	return result

}

// func main() {
// 	tree := NewBtree(5) // You can change the degree here

// 	// Insert more elements
// 	for i := 1; i <= 202; i++ {
// 		tree.Insert(i)
// 	}

// 	fmt.Println("BFS traversal of the B-tree (level-wise):")
// 	tree.BfsTraversal()
// }
