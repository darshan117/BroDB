// This package implements the heap data structure

package coreAlgo

import "fmt"

// heapify up after inserting an element
// heapify down after removing the largest element

// let some max slot array capacity be added here
// let user the uint16
type Number interface {
	uint16 | uint64 | uint
}

type Heap[T Number] struct {
	items []T
}

// get left child index
func leftChildIndex[T Number](parent T) T { return 2*parent + 1 }

// get right child index
func rightChildIndex[T Number](parent T) T { return 2*parent + 2 }

// get the parent index
func parentIndex[T Number](child T) T {
	if child == 0 {
		return 0
	}
	return T((child - 1) / 2)
}

// check hasleftchild
func (htree *Heap[T]) hasLeftChild(parent T) bool {
	return T(len(htree.items)) > leftChildIndex(parent)
}

// check hasrightchild
func (htree *Heap[T]) hasRightChild(parent T) bool {
	return T(len(htree.items)) > rightChildIndex(parent)
}

// check parent
func (htree *Heap[T]) hasParent(child T) bool { return parentIndex(child) >= 0 }

// get parent
func (htree *Heap[T]) getParent(child T) T {
	return htree.items[parentIndex(child)]
}
func (htree *Heap[T]) getLeftChild(parent T) T  { return htree.items[leftChildIndex(parent)] }
func (htree *Heap[T]) getRightChild(parent T) T { return htree.items[rightChildIndex(parent)] }

// swap
func (htree *Heap[T]) swap(idx1, idx2 T) {
	htree.items[idx1], htree.items[idx2] = htree.items[idx2], htree.items[idx1]
}

// it first append the item to the end of the list
// then it find it is swapped with parent if the parent is
// less then the item value
func (htree *Heap[T]) Add(item T) {
	htree.items = append(htree.items, item)
	htree.heapifyUp()
}

func (htree *Heap[T]) Remove() (T, error) {
	if len(htree.items) > 0 {
		temp := htree.items[0]
		htree.items[0] = htree.items[len(htree.items)-1]
		htree.heapifyDown()
		htree.items = htree.items[:len(htree.items)-1]

		return temp, nil
	}
	return T(0), fmt.Errorf("error while deleting element")

}

func (htree *Heap[T]) heapifyUp() {
	if len(htree.items) == 0 {
		return
	}
	index := T(len(htree.items) - 1)
	for htree.hasParent(index) && htree.getParent(index) < htree.items[index] {
		parent := parentIndex(index)
		htree.swap(parent, index)
		index = parent
	}

}

func (htree *Heap[T]) heapifyDown() {
	index := T(0)
	for htree.hasLeftChild(index) {
		maxIdx := leftChildIndex(index)
		if htree.hasRightChild(index) {
			if htree.getRightChild(index) > htree.getLeftChild(index) {
				maxIdx = rightChildIndex(index)
			}
		}
		if htree.items[index] > htree.items[maxIdx] {
			break
		} else {
			htree.swap(index, maxIdx)
		}
		index = maxIdx
	}

}
