// This package implements the queue data structure

package coreAlgo

import "fmt"

type Queue []uint16

// might add specified no of elements
func NewQueue() Queue {

	return make(Queue, 0)
}

func (q *Queue) Push(element uint16) {
	*q = append(*q, element)
}

func (q *Queue) Pop() uint16 {
	item := (*q)[0]
	(*q) = (*q)[1:]
	return item
}

func (q *Queue) Peek() (uint16, error) {
	if !q.IsEmpty() {
		return (*q)[0], nil
	}
	return 0, fmt.Errorf("Cannot peek into the queue because the queue is empty")
}

func (q *Queue) IsEmpty() bool {
	return len(*q) == 0
}
