package pager

import (
	"errors"
	"fmt"
)

var (
	ErrPageNotFound    = errors.New("page not found")
	ErrInvalidIndex    = errors.New("invalid index")
	ErrOverflow        = errors.New("overflow occurred")
	ErrDefragmentation = errors.New("defragmentation failed")
	ErrLoadPage        = errors.New("cannot load page")
	ErrCellRemoveError = errors.New("cannot remove cell")
	ErrDbWriteError    = errors.New("db write error")
	ErrNoRoom          = errors.New("no room for new element in the page")
	ErrOther           = errors.New("other error")
)

type PageError struct {
	FuncName string
	ErrType  error
	Err      error
}

func (e *PageError) Error() string {
	return fmt.Sprintf(" %s: %v: %v", e.FuncName, e.ErrType, e.Err)
}

func (e *PageError) Unwrap() error {
	return e.Err
}

func PagerError(funcname string, errType error, err error) *PageError {
	return &PageError{
		FuncName: funcname,
		ErrType:  errType,
		Err:      err,
	}
}
