package ml

import (
	"fmt"
	//"runtime"
	"sync"
	"unsafe"
)

/*
#cgo pkg-config: python3
#cgo LDFLAGS: -lpython3.8
#include "glue.h"
*/
import "C"

var (
	initOnce sync.Once
	initErr  error
)

func initialize() {
	initOnce.Do(func() {
		C.init_python()
		initErr = pyLastError()
	})
}

type ML struct {
	fn *C.PyObject
}

func NewAutogluon(moduleName, funcName string) (*ML, error) {
	initialize()
	if initErr != nil {
		return nil, initErr
	}
	fn, err := loadPyFunc(moduleName, funcName)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return &ML{fn}, nil
}

func (a *ML) Predict(data []float64) ([]int, error) {

	if a.fn == nil {
		return nil, fmt.Errorf("closed")
	}

	if len(data) == 0 {
		return nil, nil
	}
	carr := (*C.double)(&(data[0]))
	res := C.predict(a.fn, carr, (C.long)(len(data)))

	//runtime.KeepAlive(data)
	if res.err != 0 {
		return nil, pyLastError()
	}

	// Create a Go slice from C long*
	indices, err := cArrToSlice(res.indices, res.size)
	if err != nil {
		return nil, err
	}

	// Free Python array object
	C.py_decref(res.obj)
	return indices, nil
}

func (a *ML) Learn() {

	if a.fn == nil {
		return
	}
	C.learn(a.fn)

}

func (a *ML) Close() {
	if a.fn == nil {
		return
	}
	C.py_decref(a.fn)
	a.fn = nil
}

func loadPyFunc(moduleName, funcName string) (*C.PyObject, error) {

	cMod := C.CString(moduleName)
	cFunc := C.CString(funcName)

	defer func() {
		C.free(unsafe.Pointer(cMod))
		C.free(unsafe.Pointer(cFunc))
	}()

	fn := C.load_func(cMod, cFunc)
	if fn == nil {
		return nil, pyLastError()
	}

	return fn, nil
}

func pyLastError() error {
	cp := C.py_last_error()
	if cp == nil {
		return nil
	}
	err := C.GoString(cp)
	return fmt.Errorf("%s", err)
}

func cArrToSlice(cArr *C.long, size C.long) ([]int, error) {
	const maxSize = 1 << 20
	if size > maxSize {
		return nil, fmt.Errorf("C array to large (%d > %d)", size, maxSize)
	}

	ptr := unsafe.Pointer(cArr)
	arr := (*[maxSize]int)(ptr)

	// Create a slice with copy of data managed by Go
	s := make([]int, size)
	copy(s, arr[:size])

	return s, nil
}
