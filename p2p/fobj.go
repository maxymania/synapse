/*
Copyright (c) 2021 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package p2p

import (
	"io"
	"sync"
	"errors"
)

var ENoDir = errors.New("p2p: Dir not Found")
var ENoFile = errors.New("p2p: File not Found")
var EDlRejected = errors.New("p2p: Download Rejected")

type Path [2]string

type FileSystem interface{
	Open(p Path) (io.ReadCloser,error)
}

type queueElement struct {
	fobj io.ReadCloser
	path Path
}

type fileQueue chan queueElement

type Token interface{
}

type TargetStore interface{
	Create(t Token, p Path) (io.WriteCloser,error)
}

type PathTokenMap struct{
	s sync.RWMutex
	m map[Path]Token
}
func (m *PathTokenMap) Get(p Path) (t Token) {
	m.s.RLock(); defer m.s.RUnlock()
	return m.m[p]
}
func (m *PathTokenMap) Put(p Path,t Token) {
	m.s.Lock(); defer m.s.Unlock()
	if m.m==nil { m.m = make(map[Path]Token) }
	if t==nil {
		delete(m.m,p)
	} else {
		m.m[p] = t
	}
}

