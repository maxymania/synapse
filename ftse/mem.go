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


package ftse

import (
	"sync"
	"github.com/RoaringBitmap/roaring"
	"fmt"
)

type MemDir struct{
	m sync.RWMutex
	f map[string]uint32
	i map[string]*roaring.Bitmap
	z *roaring.Bitmap
	s [][]string
	p []Path
	b [][]byte
}
var _ Dir = (*MemDir)(nil)

func (m *MemDir) String() string {
	return fmt.Sprintf("{ f=%v ; i=%v ; s=%v ; p=%v }",m.f,m.i,m.s,m.p)
}

func (m *MemDir) lock() func() {
	m.m.Lock()
	return m.m.Unlock
}
func (m *MemDir) rlock() func() {
	m.m.RLock()
	return m.m.RUnlock
}
func (m *MemDir) prepare() {
	if m.f==nil { m.f = make(map[string]uint32) }
	if m.i==nil { m.i = make(map[string]*roaring.Bitmap) }
	if m.z==nil { m.z = roaring.New() }
}
func (m *MemDir) idel(i uint32) {
	if uint32(len(m.s)) <= i { return }
	for _,kw := range m.s[i] {
		b := m.i[kw]
		if b==nil { continue }
		b.Remove(i)
	}
	m.s[i] = nil
	m.p[i] = Path{}
}

func (m *MemDir) alloc() (uint32,bool) {
	if m.z==nil { return 0,false }
	if m.z.IsEmpty() { return 0,false }
	i := m.z.Minimum()
	m.z.Remove(i)
	return i,true
}

func (m *MemDir) PutTrack(path Path, keys []string, doc []byte) {
	defer m.lock()()
	var i uint32
	
	p := path[0]+"/"+path[1]+"/"+path[2]
	
	m.prepare()
	
	doc = append([]byte(nil),doc...)
	if j,ok := m.f[p]; ok {
		i = uint32(j)
		m.idel(i)
		m.s[i] = keys
		m.p[i] = path
		m.b[i] = doc
	} else if j,ok := m.alloc(); ok {
		i = j
		m.s[i] = keys
		m.p[i] = path
		m.b[i] = doc
		m.f[p] = i
	} else {
		m.f[p] = i
		i = uint32(len(m.s))
		m.s = append(m.s,keys)
		m.p = append(m.p,path)
		m.b = append(m.b,doc)
	}
	
	for _,kw := range m.s[i] {
		b := m.i[kw]
		if b==nil {
			b = roaring.New()
			m.i[kw] = b
		}
		b.Add(i)
	}
}

func (m *MemDir) DelTrack(path Path) {
	defer m.lock()()
	
	p := path[0]+"/"+path[1]+"/"+path[2]
	
	m.prepare()
	
	if i,ok := m.f[p]; ok {
		m.idel(i)
		delete(m.f,p)
		m.z.Add(i)
	}
}

func (m *MemDir) DelAll(domain string) {
	defer m.lock()()
	
	m.prepare()
	
	pre := domain+"/"
	l := len(pre)
	
	for p,i := range m.f {
		if len(p)<l { continue }
		if p[:l]!=pre { continue }
		m.idel(i)
		delete(m.f,p)
		m.z.Add(i)
	}
}

func (m *MemDir) Lookup(keys []string, max int) []Result {
	defer m.rlock()()
	
	imb := make([]*roaring.Bitmap,len(keys))
	for i,key := range keys {
		b := m.i[key]
		if b==nil { return nil }
		imb[i] = b
	}
	res := roaring.FastAnd(imb...)
	
	pth := make([]Result,0,res.GetCardinality())
	iter := res.Iterator()
	
	L := uint32(len(m.p))
	
	for iter.HasNext() {
		i := iter.Next()
		if i>=L { continue }
		pth = append(pth,Result{m.p[i],m.b[i]})
		if len(pth)>=max { break }
	}
	
	return pth
}

