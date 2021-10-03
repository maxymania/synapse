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

package alloc

import (
	slab "github.com/couchbase/go-slab"
)

type locker chan int
func (l locker) Lock() { l <- 1 }
func (l locker) Unlock() { <- l }

type Alloc struct{
	m       locker
	arena   *slab.Arena
	recycle chan []byte
}

/*
Creates a new allocator with a slab allocator underneath.

The ideal parameters are (48,1<<20,2,nil).
*/
func NewAlloc(startChunkSize int, slabSize int, growthFactor float64, malloc func(size int) []byte) *Alloc {
	c := new(Alloc)
	c.m = make(locker,1)
	c.arena = slab.NewArena(startChunkSize,slabSize,growthFactor,malloc)
	c.recycle = make(chan []byte,128)
	return c
}

func (c *Alloc) freeAll() {
	for {
		select {
		case buf := <- c.recycle:
			if c.arena.Owns(buf) {
				c.arena.DecRef(buf)
			}
			continue
		default:
		}
		break
	}
}
func (c *Alloc) Alloc(i int) []byte {
	c.m.Lock()
	defer c.m.Unlock()
	
	c.freeAll()
	a := c.arena.Alloc(i)
	if len(a)==0 { a = make([]byte,i) }
	return a
}

func (c *Alloc) Free(buf []byte) {
	if buf==nil { return }
	select {
	case c.recycle <- buf: return
	default:
	}
	select {
	case c.recycle <- buf: return
	case c.m <- 1:
		defer c.m.Unlock()
		if c.arena.Owns(buf) { c.arena.DecRef(buf) }
	}
}

