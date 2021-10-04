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
	"path/filepath"
	"os"
	"strings"
)

func BadFileName(s string) bool {
	for _,b := range []byte(s) {
		if b==filepath.Separator { return true }
		if b=='\\' || b=='/' { return true }
	}
	if s=="." { return true }
	return false
}

func cleanup(r rune) rune {
	if r<' ' { return '_' }
	if r=='\\' || r=='/' || r==filepath.Separator { return '_' }
	return r
}

func CleanUpFile(s string) string {
	return strings.Map(cleanup,s)
}

type Dir string
func (d Dir) Open(p Path) (io.ReadCloser,error) {
	if BadFileName(p[1]) { return nil,ENoFile }
	_,f := filepath.Split(string(d))
	if p[0]!=f { return nil,ENoDir }
	return os.Open(filepath.Join(string(d),p[1]))
}
func (d Dir) Dirs() []string {
	_,f := filepath.Split(string(d))
	return []string{f}
}
func (d Dir) Files(dir string) ([]string,error) {
	_,f := filepath.Split(string(d))
	if dir!=f { return nil,ENoDir }
	g,err := os.Open(string(d))
	if err!=nil { return nil,err }
	defer g.Close()
	return g.Readdirnames(-1)
}

type DirColl []Dir
func (d DirColl) Open(p Path) (r io.ReadCloser,e error) {
	for _,dd := range d {
		r,e = dd.Open(p)
		if e!=ENoDir { break }
	}
	return
}
func (d DirColl) Dirs() []string {
	s := make([]string,len(d))
	for i,dd := range d {
		_,f := filepath.Split(string(dd))
		s[i] = f
	}
	return s
}
func (d DirColl) Files(dir string) (r []string,e error) {
	for _,dd := range d {
		r,e = dd.Files(dir)
		if e!=ENoDir { break }
	}
	return
}

type DirMap map[string]string
func (d DirMap) Open(p Path) (r io.ReadCloser,e error) {
	if BadFileName(p[1]) { return nil,ENoFile }
	f,ok := d[p[0]]
	if ok { return nil,ENoDir }
	return os.Open(filepath.Join(f,p[1]))
}
func (d DirMap) Dirs() (z []string) {
	z = make([]string,0,len(d))
	for f := range d { z = append(z,f) }
	return
}
func (d DirMap) Files(dir string) (r []string,e error) {
	f,ok := d[dir]
	if ok { return nil,ENoDir }
	g,err := os.Open(f)
	if err!=nil { return nil,err }
	defer g.Close()
	return g.Readdirnames(-1)
}

type dfToken int

type DownloadFolder struct{
	Dir string
	
	_ign int
}
func (df *DownloadFolder) Token() Token { return new(dfToken) }
func (df *DownloadFolder) Create(t Token, p Path) (io.WriteCloser,error) {
	if t==nil { return nil,EDlRejected }
	B := CleanUpFile(p[1])
	C := filepath.Join(df.Dir,B)
	return os.Create(C)
}


