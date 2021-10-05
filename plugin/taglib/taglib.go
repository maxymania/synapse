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


package taglib


import (
	bson "github.com/mad-day/bsonbox/bsoncore"

	"io"
	"github.com/maxymania/synapse/p2p"
	"github.com/dhowden/tag"
	"fmt"
)

var ENoSeekSupport = fmt.Errorf("taglib: no seek support")

type TagMDA int

func (TagMDA) GetMetadata(fs p2p.FileSystem, pth p2p.Path) (bson.Document,error) {
	f,err := fs.Open(pth)
	if err!=nil { return nil,err }
	defer f.Close()
	r,ok := f.(io.ReadSeeker)
	if !ok { return nil,ENoSeekSupport }
	md,err := tag.ReadFrom(r)
	if err!=nil { return nil,err }
	doc := bson.NewDocumentBuilder().
		AppendString("_",pth[0]).
		AppendString("f",pth[1]).
		AppendString("t",md.Title()).
		AppendString("a",md.Album()).
		AppendString("p",md.Artist()).
		AppendString("g",md.Genre()).
		Build()
	return doc,err
}

