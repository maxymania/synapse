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


// Fulltext Search engine
package ftse

import (
	bson "github.com/mad-day/bsonbox/bsoncore"
	"github.com/maxymania/synapse/c2s"
	"strings"
	"unicode"
	"fmt"
)

var eNull = fmt.Errorf("ftse:null")

func canonicalize(r rune) rune {
	switch r {
	case '\'','`','´': return '_'
	case '_': return ' '
	}
	return unicode.ToLower(r)
}
func fields(r rune) bool {
	return !(r == '_' ||
		unicode.IsLetter(r) ||
		unicode.IsDigit(r))
}

func splitup(s string) []string {
	s = strings.Map(canonicalize,s)
	return strings.FieldsFunc(s,fields)
}
func prepends(kwds []string, pre string) {
	for i := range kwds { kwds[i] = pre+kwds[i] }
}

func unify(t, words []string) []string {
	t = t[:0]
	m := make(map[string]bool)
	for _,w := range words {
		if m[w] { continue }
		m[w] = true
		t = append(t,w)
	}
	return t
}

type Path [3]string

func (p *Path) CreateMeta() bson.Document {
	return bson.NewDocumentBuilder().
		AppendString("",p[1]).
		AppendString("f",p[2]).
		Build()
}

type Result struct{
	Path Path
	Meta bson.Document
}
func (r *Result) GetMeta() bson.Document {
	doc,_,ok := bson.ReadDocument(r.Meta)
	if !ok { return r.Path.CreateMeta() }
	return doc
}


type Dir interface{
	PutTrack(path Path, keys []string, doc []byte)
	DelTrack(path Path)
	DelAll(domain string)
	Lookup(keys []string,max int) []Result
	
}
type FTSI struct{
	Dir
}


func (f *FTSI) RetractAll(tok c2s.Srv_Token) {
	if tok.Status()!=c2s.Accepted { return }
	dom := tok.Domain()
	f.DelAll(dom)
}

func (f *FTSI) Publish(tok c2s.Srv_Token, doc bson.Document) {
	if tok.Status()!=c2s.Accepted { return }
	elems,_ := doc.Elements()
	if len(elems) < 2 { return }
	var pth Path
	pth[0] = tok.Domain()
	pth[1],_ = elems[0].Value().StringValueOK()
	pth[2],_ = elems[1].Value().StringValueOK()
	kwds := splitup(pth[2])
	prepends(kwds,"f")
	for _,elem := range elems[2:] {
		fld,_ := elem.Value().StringValueOK()
		k := elem.Key()
		tmp := splitup(fld)
		prepends(tmp,k)
		kwds = append(kwds,tmp...)
	}
	kwds = unify(kwds,kwds)
	f.PutTrack(pth,kwds,doc)
}

func (f *FTSI) Retract(tok c2s.Srv_Token, doc bson.Document) {
	if tok.Status()!=c2s.Accepted { return }
	elems,_ := doc.Elements()
	if len(elems) < 2 { return }
	var pth Path
	pth[0] = tok.Domain()
	pth[1],_ = elems[0].Value().StringValueOK()
	pth[2],_ = elems[1].Value().StringValueOK()
	f.DelTrack(pth)
}

func (f *FTSI) Query(tok c2s.Srv_Token, terms bson.Document, max int) bson.Document {
	elems,_ := terms.Elements()
	kwds := []string{}
	for _,elem := range elems {
		fld,_ := elem.Value().StringValueOK()
		k := elem.Key()
		tmp := splitup(fld)
		prepends(tmp,k)
		kwds = append(kwds,tmp...)
	}
	kwds = unify(kwds,kwds)
	
	db := bson.NewDocumentBuilder()
	for _,res := range f.Lookup(kwds,max) {
		db = db.AppendDocument(res.Path[0],res.GetMeta())
	}
	
	return db.Build()
}

