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
	bson "github.com/mad-day/bsonbox/bsoncore"
	"fmt"
)

func pack(i ...interface{}) bson.Document {
	db := bson.NewDocumentBuilder()
	for len(i)>=2 {
		k := fmt.Sprint(i[0])
		switch e := i[1].(type) {
		case string: db.AppendString(k,e)
		case []byte: db.AppendBinary(k,1,e)
		case int32: db.AppendInt32(k,e)
		case int64: db.AppendInt64(k,e)
		case int:  db.AppendInt32(k,int32(e))
		case bson.Document: db.AppendDocument(k,e)
		}
		i = i[2:]
	}
	return db.Build()
}
func pick(doc bson.Document) bson.Value {
	e,err := doc.IndexErr(0)
	if err!=nil { return bson.Value{} }
	return e.Value()
}
