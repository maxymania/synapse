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


/*
Connection Codec and some Cryptography primitives.
*/
package proto

import (
	"io"
	bson "github.com/mad-day/bsonbox/bsoncore"
	"crypto/elliptic"
	"crypto/sha256"
	"bytes"
)

/*
-------------------------------------------------------------------------------
*                             Allocator Interface
-------------------------------------------------------------------------------
*/

type Allocator interface{
	Alloc(int) []byte
	Free([]byte)
}
type noAlloc int
func (noAlloc) Alloc(i int) []byte { return make([]byte,i) }
func (noAlloc) Free([]byte) { }

var anoop Allocator = noAlloc(0)

/*
-------------------------------------------------------------------------------
*                             Connection Codec
-------------------------------------------------------------------------------
*/

type Conn struct{
	conn    io.ReadWriteCloser
	arena   Allocator
}

func NewConn(cw io.ReadWriteCloser, a Allocator) *Conn {
	if a==nil { a = anoop }
	c := new(Conn)
	c.conn = cw
	c.arena = a
	return c
}

func (c *Conn) Free(buf []byte) { c.arena.Free(buf) }
func (c *Conn) ifree(buf []byte) []byte {
	if buf!=nil { c.arena.Free(buf) }
	return nil
}
func (c *Conn) Close() error {
	return c.conn.Close()
}
func (c *Conn) ReadDocument() (doc bson.Document,err error) {
	doc,err = bson.NewDocumentFromReader2(c.conn,c.arena.Alloc)
	if err!=nil { doc = c.ifree(doc) }
	return
}
func (c *Conn) WriteDocument(doc bson.Document) (error) {
	doc,_,ok := bson.ReadDocument(doc)
	if !ok { return nil }
	_,err := c.conn.Write(doc)
	return err
}

/*
-------------------------------------------------------------------------------
*                                  Cryptography
-------------------------------------------------------------------------------
*/

var curve = elliptic.P256()

func GenKeyPair(rand io.Reader) (pub,pri []byte,err error){
	pri = make([]byte,32)
	_,err = io.ReadFull(rand,pri)
	x,y := curve.ScalarBaseMult(pri)
	pub = elliptic.Marshal(curve,x,y)
	return
}
func Handshake(other,pri []byte) (res []byte) {
	x,y := elliptic.Unmarshal(curve,other)
	if x==nil || y==nil { return }
	x,y = curve.ScalarMult(x,y,pri)
	res = elliptic.Marshal(curve,x,y)
	return
}

func dohash(b []byte) []byte {
	r := sha256.New()
	r.Write(b)
	return r.Sum(make([]byte,0,32))
}

// Duplicates a buffer by allocating memory and copying the contents.
// This function works in-place.
func bdup(pb *[]byte) {
	var nb []byte = nil
	b := *pb
	if len(b)!=0 {
		nb = make([]byte,len(b))
		copy(nb,b)
	}
	*pb = nb
}

type KeyPair struct{
	Pub, Pri []byte
	Domain string
}

func (kp *KeyPair) Step1() bson.Document {
	db := bson.NewDocumentBuilder()
	db.AppendBinary("login",'c',kp.Pub)
	db.AppendString("domain",kp.Domain)
	return db.Build()
}

func (kp *KeyPair) Step2(resp bson.Document) bson.Document {
	_,other,_ := resp.Lookup("chal").BinaryOK()
	sk := Handshake(other,kp.Pri)
	if sk==nil { return nil }
	sk = dohash(sk)
	
	db := bson.NewDocumentBuilder()
	db.AppendBinary("sha2",'s',sk)
	return db.Build()
}

type ServerAuth struct{
	Rand io.Reader
	Pub []byte
	Domain string
	sk []byte
}

func (sa *ServerAuth) OnePassPrep(pub []byte, domain string) bson.Document {
	db := bson.NewDocumentBuilder()
	db.AppendBinary("login",'c',pub)
	db.AppendString("domain",domain)
	return sa.Step1(db.Build())
}

func (sa *ServerAuth) Step1(doc bson.Document) bson.Document {
	pub,pri,err := GenKeyPair(sa.Rand)
	if err!=nil { return nil }
	var ok1,ok2 bool
	_,sa.Pub,ok1 = doc.Lookup("login").BinaryOK()
	sa.Domain,ok2 = doc.Lookup("domain").StringValueOK()
	
	// Make a copy of that buffer. Do not share memory with the document!
	bdup(&sa.Pub)
	
	if !(ok1 && ok2) { return nil }
	
	sk := Handshake(sa.Pub,pri)
	if sk==nil { return nil }
	sa.sk = dohash(sk)
	
	db := bson.NewDocumentBuilder()
	db.AppendBinary("chal",'c',pub)
	return db.Build()
}
func (sa *ServerAuth) Step2(doc bson.Document) (ok bool) {
	_,osk,_ := doc.Lookup("sha2").BinaryOK()
	ok = len(sa.sk)>0 && bytes.Equal(sa.sk,osk)
	return
}

