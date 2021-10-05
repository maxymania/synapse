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


package c2s

import (
	"io"
	bson "github.com/mad-day/bsonbox/bsoncore"
	"github.com/maxymania/synapse/proto"
	"fmt"
	"sync"
)

var eAuthFailed = fmt.Errorf("c2s: Auth Failed")

var eCryptoError = fmt.Errorf("c2s: crypto error")

var eProtocolError = fmt.Errorf("c2s: protocol error")

func elookup(elems []bson.Element,n string) (val bson.Value) {
	for _,elem := range elems {
		if string(elem.KeyBytes())!=n { continue }
		val = elem.Value()
		break
	}
	return
}

func bdClone(elem bson.Element) bson.Element {
	nelem := make(bson.Element,len(elem))
	copy(nelem,elem)
	return nelem
}
func bdClones(elems []bson.Element) {
	for i := range elems {
		elems[i] = bdClone(elems[i])
	}
}

type Status int32
const (
	Pending  Status = iota
	Rejected
	Accepted
)

/*
-------------------------------------------------------------------------------
*                                    Server
-------------------------------------------------------------------------------
*/

type Srv_Token interface{
	Status() Status
	Domain() string
}

type Srv_Auth interface{
	Login(pub []byte, domain string) Srv_Token
}

type Srv_Queries interface{
	RetractAll(tok Srv_Token)
	
	Publish(tok Srv_Token, doc bson.Document)
	Retract(tok Srv_Token, doc bson.Document)
	
	Query(tok Srv_Token, terms bson.Document, max int) bson.Document
}

type Server struct{
	Arena proto.Allocator
	Rand  io.Reader
	Auth  Srv_Auth
	Query Srv_Queries
}

func (s *Server) Validate() bool {
	return s.Rand!=nil &&
		s.Auth!=nil &&
		s.Query!=nil
}

type connServer struct {
	*Server
	pc  *proto.Conn
	sa  proto.ServerAuth
	tok Srv_Token
}


func (s *connServer) handshake() (err error) {
	t := s.pc
	var recv,chal bson.Document
	recv,err = t.ReadDocument()
	if err!=nil { return }
	chal = s.sa.Step1(recv)
	t.Free(recv)
	if chal==nil { return eCryptoError }
	err = t.WriteDocument(chal)
	if err!=nil { return }
	recv,err = t.ReadDocument()
	if err!=nil { return }
	ok := s.sa.Step2(recv)
	t.Free(recv)
	if !ok { err = eAuthFailed }
	return
}

func (s *connServer) ready(msg bson.Document, elems []bson.Element) (err error) {
	status := s.tok.Status()
	
	res := bson.NewDocumentBuilder().
		AppendInt32("status",int32(status)).
		Build()
	err = s.pc.WriteDocument(res)
	return
}

func (s *connServer) publish(msg bson.Document, elems []bson.Element) (err error) {
	for _,elem := range elems {
		arr,ok := elem.Value().DocumentOK()
		if !ok { continue }
		s.Query.Publish(s.tok, arr)
	}
	return
}

func (s *connServer) retract(msg bson.Document, elems []bson.Element) (err error) {
	for _,elem := range elems {
		arr,ok := elem.Value().DocumentOK()
		if !ok { continue }
		s.Query.Retract(s.tok, arr)
	}
	return
}

func (s *connServer) query(msg bson.Document, elems []bson.Element) (err error) {
	if s.tok.Status()==Rejected {
		resp := bson.NewDocumentBuilder().Build()
		err = s.pc.WriteDocument(resp)
		return
	}
	terms,ok := elems[0].Value().DocumentOK()
	if !ok { return eProtocolError }
	i := int32(1<<10)
	if j,ok := elookup(elems,"max").Int32OK() ; ok { i = j }
	resp := s.Query.Query(s.tok,terms,int(i))
	err = s.pc.WriteDocument(resp)
	return
}

func (s *connServer) serve() (err error) {
	var msg bson.Document
	var elems []bson.Element
	msg,err = s.pc.ReadDocument()
	if err!=nil { return }
	defer s.pc.Free(msg)
	elems,err = msg.Elements()
	if err!=nil { return }
	if len(elems)==0 { return }
	switch elems[0].Key() {
	case "ready": err = s.ready(msg, elems)
	case "publish": err = s.publish(msg, elems)
	case "retract": err = s.retract(msg, elems)
	case "sweep": s.Query.RetractAll(s.tok)
	case "query": err = s.query(msg, elems)
	}
	return
}


func (s *Server) prepare(conn io.ReadWriteCloser) *connServer {
	t := new(connServer)
	t.Server = s
	t.pc = proto.NewConn(conn,s.Arena)
	t.sa.Rand = s.Rand
	return t
}

func (s *Server) Serve(conn io.ReadWriteCloser) {
	defer conn.Close()
	t := s.prepare(conn)
	err := t.handshake()
	if err!=nil { return }
	t.tok = t.Auth.Login(t.sa.Pub,t.sa.Domain)
	if t.tok==nil { return }
	defer t.Query.RetractAll(t.tok)
	for {
		err = t.serve()
		if err!=nil { return }
	}
}


/*
-------------------------------------------------------------------------------
*                                    Client
-------------------------------------------------------------------------------
*/

type ClientContext struct{
	Arena proto.Allocator
	KP proto.KeyPair
}
func (cc *ClientContext) Validate() bool {
	return len(cc.KP.Pub)>0 && len(cc.KP.Pri)>0
}

type Client struct{
	*ClientContext
	conn *proto.Conn
	m sync.Mutex
}
func (c *Client) lock() func() {
	c.m.Lock(); return c.m.Unlock
}

func cfatal(pc **Client,err error) bool {
	if err==nil { return false }
	(*pc).conn.Close()
	*pc = nil
	return true
}

func (c *Client) handshake() (err error) {
	var recv,send bson.Document
	send = c.KP.Step1()
	if send==nil { return eCryptoError }
	err = c.conn.WriteDocument(send)
	if err!=nil { return }
	recv,err = c.conn.ReadDocument()
	if err!=nil { return }
	send = c.KP.Step2(recv)
	if send==nil { return eCryptoError }
	err = c.conn.WriteDocument(send)
	return
}



func (cc *ClientContext) NewClient(conn io.ReadWriteCloser) (cli *Client,err error) {
	cli = &Client{ClientContext:cc,conn:proto.NewConn(conn,cc.Arena)}
	err = cli.handshake()
	if cfatal(&cli,err) { return }
	return
}

func (c *Client) Close() error { return c.conn.Close() }
func (c *Client) Status() (Status,error) {
	defer c.lock()()
	doc := bson.NewDocumentBuilder().AppendString("ready","").Build()
	err := c.conn.WriteDocument(doc)
	if err!=nil { return 0,err }
	resp,err := c.conn.ReadDocument()
	if err!=nil { return 0,err }
	i,ok := resp.Lookup("status").Int32OK()
	if !ok { return 0,eProtocolError }
	return Status(i),nil
}

func (c *Client) RetractAll() error {
	defer c.lock()()
	doc := bson.NewDocumentBuilder().AppendString("sweep","").Build()
	return c.conn.WriteDocument(doc)
}

func (c *Client) Publish(files []bson.Document) error {
	defer c.lock()()
	if len(files)==0 { return nil }
	db := bson.NewDocumentBuilder().AppendDocument("publish",files[0])
	for _,f := range files[1:] {
		db = db.AppendDocument("",f)
	}
	return c.conn.WriteDocument(db.Build())
}

func (c *Client) Retract(files []bson.Document) error {
	defer c.lock()()
	if len(files)==0 { return nil }
	db := bson.NewDocumentBuilder().AppendDocument("retract",files[0])
	for _,f := range files[1:] {
		db = db.AppendDocument("",f)
	}
	return c.conn.WriteDocument(db.Build())
}

func (c *Client) Query(terms bson.Document,max int) ([]bson.Element,error) {
	defer c.lock()()
	db := bson.NewDocumentBuilder().AppendDocument("query",terms)
	if max>0 { db.AppendInt32("max",int32(max)) }
	doc := db.Build()
	err := c.conn.WriteDocument(doc)
	if err!=nil { return nil,err }
	doc,err = c.conn.ReadDocument()
	if err!=nil { return nil,err }
	elems,err := doc.Elements()
	bdClones(elems)
	return elems,err
}


