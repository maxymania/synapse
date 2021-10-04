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
	bson "github.com/mad-day/bsonbox/bsoncore"
	"github.com/maxymania/synapse/proto"
	"fmt"
	"sync"
)

var ECryptoError = fmt.Errorf("p2p: Crypto Error")
var EProtocolError = fmt.Errorf("p2p: Protocol Error")

type signal chan int
type mqueue chan bson.Document


/*
-------------------------------------------------------------------------------
*                             Server Implementation
-------------------------------------------------------------------------------
*/

type Server struct{
	Arena proto.Allocator
	FS    FileSystem
	KP    proto.KeyPair
}

type connServer struct{
	*Server
	pc     *proto.Conn
	alive  signal
	outhi  mqueue // High priority queue
	outlo  mqueue // Low priority queue
	downl  fileQueue
}


func (s *Server) prepare(conn io.ReadWriteCloser) *connServer {
	t := new(connServer)
	t.Server = s
	t.pc = proto.NewConn(conn,s.Arena)
	t.alive = make(signal)
	t.outhi = make(mqueue,32)
	t.outlo = make(mqueue,16)
	t.downl = make(fileQueue,8) // 8 Downloads gleichzeitig
	return t
}

func (s *Server) Serve(conn io.ReadWriteCloser) {
	c := s.prepare(conn)
	c.serve()
}

// This method loops over all the files in the download queue and releases them.
func (c *connServer) destroy() {
	for {
		select {
		case felem := <- c.downl:
			if felem.fobj!=nil { felem.fobj.Close() }
		default: return
		}
	}
}

func (c *connServer) writer() {
	for {
		select {
		case <- c.alive:
		case msg := <- c.outhi: c.pc.WriteDocument(msg)
		default:
		}
		select {
		case <- c.alive:
		case msg := <- c.outhi: c.pc.WriteDocument(msg)
		case msg := <- c.outlo: c.pc.WriteDocument(msg)
		}
	}
}

func (c *connServer) filewrite(felem queueElement) {
	if felem.fobj==nil { return }
	defer felem.fobj.Close()
	{
		d := pack("d",felem.path[0],"f",felem.path[1])
		c.outlo <- pack("dl.start",d)
	}
	buf := make([]byte,1<<13)
	for {
		n,_ := felem.fobj.Read(buf)
		if n==0 { break }
		c.outlo <- pack("dl.bin",buf[:n])
		if n<len(buf) { break }
	}
	c.outlo <- pack("dl.end",0)
}

func (c *connServer) filewriter() {
	for {
		select {
		case <- c.alive:
		case felem := <- c.downl: c.filewrite(felem)
		}
	}
}

func (c *connServer) serve() {
	defer c.pc.Close()
	defer c.destroy()
	defer close(c.alive)
	go c.writer()
	go c.filewriter()
	for {
		err := c.serveReq()
		if err!=nil { return }
	}
}

func (c *connServer) serveReq() (err error) {
	var msg,send bson.Document
	var elems []bson.Element
	msg,err = c.pc.ReadDocument()
	if err!=nil { return }
	defer c.pc.Free(msg)
	elems,err = msg.Elements()
	if len(elems)<1 { return }
	switch string(elems[0].KeyBytes()) {
	case "hs.s1":
		send = c.KP.Step1()
		if send==nil { send = pack() }
		send = pack("hs.s1",send)
		c.outhi <- send
	case "hs.s2":
		send,_ = elems[0].Value().DocumentOK()
		if send==nil { send = pack() }
		send = c.KP.Step2(send)
		if send==nil { send = pack() }
		send = pack("hs.s2",send)
		c.outhi <- send
	case "getfile":
		if len(elems)<2 { return }
		var qe queueElement
		qe.path[0],_ = elems[0].Value().StringValueOK()
		qe.path[1],_ = elems[1].Value().StringValueOK()
		qe.fobj,err = c.FS.Open(qe.path)
		if err!=nil {
			c.outhi <- pack("putfile",404,"txt",err.Error())
			err = nil
			return
		}
		select {
		case c.downl <- qe:
			c.outhi<- pack("putfile",200,"d",qe.path[0],"f",qe.path[1])
			return
		default:
		}
		qe.fobj.Close()
		c.outhi <- pack("putfile",204,"txt","queue ran full")
	}
	
	return
}

/*
-------------------------------------------------------------------------------
*                             Client Implementation
-------------------------------------------------------------------------------
*/

func rescue(err *error) {
	i := recover()
	if i==nil { return }
	if e,ok := i.(error); ok { *err = e; return }
	*err = fmt.Errorf("%v",i)
}

func hasprefix(b []byte,s string) bool {
	if len(b)<len(s) { return false }
	return string(b[:len(s)])==s
}

func d2path(d bson.Document) (p Path) {
	for i := range p {
		e,_ := d.IndexErr(uint(i))
		//if err==nil { p[i],_ = e.Value().StringValueOK() }
		p[i],_ = e.Value().StringValueOK()
	}
	return
}

type ClientContext struct{
	Arena  proto.Allocator
	Target TargetStore
}

type Client struct{
	*ClientContext
	pc *proto.Conn
	alive   signal
	appmsg  mqueue
	filemsg mqueue
	toks    PathTokenMap
	
	pcm     sync.Mutex
}

func (cc *ClientContext) prepare(conn io.ReadWriteCloser) *Client {
	t := new(Client)
	t.ClientContext = cc
	t.pc = proto.NewConn(conn,cc.Arena)
	t.alive   = make(signal)
	t.appmsg  = make(mqueue,32)
	t.filemsg = make(mqueue,8)
	return t
}

func (c *Client) lock() func() {
	c.pcm.Lock(); return c.pcm.Unlock
}

func (c *Client) reader() {
	for {
		select {
		case <- c.alive: goto done
		default:
		}
		msg,err := c.pc.ReadDocument()
		if err!=nil { c.Close(); continue }
		elem,err := msg.IndexErr(0)
		if err!=nil { c.pc.Free(msg); continue }
		kb := elem.KeyBytes()
		if hasprefix(kb,"dl.") {
			c.filemsg <- msg
		} else {
			c.appmsg <- msg
		}
	}
	done:
	for {
		select {
		case msg := <- c.appmsg: c.pc.Free(msg)
		case msg := <- c.filemsg: c.pc.Free(msg)
		}
	}
}
func (c *Client) filewriter() {
	var cf io.WriteCloser
	setFile := func(ncf io.WriteCloser) {
		if cf!=nil { cf.Close() }
		cf = ncf
	}
	defer setFile(nil)
	for {
		var msg bson.Document
		select {
		case <- c.alive: return
		case msg = <- c.filemsg:
		}
		elem,_ := msg.IndexErr(0)
		switch string(elem.KeyBytes()) {
		case "dl.start":
			hdr,ok := elem.Value().DocumentOK()
			if !ok { goto done }
			path := d2path(hdr)
			token := c.toks.Get(path)
			c.toks.Put(path,nil)
			ncf,err := c.Target.Create(token,path)
			if err!=nil { goto done }
			setFile(ncf)
		case "dl.bin":
			_,data,ok := elem.Value().BinaryOK()
			if !ok { goto done }
			if cf!=nil {
				cf.Write(data)
			}
		case "dl.end":
			if cf!=nil {
				cf.Close()
				cf = nil
			}
		}
		done:
		c.pc.Free(msg)
	}
}


func (cc *ClientContext) NewClient(conn io.ReadWriteCloser) (*Client,error) {
	if conn==nil { return nil,fmt.Errorf("p2p: conn = ",conn) }
	c := cc.prepare(conn)
	go c.reader()
	go c.filewriter()
	return c,nil
}

func (c *Client) Alive() bool {
	select {
	case <- c.alive: return true
	default: return false
	}
	panic("unreachable")
}
func (c *Client) readMessage() (bson.Document,error) {
	select {
	case msg := <- c.appmsg: return msg,nil
	case <- c.alive: return nil,io.EOF
	}
}
func (c *Client) Close() (err error) {
	defer rescue(&err)
	defer close(c.alive)
	err = c.pc.Close()
	return
}
func (c *Client) Authenticate(sa *proto.ServerAuth) (ok bool,err error) {
	var pl bson.Document
	pl,err = c.step1(sa)
	if err!=nil { return }
	if pl==nil { return true,ECryptoError }
	ok,err = c.step2(pl,sa)
	return
}
func (c *Client) AuthStep2(sa *proto.ServerAuth, pub []byte, dom string) (ok bool,err error) {
	var pl bson.Document
	pl = sa.OnePassPrep(pub,dom)
	if pl==nil { return true,ECryptoError }
	ok,err = c.step2(pl,sa)
	return
}
func (c *Client) step1(sa *proto.ServerAuth) (pl bson.Document, err error) {
	defer c.lock()()
	var msg bson.Document
	err = c.pc.WriteDocument(pack("hs.s1",""))
	if err!=nil { return }
	msg,err = c.readMessage()
	if err!=nil { return }
	defer c.pc.Free(msg)
	msg,_ = msg.Lookup("hs.s1").DocumentOK()
	pl = sa.Step1(msg)
	return
}
func (c *Client) step2(pl bson.Document, sa *proto.ServerAuth) (ok bool,err error) {
	defer c.lock()()
	var msg bson.Document
	err = c.pc.WriteDocument(pack("hs.s2",pl))
	if err!=nil { return }
	msg,err = c.readMessage()
	if err!=nil { return }
	defer c.pc.Free(msg)
	msg,_ = msg.Lookup("hs.s2").DocumentOK()
	ok = sa.Step2(msg)
	return
}

func (c *Client) GetFile(tok Token,path Path) (dataerr, err error) {
	defer c.lock()()
	var msg bson.Document
	var elems []bson.Element
	c.toks.Put(path,tok)
	err = c.pc.WriteDocument(pack("getfile",path[0],"f",path[1]))
	if err!=nil { return }
	msg,err = c.readMessage()
	if err!=nil { return }
	defer c.pc.Free(msg)
	
	elems,err = msg.Elements()
	if err!=nil { return }
	
	if len(elems)<2 { err = EProtocolError; return }
	
	code,_ := elems[0].Value().Int32OK()
	if code!=200 {
		s,_ := elems[1].Value().StringValueOK()
		dataerr = fmt.Errorf("%v",s)
	}
	
	return
}


