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


package servent

import (
	"net"
	"context"
	"golang.org/x/net/proxy"
	bson "github.com/mad-day/bsonbox/bsoncore"

	"github.com/maxymania/synapse/c2s"
	"github.com/maxymania/synapse/p2p"
	//"github.com/maxymania/synapse/ftse"
	"github.com/maxymania/synapse/proto"
	//"github.com/maxymania/synapse/alloc"
	"github.com/maxymania/synapse/globals"
	"time"
	"sync"
)


func stoploop(ctx context.Context,d time.Duration) bool {
	select {
	case <- ctx.Done(): return true
	default:
	}
	select {
	case <- ctx.Done(): return true
	case <- time.After(d):
	}
	return false
}

type MetadataAdapter interface{
	GetMetadata(fs p2p.FileSystem, pth p2p.Path) (bson.Document,error)
}

type ServentConfig struct{
	FS     p2p.FileSystemEx
	TS     p2p.TargetStore
	MDA    MetadataAdapter
	Arena  proto.Allocator
	KP     proto.KeyPair
	Dialer proxy.Dialer
}

const (
	what_change = iota
	what_create
	what_remove
)

type fsev struct{
	paths []p2p.Path
	what int
}

func wakeup(s chan int) {
	select {
	case s <- 1:
	default:
	}
}

type serverConn struct{
	cli    *c2s.Client
	fs     p2p.FileSystemEx
	mda    MetadataAdapter
	alive  chan int
	signal chan int
	queue  chan fsev
	status c2s.Status
	commit bool
}
func serverConn_new(cli *c2s.Client,fs p2p.FileSystemEx, mda MetadataAdapter) (s *serverConn) {
	s = new(serverConn)
	s.cli = cli
	s.fs = fs
	s.mda = mda
	s.alive = make(chan int)
	s.signal = make(chan int,1)
	s.queue = make(chan fsev,128)
	s.status = 0
	return
}
func (s *serverConn) Alive() bool {
	select {
	case <- s.alive: return false
	default: return true
	}
	panic("unreachable")
}
func (s *serverConn) sendAll() {
	defer func(){ s.commit = false }()
	docs := make([]bson.Document,0,1<<9)
	var err error
	for _,dir := range s.fs.Dirs() {
		fils,_ := s.fs.Files(dir)
		for _,file := range fils {
			pth := p2p.Path{dir,file}
			doc := bson.Document(nil)
			if s.mda!=nil {
				doc,err = s.mda.GetMetadata(s.fs,pth)
				if err!=nil { doc = nil }
			}
			if doc==nil {
				doc = bson.NewDocumentBuilder().AppendString("_",pth[0]).AppendString("f",pth[1]).Build()
			}
			docs = append(docs,doc)
			if len(docs)<cap(docs) { continue }
			s.cli.Publish(docs)
			docs = docs[:0]
		}
	}
	if len(docs)>0 {
		s.cli.Publish(docs)
	}
}
func (s *serverConn) sendMany(pths []p2p.Path) {
	docs := make([]bson.Document,0,len(pths))
	var err error
	for _,pth := range pths {
		doc := bson.Document(nil)
		if s.mda!=nil {
			doc,err = s.mda.GetMetadata(s.fs,pth)
			if err!=nil { doc = nil }
		}
		if doc==nil {
			doc = bson.NewDocumentBuilder().AppendString("_",pth[0]).AppendString("f",pth[1]).Build()
		}
		docs = append(docs,doc)
	}
	if len(docs)>0 {
		s.cli.Publish(docs)
	}
}
func (s *serverConn) delMany(pths []p2p.Path) {
	docs := make([]bson.Document,0,len(pths))
	for _,pth := range pths {
		doc := bson.NewDocumentBuilder().AppendString("_",pth[0]).AppendString("f",pth[1]).Build()
		docs = append(docs,doc)
	}
	if len(docs)>0 {
		s.cli.Retract(docs)
	}
}

func (s *serverConn) serve() {
	tk := time.NewTicker(time.Second*3)
	defer tk.Stop()
	defer close(s.alive)
	for {
		select {
		case <- s.signal: // liveness-check
			_,err := s.cli.Status()
			if err!=nil { return }
		case <- tk.C: // timer.
			if s.status!=c2s.Pending { continue } // Done!
			status2,err := s.cli.Status()
			if err!=nil { return }
			s.status = status2
			if status2==c2s.Rejected { return }
			s.commit = true
			go s.sendAll()
		case f := <- s.queue: // queue
			if s.status!=c2s.Accepted { continue }
			if s.commit { continue } // s.sendAll is active
			switch f.what {
			case what_change,what_create:
				s.sendMany(f.paths)
			case what_remove:
				s.delMany(f.paths)
			}
		}
	}
}

type Servent struct{
	ServentConfig
	srv    *p2p.Server
	cli    *p2p.ClientContext
	idxcli *c2s.ClientContext
	
	cllck sync.RWMutex
	clist sync.Map
	
	idxlck  sync.RWMutex
	idxlist sync.Map
}
func (cfg *ServentConfig) Create() *Servent {
	s := new(Servent)
	s.ServentConfig = *cfg
	s.srv    = &p2p.Server{Arena:s.Arena,FS:s.FS,KP:s.KP}
	s.cli    = &p2p.ClientContext{Arena:s.Arena,Target:s.TS}
	s.idxcli = &c2s.ClientContext{Arena:s.Arena,KP:s.KP}
	return s
}

func (s *Servent) ServeP2PConn(c net.Conn) {
	s.srv.Serve(c)
}
func (s *Servent) ServeP2P(ctx context.Context, l net.Listener) {
	for {
		c,e := l.Accept()
		if e!=nil {
			if stoploop(ctx,time.Second) { break }
		}
		go s.ServeP2PConn(c)
	}
}
func (s *Servent) cliRem(domain interface{}, raw interface{}) {
	s.cllck.Lock(); defer s.cllck.Unlock()
	nraw,_ := s.clist.Load(domain)
	
	if nraw!=raw { return } // in this case, there is nothing to be done.
	
	s.clist.Delete(domain)
}

func (s *Servent) GetClient(domain string) (*p2p.Client,error) {
	raw,_ := s.clist.Load(domain)
	cli,ok := raw.(*p2p.Client)
	
	if ok {
		if cli.Alive() { return cli,nil }
		s.cliRem(domain,raw)
	}
	
	addr := net.JoinHostPort(domain,globals.Port_p2p)
	conn,err := s.Dialer.Dial("tcp",addr)
	if err!=nil { return nil,err }
	
	cli,err = s.cli.NewClient(conn)
	if err!=nil { conn.Close(); return nil,err } // This should not happen!
	
	s.cllck.RLock()
	raw,toolate := s.clist.LoadOrStore(domain,cli)
	s.cllck.RUnlock()
	
	if toolate {
		// Another goroutine has already opened-up a connection to this host.
		cli.Close()
		cli = raw.(*p2p.Client)
	}
	
	return cli,nil
}
func (s *Servent) RemoveClient(domain string) {
	raw,_ := s.clist.Load(domain)
	cli,ok := raw.(*p2p.Client)
	
	if ok {
		s.cliRem(domain,raw)
		if cli.Alive() {
			cli.Close()
		}
	}
}
func (s *Servent) idxcliRem(domain interface{}, raw interface{}) {
	s.idxlck.Lock(); defer s.idxlck.Unlock()
	nraw,_ := s.idxlist.Load(domain)
	
	if nraw!=raw { return } // in this case, there is nothing to be done.
	
	s.idxlist.Delete(domain)
}

func (s *Servent) getConnection(domain string) (*serverConn,error){
	raw,_ := s.idxlist.Load(domain)
	cli,ok := raw.(*serverConn)
	
	if ok {
		if cli.Alive() { return cli,nil }
		s.idxcliRem(domain,raw)
	}
	
	addr := net.JoinHostPort(domain,globals.Port_c2s)
	conn,err := s.Dialer.Dial("tcp",addr)
	if err!=nil { return nil,err }
	
	lcli,err := s.idxcli.NewClient(conn)
	if err!=nil { return nil,err }
	
	cli = serverConn_new(lcli,s.FS,s.MDA)
	
	s.idxlck.RLock()
	raw,toolate := s.idxlist.LoadOrStore(domain,cli)
	s.idxlck.RUnlock()
	
	if toolate {
		// Another goroutine has already opened-up a connection to this host.
		cli.cli.Close()
		cli = raw.(*serverConn)
	} else {
		cli.serve()
	}
	
	return cli,nil
}
func (s *Servent) AddServer(domain string) (error) {
	_,err := s.getConnection(domain)
	return err
}
func (s *Servent) RemoveServer(domain string) {
	raw,_ := s.idxlist.Load(domain)
	cli,ok := raw.(*serverConn)
	
	if ok {
		s.idxcliRem(domain,raw)
		if cli.Alive() {
			cli.cli.Close()
			wakeup(cli.signal)
		}
	}
}
func (s *Servent) GetServers() []string {
	srv := make([]string,0,8)
	s.idxlist.Range(func(key, value interface{}) bool {
		host,ok := key.(string)
		if ok { srv = append(srv,host) }
		return true
	})
	return srv
}
func (s *Servent) obtainConnections() []*serverConn {
	srv := make([]*serverConn,0,8)
	s.idxlist.Range(func(key, value interface{}) bool {
		sc,_ := value.(*serverConn)
		if sc!=nil { srv = append(srv,sc) }
		return true
	})
	return srv
}
func (s *Servent) Query(terms bson.Document,maxPerConn int) (res []bson.Element,err error) {
	conns := s.obtainConnections()
	for _,conn := range conns {
		sres,err2 := conn.cli.Query(terms,maxPerConn)
		if err==nil { err = err2 }
		if err!=nil { wakeup(conn.signal) } // cause an error-check in the goroutine.
		res = append(res,sres...)
	}
	if len(res)>0 { err = nil }
	return
}
func (s *Servent) update(pths []p2p.Path, what int) {
	conns := s.obtainConnections()
	for _,conn := range conns {
		select {
		case conn.queue <- fsev{pths,what}:
		case <- conn.alive:
		}
	}
}

func (s *Servent) Created(pths []p2p.Path) {
	s.update(pths,what_create)
}
func (s *Servent) Changed(pths []p2p.Path) {
	s.update(pths,what_change)
}
func (s *Servent) Removed(pths []p2p.Path) {
	s.update(pths,what_remove)
}

